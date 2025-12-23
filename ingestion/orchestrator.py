"""Main ETL orchestrator."""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from core.database import get_db_session
from core.exceptions import ETLException
from core.models import SourceType
from ingestion.api_extractor import APIExtractor
from ingestion.coingecko_extractor import CoinGeckoExtractor
from ingestion.csv_extractor import CSVExtractor, MultiCSVExtractor
from ingestion.rss_extractor import RSSExtractor
from services.etl_tracker import ETLRunTracker
from services.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


class ETLOrchestrator:
    """Orchestrates ETL runs across all data sources."""

    def __init__(
        self,
        parallel: bool = False,
        fail_on_error: bool = False,
    ):
        self.parallel = parallel
        self.fail_on_error = fail_on_error
        self.rate_limiter = RateLimiter()
        self.results: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def _run_extractor(self, extractor_class, db: Session, **kwargs) -> Dict[str, Any]:
        """Run a single extractor."""
        try:
            extractor = extractor_class(db=db, rate_limiter=self.rate_limiter, **kwargs)
            return extractor.run()
        except Exception as e:
            logger.error(f"Extractor {extractor_class.__name__} failed: {e}", exc_info=True)
            if self.fail_on_error:
                raise
            return {
                "extractor": extractor_class.__name__,
                "status": "failed",
                "error": str(e),
            }

    def run_api(self, db: Optional[Session] = None, **kwargs) -> Dict[str, Any]:
        """Run API extractor."""
        if db is None:
            with get_db_session() as db:
                return self._run_extractor(APIExtractor, db, **kwargs)
        return self._run_extractor(APIExtractor, db, **kwargs)

    def run_csv(self, db: Optional[Session] = None, **kwargs) -> Dict[str, Any]:
        """Run CSV extractor."""
        if db is None:
            with get_db_session() as db:
                return self._run_extractor(CSVExtractor, db, **kwargs)
        return self._run_extractor(CSVExtractor, db, **kwargs)

    def run_rss(self, db: Optional[Session] = None, **kwargs) -> Dict[str, Any]:
        """Run RSS/CoinGecko extractor (second API source)."""
        if db is None:
            with get_db_session() as db:
                return self._run_extractor(CoinGeckoExtractor, db, **kwargs)
        return self._run_extractor(CoinGeckoExtractor, db, **kwargs)

    def run_coingecko(self, db: Optional[Session] = None, **kwargs) -> Dict[str, Any]:
        """Run CoinGecko extractor (alias for run_rss)."""
        return self.run_rss(db, **kwargs)

    def run_all(self) -> List[Dict[str, Any]]:
        """Run all extractors."""
        self.results = []
        start_time = datetime.utcnow()

        logger.info("Starting ETL run for all sources")

        if self.parallel:
            # Run extractors in parallel
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {}

                with get_db_session() as db:
                    futures[executor.submit(self.run_api, db)] = "api"

                with get_db_session() as db:
                    futures[executor.submit(self.run_csv, db)] = "csv"

                with get_db_session() as db:
                    futures[executor.submit(self.run_rss, db)] = "rss"

                for future in as_completed(futures):
                    source = futures[future]
                    try:
                        result = future.result()
                        with self._lock:
                            self.results.append(result)
                    except Exception as e:
                        logger.error(f"ETL failed for {source}: {e}")
                        with self._lock:
                            self.results.append(
                                {
                                    "source_type": source,
                                    "status": "failed",
                                    "error": str(e),
                                }
                            )
        else:
            # Run extractors sequentially
            with get_db_session() as db:
                try:
                    self.results.append(self.run_api(db))
                except Exception as e:
                    logger.error(f"API ETL failed: {e}")
                    self.results.append({"source_type": "api", "status": "failed", "error": str(e)})

            with get_db_session() as db:
                try:
                    self.results.append(self.run_csv(db))
                except Exception as e:
                    logger.error(f"CSV ETL failed: {e}")
                    self.results.append({"source_type": "csv", "status": "failed", "error": str(e)})

            with get_db_session() as db:
                try:
                    self.results.append(self.run_rss(db))
                except Exception as e:
                    logger.error(f"RSS ETL failed: {e}")
                    self.results.append({"source_type": "rss", "status": "failed", "error": str(e)})

        duration = (datetime.utcnow() - start_time).total_seconds()

        summary = {
            "total_duration_seconds": duration,
            "sources_processed": len(self.results),
            "successful": sum(1 for r in self.results if r.get("status") == "success"),
            "failed": sum(1 for r in self.results if r.get("status") == "failed"),
            "results": self.results,
        }

        logger.info(
            f"ETL run completed",
            extra={
                "duration_seconds": duration,
                "sources_processed": len(self.results),
                "successful": summary["successful"],
                "failed": summary["failed"],
            },
        )

        return self.results

    def run_with_failure_injection(
        self,
        fail_at_record: int = 50,
        source_type: str = "csv",
    ) -> Dict[str, Any]:
        """
        Run ETL with controlled failure injection for testing recovery.

        Args:
            fail_at_record: Record number at which to inject failure
            source_type: Source type to inject failure in
        """
        logger.warning(f"Running ETL with failure injection at record {fail_at_record}")

        with get_db_session() as db:
            if source_type == "csv":
                extractor = CSVExtractor(db=db, rate_limiter=self.rate_limiter)
            elif source_type == "api":
                extractor = APIExtractor(db=db, rate_limiter=self.rate_limiter)
            else:
                extractor = RSSExtractor(db=db, rate_limiter=self.rate_limiter)

            # Override extract to inject failure
            original_extract = extractor.extract
            record_count = 0

            def failing_extract():
                nonlocal record_count
                for record in original_extract():
                    record_count += 1
                    if record_count == fail_at_record:
                        raise ETLException(f"Injected failure at record {fail_at_record}")
                    yield record

            extractor.extract = failing_extract

            try:
                return extractor.run()
            except ETLException as e:
                logger.info(f"Failure injection triggered: {e}")
                return {
                    "status": "failed_injection",
                    "records_before_failure": record_count - 1,
                    "error": str(e),
                }


def run_scheduled_etl():
    """Entry point for scheduled ETL runs."""
    from core.database import init_db
    from core.logging_config import setup_logging

    setup_logging()
    init_db()

    orchestrator = ETLOrchestrator(parallel=False)
    results = orchestrator.run_all()

    return results


if __name__ == "__main__":
    run_scheduled_etl()
