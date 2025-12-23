"""Base extractor class for ETL sources."""

import hashlib
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Generator, List, Optional

from sqlalchemy.orm import Session

from core.models import RunStatus, SourceType
from services.checkpoint import CheckpointManager
from services.etl_tracker import ETLRunTracker
from services.rate_limiter import RateLimiter
from services.schema_drift import SchemaDriftDetector

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """Abstract base class for data extractors."""

    source_type: SourceType

    def __init__(
        self,
        db: Session,
        rate_limiter: Optional[RateLimiter] = None,
        detect_schema_drift: bool = True,
    ):
        self.db = db
        self.rate_limiter = rate_limiter or RateLimiter()
        self.checkpoint_manager = CheckpointManager(db)
        self.drift_detector = SchemaDriftDetector(db) if detect_schema_drift else None
        self.run_tracker = ETLRunTracker(db)

        # Statistics
        self.records_extracted = 0
        self.records_transformed = 0
        self.records_loaded = 0
        self.records_skipped = 0
        self.records_failed = 0

    @staticmethod
    def compute_checksum(data: Dict[str, Any]) -> str:
        """Compute a checksum for deduplication."""
        import json

        content = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(content.encode()).hexdigest()

    @abstractmethod
    def extract(self) -> Generator[Dict[str, Any], None, None]:
        """
        Extract data from the source.
        Yields raw data records as dictionaries.
        """
        pass

    @abstractmethod
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw data into unified schema.
        Returns transformed data dictionary.
        """
        pass

    @abstractmethod
    def load_raw(self, raw_data: Dict[str, Any]) -> int:
        """
        Load raw data into the raw_* table.
        Returns the raw record ID.
        """
        pass

    @abstractmethod
    def load_unified(self, transformed_data: Dict[str, Any], raw_id: int) -> int:
        """
        Load transformed data into unified_data table.
        Returns the unified record ID.
        """
        pass

    def should_process(self, source_id: str) -> bool:
        """Check if this record should be processed (for incremental ingestion)."""
        last_id = self.checkpoint_manager.get_last_source_id(self.source_type)
        if last_id is None:
            return True
        # Simple string comparison - works for most ID formats
        return source_id > last_id

    def run(self) -> Dict[str, Any]:
        """
        Execute the full ETL pipeline.
        Returns run statistics.
        """
        # Get checkpoint info as serializable dict (not the ORM object)
        checkpoint = self.checkpoint_manager.get_checkpoint(self.source_type)
        checkpoint_info = None
        if checkpoint:
            checkpoint_info = {
                "last_source_id": checkpoint.last_source_id,
                "last_offset": checkpoint.last_offset,
                "last_processed_at": (
                    checkpoint.last_processed_at.isoformat()
                    if checkpoint.last_processed_at
                    else None
                ),
            }

        run = self.run_tracker.start_run(
            source_type=self.source_type, metadata={"checkpoint": checkpoint_info}
        )

        last_source_id = None

        try:
            for raw_data in self.extract():
                try:
                    source_id = self.get_source_id(raw_data)

                    # Incremental ingestion check
                    if not self.should_process(source_id):
                        self.records_skipped += 1
                        continue

                    self.records_extracted += 1

                    # Schema drift detection
                    if self.drift_detector:
                        drifts = self.drift_detector.detect_drift(self.source_type, raw_data)
                        if drifts:
                            self.drift_detector.record_drifts(self.source_type, drifts)

                    # Load raw data
                    raw_id = self.load_raw(raw_data)

                    # Transform
                    transformed = self.transform(raw_data)
                    self.records_transformed += 1

                    # Load unified data
                    self.load_unified(transformed, raw_id)
                    self.records_loaded += 1

                    last_source_id = source_id

                except Exception as e:
                    logger.error(f"Error processing record: {e}", exc_info=True)
                    self.records_failed += 1
                    continue

            # Update checkpoint on success
            if last_source_id:
                self.checkpoint_manager.update_checkpoint(
                    source_type=self.source_type,
                    last_source_id=last_source_id,
                    metadata={"records_processed": self.records_loaded},
                )

            status = RunStatus.SUCCESS if self.records_failed == 0 else RunStatus.PARTIAL

        except Exception as e:
            logger.error(f"ETL run failed: {e}", exc_info=True)
            status = RunStatus.FAILED

            self.run_tracker.complete_run(
                run=run,
                status=status,
                records_extracted=self.records_extracted,
                records_transformed=self.records_transformed,
                records_loaded=self.records_loaded,
                records_skipped=self.records_skipped,
                records_failed=self.records_failed,
                error=e,
            )
            raise

        self.run_tracker.complete_run(
            run=run,
            status=status,
            records_extracted=self.records_extracted,
            records_transformed=self.records_transformed,
            records_loaded=self.records_loaded,
            records_skipped=self.records_skipped,
            records_failed=self.records_failed,
            checkpoint_data={
                "last_source_id": last_source_id,
            },
        )

        return {
            "run_id": run.run_id,
            "source_type": self.source_type.value,
            "status": status.value,
            "records_extracted": self.records_extracted,
            "records_transformed": self.records_transformed,
            "records_loaded": self.records_loaded,
            "records_skipped": self.records_skipped,
            "records_failed": self.records_failed,
        }

    @abstractmethod
    def get_source_id(self, raw_data: Dict[str, Any]) -> str:
        """Extract the unique source ID from raw data."""
        pass
