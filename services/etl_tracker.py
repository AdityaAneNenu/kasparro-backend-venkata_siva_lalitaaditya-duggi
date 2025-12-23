"""ETL run tracking and statistics service."""

import logging
import traceback
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, func
from sqlalchemy.orm import Session

from core.models import ETLRun, RunStatus, SourceType, UnifiedData
from schemas.data_schemas import ETLRunCreate, ETLRunResponse, ETLRunUpdate

logger = logging.getLogger(__name__)


class ETLRunTracker:
    """Tracks ETL run metadata and statistics."""

    def __init__(self, db: Session):
        self.db = db

    def start_run(
        self,
        source_type: SourceType,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ETLRun:
        """Start a new ETL run and return the run record."""
        run = ETLRun(
            run_id=str(uuid.uuid4()),
            source_type=source_type,
            status=RunStatus.RUNNING,
            started_at=datetime.utcnow(),
            run_metadata=metadata or {},
        )
        self.db.add(run)
        self.db.commit()
        self.db.refresh(run)

        logger.info(
            f"ETL run started",
            extra={
                "run_id": run.run_id,
                "source_type": source_type.value,
            },
        )

        return run

    def complete_run(
        self,
        run: ETLRun,
        status: RunStatus,
        records_extracted: int = 0,
        records_transformed: int = 0,
        records_loaded: int = 0,
        records_skipped: int = 0,
        records_failed: int = 0,
        error: Optional[Exception] = None,
        checkpoint_data: Optional[Dict[str, Any]] = None,
    ) -> ETLRun:
        """Complete an ETL run with final statistics."""
        run.status = status
        run.completed_at = datetime.utcnow()
        run.duration_seconds = (run.completed_at - run.started_at).total_seconds()

        run.records_extracted = records_extracted
        run.records_transformed = records_transformed
        run.records_loaded = records_loaded
        run.records_skipped = records_skipped
        run.records_failed = records_failed

        if error:
            run.error_message = str(error)
            run.error_traceback = traceback.format_exc()

        if checkpoint_data:
            run.checkpoint_data = checkpoint_data

        self.db.commit()
        self.db.refresh(run)

        logger.info(
            f"ETL run completed",
            extra={
                "run_id": run.run_id,
                "source_type": run.source_type.value,
                "status": status.value,
                "duration_seconds": run.duration_seconds,
                "records_loaded": records_loaded,
            },
        )

        return run

    def get_run(self, run_id: str) -> Optional[ETLRun]:
        """Get a specific run by ID."""
        return self.db.query(ETLRun).filter(ETLRun.run_id == run_id).first()

    def get_last_run(self, source_type: Optional[SourceType] = None) -> Optional[ETLRun]:
        """Get the most recent run, optionally filtered by source type."""
        query = self.db.query(ETLRun)

        if source_type:
            query = query.filter(ETLRun.source_type == source_type)

        return query.order_by(ETLRun.started_at.desc()).first()

    def get_last_successful_run(self, source_type: Optional[SourceType] = None) -> Optional[ETLRun]:
        """Get the most recent successful run."""
        query = self.db.query(ETLRun).filter(ETLRun.status == RunStatus.SUCCESS)

        if source_type:
            query = query.filter(ETLRun.source_type == source_type)

        return query.order_by(ETLRun.started_at.desc()).first()

    def get_last_failed_run(self, source_type: Optional[SourceType] = None) -> Optional[ETLRun]:
        """Get the most recent failed run."""
        query = self.db.query(ETLRun).filter(ETLRun.status == RunStatus.FAILED)

        if source_type:
            query = query.filter(ETLRun.source_type == source_type)

        return query.order_by(ETLRun.started_at.desc()).first()

    def get_runs(
        self,
        source_type: Optional[SourceType] = None,
        status: Optional[RunStatus] = None,
        limit: int = 10,
        offset: int = 0,
    ) -> List[ETLRun]:
        """Get a list of runs with optional filters."""
        query = self.db.query(ETLRun)

        if source_type:
            query = query.filter(ETLRun.source_type == source_type)
        if status:
            query = query.filter(ETLRun.status == status)

        return query.order_by(ETLRun.started_at.desc()).offset(offset).limit(limit).all()

    def get_stats(self, hours: int = 24) -> Dict[str, Any]:
        """Get ETL statistics for the specified time period."""
        cutoff = datetime.utcnow() - timedelta(hours=hours)

        # Total records in unified data
        total_records = self.db.query(func.count(UnifiedData.id)).scalar() or 0

        # Records by source
        records_by_source = dict(
            self.db.query(UnifiedData.source_type, func.count(UnifiedData.id))
            .group_by(UnifiedData.source_type)
            .all()
        )
        records_by_source = {k.value: v for k, v in records_by_source.items()}

        # Runs in period
        runs_query = self.db.query(ETLRun).filter(ETLRun.started_at >= cutoff)
        runs = runs_query.all()

        total_runs = len(runs)
        successful_runs = sum(1 for r in runs if r.status == RunStatus.SUCCESS)

        # Average duration of successful runs
        completed_runs = [r for r in runs if r.duration_seconds is not None]
        avg_duration = (
            sum(r.duration_seconds for r in completed_runs) / len(completed_runs)
            if completed_runs
            else 0.0
        )

        # Last success and failure
        last_success = self.get_last_successful_run()
        last_failure = self.get_last_failed_run()

        return {
            "total_records_processed": total_records,
            "records_by_source": records_by_source,
            "runs_in_period": total_runs,
            "success_rate": successful_runs / total_runs if total_runs > 0 else 0.0,
            "average_duration_seconds": avg_duration,
            "last_success": last_success.completed_at if last_success else None,
            "last_failure": last_failure.completed_at if last_failure else None,
            "period_hours": hours,
        }

    def compare_runs(self, run_id_1: str, run_id_2: str) -> Dict[str, Any]:
        """Compare two ETL runs for anomaly detection."""
        run1 = self.get_run(run_id_1)
        run2 = self.get_run(run_id_2)

        if not run1 or not run2:
            return {"error": "One or both runs not found"}

        comparison = {
            "run_1": {
                "run_id": run1.run_id,
                "source_type": run1.source_type.value,
                "status": run1.status.value,
                "records_loaded": run1.records_loaded,
                "duration_seconds": run1.duration_seconds,
                "started_at": run1.started_at.isoformat() if run1.started_at else None,
            },
            "run_2": {
                "run_id": run2.run_id,
                "source_type": run2.source_type.value,
                "status": run2.status.value,
                "records_loaded": run2.records_loaded,
                "duration_seconds": run2.duration_seconds,
                "started_at": run2.started_at.isoformat() if run2.started_at else None,
            },
            "differences": {},
            "anomalies": [],
        }

        # Calculate differences
        if run1.records_loaded and run2.records_loaded:
            records_diff = run2.records_loaded - run1.records_loaded
            records_pct = (
                (records_diff / run1.records_loaded * 100) if run1.records_loaded > 0 else 0
            )
            comparison["differences"]["records_loaded"] = {
                "absolute": records_diff,
                "percentage": records_pct,
            }

            # Flag anomaly if > 50% change
            if abs(records_pct) > 50:
                comparison["anomalies"].append(
                    {
                        "type": "record_count_anomaly",
                        "description": f"Record count changed by {records_pct:.1f}%",
                        "severity": "high" if abs(records_pct) > 90 else "medium",
                    }
                )

        if run1.duration_seconds and run2.duration_seconds:
            duration_diff = run2.duration_seconds - run1.duration_seconds
            duration_pct = (
                (duration_diff / run1.duration_seconds * 100) if run1.duration_seconds > 0 else 0
            )
            comparison["differences"]["duration"] = {
                "absolute": duration_diff,
                "percentage": duration_pct,
            }

            # Flag anomaly if > 100% slower
            if duration_pct > 100:
                comparison["anomalies"].append(
                    {
                        "type": "duration_anomaly",
                        "description": f"Run took {duration_pct:.1f}% longer",
                        "severity": "medium",
                    }
                )

        # Status change anomaly
        if run1.status != run2.status:
            comparison["anomalies"].append(
                {
                    "type": "status_change",
                    "description": f"Status changed from {run1.status.value} to {run2.status.value}",
                    "severity": "high" if run2.status == RunStatus.FAILED else "low",
                }
            )

        return comparison
