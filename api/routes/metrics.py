"""Prometheus metrics endpoint."""

import logging
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends
from sqlalchemy import func
from sqlalchemy.orm import Session

from core.database import get_db
from core.models import ETLRun, RunStatus, SourceType, UnifiedData

logger = logging.getLogger(__name__)
router = APIRouter()


def format_prometheus_metric(
    name: str, value: float, labels: dict = None, help_text: str = None, metric_type: str = "gauge"  # type: ignore[assignment]
) -> str:
    """Format a metric in Prometheus format."""
    lines = []

    if help_text:
        lines.append(f"# HELP {name} {help_text}")
    lines.append(f"# TYPE {name} {metric_type}")

    if labels:
        label_str = ",".join([f'{k}="{v}"' for k, v in labels.items()])
        lines.append(f"{name}{{{label_str}}} {value}")
    else:
        lines.append(f"{name} {value}")

    return "\n".join(lines)


@router.get("/metrics")
async def get_metrics(db: Session = Depends(get_db)):
    """
    Prometheus metrics endpoint.

    Exposes:
    - Total records by source
    - ETL run counts and durations
    - Error counts
    - Last run timestamps
    """
    metrics = []

    # Total records by source
    records_by_source = (
        db.query(UnifiedData.source_type, func.count(UnifiedData.id))
        .group_by(UnifiedData.source_type)
        .all()
    )

    for source_type, count in records_by_source:
        metrics.append(
            format_prometheus_metric(
                "kaspero_records_total",
                count,
                {"source": source_type.value},
                "Total number of records by source",
                "gauge",
            )
        )

    # ETL runs in last 24 hours
    cutoff = datetime.utcnow() - timedelta(hours=24)

    for status in RunStatus:
        count = (
            db.query(func.count(ETLRun.id))
            .filter(ETLRun.status == status, ETLRun.started_at >= cutoff)
            .scalar()
            or 0
        )

        metrics.append(
            format_prometheus_metric(
                "kaspero_etl_runs_24h",
                count,
                {"status": status.value},
                "ETL runs in last 24 hours by status",
                "gauge",
            )
        )

    # Average ETL duration by source
    avg_durations = (
        db.query(ETLRun.source_type, func.avg(ETLRun.duration_seconds))
        .filter(ETLRun.duration_seconds.isnot(None), ETLRun.started_at >= cutoff)
        .group_by(ETLRun.source_type)
        .all()
    )

    for source_type, avg_duration in avg_durations:
        if avg_duration:
            metrics.append(
                format_prometheus_metric(
                    "kaspero_etl_duration_seconds",
                    float(avg_duration),
                    {"source": source_type.value},
                    "Average ETL duration in seconds",
                    "gauge",
                )
            )

    # Last successful run timestamp by source
    for source in SourceType:
        last_success = (
            db.query(ETLRun)
            .filter(ETLRun.source_type == source, ETLRun.status == RunStatus.SUCCESS)
            .order_by(ETLRun.completed_at.desc())
            .first()
        )

        if last_success and last_success.completed_at:
            metrics.append(
                format_prometheus_metric(
                    "kaspero_etl_last_success_timestamp",
                    last_success.completed_at.timestamp(),
                    {"source": source.value},
                    "Timestamp of last successful ETL run",
                    "gauge",
                )
            )

    # Failed records in last 24h
    failed_records = (
        db.query(func.sum(ETLRun.records_failed)).filter(ETLRun.started_at >= cutoff).scalar() or 0
    )

    metrics.append(
        format_prometheus_metric(
            "kaspero_etl_failed_records_24h",
            failed_records,
            help_text="Number of failed records in last 24 hours",
            metric_type="gauge",
        )
    )

    # Skipped records (duplicates) in last 24h
    skipped_records = (
        db.query(func.sum(ETLRun.records_skipped)).filter(ETLRun.started_at >= cutoff).scalar() or 0
    )

    metrics.append(
        format_prometheus_metric(
            "kaspero_etl_skipped_records_24h",
            skipped_records,
            help_text="Number of skipped (duplicate) records in last 24 hours",
            metric_type="gauge",
        )
    )

    return "\n\n".join(metrics) + "\n"
