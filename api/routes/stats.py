"""API routes for ETL statistics and run management."""

import logging
from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from api.dependencies import RequestContext, get_request_context
from core.database import get_db
from core.models import ETLRun, RunStatus, SchemaDrift, SourceType
from schemas.data_schemas import (
    APIMetadata,
    ETLRunResponse,
    ETLStatsResponse,
    SchemaDriftResponse,
)
from services.checkpoint import CheckpointManager
from services.etl_tracker import ETLRunTracker

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/stats", response_model=ETLStatsResponse)
async def get_etl_stats(
    hours: int = Query(24, ge=1, le=720, description="Time period in hours"),
    ctx: RequestContext = Depends(get_request_context),
    db: Session = Depends(get_db),
):
    """
    Get ETL run statistics and summaries.

    Returns:
    - Records processed
    - Duration statistics
    - Last success & failure timestamps
    - Run metadata
    """
    tracker = ETLRunTracker(db)
    stats = tracker.get_stats(hours=hours)

    # Get last run details
    last_run = tracker.get_last_run()
    last_run_response = None
    if last_run:
        last_run_response = ETLRunResponse.model_validate(last_run)

    return ETLStatsResponse(
        total_records_processed=stats["total_records_processed"],
        records_by_source=stats["records_by_source"],
        last_run=last_run_response,
        last_success=stats["last_success"],
        last_failure=stats["last_failure"],
        runs_last_24h=stats["runs_in_period"],
        success_rate=stats["success_rate"],
        average_duration_seconds=stats["average_duration_seconds"],
        meta=APIMetadata(
            request_id=ctx.request_id,
            api_latency_ms=ctx.elapsed_ms,
            timestamp=datetime.utcnow(),
        ),
    )


@router.get("/runs", response_model=List[ETLRunResponse])
async def get_runs(
    source_type: Optional[str] = Query(None, description="Filter by source type"),
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(10, ge=1, le=100, description="Number of runs to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    ctx: RequestContext = Depends(get_request_context),
    db: Session = Depends(get_db),
):
    """
    Get list of ETL runs with optional filters.
    """
    tracker = ETLRunTracker(db)

    # Parse filters
    st = None
    if source_type:
        try:
            st = SourceType(source_type.lower())
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid source_type")

    rs = None
    if status:
        try:
            rs = RunStatus(status.lower())
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid status")

    runs = tracker.get_runs(source_type=st, status=rs, limit=limit, offset=offset)

    return [ETLRunResponse.model_validate(run) for run in runs]


@router.get("/runs/{run_id}", response_model=ETLRunResponse)
async def get_run(
    run_id: str,
    ctx: RequestContext = Depends(get_request_context),
    db: Session = Depends(get_db),
):
    """Get details of a specific ETL run."""
    tracker = ETLRunTracker(db)
    run = tracker.get_run(run_id)

    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    return ETLRunResponse.model_validate(run)


@router.get("/compare-runs")
async def compare_runs(
    run_id_1: str = Query(..., description="First run ID"),
    run_id_2: str = Query(..., description="Second run ID"),
    ctx: RequestContext = Depends(get_request_context),
    db: Session = Depends(get_db),
):
    """
    Compare two ETL runs for anomaly detection.

    Identifies differences in record counts, duration, and status.
    """
    tracker = ETLRunTracker(db)
    comparison = tracker.compare_runs(run_id_1, run_id_2)

    if "error" in comparison:
        raise HTTPException(status_code=404, detail=comparison["error"])

    comparison["meta"] = {
        "request_id": ctx.request_id,
        "api_latency_ms": ctx.elapsed_ms,
    }

    return comparison


@router.get("/checkpoints")
async def get_checkpoints(
    ctx: RequestContext = Depends(get_request_context),
    db: Session = Depends(get_db),
):
    """Get current checkpoints for all data sources."""
    checkpoint_manager = CheckpointManager(db)
    checkpoints = checkpoint_manager.get_all_checkpoints()

    return {
        "checkpoints": checkpoints,
        "meta": {
            "request_id": ctx.request_id,
            "api_latency_ms": ctx.elapsed_ms,
        },
    }


@router.get("/schema-drifts", response_model=List[SchemaDriftResponse])
async def get_schema_drifts(
    source_type: Optional[str] = Query(None, description="Filter by source type"),
    resolved: Optional[bool] = Query(None, description="Filter by resolved status"),
    limit: int = Query(50, ge=1, le=200, description="Number of records to return"),
    ctx: RequestContext = Depends(get_request_context),
    db: Session = Depends(get_db),
):
    """Get detected schema drifts."""
    query = db.query(SchemaDrift)

    if source_type:
        try:
            st = SourceType(source_type.lower())
            query = query.filter(SchemaDrift.source_type == st)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid source_type")

    if resolved is not None:
        query = query.filter(SchemaDrift.resolved == resolved)

    drifts = query.order_by(SchemaDrift.detected_at.desc()).limit(limit).all()

    return [SchemaDriftResponse.model_validate(drift) for drift in drifts]


@router.post("/schema-drifts/{drift_id}/resolve")
async def resolve_schema_drift(
    drift_id: int,
    ctx: RequestContext = Depends(get_request_context),
    db: Session = Depends(get_db),
):
    """Mark a schema drift as resolved."""
    drift = db.query(SchemaDrift).filter(SchemaDrift.id == drift_id).first()

    if not drift:
        raise HTTPException(status_code=404, detail="Schema drift not found")

    drift.resolved = True
    drift.resolved_at = datetime.utcnow()
    db.commit()

    return {"status": "resolved", "drift_id": drift_id}
