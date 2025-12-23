"""API routes for health check and system status."""

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.dependencies import RequestContext, get_request_context
from core.database import check_db_connection, get_db
from core.models import ETLRun, RunStatus
from schemas.data_schemas import HealthStatus

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/health", response_model=HealthStatus)
async def health_check(
    ctx: RequestContext = Depends(get_request_context),
    db: Session = Depends(get_db),
):
    """
    Health check endpoint.

    Reports:
    - Overall status
    - Database connectivity
    - ETL last run status and timestamp
    """
    # Check database
    db_healthy = False
    try:
        db.execute(text("SELECT 1"))
        db_healthy = True
    except Exception as e:
        logger.error(f"Database health check failed: {e}")

    # Get last ETL run
    last_run = db.query(ETLRun).order_by(ETLRun.started_at.desc()).first()

    etl_last_run: Optional[datetime] = None
    etl_last_status: Optional[str] = None

    if last_run:
        etl_last_run = last_run.completed_at or last_run.started_at
        etl_last_status = last_run.status.value

    # Determine overall status
    if db_healthy:
        status = "healthy"
    else:
        status = "unhealthy"

    return HealthStatus(
        status=status,
        database=db_healthy,
        etl_last_run=etl_last_run,
        etl_last_status=etl_last_status,
        version="1.0.0",
    )


@router.get("/ready")
async def readiness_check(db: Session = Depends(get_db)):
    """
    Kubernetes readiness probe endpoint.
    Returns 200 if service is ready to accept traffic.
    """
    try:
        db.execute(text("SELECT 1"))
        return {"status": "ready"}
    except Exception as e:
        logger.error(f"Readiness check failed: {e}")
        return {"status": "not_ready", "error": str(e)}


@router.get("/live")
async def liveness_check():
    """
    Kubernetes liveness probe endpoint.
    Returns 200 if service is alive.
    """
    return {"status": "alive", "timestamp": datetime.utcnow().isoformat()}
