"""API routes for data endpoints."""

import logging
from datetime import datetime
from math import ceil
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from api.dependencies import RequestContext, get_request_context
from core.database import get_db
from core.models import SourceType, UnifiedData
from schemas.data_schemas import (
    APIMetadata,
    DataListResponse,
    PaginationMeta,
    UnifiedDataResponse,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/data", response_model=DataListResponse)
async def get_data(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    source_type: Optional[str] = Query(None, description="Filter by source type (api, csv, rss)"),
    category: Optional[str] = Query(None, description="Filter by category"),
    author: Optional[str] = Query(None, description="Filter by author"),
    search: Optional[str] = Query(None, description="Search in title and description"),
    start_date: Optional[datetime] = Query(None, description="Filter by published date (start)"),
    end_date: Optional[datetime] = Query(None, description="Filter by published date (end)"),
    sort_by: str = Query("created_at", description="Sort field"),
    sort_order: str = Query("desc", description="Sort order (asc/desc)"),
    ctx: RequestContext = Depends(get_request_context),
    db: Session = Depends(get_db),
):
    """
    Get paginated and filtered data from unified store.

    Returns metadata including request_id and api_latency_ms.
    """
    # Build query
    query = db.query(UnifiedData)

    # Apply filters
    if source_type:
        try:
            st = SourceType(source_type.lower())
            query = query.filter(UnifiedData.source_type == st)
        except ValueError:
            raise HTTPException(
                status_code=400, detail=f"Invalid source_type. Must be one of: api, csv, rss"
            )

    if category:
        query = query.filter(UnifiedData.category.ilike(f"%{category}%"))

    if author:
        query = query.filter(UnifiedData.author.ilike(f"%{author}%"))

    if search:
        query = query.filter(
            or_(
                UnifiedData.title.ilike(f"%{search}%"),
                UnifiedData.description.ilike(f"%{search}%"),
            )
        )

    if start_date:
        query = query.filter(UnifiedData.published_at >= start_date)

    if end_date:
        query = query.filter(UnifiedData.published_at <= end_date)

    # Get total count
    total_items = query.count()
    total_pages = ceil(total_items / page_size) if total_items > 0 else 1

    # Apply sorting
    sort_column = getattr(UnifiedData, sort_by, UnifiedData.created_at)
    if sort_order.lower() == "desc":
        query = query.order_by(sort_column.desc())
    else:
        query = query.order_by(sort_column.asc())

    # Apply pagination
    offset = (page - 1) * page_size
    items = query.offset(offset).limit(page_size).all()

    # Build response
    return DataListResponse(
        data=[UnifiedDataResponse.model_validate(item) for item in items],
        pagination=PaginationMeta(
            page=page,
            page_size=page_size,
            total_items=total_items,
            total_pages=total_pages,
        ),
        meta=APIMetadata(
            request_id=ctx.request_id,
            api_latency_ms=ctx.elapsed_ms,
            timestamp=datetime.utcnow(),
        ),
    )


@router.get("/data/{data_id}", response_model=UnifiedDataResponse)
async def get_data_by_id(
    data_id: int,
    ctx: RequestContext = Depends(get_request_context),
    db: Session = Depends(get_db),
):
    """Get a single data record by ID."""
    item = db.query(UnifiedData).filter(UnifiedData.id == data_id).first()

    if not item:
        raise HTTPException(status_code=404, detail="Data not found")

    return UnifiedDataResponse.model_validate(item)
