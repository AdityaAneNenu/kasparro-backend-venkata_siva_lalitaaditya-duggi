"""Pydantic schemas for data validation and serialization."""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class SourceType(str, Enum):
    """Data source type enumeration."""

    API = "api"
    CSV = "csv"
    RSS = "rss"


class RunStatus(str, Enum):
    """ETL run status enumeration."""

    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"


# ============== RAW DATA SCHEMAS ==============


class RawDataBase(BaseModel):
    """Base schema for raw data."""

    source_id: str
    raw_payload: Dict[str, Any]
    checksum: Optional[str] = None


class RawAPIDataCreate(RawDataBase):
    """Schema for creating raw API data."""

    pass


class RawAPIDataResponse(RawDataBase):
    """Schema for raw API data response."""

    id: int
    ingested_at: datetime

    model_config = ConfigDict(from_attributes=True)


class RawCSVDataCreate(RawDataBase):
    """Schema for creating raw CSV data."""

    source_file: str
    row_number: int


class RawCSVDataResponse(RawDataBase):
    """Schema for raw CSV data response."""

    id: int
    source_file: str
    row_number: int
    ingested_at: datetime

    model_config = ConfigDict(from_attributes=True)


class RawRSSDataCreate(RawDataBase):
    """Schema for creating raw RSS data."""

    feed_url: str


class RawRSSDataResponse(RawDataBase):
    """Schema for raw RSS data response."""

    id: int
    feed_url: str
    ingested_at: datetime

    model_config = ConfigDict(from_attributes=True)


# ============== UNIFIED DATA SCHEMAS ==============


class UnifiedDataBase(BaseModel):
    """Base schema for unified data."""

    title: Optional[str] = None
    description: Optional[str] = None
    content: Optional[str] = None
    author: Optional[str] = None
    category: Optional[str] = None
    tags: Optional[List[str]] = None
    url: Optional[str] = None
    published_at: Optional[datetime] = None
    extra_data: Optional[Dict[str, Any]] = None

    @field_validator("title", "description", "content", "author", "category", mode="before")
    @classmethod
    def clean_string(cls, v):
        if v is None:
            return None
        if isinstance(v, str):
            return v.strip()[:500] if v.strip() else None
        return str(v).strip()[:500]

    @field_validator("tags", mode="before")
    @classmethod
    def clean_tags(cls, v):
        if v is None:
            return None
        if isinstance(v, str):
            return [t.strip() for t in v.split(",") if t.strip()]
        if isinstance(v, list):
            return [str(t).strip() for t in v if t]
        return None


class UnifiedDataCreate(UnifiedDataBase):
    """Schema for creating unified data."""

    source_type: SourceType
    source_id: str
    raw_id: int


class UnifiedDataResponse(UnifiedDataBase):
    """Schema for unified data response."""

    id: int
    source_type: SourceType
    source_id: str
    raw_id: int
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


# ============== ETL METADATA SCHEMAS ==============


class ETLCheckpointBase(BaseModel):
    """Base schema for ETL checkpoint."""

    source_type: SourceType
    last_source_id: Optional[str] = None
    last_processed_at: Optional[datetime] = None
    last_offset: int = 0
    metadata: Optional[Dict[str, Any]] = None


class ETLCheckpointCreate(ETLCheckpointBase):
    """Schema for creating ETL checkpoint."""

    pass


class ETLCheckpointResponse(ETLCheckpointBase):
    """Schema for ETL checkpoint response."""

    id: int
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class ETLRunBase(BaseModel):
    """Base schema for ETL run."""

    run_id: str
    source_type: SourceType
    status: RunStatus = RunStatus.RUNNING


class ETLRunCreate(ETLRunBase):
    """Schema for creating ETL run."""

    pass


class ETLRunUpdate(BaseModel):
    """Schema for updating ETL run."""

    status: Optional[RunStatus] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    records_extracted: Optional[int] = None
    records_transformed: Optional[int] = None
    records_loaded: Optional[int] = None
    records_skipped: Optional[int] = None
    records_failed: Optional[int] = None
    error_message: Optional[str] = None
    error_traceback: Optional[str] = None
    checkpoint_data: Optional[Dict[str, Any]] = None
    run_metadata: Optional[Dict[str, Any]] = None


class ETLRunResponse(ETLRunBase):
    """Schema for ETL run response."""

    id: int
    started_at: datetime
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    records_extracted: int = 0
    records_transformed: int = 0
    records_loaded: int = 0
    records_skipped: int = 0
    records_failed: int = 0
    error_message: Optional[str] = None
    checkpoint_data: Optional[Dict[str, Any]] = None
    run_metadata: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(from_attributes=True)


# ============== SCHEMA DRIFT SCHEMAS ==============


class SchemaDriftBase(BaseModel):
    """Base schema for schema drift detection."""

    source_type: SourceType
    field_name: str
    expected_type: Optional[str] = None
    actual_type: Optional[str] = None
    drift_type: str  # new_field, missing_field, type_change
    confidence_score: Optional[float] = None
    sample_value: Optional[str] = None


class SchemaDriftCreate(SchemaDriftBase):
    """Schema for creating schema drift record."""

    pass


class SchemaDriftResponse(SchemaDriftBase):
    """Schema for schema drift response."""

    id: int
    detected_at: datetime
    resolved: bool = False
    resolved_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


# ============== API RESPONSE SCHEMAS ==============


class PaginationMeta(BaseModel):
    """Pagination metadata."""

    page: int = Field(ge=1)
    page_size: int = Field(ge=1, le=100)
    total_items: int
    total_pages: int


class APIMetadata(BaseModel):
    """API response metadata."""

    request_id: str
    api_latency_ms: float
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class DataListResponse(BaseModel):
    """Response schema for /data endpoint."""

    data: List[UnifiedDataResponse]
    pagination: PaginationMeta
    meta: APIMetadata


class HealthStatus(BaseModel):
    """Health check response."""

    status: str
    database: bool
    etl_last_run: Optional[datetime] = None
    etl_last_status: Optional[str] = None
    version: str = "1.0.0"


class ETLStatsResponse(BaseModel):
    """Response schema for /stats endpoint."""

    total_records_processed: int
    records_by_source: Dict[str, int]
    last_run: Optional[ETLRunResponse] = None
    last_success: Optional[datetime] = None
    last_failure: Optional[datetime] = None
    runs_last_24h: int
    success_rate: float
    average_duration_seconds: float
    meta: APIMetadata
