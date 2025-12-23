"""SQLAlchemy database models."""

import enum
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
)
from sqlalchemy import Enum as SQLEnum
from sqlalchemy import (
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()  # type: Any


class SourceType(str, enum.Enum):
    """Data source types."""

    API = "api"
    CSV = "csv"
    RSS = "rss"


class RunStatus(str, enum.Enum):
    """ETL run status."""

    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"


# ============== RAW DATA TABLES ==============


class RawAPIData(Base):
    """Raw data from API source."""

    __tablename__ = "raw_api_data"

    id = Column(Integer, primary_key=True, index=True)
    source_id = Column(String(255), unique=True, nullable=False, index=True)
    raw_payload = Column(JSON, nullable=False)
    ingested_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    checksum = Column(String(64), nullable=True)

    __table_args__ = (Index("idx_raw_api_ingested", "ingested_at"),)


class RawCSVData(Base):
    """Raw data from CSV source."""

    __tablename__ = "raw_csv_data"

    id = Column(Integer, primary_key=True, index=True)
    source_id = Column(String(255), unique=True, nullable=False, index=True)
    raw_payload = Column(JSON, nullable=False)
    source_file = Column(String(255), nullable=False)
    row_number = Column(Integer, nullable=False)
    ingested_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    checksum = Column(String(64), nullable=True)

    __table_args__ = (
        Index("idx_raw_csv_ingested", "ingested_at"),
        Index("idx_raw_csv_source_file", "source_file"),
    )


class RawRSSData(Base):
    """Raw data from RSS source."""

    __tablename__ = "raw_rss_data"

    id = Column(Integer, primary_key=True, index=True)
    source_id = Column(String(255), unique=True, nullable=False, index=True)
    raw_payload = Column(JSON, nullable=False)
    feed_url = Column(String(512), nullable=False)
    ingested_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    checksum = Column(String(64), nullable=True)

    __table_args__ = (Index("idx_raw_rss_ingested", "ingested_at"),)


# ============== UNIFIED SCHEMA ==============


class UnifiedData(Base):
    """Unified normalized data from all sources."""

    __tablename__ = "unified_data"

    id = Column(Integer, primary_key=True, index=True)
    source_type = Column(SQLEnum(SourceType), nullable=False, index=True)
    source_id = Column(String(255), nullable=False)

    # Common normalized fields
    title = Column(String(500), nullable=True)
    description = Column(Text, nullable=True)
    content = Column(Text, nullable=True)
    author = Column(String(255), nullable=True)
    category = Column(String(255), nullable=True, index=True)
    tags = Column(JSON, nullable=True)
    url = Column(String(1024), nullable=True)
    published_at = Column(DateTime, nullable=True, index=True)

    # Metadata
    raw_id = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Extra fields stored as JSON for flexibility
    extra_data = Column(JSON, nullable=True)

    __table_args__ = (
        UniqueConstraint("source_type", "source_id", name="uq_unified_source"),
        Index("idx_unified_created", "created_at"),
    )


# ============== ETL METADATA TABLES ==============


class ETLCheckpoint(Base):
    """Checkpoint tracking for incremental ingestion."""

    __tablename__ = "etl_checkpoints"

    id = Column(Integer, primary_key=True, index=True)
    source_type = Column(SQLEnum(SourceType), nullable=False, unique=True)
    last_source_id = Column(String(255), nullable=True)
    last_processed_at = Column(DateTime, nullable=True)
    last_offset = Column(Integer, default=0)
    checkpoint_metadata = Column(
        JSON, nullable=True
    )  # Renamed from 'metadata' (reserved in SQLAlchemy)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ETLRun(Base):
    """ETL run tracking and metadata."""

    __tablename__ = "etl_runs"

    id = Column(Integer, primary_key=True, index=True)
    run_id = Column(String(36), unique=True, nullable=False, index=True)
    source_type = Column(SQLEnum(SourceType), nullable=False, index=True)
    status = Column(SQLEnum(RunStatus), nullable=False, default=RunStatus.RUNNING)

    started_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    completed_at = Column(DateTime, nullable=True)
    duration_seconds = Column(Float, nullable=True)

    records_extracted = Column(Integer, default=0)
    records_transformed = Column(Integer, default=0)
    records_loaded = Column(Integer, default=0)
    records_skipped = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)

    error_message = Column(Text, nullable=True)
    error_traceback = Column(Text, nullable=True)

    # Run metadata
    checkpoint_data = Column(JSON, nullable=True)
    run_metadata = Column(JSON, nullable=True)

    __table_args__ = (
        Index("idx_etl_runs_started", "started_at"),
        Index("idx_etl_runs_status", "status"),
    )


class SchemaDrift(Base):
    """Schema drift detection records."""

    __tablename__ = "schema_drift"

    id = Column(Integer, primary_key=True, index=True)
    source_type = Column(SQLEnum(SourceType), nullable=False, index=True)
    detected_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    field_name = Column(String(255), nullable=False)
    expected_type = Column(String(100), nullable=True)
    actual_type = Column(String(100), nullable=True)

    drift_type = Column(String(50), nullable=False)  # new_field, missing_field, type_change
    confidence_score = Column(Float, nullable=True)

    sample_value = Column(Text, nullable=True)
    resolved = Column(Boolean, default=False)
    resolved_at = Column(DateTime, nullable=True)

    __table_args__ = (Index("idx_schema_drift_detected", "detected_at"),)
