"""CSV data extractor."""

import csv
import logging
import os
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from core.config import get_settings
from core.exceptions import ExtractionError
from core.models import RawCSVData, SourceType, UnifiedData
from ingestion.base import BaseExtractor
from services.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)
settings = get_settings()


class CSVExtractor(BaseExtractor):
    """Extractor for CSV data source."""

    source_type = SourceType.CSV

    def __init__(
        self,
        db: Session,
        csv_path: Optional[str] = None,
        encoding: str = "utf-8",
        delimiter: str = ",",
        rate_limiter: Optional[RateLimiter] = None,
        **kwargs,
    ):
        super().__init__(db, rate_limiter, **kwargs)
        self.csv_path = csv_path or settings.CSV_SOURCE_PATH
        self.encoding = encoding
        self.delimiter = delimiter

    def _detect_encoding(self, file_path: str) -> str:
        """Attempt to detect file encoding."""
        encodings_to_try = ["utf-8", "utf-8-sig", "latin-1", "cp1252", "iso-8859-1"]

        for encoding in encodings_to_try:
            try:
                with open(file_path, "r", encoding=encoding) as f:
                    f.read(1024)
                return encoding
            except UnicodeDecodeError:
                continue

        return "utf-8"  # Fallback

    def _clean_value(self, value: Any) -> Any:
        """Clean and normalize a CSV value."""
        if value is None:
            return None

        if isinstance(value, str):
            value = value.strip()
            if value.lower() in ("", "null", "none", "n/a", "na", "-"):
                return None

            # Try to convert to number
            try:
                if "." in value:
                    return float(value)
                return int(value)
            except ValueError:
                pass

            # Try to parse as boolean
            if value.lower() in ("true", "yes", "1"):
                return True
            if value.lower() in ("false", "no", "0"):
                return False

        return value

    def extract(self) -> Generator[Dict[str, Any], None, None]:
        """Extract data from CSV file."""
        if not os.path.exists(self.csv_path):
            logger.warning(f"CSV file not found: {self.csv_path}")
            return

        # Get checkpoint for incremental ingestion
        checkpoint = self.checkpoint_manager.get_checkpoint(self.source_type)
        last_row = checkpoint.last_offset if checkpoint else 0

        # Detect encoding
        encoding = self._detect_encoding(self.csv_path)
        logger.info(f"Reading CSV with encoding: {encoding}")

        try:
            with open(self.csv_path, "r", encoding=encoding, newline="") as f:
                # Try to detect dialect
                sample = f.read(4096)
                f.seek(0)

                try:
                    dialect = csv.Sniffer().sniff(sample)
                except csv.Error:
                    dialect = csv.excel

                reader = csv.DictReader(f, dialect=dialect)

                for row_num, row in enumerate(reader, start=1):
                    # Skip rows already processed (incremental)
                    if row_num <= last_row:
                        self.records_skipped += 1
                        continue

                    # Clean values
                    cleaned_row = {k: self._clean_value(v) for k, v in row.items() if k is not None}

                    # Add metadata
                    cleaned_row["_row_number"] = row_num
                    cleaned_row["_source_file"] = os.path.basename(self.csv_path)

                    yield cleaned_row

        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise ExtractionError(f"CSV extraction failed: {e}")

    def get_source_id(self, raw_data: Dict[str, Any]) -> str:
        """Get unique source ID from CSV data."""
        # Use row number and filename as composite ID
        row_num = raw_data.get("_row_number", 0)
        source_file = raw_data.get("_source_file", "unknown")
        return f"{source_file}:{row_num}"

    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform CSV data to unified schema."""
        # Remove metadata fields
        data = {k: v for k, v in raw_data.items() if not k.startswith("_")}

        # Parse date fields
        published_at = None
        for date_field in ["date", "created_at", "timestamp", "published_at", "created_date"]:
            if date_field in data and data[date_field]:
                try:
                    if isinstance(data[date_field], datetime):
                        published_at = data[date_field]
                    else:
                        # Try multiple date formats
                        for fmt in [
                            "%Y-%m-%d",
                            "%Y-%m-%d %H:%M:%S",
                            "%d/%m/%Y",
                            "%m/%d/%Y",
                            "%Y/%m/%d",
                            "%d-%m-%Y",
                        ]:
                            try:
                                published_at = datetime.strptime(str(data[date_field]), fmt)
                                break
                            except ValueError:
                                continue
                    if published_at:
                        break
                except (ValueError, TypeError):
                    continue

        # Map common field names
        title = data.get("title") or data.get("name") or data.get("headline") or data.get("subject")

        description = (
            data.get("description")
            or data.get("summary")
            or data.get("desc")
            or data.get("abstract")
        )

        content = data.get("content") or data.get("body") or data.get("text") or data.get("message")

        author = (
            data.get("author")
            or data.get("creator")
            or data.get("user")
            or data.get("writer")
            or data.get("by")
        )

        category = (
            data.get("category") or data.get("type") or data.get("group") or data.get("section")
        )

        tags = data.get("tags")
        if isinstance(tags, str):
            tags = [t.strip() for t in tags.split(",") if t.strip()]

        url = data.get("url") or data.get("link") or data.get("href")

        # Collect remaining fields as extra data
        mapped_fields = {
            "title",
            "name",
            "headline",
            "subject",
            "description",
            "summary",
            "desc",
            "abstract",
            "content",
            "body",
            "text",
            "message",
            "author",
            "creator",
            "user",
            "writer",
            "by",
            "category",
            "type",
            "group",
            "section",
            "tags",
            "url",
            "link",
            "href",
            "date",
            "created_at",
            "timestamp",
            "published_at",
            "created_date",
        }

        extra_data = {k: v for k, v in data.items() if k.lower() not in mapped_fields}

        return {
            "title": title,
            "description": description,
            "content": content,
            "author": author,
            "category": category,
            "tags": tags if tags else None,
            "url": url,
            "published_at": published_at,
            "extra_data": extra_data if extra_data else None,
        }

    def load_raw(self, raw_data: Dict[str, Any]) -> int:
        """Load raw CSV data with upsert (idempotent)."""
        source_id = self.get_source_id(raw_data)
        checksum = self.compute_checksum(raw_data)
        row_number = raw_data.get("_row_number", 0)
        source_file = raw_data.get("_source_file", "unknown")

        # Remove metadata before storing
        payload = {k: v for k, v in raw_data.items() if not k.startswith("_")}

        stmt = (
            insert(RawCSVData)
            .values(
                source_id=source_id,
                raw_payload=payload,
                source_file=source_file,
                row_number=row_number,
                checksum=checksum,
                ingested_at=datetime.utcnow(),
            )
            .on_conflict_do_update(
                index_elements=["source_id"],
                set_={
                    "raw_payload": payload,
                    "checksum": checksum,
                    "ingested_at": datetime.utcnow(),
                },
            )
            .returning(RawCSVData.id)
        )

        result = self.db.execute(stmt)
        self.db.commit()

        return result.scalar_one()

    def load_unified(self, transformed_data: Dict[str, Any], raw_id: int) -> int:
        """Load transformed data into unified table with upsert."""
        source_id = str(raw_id)

        stmt = (
            insert(UnifiedData)
            .values(
                source_type=self.source_type, source_id=source_id, raw_id=raw_id, **transformed_data
            )
            .on_conflict_do_update(
                constraint="uq_unified_source",
                set_={"raw_id": raw_id, "updated_at": datetime.utcnow(), **transformed_data},
            )
            .returning(UnifiedData.id)
        )

        result = self.db.execute(stmt)
        self.db.commit()

        return result.scalar_one()


class MultiCSVExtractor:
    """Extractor for multiple CSV files with different schemas."""

    def __init__(
        self,
        db: Session,
        csv_paths: Optional[List[str]] = None,
        rate_limiter: Optional[RateLimiter] = None,
    ):
        self.db = db
        self.csv_paths = csv_paths or [settings.CSV_SOURCE_PATH, settings.CSV_SOURCE_2_PATH]
        self.rate_limiter = rate_limiter or RateLimiter()
        self.results = []

    def run(self) -> List[Dict[str, Any]]:
        """Run extraction for all CSV files."""
        for csv_path in self.csv_paths:
            if os.path.exists(csv_path):
                extractor = CSVExtractor(
                    db=self.db,
                    csv_path=csv_path,
                    rate_limiter=self.rate_limiter,
                )
                result = extractor.run()
                self.results.append(result)
            else:
                logger.warning(f"CSV file not found: {csv_path}")

        return self.results
