"""Schema drift detection service."""

import logging
from dataclasses import dataclass
from datetime import datetime
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy.orm import Session

from core.config import get_settings
from core.exceptions import SchemaDriftError
from core.models import SchemaDrift, SourceType

logger = logging.getLogger(__name__)
settings = get_settings()


@dataclass
class DriftResult:
    """Result of schema drift detection."""

    field_name: str
    drift_type: str  # new_field, missing_field, type_change
    expected_type: Optional[str]
    actual_type: Optional[str]
    confidence_score: float
    sample_value: Optional[str]


class SchemaDriftDetector:
    """Detects schema changes in incoming data."""

    # Expected schema definitions per source type
    EXPECTED_SCHEMAS: Dict[str, Dict[str, str]] = {
        "api": {
            "id": "str",
            "title": "str",
            "description": "str",
            "content": "str",
            "author": "str",
            "category": "str",
            "tags": "list",
            "url": "str",
            "created_at": "datetime",
            "updated_at": "datetime",
        },
        "csv": {
            "id": "str",
            "name": "str",
            "description": "str",
            "category": "str",
            "value": "float",
            "date": "datetime",
            "active": "bool",
        },
        "rss": {
            "guid": "str",
            "title": "str",
            "description": "str",
            "link": "str",
            "author": "str",
            "pubDate": "datetime",
            "category": "str",
        },
    }

    def __init__(self, db: Session, confidence_threshold: float = None):  # type: ignore[assignment]
        self.db = db
        self.confidence_threshold = (
            confidence_threshold or settings.SCHEMA_DRIFT_CONFIDENCE_THRESHOLD
        )

    def _get_python_type(self, value: Any) -> str:
        """Get the type name of a Python value."""
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, int):
            return "int"
        if isinstance(value, float):
            return "float"
        if isinstance(value, str):
            return "str"
        if isinstance(value, list):
            return "list"
        if isinstance(value, dict):
            return "dict"
        if isinstance(value, datetime):
            return "datetime"
        return type(value).__name__

    def _fuzzy_match_field(
        self, field_name: str, expected_fields: Set[str]
    ) -> Tuple[Optional[str], float]:
        """Find the best fuzzy match for a field name."""
        best_match = None
        best_score = 0.0

        for expected_field in expected_fields:
            # Direct match
            if field_name.lower() == expected_field.lower():
                return expected_field, 1.0

            # Fuzzy match using SequenceMatcher
            score = SequenceMatcher(None, field_name.lower(), expected_field.lower()).ratio()
            if score > best_score:
                best_score = score
                best_match = expected_field

        return best_match, best_score

    def _types_compatible(self, expected: str, actual: str) -> bool:
        """Check if types are compatible (allow some flexibility)."""
        compatible_pairs = [
            ("int", "float"),
            ("float", "int"),
            ("str", "int"),
            ("str", "float"),
            ("datetime", "str"),
            ("list", "str"),  # JSON string that might be a list
        ]

        if expected == actual:
            return True
        if (expected, actual) in compatible_pairs or (actual, expected) in compatible_pairs:
            return True
        return False

    def detect_drift(
        self,
        source_type: SourceType,
        data: Dict[str, Any],
    ) -> List[DriftResult]:
        """
        Detect schema drift in a single record.

        Args:
            source_type: The type of data source
            data: The data record to check

        Returns:
            List of drift results
        """
        drifts: List[DriftResult] = []
        source_key = source_type.value

        expected_schema = self.EXPECTED_SCHEMAS.get(source_key, {})
        expected_fields = set(expected_schema.keys())
        actual_fields = set(data.keys())

        # Check for new fields
        for field in actual_fields - expected_fields:
            match, score = self._fuzzy_match_field(field, expected_fields)

            if score >= self.confidence_threshold:
                # Likely a renamed field
                drifts.append(
                    DriftResult(
                        field_name=field,
                        drift_type="renamed_field",
                        expected_type=expected_schema.get(match),
                        actual_type=self._get_python_type(data.get(field)),
                        confidence_score=score,
                        sample_value=str(data.get(field))[:200] if data.get(field) else None,
                    )
                )
            else:
                # Truly new field
                drifts.append(
                    DriftResult(
                        field_name=field,
                        drift_type="new_field",
                        expected_type=None,
                        actual_type=self._get_python_type(data.get(field)),
                        confidence_score=1.0 - score if match else 1.0,
                        sample_value=str(data.get(field))[:200] if data.get(field) else None,
                    )
                )

        # Check for missing fields
        for field in expected_fields - actual_fields:
            match, score = self._fuzzy_match_field(field, actual_fields)

            if score < self.confidence_threshold:
                # Field is truly missing (not just renamed)
                drifts.append(
                    DriftResult(
                        field_name=field,
                        drift_type="missing_field",
                        expected_type=expected_schema.get(field),
                        actual_type=None,
                        confidence_score=1.0 - score if match else 1.0,
                        sample_value=None,
                    )
                )

        # Check for type changes in existing fields
        for field in expected_fields & actual_fields:
            expected_type = expected_schema.get(field)
            actual_type = self._get_python_type(data.get(field))

            if not self._types_compatible(expected_type, actual_type):
                drifts.append(
                    DriftResult(
                        field_name=field,
                        drift_type="type_change",
                        expected_type=expected_type,
                        actual_type=actual_type,
                        confidence_score=1.0,
                        sample_value=str(data.get(field))[:200] if data.get(field) else None,
                    )
                )

        return drifts

    def record_drifts(
        self,
        source_type: SourceType,
        drifts: List[DriftResult],
    ) -> List[SchemaDrift]:
        """Record detected drifts to the database."""
        if not drifts:
            return []

        records = []
        for drift in drifts:
            # Log warning
            logger.warning(
                f"Schema drift detected [{source_type.value}]: "
                f"{drift.drift_type} - {drift.field_name} "
                f"(expected: {drift.expected_type}, actual: {drift.actual_type}, "
                f"confidence: {drift.confidence_score:.2f})"
            )

            # Create database record
            record = SchemaDrift(
                source_type=source_type,
                field_name=drift.field_name,
                drift_type=drift.drift_type,
                expected_type=drift.expected_type,
                actual_type=drift.actual_type,
                confidence_score=drift.confidence_score,
                sample_value=drift.sample_value,
            )
            self.db.add(record)
            records.append(record)

        try:
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            logger.error(f"Failed to record schema drifts: {e}")

        return records

    def get_unresolved_drifts(
        self,
        source_type: Optional[SourceType] = None,
    ) -> List[SchemaDrift]:
        """Get all unresolved schema drifts."""
        query = self.db.query(SchemaDrift).filter(SchemaDrift.resolved == False)

        if source_type:
            query = query.filter(SchemaDrift.source_type == source_type)

        return query.order_by(SchemaDrift.detected_at.desc()).all()

    def resolve_drift(self, drift_id: int) -> bool:
        """Mark a drift as resolved."""
        drift = self.db.query(SchemaDrift).filter(SchemaDrift.id == drift_id).first()
        if drift:
            drift.resolved = True
            drift.resolved_at = datetime.utcnow()
            self.db.commit()
            return True
        return False

    def update_expected_schema(
        self,
        source_type: str,
        schema: Dict[str, str],
    ) -> None:
        """Update the expected schema for a source type."""
        self.EXPECTED_SCHEMAS[source_type] = schema
        logger.info(f"Updated expected schema for {source_type}")
