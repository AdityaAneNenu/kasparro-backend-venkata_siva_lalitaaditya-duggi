"""Tests for schema drift detection."""

from datetime import datetime

import pytest

from core.models import SchemaDrift, SourceType
from services.schema_drift import DriftResult, SchemaDriftDetector


class TestSchemaDriftDetection:
    """Test schema drift detection."""

    def test_detect_new_field(self, db_session):
        """Test detection of new fields."""
        detector = SchemaDriftDetector(db_session)

        data = {
            "id": "123",
            "title": "Test",
            "new_unexpected_field": "value",  # New field
        }

        drifts = detector.detect_drift(SourceType.API, data)

        new_field_drifts = [d for d in drifts if d.drift_type == "new_field"]
        assert len(new_field_drifts) > 0
        assert any(d.field_name == "new_unexpected_field" for d in new_field_drifts)

    def test_detect_missing_field(self, db_session):
        """Test detection of missing expected fields."""
        detector = SchemaDriftDetector(db_session)

        # API schema expects: id, title, description, content, author, etc.
        data = {
            "id": "123",
            # Missing title, description, etc.
        }

        drifts = detector.detect_drift(SourceType.API, data)

        missing_field_drifts = [d for d in drifts if d.drift_type == "missing_field"]
        assert len(missing_field_drifts) > 0

    def test_detect_type_change(self, db_session):
        """Test detection of type changes."""
        detector = SchemaDriftDetector(db_session)

        data = {
            "id": "123",
            "title": "Test",
            "tags": 123,  # Should be list, not int
        }

        drifts = detector.detect_drift(SourceType.API, data)

        type_change_drifts = [d for d in drifts if d.drift_type == "type_change"]
        # tags expected to be list, got int
        assert any(d.field_name == "tags" for d in type_change_drifts)

    def test_fuzzy_match_renamed_field(self, db_session):
        """Test fuzzy matching for renamed fields."""
        detector = SchemaDriftDetector(db_session, confidence_threshold=0.7)

        data = {
            "id": "123",
            "title": "Test",
            "descrption": "Misspelled description",  # Similar to "description"
        }

        drifts = detector.detect_drift(SourceType.API, data)

        # Should detect this as a potential rename
        renamed_drifts = [d for d in drifts if d.drift_type == "renamed_field"]
        # The fuzzy match should identify "descrption" as similar to "description"

    def test_compatible_types_not_flagged(self, db_session):
        """Test that compatible types are not flagged as drift."""
        detector = SchemaDriftDetector(db_session)

        # int and float should be compatible
        assert detector._types_compatible("int", "float") == True
        assert detector._types_compatible("float", "int") == True

        # str and int should be compatible (string representation)
        assert detector._types_compatible("str", "int") == True

    def test_confidence_score_calculation(self, db_session):
        """Test confidence score is calculated correctly."""
        detector = SchemaDriftDetector(db_session)

        data = {
            "id": "123",
            "completely_new_field": "value",
        }

        drifts = detector.detect_drift(SourceType.API, data)

        # New fields should have high confidence
        for drift in drifts:
            if drift.drift_type == "new_field":
                assert drift.confidence_score >= 0.5


class TestSchemaDriftRecording:
    """Test schema drift recording."""

    def test_record_drifts_to_database(self, db_session):
        """Test that drifts are recorded to database."""
        detector = SchemaDriftDetector(db_session)

        drifts = [
            DriftResult(
                field_name="test_field",
                drift_type="new_field",
                expected_type=None,
                actual_type="str",
                confidence_score=0.9,
                sample_value="test value",
            )
        ]

        records = detector.record_drifts(SourceType.API, drifts)

        assert len(records) == 1

        # Verify in database
        db_drift = db_session.query(SchemaDrift).first()
        assert db_drift is not None
        assert db_drift.field_name == "test_field"
        assert db_drift.drift_type == "new_field"

    def test_get_unresolved_drifts(self, db_session):
        """Test retrieving unresolved drifts."""
        detector = SchemaDriftDetector(db_session)

        # Create some drifts
        drifts = [
            DriftResult("field1", "new_field", None, "str", 0.9, None),
            DriftResult("field2", "missing_field", "str", None, 0.9, None),
        ]
        detector.record_drifts(SourceType.API, drifts)

        # Get unresolved
        unresolved = detector.get_unresolved_drifts()

        assert len(unresolved) == 2
        assert all(not d.resolved for d in unresolved)

    def test_resolve_drift(self, db_session):
        """Test resolving a drift."""
        detector = SchemaDriftDetector(db_session)

        # Create drift
        drifts = [DriftResult("test", "new_field", None, "str", 0.9, None)]
        records = detector.record_drifts(SourceType.API, drifts)

        drift_id = records[0].id

        # Resolve
        result = detector.resolve_drift(drift_id)

        assert result == True

        # Verify resolved
        drift = db_session.query(SchemaDrift).filter(SchemaDrift.id == drift_id).first()
        assert drift.resolved == True
        assert drift.resolved_at is not None

    def test_filter_by_source_type(self, db_session):
        """Test filtering drifts by source type."""
        detector = SchemaDriftDetector(db_session)

        # Create drifts for different sources
        detector.record_drifts(
            SourceType.API, [DriftResult("api_field", "new_field", None, "str", 0.9, None)]
        )
        detector.record_drifts(
            SourceType.CSV, [DriftResult("csv_field", "new_field", None, "str", 0.9, None)]
        )

        # Filter by API
        api_drifts = detector.get_unresolved_drifts(SourceType.API)

        assert len(api_drifts) == 1
        assert api_drifts[0].source_type == SourceType.API


class TestSchemaUpdate:
    """Test expected schema updates."""

    def test_update_expected_schema(self, db_session):
        """Test updating expected schema."""
        detector = SchemaDriftDetector(db_session)

        new_schema = {
            "id": "str",
            "new_field": "str",
            "another_field": "int",
        }

        detector.update_expected_schema("api", new_schema)

        # The new schema should be used for detection
        assert detector.EXPECTED_SCHEMAS["api"] == new_schema
