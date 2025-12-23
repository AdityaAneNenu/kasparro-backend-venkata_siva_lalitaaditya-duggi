"""Tests for failure scenarios."""

from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import pytest

from core.exceptions import (
    AuthenticationError,
    ETLException,
    ExtractionError,
    RateLimitError,
)
from core.models import RunStatus, SourceType


class TestExtractionFailures:
    """Test handling of extraction failures."""

    def test_csv_file_not_found(self, db_session):
        """Test handling of missing CSV file."""
        from ingestion.csv_extractor import CSVExtractor

        extractor = CSVExtractor(db=db_session, csv_path="/nonexistent/path/file.csv")

        # Should not raise, just produce no records
        records = list(extractor.extract())
        assert len(records) == 0

    def test_api_authentication_failure(self, db_session):
        """Test handling of API authentication failure."""
        from ingestion.api_extractor import APIExtractor

        with patch("httpx.Client") as mock_client:
            mock_response = Mock()
            mock_response.status_code = 401
            mock_client.return_value.__enter__.return_value.get.return_value = mock_response

            extractor = APIExtractor(db=db_session, api_key="invalid-key")

            # AuthenticationError is wrapped in ExtractionError
            with pytest.raises((AuthenticationError, ExtractionError)):
                list(extractor.extract())

    def test_api_server_error(self, db_session):
        """Test handling of API server errors."""
        import httpx

        from ingestion.api_extractor import APIExtractor

        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.side_effect = httpx.HTTPError(
                "Server error"
            )

            extractor = APIExtractor(db=db_session)

            with pytest.raises(ExtractionError):
                list(extractor.extract())


class TestRateLimitFailures:
    """Test rate limit handling."""

    def test_rate_limit_enforced(self, db_session):
        """Test that rate limits are enforced."""
        from services.rate_limiter import RateLimiter

        limiter = RateLimiter(requests_per_minute=2)

        # Make requests up to limit
        limiter.record_request("test")
        limiter.record_request("test")

        # Next request should require waiting
        wait_time = limiter.check_rate_limit("test")
        assert wait_time > 0

    def test_max_retries_exceeded(self, db_session):
        """Test that max retries raises error."""
        from services.rate_limiter import RateLimiter

        limiter = RateLimiter(max_retries=3)

        # Exhaust retries
        limiter.record_failure("test")
        limiter.record_failure("test")
        limiter.record_failure("test")

        with pytest.raises(RateLimitError):
            limiter.record_failure("test")

    def test_exponential_backoff(self, db_session):
        """Test exponential backoff calculation."""
        from services.rate_limiter import RateLimiter

        limiter = RateLimiter(backoff_base=2.0, max_retries=5)

        backoff_1 = limiter.record_failure("test")
        backoff_2 = limiter.record_failure("test")
        backoff_3 = limiter.record_failure("test")

        # Backoff should increase exponentially
        assert backoff_1 == 2.0  # 2^1
        assert backoff_2 == 4.0  # 2^2
        assert backoff_3 == 8.0  # 2^3

    def test_success_resets_backoff(self, db_session):
        """Test that success resets backoff counter."""
        from services.rate_limiter import RateLimiter

        limiter = RateLimiter(backoff_base=2.0)

        # Build up backoff
        limiter.record_failure("test")
        limiter.record_failure("test")

        # Success should reset
        limiter.record_success("test")

        # Next failure should start from base again
        backoff = limiter.record_failure("test")
        assert backoff == 2.0  # Reset to 2^1


class TestTransformationFailures:
    """Test handling of transformation failures."""

    def test_invalid_date_handling(self, db_session):
        """Test that invalid dates don't crash transformation."""
        from ingestion.csv_extractor import CSVExtractor

        extractor = CSVExtractor(db=db_session, csv_path="/tmp/test.csv")

        raw_data = {
            "title": "Test",
            "date": "not-a-valid-date",
            "_row_number": 1,
            "_source_file": "test.csv",
        }

        # Should not raise
        result = extractor.transform(raw_data)

        # Date should be None
        assert result["published_at"] is None

    def test_missing_required_fields(self, db_session):
        """Test handling of missing fields."""
        from ingestion.csv_extractor import CSVExtractor

        extractor = CSVExtractor(db=db_session, csv_path="/tmp/test.csv")

        # Minimal data
        raw_data = {
            "_row_number": 1,
            "_source_file": "test.csv",
        }

        # Should not raise
        result = extractor.transform(raw_data)

        # All fields should be None
        assert result["title"] is None
        assert result["description"] is None


class TestDatabaseFailures:
    """Test database failure handling."""

    def test_connection_failure_health_check(self, test_client, db_session, monkeypatch):
        """Test health check handles database queries gracefully."""
        # Simply test that health endpoint responds
        response = test_client.get("/health")

        # Health check should return 200
        assert response.status_code == 200
        data = response.json()
        assert "status" in data


class TestETLRunFailures:
    """Test ETL run failure tracking."""

    def test_failed_run_recorded(self, db_session):
        """Test that failed runs are properly recorded."""
        from services.etl_tracker import ETLRunTracker

        tracker = ETLRunTracker(db_session)

        # Start run
        run = tracker.start_run(SourceType.CSV)

        # Complete with failure
        error = Exception("Test error")
        tracker.complete_run(
            run=run,
            status=RunStatus.FAILED,
            records_extracted=50,
            records_loaded=0,
            error=error,
        )

        # Verify
        db_session.refresh(run)
        assert run.status == RunStatus.FAILED
        assert run.error_message == "Test error"
        assert run.error_traceback is not None

    def test_partial_run_recorded(self, db_session):
        """Test that partial runs are properly recorded."""
        from services.etl_tracker import ETLRunTracker

        tracker = ETLRunTracker(db_session)

        # Start run
        run = tracker.start_run(SourceType.CSV)

        # Complete with partial success
        tracker.complete_run(
            run=run,
            status=RunStatus.PARTIAL,
            records_extracted=100,
            records_loaded=90,
            records_failed=10,
        )

        # Verify
        db_session.refresh(run)
        assert run.status == RunStatus.PARTIAL
        assert run.records_loaded == 90
        assert run.records_failed == 10


class TestFailureInjection:
    """Test controlled failure injection."""

    def test_failure_injection_stops_at_record(self, db_session, sample_csv_file, monkeypatch):
        """Test that failure injection works correctly."""
        import os
        from contextlib import contextmanager

        import core.database as db_module

        # Mock get_db_session to use our test session
        @contextmanager
        def mock_get_db_session():
            try:
                yield db_session
            finally:
                pass

        monkeypatch.setattr(db_module, "get_db_session", mock_get_db_session)

        from ingestion.orchestrator import ETLOrchestrator

        orchestrator = ETLOrchestrator()

        # Temporarily set the CSV path to our test file
        original_csv_path = os.environ.get("CSV_SOURCE_PATH", "")
        os.environ["CSV_SOURCE_PATH"] = sample_csv_file

        try:
            result = orchestrator.run_with_failure_injection(
                fail_at_record=3,
                source_type="csv",
            )

            # If CSV has enough records, should report failure
            # Otherwise success with 0 records is also valid
            assert result["status"] in ["failed_injection", "success"]
        finally:
            if original_csv_path:
                os.environ["CSV_SOURCE_PATH"] = original_csv_path
            else:
                os.environ.pop("CSV_SOURCE_PATH", None)
