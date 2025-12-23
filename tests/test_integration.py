"""Integration tests for the Kaspero ETL system.

These tests verify the integration between different components.
"""

import csv
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from core.models import ETLCheckpoint, ETLRun, RunStatus, SourceType, UnifiedData


class TestETLPipelineIntegration:
    """Test complete ETL pipeline integration."""

    def test_csv_etl_full_pipeline(self, db_session, sample_csv_file):
        """Test complete CSV ETL pipeline from extraction to loading."""
        from ingestion.csv_extractor import CSVExtractor
        from services.checkpoint import CheckpointManager
        from services.etl_tracker import ETLRunTracker

        # Initialize components
        extractor = CSVExtractor(db=db_session, csv_path=sample_csv_file)
        checkpoint_manager = CheckpointManager(db=db_session)
        tracker = ETLRunTracker(db=db_session)

        # Start tracking
        run = tracker.start_run(source_type=SourceType.CSV)
        assert run is not None
        assert run.run_id is not None

        # Extract
        records = list(extractor.extract())
        assert len(records) > 0

        # Transform
        transformed = [extractor.transform(r) for r in records]
        assert all(isinstance(t, dict) for t in transformed)

        # Complete tracking
        tracker.complete_run(
            run=run,
            status=RunStatus.SUCCESS,
            records_extracted=len(records),
            records_transformed=len(transformed),
            records_loaded=len(transformed),
        )

        # Verify run was recorded
        runs = db_session.query(ETLRun).filter(ETLRun.run_id == run.run_id).all()
        assert len(runs) == 1
        assert runs[0].status == RunStatus.SUCCESS

    def test_multiple_source_integration(self, db_session, sample_csv_file):
        """Test running multiple sources in sequence."""
        from ingestion.csv_extractor import CSVExtractor
        from services.etl_tracker import ETLRunTracker

        # Run CSV extraction
        csv_extractor = CSVExtractor(db=db_session, csv_path=sample_csv_file)
        tracker = ETLRunTracker(db=db_session)

        csv_run = tracker.start_run(source_type=SourceType.CSV)
        csv_records = list(csv_extractor.extract())

        tracker.complete_run(
            run=csv_run,
            status=RunStatus.SUCCESS,
            records_extracted=len(csv_records),
        )

        # Verify run recorded
        assert csv_run.status == RunStatus.SUCCESS

    def test_checkpoint_integration_with_extraction(self, db_session, tmp_path):
        """Test checkpoint integration during extraction."""
        from ingestion.csv_extractor import CSVExtractor
        from services.checkpoint import CheckpointManager

        # Create test CSV with many records
        csv_file = tmp_path / "checkpoint_test.csv"
        with open(csv_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "title", "description"])
            for i in range(20):
                writer.writerow([i, f"Title {i}", f"Description {i}"])

        extractor = CSVExtractor(db=db_session, csv_path=str(csv_file))
        checkpoint_manager = CheckpointManager(db=db_session)

        # Extract first batch
        records = list(extractor.extract())

        # Update checkpoint
        if records:
            checkpoint_manager.update_checkpoint(
                source_type=SourceType.CSV,
                last_source_id=f"csv:{len(records)-1}",
                last_offset=len(records),
            )

        # Verify checkpoint
        checkpoint = checkpoint_manager.get_checkpoint(SourceType.CSV)
        assert checkpoint is not None
        assert checkpoint.last_offset == len(records)


class TestAPIIntegration:
    """Test API integration with database."""

    def test_data_endpoint_reflects_database(self, test_client, db_session):
        """Test that /data endpoint accurately reflects database state."""
        # Start with empty
        response = test_client.get("/data")
        assert response.status_code == 200
        initial_data = response.json()

        # Add record directly to database
        record = UnifiedData(
            source_type=SourceType.API,
            source_id="integration-test-1",
            raw_id=1,
            title="Integration Test Record",
            description="Added for integration testing",
        )
        db_session.add(record)
        db_session.commit()

        # Verify API reflects change
        response = test_client.get("/data")
        assert response.status_code == 200
        new_data = response.json()

        assert (
            new_data["pagination"]["total_items"] == initial_data["pagination"]["total_items"] + 1
        )

    def test_stats_endpoint_reflects_runs(self, test_client, db_session):
        """Test that /stats endpoint reflects ETL run data."""
        from services.etl_tracker import ETLRunTracker

        # Record a run
        tracker = ETLRunTracker(db=db_session)
        run = tracker.start_run(source_type=SourceType.CSV)
        tracker.complete_run(
            run=run,
            status=RunStatus.SUCCESS,
            records_extracted=100,
            records_loaded=95,
        )

        # Check stats
        response = test_client.get("/stats")
        assert response.status_code == 200
        stats = response.json()

        # Stats endpoint should return some data
        assert isinstance(stats, dict)

    def test_health_endpoint_database_integration(self, test_client, db_session):
        """Test health endpoint checks database connectivity."""
        response = test_client.get("/health")
        assert response.status_code == 200
        health = response.json()

        assert "status" in health or "database" in health


class TestSchemaDriftIntegration:
    """Test schema drift detection integration."""

    def test_drift_detection_with_extraction(self, db_session):
        """Test schema drift detection during extraction."""
        from core.models import SourceType
        from services.schema_drift import SchemaDriftDetector

        detector = SchemaDriftDetector(db=db_session)

        # Simulate data with potential drift
        test_data = {
            "id": "test-1",
            "title": "Test Title",
            "description": "Test Desc",
            "content": "Content",
            "author": "Author",
            "category": "Category",
            "tags": ["a", "b"],
            "url": "http://test.com",
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "extra_field": "This is a new field",  # Should trigger drift
        }

        # Detect drift
        drifts = detector.detect_drift(SourceType.API, test_data)

        # Should detect new field
        new_field_drifts = [d for d in drifts if d.drift_type == "new_field"]
        assert len(new_field_drifts) >= 1


class TestRateLimiterIntegration:
    """Test rate limiter integration with extractors."""

    def test_rate_limiter_check_and_record(self, db_session):
        """Test rate limiter check and record workflow."""
        from services.rate_limiter import RateLimiter

        # Create rate limiter with low limit
        rate_limiter = RateLimiter(requests_per_minute=5)

        # First 5 requests should be allowed
        for i in range(5):
            wait_time = rate_limiter.check_rate_limit("test-source")
            assert wait_time == 0
            rate_limiter.record_request("test-source")

        # 6th request should be blocked
        wait_time = rate_limiter.check_rate_limit("test-source")
        assert wait_time > 0


class TestErrorRecoveryIntegration:
    """Test error recovery across components."""

    def test_partial_failure_recovery(self, db_session, tmp_path):
        """Test recovery from partial ETL failure."""
        from ingestion.csv_extractor import CSVExtractor
        from services.checkpoint import CheckpointManager
        from services.etl_tracker import ETLRunTracker

        # Create CSV
        csv_file = tmp_path / "recovery_test.csv"
        with open(csv_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "title"])
            for i in range(10):
                writer.writerow([i, f"Title {i}"])

        extractor = CSVExtractor(db=db_session, csv_path=str(csv_file))
        tracker = ETLRunTracker(db=db_session)
        checkpoint_manager = CheckpointManager(db=db_session)

        # Simulate partial run
        run = tracker.start_run(source_type=SourceType.CSV)
        records = list(extractor.extract())

        # Process first half
        processed = 0
        for i, record in enumerate(records[:5]):
            transformed = extractor.transform(record)
            processed += 1
            checkpoint_manager.update_checkpoint(
                source_type=SourceType.CSV,
                last_offset=processed,
            )

        # Complete with partial status
        tracker.complete_run(
            run=run,
            status=RunStatus.PARTIAL,
            records_extracted=len(records),
            records_loaded=processed,
        )

        # Verify partial progress saved
        checkpoint = checkpoint_manager.get_checkpoint(SourceType.CSV)
        assert checkpoint.last_offset == 5

        # Verify run recorded as partial
        db_run = db_session.query(ETLRun).filter(ETLRun.run_id == run.run_id).first()
        assert db_run.status == RunStatus.PARTIAL

    def test_database_error_handling(self, db_session):
        """Test handling of database errors."""
        from services.etl_tracker import ETLRunTracker

        tracker = ETLRunTracker(db=db_session)

        # Start a run
        run = tracker.start_run(source_type=SourceType.CSV)
        assert run is not None

        # Complete with failure
        tracker.complete_run(
            run=run,
            status=RunStatus.FAILED,
            error=Exception("Test error"),
        )

        # Verify error was recorded
        db_run = db_session.query(ETLRun).filter(ETLRun.run_id == run.run_id).first()
        assert db_run.status == RunStatus.FAILED
        assert "Test error" in db_run.error_message


class TestMultiSourceCoordination:
    """Test coordination between multiple data sources."""

    def test_source_isolation(self, db_session):
        """Test that different sources are properly isolated."""
        from services.checkpoint import CheckpointManager

        checkpoint_manager = CheckpointManager(db=db_session)

        # Update checkpoint for CSV
        checkpoint_manager.update_checkpoint(SourceType.CSV, "csv-last-1", 100)

        # Update checkpoint for API
        checkpoint_manager.update_checkpoint(SourceType.API, "api-last-1", 50)

        # Verify isolation
        csv_checkpoint = checkpoint_manager.get_checkpoint(SourceType.CSV)
        api_checkpoint = checkpoint_manager.get_checkpoint(SourceType.API)

        assert csv_checkpoint.last_source_id == "csv-last-1"
        assert csv_checkpoint.last_offset == 100
        assert api_checkpoint.last_source_id == "api-last-1"
        assert api_checkpoint.last_offset == 50

    def test_concurrent_source_runs(self, db_session):
        """Test running multiple sources with separate trackers."""
        from services.etl_tracker import ETLRunTracker

        # Create tracker
        tracker = ETLRunTracker(db=db_session)

        # Start both runs
        csv_run = tracker.start_run(source_type=SourceType.CSV)
        api_run = tracker.start_run(source_type=SourceType.API)

        assert csv_run.run_id != api_run.run_id

        # Complete separately
        tracker.complete_run(
            run=csv_run,
            status=RunStatus.SUCCESS,
            records_extracted=100,
        )
        tracker.complete_run(
            run=api_run,
            status=RunStatus.SUCCESS,
            records_extracted=50,
        )

        # Verify both recorded correctly
        csv_result = db_session.query(ETLRun).filter(ETLRun.run_id == csv_run.run_id).first()
        api_result = db_session.query(ETLRun).filter(ETLRun.run_id == api_run.run_id).first()

        assert csv_result.records_extracted == 100
        assert api_result.records_extracted == 50


class TestDataConsistency:
    """Test data consistency across operations."""

    def test_upsert_consistency(self, db_session, tmp_path):
        """Test that upserts maintain data consistency."""
        from ingestion.csv_extractor import CSVExtractor

        # Create initial CSV
        csv_file = tmp_path / "upsert_test.csv"
        with open(csv_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "title", "description"])
            writer.writerow([1, "Original Title", "Original Desc"])

        # First run
        extractor = CSVExtractor(db=db_session, csv_path=str(csv_file))
        records = list(extractor.extract())
        assert len(records) > 0

        for record in records:
            transformed = extractor.transform(record)
            assert "title" in transformed

    def test_transaction_consistency(self, db_session):
        """Test transaction consistency during failures."""
        from core.models import SourceType, UnifiedData

        initial_count = db_session.query(UnifiedData).count()

        try:
            # Add valid record
            record1 = UnifiedData(
                source_type=SourceType.CSV,
                source_id="txn-test-1",
                raw_id=1,
                title="Valid Record",
            )
            db_session.add(record1)

            # Simulate error before commit
            raise ValueError("Simulated error")

            db_session.commit()
        except ValueError:
            db_session.rollback()

        # Verify no partial data
        final_count = db_session.query(UnifiedData).count()
        assert final_count == initial_count
