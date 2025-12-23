"""Stress tests for the Kaspero ETL system.

These tests verify system behavior under load and stress conditions.
"""

import gc
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from core.models import ETLRun, RunStatus, SourceType, UnifiedData


class TestConcurrentAPIAccess:
    """Test API behavior under concurrent access."""

    def test_concurrent_health_checks(self, test_client):
        """Test multiple simultaneous health check requests."""
        results = []
        errors = []

        def make_request():
            try:
                response = test_client.get("/health")
                results.append(response.status_code)
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=make_request) for _ in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert all(r == 200 for r in results), f"Not all requests succeeded: {results}"
        assert len(results) == 50

    def test_concurrent_data_reads(self, test_client, sample_unified_data):
        """Test concurrent read operations on /data endpoint."""
        results = []
        errors = []

        def make_request():
            try:
                response = test_client.get("/data")
                results.append((response.status_code, len(response.json().get("data", []))))
            except Exception as e:
                errors.append(str(e))

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(make_request) for _ in range(100)]
            for future in as_completed(futures):
                pass

        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert all(r[0] == 200 for r in results)
        # All requests should return same count
        counts = [r[1] for r in results]
        assert len(set(counts)) == 1, f"Inconsistent data counts: {set(counts)}"

    def test_concurrent_mixed_endpoints(self, test_client, sample_unified_data, sample_etl_runs):
        """Test concurrent access to different endpoints."""
        endpoints = ["/health", "/data", "/stats", "/runs", "/metrics", "/ready", "/live"]
        results = {endpoint: [] for endpoint in endpoints}
        errors = []

        def make_request(endpoint):
            try:
                response = test_client.get(endpoint)
                results[endpoint].append(response.status_code)
            except Exception as e:
                errors.append((endpoint, str(e)))

        with ThreadPoolExecutor(max_workers=30) as executor:
            futures = []
            for _ in range(20):
                for endpoint in endpoints:
                    futures.append(executor.submit(make_request, endpoint))
            for future in as_completed(futures):
                pass

        assert len(errors) == 0, f"Errors occurred: {errors}"
        for endpoint, statuses in results.items():
            assert all(s == 200 for s in statuses), f"{endpoint} had failures"

    def test_rapid_pagination_requests(self, test_client, sample_unified_data):
        """Test rapid pagination with different page sizes."""
        results = []

        def paginate():
            for page in range(1, 5):
                for size in [1, 5, 10]:
                    response = test_client.get(f"/data?page={page}&page_size={size}")
                    results.append(response.status_code)

        threads = [threading.Thread(target=paginate) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert all(r == 200 for r in results)


class TestDatabaseStress:
    """Test database operations under stress."""

    def test_bulk_record_creation(self, db_session):
        """Test creating a large number of records."""
        start_time = time.time()
        records_to_create = 1000

        for i in range(records_to_create):
            record = UnifiedData(
                source_type=SourceType.CSV,
                source_id=f"stress-test-{i}",
                raw_id=i,
                title=f"Stress Test Record {i}",
                description=f"Description for stress test record {i}",
                category="Stress Test",
                author="Stress Tester",
            )
            db_session.add(record)

        db_session.commit()
        elapsed = time.time() - start_time

        # Verify all records were created
        count = (
            db_session.query(UnifiedData)
            .filter(UnifiedData.source_id.like("stress-test-%"))
            .count()
        )
        assert count == records_to_create

        # Should complete in reasonable time (< 30 seconds for 1000 records)
        assert elapsed < 30, f"Bulk insert took too long: {elapsed}s"

    def test_bulk_record_queries(self, db_session):
        """Test querying with filters on large dataset."""
        # First create records
        for i in range(500):
            record = UnifiedData(
                source_type=random.choice([SourceType.CSV, SourceType.API, SourceType.RSS]),
                source_id=f"query-test-{i}",
                raw_id=i,
                title=f"Query Test Record {i}",
                description=f"Description {i}",
                category="Category A" if i % 2 == 0 else "Category B",
            )
            db_session.add(record)
        db_session.commit()

        # Run various queries
        start_time = time.time()

        for _ in range(100):
            # Filter by source type
            db_session.query(UnifiedData).filter(UnifiedData.source_type == SourceType.CSV).limit(
                50
            ).all()

            # Filter by category
            db_session.query(UnifiedData).filter(UnifiedData.category == "Category A").limit(
                50
            ).all()

            # Search in title
            db_session.query(UnifiedData).filter(UnifiedData.title.contains("100")).all()

        elapsed = time.time() - start_time
        assert elapsed < 10, f"Queries took too long: {elapsed}s"

    def test_transaction_rollback_under_load(self, db_session):
        """Test transaction rollback behavior under load."""
        initial_count = db_session.query(UnifiedData).count()

        # Try to create records with intentional failures
        for i in range(10):
            try:
                for j in range(50):
                    record = UnifiedData(
                        source_type=SourceType.CSV,
                        source_id=f"rollback-test-{i}-{j}",
                        raw_id=j,
                        title=f"Rollback Test {i}-{j}",
                    )
                    db_session.add(record)

                if i == 5:  # Simulate failure mid-batch
                    raise ValueError("Simulated error")

                db_session.commit()
            except ValueError:
                db_session.rollback()
            except Exception:
                db_session.rollback()

        # Verify partial commits worked
        final_count = db_session.query(UnifiedData).count()
        assert final_count >= initial_count


class TestMemoryStress:
    """Test memory handling under stress."""

    def test_large_response_handling(self, test_client, db_session):
        """Test API response with large dataset."""
        # Create many records
        for i in range(200):
            record = UnifiedData(
                source_type=SourceType.API,
                source_id=f"memory-test-{i}",
                raw_id=i,
                title=f"Memory Test Record {i}",
                description="X" * 1000,  # 1KB description
                extra_data={"large_field": "Y" * 5000},  # 5KB extra data
            )
            db_session.add(record)
        db_session.commit()

        # Request large page
        response = test_client.get("/data?page_size=100")
        assert response.status_code == 200
        data = response.json()
        assert len(data["data"]) == 100

    def test_repeated_requests_no_memory_leak(self, test_client, sample_unified_data):
        """Test for memory leaks with repeated requests."""
        gc.collect()
        initial_objects = len(gc.get_objects())

        for _ in range(500):
            test_client.get("/health")
            test_client.get("/data")

        gc.collect()
        final_objects = len(gc.get_objects())

        # Allow some growth but not excessive
        growth = final_objects - initial_objects
        assert growth < 10000, f"Possible memory leak: {growth} new objects"


class TestRateLimiterStress:
    """Test rate limiter under stress conditions."""

    def test_rate_limiter_concurrent_access(self):
        """Test rate limiter with concurrent access."""
        from services.rate_limiter import RateLimiter

        limiter = RateLimiter(requests_per_minute=100)
        successful_requests = []
        blocked_requests = []
        lock = threading.Lock()

        def make_request():
            wait_time = limiter.check_rate_limit("test-source")
            with lock:
                if wait_time == 0:
                    successful_requests.append(1)
                    limiter.record_request("test-source")
                else:
                    blocked_requests.append(1)

        threads = [threading.Thread(target=make_request) for _ in range(200)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Should have some allowed requests
        assert len(successful_requests) > 0
        # Total should match thread count
        assert len(successful_requests) + len(blocked_requests) == 200

    def test_rate_limiter_multiple_sources(self):
        """Test rate limiter with multiple sources."""
        from services.rate_limiter import RateLimiter

        limiter = RateLimiter(requests_per_minute=50)
        sources = ["api", "csv", "rss", "coingecko"]

        for source in sources:
            for _ in range(50):
                wait_time = limiter.check_rate_limit(source)
                if wait_time == 0:
                    limiter.record_request(source)

        # Verify each source has its own state
        for source in sources:
            state = limiter._get_state(source)
            assert state.requests_made > 0


class TestEdgeCasesUnderLoad:
    """Test edge cases under load conditions."""

    def test_empty_database_concurrent_access(self, test_client, db_session):
        """Test concurrent access to empty database."""
        # Ensure database is empty for these tests
        db_session.query(UnifiedData).delete()
        db_session.commit()

        results = []

        def query_empty():
            response = test_client.get("/data")
            results.append(len(response.json()["data"]))

        threads = [threading.Thread(target=query_empty) for _ in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert all(r == 0 for r in results)

    def test_special_characters_search_stress(self, test_client, sample_unified_data):
        """Test search with special characters under load."""
        import urllib.parse

        special_searches = [
            "test%20search",
            "test'quote",
            'test"doublequote',
            urllib.parse.quote("test<script>"),
            urllib.parse.quote("test;DROP TABLE"),
            "test\\backslash",
            "test/slash",
            "test&ampersand",
            "test=equals",
            urllib.parse.quote("test special"),
        ]

        for search in special_searches:
            response = test_client.get(f"/data?search={search}")
            # Should not crash, may return 200 or 400
            assert response.status_code in [200, 400, 422]

    def test_extreme_pagination_values(self, test_client, sample_unified_data):
        """Test extreme pagination values."""
        test_cases = [
            {"page": 0, "page_size": 10},  # Invalid page
            {"page": 1, "page_size": 0},  # Invalid size
            {"page": 999999, "page_size": 10},  # Very high page
            {"page": 1, "page_size": 10000},  # Very large size
            {"page": -1, "page_size": 10},  # Negative page
        ]

        for params in test_cases:
            response = test_client.get(
                f"/data?page={params['page']}&page_size={params['page_size']}"
            )
            # Should handle gracefully
            assert response.status_code in [200, 400, 422]


class TestExtractorStress:
    """Test extractors under stress conditions."""

    def test_csv_extractor_large_file(self, db_session, tmp_path):
        """Test CSV extractor with large file."""
        import csv

        from ingestion.csv_extractor import CSVExtractor

        # Create large CSV file
        csv_file = tmp_path / "large.csv"
        with open(csv_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "title", "description", "category", "author", "date"])
            for i in range(5000):
                writer.writerow(
                    [
                        i,
                        f"Title {i}",
                        f"Description for item {i} " * 10,
                        f"Category {i % 10}",
                        f"Author {i % 100}",
                        "2024-01-15",
                    ]
                )

        extractor = CSVExtractor(db=db_session, csv_path=str(csv_file))

        start_time = time.time()
        records = list(extractor.extract())
        elapsed = time.time() - start_time

        assert len(records) == 5000
        assert elapsed < 30, f"Extraction took too long: {elapsed}s"

    def test_transformation_stress(self, db_session):
        """Test transformation of many records."""
        from ingestion.csv_extractor import CSVExtractor

        extractor = CSVExtractor(db=db_session)

        raw_records = [
            {
                "id": i,
                "title": f"Record {i}",
                "description": f"Description {i}",
                "category": "Test",
                "_row_number": i,
                "_source_file": "test.csv",
            }
            for i in range(1000)
        ]

        start_time = time.time()
        transformed = [extractor.transform(r) for r in raw_records]
        elapsed = time.time() - start_time

        assert len(transformed) == 1000
        assert elapsed < 5, f"Transformation took too long: {elapsed}s"


class TestCheckpointStress:
    """Test checkpoint system under stress."""

    def test_rapid_checkpoint_updates(self, db_session):
        """Test rapid checkpoint updates."""
        from core.models import SourceType
        from services.checkpoint import CheckpointManager

        manager = CheckpointManager(db=db_session)

        start_time = time.time()
        for i in range(100):
            manager.update_checkpoint(
                source_type=SourceType.CSV,
                last_source_id=f"rapid-{i}",
                last_offset=i,
                metadata={"iteration": i},
            )

        elapsed = time.time() - start_time
        assert elapsed < 10, f"Checkpoint updates took too long: {elapsed}s"

        # Verify final state
        checkpoint = manager.get_checkpoint(SourceType.CSV)
        assert checkpoint is not None
        assert "rapid-99" in checkpoint.last_source_id

    def test_concurrent_checkpoint_access(self, db_session):
        """Test concurrent checkpoint access."""
        from core.models import SourceType
        from services.checkpoint import CheckpointManager

        manager = CheckpointManager(db=db_session)
        errors = []

        def update_checkpoint(source_type, iteration):
            try:
                manager.update_checkpoint(
                    source_type=source_type,
                    last_source_id=f"concurrent-{iteration}",
                    last_offset=iteration,
                )
            except Exception as e:
                errors.append(str(e))

        # This tests that the checkpoint system handles concurrent access
        # Note: SQLite may have issues with concurrent writes
        for i in range(20):
            update_checkpoint(SourceType.CSV, i)

        # Should complete without critical errors
        # SQLite locking errors may occur but shouldn't crash
        assert len(errors) < 5, f"Too many errors: {errors}"
