"""Performance tests for the Kaspero ETL system.

These tests measure and verify performance characteristics.
"""

import statistics
import time
from datetime import datetime, timedelta
from functools import wraps
from typing import Callable, List

import pytest

from core.models import ETLRun, RunStatus, SourceType, UnifiedData


def measure_time(func: Callable) -> Callable:
    """Decorator to measure function execution time."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        return result, elapsed

    return wrapper


class PerformanceMetrics:
    """Helper class to collect and analyze performance metrics."""

    def __init__(self):
        self.measurements: List[float] = []

    def add(self, value: float):
        self.measurements.append(value)

    @property
    def mean(self) -> float:
        return statistics.mean(self.measurements) if self.measurements else 0

    @property
    def median(self) -> float:
        return statistics.median(self.measurements) if self.measurements else 0

    @property
    def stdev(self) -> float:
        return statistics.stdev(self.measurements) if len(self.measurements) > 1 else 0

    @property
    def p95(self) -> float:
        if not self.measurements:
            return 0
        sorted_values = sorted(self.measurements)
        index = int(len(sorted_values) * 0.95)
        return sorted_values[min(index, len(sorted_values) - 1)]

    @property
    def p99(self) -> float:
        if not self.measurements:
            return 0
        sorted_values = sorted(self.measurements)
        index = int(len(sorted_values) * 0.99)
        return sorted_values[min(index, len(sorted_values) - 1)]


class TestAPIPerformance:
    """Test API endpoint performance."""

    def test_health_endpoint_latency(self, test_client):
        """Test health endpoint response time."""
        metrics = PerformanceMetrics()

        # Warm up
        for _ in range(5):
            test_client.get("/health")

        # Measure
        for _ in range(100):
            start = time.perf_counter()
            response = test_client.get("/health")
            elapsed = time.perf_counter() - start
            assert response.status_code == 200
            metrics.add(elapsed)

        # Assertions
        assert metrics.mean < 0.1, f"Mean latency too high: {metrics.mean:.3f}s"
        assert metrics.p95 < 0.2, f"P95 latency too high: {metrics.p95:.3f}s"
        assert metrics.p99 < 0.5, f"P99 latency too high: {metrics.p99:.3f}s"

    def test_data_endpoint_latency(self, test_client, sample_unified_data):
        """Test data endpoint response time."""
        metrics = PerformanceMetrics()

        # Warm up
        for _ in range(5):
            test_client.get("/data")

        # Measure
        for _ in range(50):
            start = time.perf_counter()
            response = test_client.get("/data")
            elapsed = time.perf_counter() - start
            assert response.status_code == 200
            metrics.add(elapsed)

        assert metrics.mean < 0.2, f"Mean latency too high: {metrics.mean:.3f}s"
        assert metrics.p95 < 0.5, f"P95 latency too high: {metrics.p95:.3f}s"

    def test_data_endpoint_with_filters_latency(self, test_client, sample_unified_data):
        """Test data endpoint with filters response time."""
        metrics = PerformanceMetrics()

        filters = [
            "?source_type=csv",
            "?category=Test",
            "?search=Article",
            "?page=1&page_size=5",
            "?source_type=csv&category=Test",
        ]

        for _ in range(10):
            for filter_str in filters:
                start = time.perf_counter()
                response = test_client.get(f"/data{filter_str}")
                elapsed = time.perf_counter() - start
                assert response.status_code == 200
                metrics.add(elapsed)

        assert metrics.mean < 0.3, f"Mean latency with filters too high: {metrics.mean:.3f}s"

    def test_stats_endpoint_latency(self, test_client, sample_unified_data, sample_etl_runs):
        """Test stats endpoint response time."""
        metrics = PerformanceMetrics()

        for _ in range(50):
            start = time.perf_counter()
            response = test_client.get("/stats")
            elapsed = time.perf_counter() - start
            assert response.status_code == 200
            metrics.add(elapsed)

        assert metrics.mean < 0.2, f"Mean latency too high: {metrics.mean:.3f}s"

    def test_metrics_endpoint_latency(self, test_client, sample_unified_data):
        """Test metrics endpoint response time."""
        metrics = PerformanceMetrics()

        for _ in range(50):
            start = time.perf_counter()
            response = test_client.get("/metrics")
            elapsed = time.perf_counter() - start
            assert response.status_code == 200
            metrics.add(elapsed)

        assert metrics.mean < 0.2, f"Mean latency too high: {metrics.mean:.3f}s"


class TestDatabasePerformance:
    """Test database operation performance."""

    def test_single_record_insert_performance(self, db_session):
        """Test single record insert performance."""
        metrics = PerformanceMetrics()

        for i in range(100):
            record = UnifiedData(
                source_type=SourceType.CSV,
                source_id=f"perf-insert-{i}",
                raw_id=i,
                title=f"Performance Test {i}",
            )

            start = time.perf_counter()
            db_session.add(record)
            db_session.commit()
            elapsed = time.perf_counter() - start
            metrics.add(elapsed)

        assert metrics.mean < 0.05, f"Mean insert time too high: {metrics.mean:.3f}s"

    def test_bulk_insert_performance(self, db_session):
        """Test bulk insert performance."""
        batch_sizes = [10, 50, 100, 500]

        for batch_size in batch_sizes:
            records = [
                UnifiedData(
                    source_type=SourceType.API,
                    source_id=f"bulk-{batch_size}-{i}",
                    raw_id=i,
                    title=f"Bulk Insert {i}",
                )
                for i in range(batch_size)
            ]

            start = time.perf_counter()
            db_session.add_all(records)
            db_session.commit()
            elapsed = time.perf_counter() - start

            records_per_second = batch_size / elapsed
            assert (
                records_per_second > 50
            ), f"Bulk insert too slow for batch {batch_size}: {records_per_second:.1f} rec/s"

    def test_query_performance_with_index(self, db_session):
        """Test query performance (assumes indexes exist)."""
        # Create test data
        for i in range(500):
            record = UnifiedData(
                source_type=SourceType.CSV if i % 2 == 0 else SourceType.API,
                source_id=f"query-perf-{i}",
                raw_id=i,
                title=f"Query Performance Test {i}",
                category="CategoryA" if i % 3 == 0 else "CategoryB",
            )
            db_session.add(record)
        db_session.commit()

        # Test query by source_type (should use index)
        metrics = PerformanceMetrics()
        for _ in range(50):
            start = time.perf_counter()
            results = (
                db_session.query(UnifiedData)
                .filter(UnifiedData.source_type == SourceType.CSV)
                .limit(100)
                .all()
            )
            elapsed = time.perf_counter() - start
            metrics.add(elapsed)

        assert metrics.mean < 0.05, f"Indexed query too slow: {metrics.mean:.3f}s"

    def test_pagination_performance(self, db_session):
        """Test pagination performance across different pages."""
        # Create test data
        for i in range(1000):
            record = UnifiedData(
                source_type=SourceType.RSS,
                source_id=f"pagination-{i}",
                raw_id=i,
                title=f"Pagination Test {i}",
            )
            db_session.add(record)
        db_session.commit()

        page_sizes = [10, 50, 100]

        for page_size in page_sizes:
            metrics = PerformanceMetrics()

            for page in range(1, 11):  # Test first 10 pages
                offset = (page - 1) * page_size

                start = time.perf_counter()
                results = db_session.query(UnifiedData).offset(offset).limit(page_size).all()
                elapsed = time.perf_counter() - start
                metrics.add(elapsed)

            assert (
                metrics.mean < 0.1
            ), f"Pagination too slow for page_size={page_size}: {metrics.mean:.3f}s"


class TestExtractorPerformance:
    """Test extractor performance."""

    def test_csv_extraction_throughput(self, db_session, tmp_path):
        """Test CSV extraction throughput."""
        import csv

        from ingestion.csv_extractor import CSVExtractor

        # Create test CSV
        csv_file = tmp_path / "perf_test.csv"
        record_count = 1000

        with open(csv_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "title", "description", "category", "author", "date"])
            for i in range(record_count):
                writer.writerow([i, f"Title {i}", f"Desc {i}", "Cat", "Author", "2024-01-15"])

        extractor = CSVExtractor(db=db_session, csv_path=str(csv_file))

        start = time.perf_counter()
        records = list(extractor.extract())
        elapsed = time.perf_counter() - start

        throughput = len(records) / elapsed
        assert throughput > 500, f"Extraction throughput too low: {throughput:.1f} rec/s"

    def test_transformation_throughput(self, db_session):
        """Test transformation throughput."""
        from ingestion.csv_extractor import CSVExtractor

        extractor = CSVExtractor(db=db_session)

        raw_records = [
            {
                "id": i,
                "title": f"Transform Test {i}",
                "description": f"Description {i}",
                "category": "Test",
                "author": "Tester",
                "date": "2024-01-15",
                "_row_number": i,
                "_source_file": "test.csv",
            }
            for i in range(1000)
        ]

        start = time.perf_counter()
        transformed = [extractor.transform(r) for r in raw_records]
        elapsed = time.perf_counter() - start

        throughput = len(transformed) / elapsed
        assert throughput > 1000, f"Transformation throughput too low: {throughput:.1f} rec/s"


class TestRateLimiterPerformance:
    """Test rate limiter performance."""

    def test_rate_limiter_check_performance(self):
        """Test rate limiter check performance."""
        from services.rate_limiter import RateLimiter

        limiter = RateLimiter(requests_per_minute=1000)
        metrics = PerformanceMetrics()

        for i in range(1000):
            start = time.perf_counter()
            limiter.check_rate_limit(f"source-{i % 10}")
            elapsed = time.perf_counter() - start
            metrics.add(elapsed)

        # Should be very fast - microseconds
        assert metrics.mean < 0.001, f"Rate limit check too slow: {metrics.mean * 1000:.3f}ms"

    def test_rate_limiter_record_performance(self):
        """Test rate limiter record performance."""
        from services.rate_limiter import RateLimiter

        limiter = RateLimiter(requests_per_minute=10000)
        metrics = PerformanceMetrics()

        for i in range(1000):
            start = time.perf_counter()
            limiter.record_request(f"source-{i % 10}")
            elapsed = time.perf_counter() - start
            metrics.add(elapsed)

        assert metrics.mean < 0.001, f"Rate limit record too slow: {metrics.mean * 1000:.3f}ms"


class TestCheckpointPerformance:
    """Test checkpoint system performance."""

    def test_checkpoint_read_performance(self, db_session):
        """Test checkpoint read performance."""
        from core.models import SourceType
        from services.checkpoint import CheckpointManager

        manager = CheckpointManager(db=db_session)

        # Create initial checkpoint
        manager.update_checkpoint(SourceType.CSV, "initial", 1)

        metrics = PerformanceMetrics()

        for _ in range(100):
            start = time.perf_counter()
            checkpoint = manager.get_checkpoint(SourceType.CSV)
            elapsed = time.perf_counter() - start
            metrics.add(elapsed)

        assert metrics.mean < 0.02, f"Checkpoint read too slow: {metrics.mean:.3f}s"

    def test_checkpoint_write_performance(self, db_session):
        """Test checkpoint write performance."""
        from core.models import SourceType
        from services.checkpoint import CheckpointManager

        manager = CheckpointManager(db=db_session)
        metrics = PerformanceMetrics()

        for i in range(50):
            start = time.perf_counter()
            manager.update_checkpoint(SourceType.CSV, f"checkpoint-{i}", i)
            elapsed = time.perf_counter() - start
            metrics.add(elapsed)

        assert metrics.mean < 0.05, f"Checkpoint write too slow: {metrics.mean:.3f}s"


class TestSchemaDriftPerformance:
    """Test schema drift detection performance."""

    def test_drift_detection_performance(self, db_session):
        """Test schema drift detection performance."""
        from core.models import SourceType
        from services.schema_drift import SchemaDriftDetector

        detector = SchemaDriftDetector(db=db_session)

        # Simulate different data records with potential drift
        test_records = [
            {
                "id": "1",
                "title": "Test",
                "description": "Desc",
                "content": "Content",
                "author": "Author",
                "category": "Cat",
                "tags": ["a", "b"],
                "url": "http://test.com",
                "created_at": "2024-01-15",
                "updated_at": "2024-01-16",
            },
            {
                "id": "2",
                "title": "Test2",
                "description": "Desc2",
                "content": "Content2",
                "author": "Author2",
                "category": "Cat2",
                "tags": ["c"],
                "url": "http://test2.com",
                "created_at": "2024-01-15",
                "updated_at": "2024-01-16",
                "new_field": "value",
            },
            {"id": "3", "title": "Test3", "description": "Desc3"},  # Missing fields
        ]

        metrics = PerformanceMetrics()

        for i in range(100):
            for record in test_records:
                start = time.perf_counter()
                drifts = detector.detect_drift(SourceType.API, record)
                elapsed = time.perf_counter() - start
                metrics.add(elapsed)

        assert metrics.mean < 0.01, f"Drift detection too slow: {metrics.mean * 1000:.3f}ms"


class TestEndToEndPerformance:
    """Test end-to-end performance scenarios."""

    def test_full_request_cycle_performance(self, test_client, sample_unified_data):
        """Test full request cycle including all processing."""
        endpoints = [
            "/health",
            "/data",
            "/data?page=1&page_size=5",
            "/data?source_type=csv",
            "/stats",
            "/metrics",
        ]

        all_metrics = {}

        for endpoint in endpoints:
            metrics = PerformanceMetrics()

            for _ in range(20):
                start = time.perf_counter()
                response = test_client.get(endpoint)
                elapsed = time.perf_counter() - start
                assert response.status_code == 200
                metrics.add(elapsed)

            all_metrics[endpoint] = metrics.mean

        # All endpoints should respond quickly
        for endpoint, mean_time in all_metrics.items():
            assert mean_time < 0.5, f"{endpoint} too slow: {mean_time:.3f}s"

    def test_sustained_load_performance(self, test_client, sample_unified_data):
        """Test performance under sustained load."""
        duration_seconds = 5
        request_count = 0
        errors = 0

        start_time = time.time()
        while time.time() - start_time < duration_seconds:
            try:
                response = test_client.get("/data")
                if response.status_code == 200:
                    request_count += 1
                else:
                    errors += 1
            except Exception:
                errors += 1

        elapsed = time.time() - start_time
        requests_per_second = request_count / elapsed

        assert errors == 0, f"Errors during sustained load: {errors}"
        assert requests_per_second > 10, f"Throughput too low: {requests_per_second:.1f} req/s"
