"""Tests for API endpoints."""

from datetime import datetime

import pytest


class TestHealthEndpoint:
    """Test /health endpoint."""

    def test_health_check_returns_status(self, test_client, db_session):
        """Test that health check returns proper status."""
        response = test_client.get("/health")

        assert response.status_code == 200
        data = response.json()

        assert "status" in data
        assert "database" in data
        assert "version" in data
        assert data["database"] == True

    def test_health_check_includes_etl_status(self, test_client, db_session, sample_etl_runs):
        """Test that health check includes ETL last run info."""
        response = test_client.get("/health")

        assert response.status_code == 200
        data = response.json()

        assert "etl_last_run" in data
        assert "etl_last_status" in data


class TestDataEndpoint:
    """Test /data endpoint."""

    def test_get_data_empty(self, test_client, db_session):
        """Test /data with no records."""
        response = test_client.get("/data")

        assert response.status_code == 200
        data = response.json()

        assert "data" in data
        assert "pagination" in data
        assert "meta" in data
        assert len(data["data"]) == 0

    def test_get_data_with_records(self, test_client, sample_unified_data):
        """Test /data returns records."""
        response = test_client.get("/data")

        assert response.status_code == 200
        data = response.json()

        assert len(data["data"]) > 0
        assert data["pagination"]["total_items"] == 10

    def test_get_data_pagination(self, test_client, sample_unified_data):
        """Test /data pagination."""
        response = test_client.get("/data?page=1&page_size=5")

        assert response.status_code == 200
        data = response.json()

        assert len(data["data"]) == 5
        assert data["pagination"]["page"] == 1
        assert data["pagination"]["page_size"] == 5
        assert data["pagination"]["total_pages"] == 2

    def test_get_data_filter_by_source_type(self, test_client, sample_unified_data):
        """Test /data filtering by source type."""
        response = test_client.get("/data?source_type=csv")

        assert response.status_code == 200
        data = response.json()

        for item in data["data"]:
            assert item["source_type"] == "csv"

    def test_get_data_filter_by_category(self, test_client, sample_unified_data):
        """Test /data filtering by category."""
        response = test_client.get("/data?category=Test")

        assert response.status_code == 200
        data = response.json()

        for item in data["data"]:
            assert "Test" in item["category"]

    def test_get_data_search(self, test_client, sample_unified_data):
        """Test /data search functionality."""
        response = test_client.get("/data?search=Article 5")

        assert response.status_code == 200
        data = response.json()

        # Should find "Test Article 5"
        assert any("5" in item["title"] for item in data["data"])

    def test_get_data_includes_metadata(self, test_client, sample_unified_data):
        """Test /data returns request metadata."""
        response = test_client.get("/data")

        assert response.status_code == 200
        data = response.json()

        assert "meta" in data
        assert "request_id" in data["meta"]
        assert "api_latency_ms" in data["meta"]
        assert "timestamp" in data["meta"]

    def test_get_data_invalid_source_type(self, test_client, db_session):
        """Test /data with invalid source type."""
        response = test_client.get("/data?source_type=invalid")

        assert response.status_code == 400
        assert "Invalid source_type" in response.json()["detail"]

    def test_get_data_by_id(self, test_client, sample_unified_data):
        """Test getting single record by ID."""
        # First get list to find an ID
        list_response = test_client.get("/data")
        first_id = list_response.json()["data"][0]["id"]

        response = test_client.get(f"/data/{first_id}")

        assert response.status_code == 200
        assert response.json()["id"] == first_id

    def test_get_data_not_found(self, test_client, db_session):
        """Test 404 for non-existent record."""
        response = test_client.get("/data/99999")

        assert response.status_code == 404


class TestStatsEndpoint:
    """Test /stats endpoint."""

    def test_get_stats(self, test_client, sample_unified_data, sample_etl_runs):
        """Test /stats returns statistics."""
        response = test_client.get("/stats")

        assert response.status_code == 200
        data = response.json()

        assert "total_records_processed" in data
        assert "records_by_source" in data
        assert "success_rate" in data
        assert "average_duration_seconds" in data
        assert "meta" in data

    def test_get_stats_with_hours_param(self, test_client, sample_etl_runs):
        """Test /stats with custom hours parameter."""
        response = test_client.get("/stats?hours=48")

        assert response.status_code == 200


class TestRunsEndpoint:
    """Test /runs endpoint."""

    def test_get_runs(self, test_client, sample_etl_runs):
        """Test /runs returns run history."""
        response = test_client.get("/runs")

        assert response.status_code == 200
        data = response.json()

        assert len(data) > 0
        assert "run_id" in data[0]
        assert "status" in data[0]

    def test_get_runs_filter_by_status(self, test_client, sample_etl_runs):
        """Test /runs filtering by status."""
        response = test_client.get("/runs?status=success")

        assert response.status_code == 200
        data = response.json()

        for run in data:
            assert run["status"] == "success"

    def test_get_run_by_id(self, test_client, sample_etl_runs):
        """Test getting single run by ID."""
        # Get list first
        list_response = test_client.get("/runs")
        run_id = list_response.json()[0]["run_id"]

        response = test_client.get(f"/runs/{run_id}")

        assert response.status_code == 200
        assert response.json()["run_id"] == run_id


class TestMetricsEndpoint:
    """Test /metrics endpoint."""

    def test_get_metrics_prometheus_format(self, test_client, sample_unified_data, sample_etl_runs):
        """Test /metrics returns Prometheus format."""
        response = test_client.get("/metrics")

        assert response.status_code == 200
        content = response.text

        # Check for Prometheus format markers
        assert "# HELP" in content or "# TYPE" in content
        assert "kaspero_" in content

    def test_get_metrics_contains_record_counts(self, test_client, sample_unified_data):
        """Test /metrics includes record counts."""
        response = test_client.get("/metrics")

        assert response.status_code == 200
        assert "kaspero_records_total" in response.text


class TestReadinessLiveness:
    """Test Kubernetes probe endpoints."""

    def test_ready_endpoint(self, test_client, db_session):
        """Test /ready endpoint."""
        response = test_client.get("/ready")

        assert response.status_code == 200
        assert response.json()["status"] == "ready"

    def test_live_endpoint(self, test_client):
        """Test /live endpoint."""
        response = test_client.get("/live")

        assert response.status_code == 200
        assert response.json()["status"] == "alive"
