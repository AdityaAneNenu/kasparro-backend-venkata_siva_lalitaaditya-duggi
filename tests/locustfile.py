"""Locust load testing file for Kaspero API.

Run with:
    locust -f tests/locustfile.py --host http://localhost:8000

Or headless:
    locust -f tests/locustfile.py --headless --users 100 --spawn-rate 10 --run-time 60s --host http://localhost:8000
"""

import json
import random

from locust import HttpUser, between, tag, task


class KasperoAPIUser(HttpUser):
    """Simulates a typical API user."""

    wait_time = between(0.5, 2.0)  # Wait 0.5-2 seconds between requests

    def on_start(self):
        """Called when a simulated user starts."""
        # Warm up - check health
        self.client.get("/health")

    @task(10)
    @tag("health")
    def health_check(self):
        """Health check endpoint - high frequency."""
        with self.client.get("/health", catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "healthy":
                    response.success()
                else:
                    response.failure(f"Unhealthy status: {data.get('status')}")
            else:
                response.failure(f"Status code: {response.status_code}")

    @task(5)
    @tag("data")
    def get_data(self):
        """Get data endpoint - medium frequency."""
        with self.client.get("/data", catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                if "data" in data and "pagination" in data:
                    response.success()
                else:
                    response.failure("Missing expected fields in response")
            else:
                response.failure(f"Status code: {response.status_code}")

    @task(3)
    @tag("data", "pagination")
    def get_data_paginated(self):
        """Get data with pagination."""
        page = random.randint(1, 5)
        page_size = random.choice([5, 10, 20, 50])

        with self.client.get(
            f"/data?page={page}&page_size={page_size}",
            name="/data?page=X&page_size=Y",
            catch_response=True,
        ) as response:
            if response.status_code == 200:
                data = response.json()
                if data["pagination"]["page"] == page:
                    response.success()
                else:
                    response.failure("Pagination mismatch")
            else:
                response.failure(f"Status code: {response.status_code}")

    @task(2)
    @tag("data", "filter")
    def get_data_filtered(self):
        """Get data with filters."""
        source_types = ["csv", "api", "rss"]
        source_type = random.choice(source_types)

        with self.client.get(
            f"/data?source_type={source_type}", name="/data?source_type=X", catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            elif response.status_code == 422:
                response.success()  # Validation error is expected for invalid types
            else:
                response.failure(f"Status code: {response.status_code}")

    @task(2)
    @tag("data", "search")
    def get_data_search(self):
        """Get data with search."""
        search_terms = ["test", "article", "data", "crypto", "bitcoin"]
        search = random.choice(search_terms)

        with self.client.get(
            f"/data?search={search}", name="/data?search=X", catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")

    @task(3)
    @tag("stats")
    def get_stats(self):
        """Get statistics endpoint."""
        with self.client.get("/stats", catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")

    @task(2)
    @tag("stats")
    def get_stats_with_hours(self):
        """Get statistics with hours parameter."""
        hours = random.choice([1, 6, 12, 24, 48, 168])

        with self.client.get(
            f"/stats?hours={hours}", name="/stats?hours=X", catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")

    @task(2)
    @tag("runs")
    def get_runs(self):
        """Get ETL runs endpoint."""
        with self.client.get("/runs", catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")

    @task(3)
    @tag("metrics")
    def get_metrics(self):
        """Get metrics endpoint (Prometheus format)."""
        with self.client.get("/metrics", catch_response=True) as response:
            if response.status_code == 200:
                if "kaspero_" in response.text or response.text:
                    response.success()
                else:
                    response.failure("Empty metrics response")
            else:
                response.failure(f"Status code: {response.status_code}")

    @task(5)
    @tag("kubernetes", "health")
    def readiness_check(self):
        """Kubernetes readiness probe."""
        with self.client.get("/ready", catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")

    @task(5)
    @tag("kubernetes", "health")
    def liveness_check(self):
        """Kubernetes liveness probe."""
        with self.client.get("/live", catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")


class HighLoadUser(HttpUser):
    """Simulates high load user with minimal wait."""

    wait_time = between(0.1, 0.5)
    weight = 1  # Lower weight - fewer of these users

    @task
    def rapid_health_check(self):
        """Rapid health checks for stress testing."""
        self.client.get("/health")

    @task
    def rapid_data_check(self):
        """Rapid data fetches."""
        self.client.get("/data?page_size=5")


class DataHeavyUser(HttpUser):
    """Simulates user making data-heavy requests."""

    wait_time = between(1, 3)
    weight = 1

    @task(5)
    def large_page_request(self):
        """Request large pages."""
        self.client.get("/data?page_size=100")

    @task(3)
    def complex_query(self):
        """Complex query with multiple filters."""
        self.client.get("/data?source_type=csv&category=Test&search=article&page_size=50")

    @task(2)
    def sequential_pages(self):
        """Request sequential pages."""
        for page in range(1, 6):
            self.client.get(f"/data?page={page}&page_size=20")


class MonitoringUser(HttpUser):
    """Simulates monitoring system checking endpoints."""

    wait_time = between(5, 10)  # Less frequent
    weight = 1

    @task
    def full_health_check(self):
        """Complete health check cycle."""
        self.client.get("/health")
        self.client.get("/ready")
        self.client.get("/live")
        self.client.get("/metrics")
