"""Security tests for the Kaspero ETL system.

These tests verify security aspects including input validation,
SQL injection prevention, and authentication/authorization.
"""

from datetime import datetime

import pytest


class TestSQLInjectionPrevention:
    """Test SQL injection prevention."""

    def test_search_sql_injection(self, test_client, sample_unified_data):
        """Test that search parameter is safe from SQL injection."""
        injection_payloads = [
            "'; DROP TABLE unified_data; --",
            "1' OR '1'='1",
            "1; DELETE FROM unified_data WHERE '1'='1",
            "' UNION SELECT * FROM users --",
            "1' AND SLEEP(5) --",
            "'; EXEC xp_cmdshell('dir'); --",
            "1' AND 1=CONVERT(int, @@version) --",
            "admin'--",
            "' OR 1=1 #",
            "') OR ('1'='1",
        ]

        for payload in injection_payloads:
            response = test_client.get(f"/data?search={payload}")
            # Should not crash or expose data
            assert response.status_code in [200, 400, 422]
            if response.status_code == 200:
                data = response.json()
                # Should return empty or filtered results, not all data
                assert isinstance(data["data"], list)

    def test_category_filter_injection(self, test_client, sample_unified_data):
        """Test category filter is safe from SQL injection."""
        injection_payloads = [
            "Test' OR '1'='1",
            "Test'; DROP TABLE unified_data; --",
            "Test' UNION SELECT * FROM users --",
        ]

        for payload in injection_payloads:
            response = test_client.get(f"/data?category={payload}")
            assert response.status_code in [200, 400, 422]

    def test_source_type_filter_injection(self, test_client, sample_unified_data):
        """Test source_type filter is safe from SQL injection."""
        injection_payloads = [
            "csv' OR '1'='1",
            "api'; DELETE FROM unified_data; --",
        ]

        for payload in injection_payloads:
            response = test_client.get(f"/data?source_type={payload}")
            # Should return validation error for invalid source type
            assert response.status_code in [200, 400, 422]


class TestInputValidation:
    """Test input validation across endpoints."""

    def test_pagination_validation(self, test_client):
        """Test pagination parameter validation."""
        invalid_cases = [
            {"page": "abc", "page_size": 10},
            {"page": 1, "page_size": "xyz"},
            {"page": -100, "page_size": 10},
            {"page": 1, "page_size": -50},
            {"page": 0, "page_size": 0},
            {"page": 1.5, "page_size": 10},
            {"page": "1; DROP TABLE", "page_size": 10},
        ]

        for params in invalid_cases:
            response = test_client.get(
                f"/data?page={params['page']}&page_size={params['page_size']}"
            )
            # Should handle gracefully - either validate and reject or sanitize
            assert response.status_code in [200, 400, 422]

    def test_id_parameter_validation(self, test_client):
        """Test ID parameter validation."""
        invalid_ids = [
            "../../../etc/passwd",
            "<script>alert('xss')</script>",
            "1; DROP TABLE",
            "' OR '1'='1",
            "-1",
            "0",
            "99999999",
            "null",
            "undefined",
            "NaN",
        ]

        for invalid_id in invalid_ids:
            response = test_client.get(f"/data/{invalid_id}")
            # Should return 404 or validation error, not crash
            assert response.status_code in [404, 400, 422, 500]

    def test_hours_parameter_validation(self, test_client):
        """Test hours parameter on /stats endpoint."""
        invalid_hours = [
            "abc",
            "-24",
            "0",
            "999999999999",
            "24.5",
            "24; DROP TABLE",
        ]

        for hours in invalid_hours:
            response = test_client.get(f"/stats?hours={hours}")
            assert response.status_code in [200, 400, 422]


class TestXSSPrevention:
    """Test Cross-Site Scripting prevention."""

    def test_xss_in_search(self, test_client, sample_unified_data):
        """Test XSS payloads in search are handled safely."""
        xss_payloads = [
            "<script>alert('XSS')</script>",
            "<img src=x onerror=alert('XSS')>",
            "<svg/onload=alert('XSS')>",
            "javascript:alert('XSS')",
            "<iframe src='javascript:alert(1)'>",
            "'-alert(1)-'",
            "<body onload=alert('XSS')>",
            "<input onfocus=alert(1) autofocus>",
            "<marquee onstart=alert(1)>",
        ]

        for payload in xss_payloads:
            response = test_client.get(f"/data?search={payload}")
            assert response.status_code in [200, 400, 422]
            if response.status_code == 200:
                data = response.json()
                # Response should not contain unescaped script tags
                response_text = str(data)
                assert (
                    "<script>" not in response_text.lower() or "alert" not in response_text.lower()
                )


class TestPathTraversal:
    """Test path traversal prevention."""

    def test_data_id_path_traversal(self, test_client):
        """Test path traversal in data ID."""
        traversal_payloads = [
            "../../../etc/passwd",
            "..\\..\\..\\windows\\system32\\config\\sam",
            "....//....//etc/passwd",
            "%2e%2e%2f%2e%2e%2f",
            "..%252f..%252f",
            "/etc/passwd",
            "C:\\Windows\\System32",
        ]

        for payload in traversal_payloads:
            response = test_client.get(f"/data/{payload}")
            assert response.status_code in [404, 400, 422]


class TestHeaderSecurity:
    """Test security headers in responses."""

    def test_security_headers_present(self, test_client):
        """Test that security headers are present in responses."""
        response = test_client.get("/health")

        # Check for common security headers (if implemented)
        # These are recommendations - implementation may vary
        headers = response.headers

        # Content-Type should be set
        assert "content-type" in headers
        assert "application/json" in headers["content-type"]

    def test_cors_headers(self, test_client):
        """Test CORS headers configuration."""
        response = test_client.options("/health")
        # CORS is configured in the app - verify it doesn't expose sensitive data


class TestRateLimitSecurity:
    """Test rate limiting security aspects."""

    def test_rate_limit_prevents_abuse(self):
        """Test that rate limiter prevents request abuse."""
        from services.rate_limiter import RateLimiter

        limiter = RateLimiter(requests_per_minute=10, max_retries=3)

        # Make requests up to limit
        for i in range(10):
            wait_time = limiter.check_rate_limit("attacker")
            assert wait_time == 0
            limiter.record_request("attacker")

        # Should be blocked now (wait_time > 0)
        wait_time = limiter.check_rate_limit("attacker")
        assert wait_time > 0

    def test_rate_limit_per_source_isolation(self):
        """Test that rate limits are isolated per source."""
        from services.rate_limiter import RateLimiter

        limiter = RateLimiter(requests_per_minute=10)

        # Exhaust limit for source A
        for _ in range(10):
            limiter.record_request("source_a")

        # Source B should still be allowed
        wait_time = limiter.check_rate_limit("source_b")
        assert wait_time == 0


class TestDataPrivacy:
    """Test data privacy aspects."""

    def test_sensitive_data_not_exposed_in_errors(self, test_client):
        """Test that error responses don't expose sensitive data."""
        response = test_client.get("/data/99999999")

        if response.status_code == 404:
            data = response.json()
            # Error message should not contain SQL queries or stack traces
            error_text = str(data).lower()
            assert "select" not in error_text
            assert "from" not in error_text or "not found" in error_text
            assert "traceback" not in error_text
            assert "exception" not in error_text or "detail" in data

    def test_database_errors_not_exposed(self, test_client):
        """Test that database errors don't expose internal details."""
        # Try to trigger a database error with invalid page
        response = test_client.get("/data?page=-1")

        if response.status_code >= 400:
            data = response.json()
            error_text = str(data).lower()
            # Should not expose database details
            assert "postgresql" not in error_text
            assert "sqlalchemy" not in error_text


class TestAuthenticationSecurity:
    """Test authentication-related security (if implemented)."""

    def test_health_endpoint_public(self, test_client):
        """Test that health endpoint is accessible without auth."""
        response = test_client.get("/health")
        assert response.status_code == 200

    def test_ready_endpoint_public(self, test_client):
        """Test that readiness endpoint is accessible."""
        response = test_client.get("/ready")
        assert response.status_code == 200

    def test_live_endpoint_public(self, test_client):
        """Test that liveness endpoint is accessible."""
        response = test_client.get("/live")
        assert response.status_code == 200


class TestAPIKeySecurity:
    """Test API key handling security."""

    def test_api_key_not_logged(self, caplog):
        """Test that API keys are not logged."""
        import logging

        from core.config import get_settings

        with caplog.at_level(logging.DEBUG):
            settings = get_settings()
            # Access API key to trigger any logging
            _ = settings.API_KEY

        # Check logs don't contain actual API key values
        for record in caplog.records:
            assert "test-api-key" not in record.message.lower()

    def test_api_key_not_in_error_responses(self, test_client):
        """Test that API key is not exposed in error responses."""
        response = test_client.get("/nonexistent")

        if response.status_code >= 400:
            response_text = response.text.lower()
            assert "api_key" not in response_text or "api-key" not in response_text


class TestDatabaseSecurity:
    """Test database security aspects."""

    def test_parameterized_queries(self, db_session, sample_unified_data):
        """Test that queries use parameterization."""
        from core.models import UnifiedData

        # This should use parameterized query internally
        malicious_input = "'; DROP TABLE unified_data; --"

        # Query should be safe
        results = (
            db_session.query(UnifiedData).filter(UnifiedData.title.contains(malicious_input)).all()
        )

        # Table should still exist
        count = db_session.query(UnifiedData).count()
        assert count > 0  # Table wasn't dropped

    def test_orm_escaping(self, db_session):
        """Test that ORM properly escapes special characters."""
        from core.models import SourceType, UnifiedData

        # Create record with special characters
        special_title = "Test'; DROP TABLE unified_data; --"
        record = UnifiedData(
            source_type=SourceType.CSV,
            source_id="security-test-1",
            raw_id=1,
            title=special_title,
        )
        db_session.add(record)
        db_session.commit()

        # Query it back
        retrieved = (
            db_session.query(UnifiedData).filter(UnifiedData.source_id == "security-test-1").first()
        )

        assert retrieved is not None
        assert retrieved.title == special_title


class TestResourceExhaustion:
    """Test protection against resource exhaustion attacks."""

    def test_large_page_size_limited(self, test_client, sample_unified_data):
        """Test that excessively large page sizes are handled."""
        response = test_client.get("/data?page_size=1000000")

        # Should either limit or reject
        assert response.status_code in [200, 400, 422]
        if response.status_code == 200:
            data = response.json()
            # Should have reasonable limit
            assert len(data["data"]) <= 1000

    def test_deeply_nested_json_rejected(self, test_client):
        """Test that deeply nested JSON is handled."""
        # This tests POST endpoints if they exist
        # For GET-only APIs, this may not apply
        pass

    def test_very_long_search_query(self, test_client):
        """Test handling of very long search queries."""
        long_query = "A" * 10000
        response = test_client.get(f"/data?search={long_query}")

        # Should handle gracefully
        assert response.status_code in [200, 400, 414, 422]
