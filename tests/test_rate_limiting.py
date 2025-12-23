"""Tests for rate limiting."""

import time
from unittest.mock import patch

import pytest

from core.exceptions import RateLimitError
from services.rate_limiter import RateLimiter, with_rate_limit


class TestRateLimiter:
    """Test rate limiter functionality."""

    def test_basic_rate_limiting(self):
        """Test basic rate limit enforcement."""
        limiter = RateLimiter(requests_per_minute=5)

        # Make 5 requests (should all be allowed)
        for _ in range(5):
            wait = limiter.check_rate_limit("test")
            assert wait == 0
            limiter.record_request("test")

        # 6th request should require waiting
        wait = limiter.check_rate_limit("test")
        assert wait > 0

    def test_window_reset(self):
        """Test that rate limit window resets after 60 seconds."""
        limiter = RateLimiter(requests_per_minute=2)

        # Use up the limit
        limiter.record_request("test")
        limiter.record_request("test")

        # Mock time passing
        state = limiter._get_state("test")
        state.window_start = time.time() - 61  # 61 seconds ago

        # Should be allowed now
        wait = limiter.check_rate_limit("test")
        assert wait == 0

    def test_separate_source_limits(self):
        """Test that different sources have separate limits."""
        limiter = RateLimiter(requests_per_minute=2)

        # Use up limit for source1
        limiter.record_request("source1")
        limiter.record_request("source1")

        # source2 should still be available
        wait = limiter.check_rate_limit("source2")
        assert wait == 0

    def test_retry_count_tracking(self):
        """Test retry count tracking."""
        limiter = RateLimiter(max_retries=3)

        limiter.record_failure("test")
        limiter.record_failure("test")

        state = limiter._get_state("test")
        assert state.retry_count == 2

    def test_success_resets_retry_count(self):
        """Test that success resets retry count."""
        limiter = RateLimiter()

        limiter.record_failure("test")
        limiter.record_failure("test")
        limiter.record_success("test")

        state = limiter._get_state("test")
        assert state.retry_count == 0
        assert state.current_backoff == 0

    def test_get_stats(self):
        """Test getting rate limiter statistics."""
        limiter = RateLimiter(requests_per_minute=10)

        limiter.record_request("test")
        limiter.record_request("test")
        limiter.record_request("test")

        stats = limiter.get_stats("test")

        assert stats["source_key"] == "test"
        assert stats["requests_made"] == 3
        assert stats["requests_limit"] == 10


class TestExponentialBackoff:
    """Test exponential backoff behavior."""

    def test_backoff_increases_exponentially(self):
        """Test that backoff increases exponentially."""
        limiter = RateLimiter(backoff_base=2.0, max_retries=5)

        backoffs = []
        for _ in range(4):
            backoffs.append(limiter.record_failure("test"))

        assert backoffs[0] == 2.0  # 2^1
        assert backoffs[1] == 4.0  # 2^2
        assert backoffs[2] == 8.0  # 2^3
        assert backoffs[3] == 16.0  # 2^4

    def test_custom_backoff_base(self):
        """Test custom backoff base."""
        limiter = RateLimiter(backoff_base=3.0, max_retries=3)

        backoff = limiter.record_failure("test")
        assert backoff == 3.0  # 3^1

        backoff = limiter.record_failure("test")
        assert backoff == 9.0  # 3^2


class TestMaxRetriesExceeded:
    """Test max retries exceeded behavior."""

    def test_raises_after_max_retries(self):
        """Test that RateLimitError is raised after max retries."""
        limiter = RateLimiter(max_retries=2)

        limiter.record_failure("test")
        limiter.record_failure("test")

        with pytest.raises(RateLimitError) as exc_info:
            limiter.record_failure("test")

        assert "Max retries" in str(exc_info.value)

    def test_rate_limit_error_contains_info(self):
        """Test that RateLimitError contains useful info."""
        limiter = RateLimiter(max_retries=1)

        limiter.record_failure("test_source")

        with pytest.raises(RateLimitError) as exc_info:
            limiter.record_failure("test_source")

        assert "test_source" in str(exc_info.value)


class TestRateLimitDecorator:
    """Test rate limit decorator."""

    def test_decorator_applies_rate_limit(self):
        """Test that decorator applies rate limiting."""
        limiter = RateLimiter(requests_per_minute=100)
        call_count = 0

        @with_rate_limit("test", limiter)
        def my_function():
            nonlocal call_count
            call_count += 1
            return "success"

        result = my_function()

        assert result == "success"
        assert call_count == 1

        stats = limiter.get_stats("test")
        assert stats["requests_made"] == 1

    def test_decorator_records_success(self):
        """Test that decorator records success."""
        limiter = RateLimiter()

        # First cause a failure
        limiter.record_failure("test")

        @with_rate_limit("test", limiter)
        def successful_function():
            return "ok"

        successful_function()

        state = limiter._get_state("test")
        assert state.retry_count == 0  # Reset after success
