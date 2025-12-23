"""Rate limiting with exponential backoff."""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Callable, Dict, Optional

from core.config import get_settings
from core.exceptions import RateLimitError

logger = logging.getLogger(__name__)
settings = get_settings()


@dataclass
class RateLimiterState:
    """State for rate limiter."""

    requests_made: int = 0
    window_start: float = field(default_factory=time.time)
    current_backoff: float = 0.0
    retry_count: int = 0
    last_request_time: float = 0.0


class RateLimiter:
    """Per-source rate limiter with exponential backoff."""

    def __init__(
        self,
        requests_per_minute: int = None,  # type: ignore[assignment]
        max_retries: int = None,  # type: ignore[assignment]
        backoff_base: float = None,  # type: ignore[assignment]
    ):
        self.requests_per_minute = requests_per_minute or settings.RATE_LIMIT_REQUESTS_PER_MINUTE
        self.max_retries = max_retries or settings.RATE_LIMIT_RETRY_MAX
        self.backoff_base = backoff_base or settings.RATE_LIMIT_BACKOFF_BASE
        self._states: Dict[str, RateLimiterState] = {}

    def _get_state(self, source_key: str) -> RateLimiterState:
        """Get or create state for a source."""
        if source_key not in self._states:
            self._states[source_key] = RateLimiterState()
        return self._states[source_key]

    def _reset_window_if_needed(self, state: RateLimiterState) -> None:
        """Reset the rate limit window if a minute has passed."""
        current_time = time.time()
        if current_time - state.window_start >= 60:
            state.requests_made = 0
            state.window_start = current_time
            state.retry_count = 0
            state.current_backoff = 0.0

    def check_rate_limit(self, source_key: str) -> float:
        """
        Check if rate limit allows a request.
        Returns wait time in seconds (0 if no wait needed).
        """
        state = self._get_state(source_key)
        self._reset_window_if_needed(state)

        if state.requests_made >= self.requests_per_minute:
            wait_time = 60 - (time.time() - state.window_start)
            return max(0, wait_time)

        return 0.0

    def record_request(self, source_key: str) -> None:
        """Record that a request was made."""
        state = self._get_state(source_key)
        state.requests_made += 1
        state.last_request_time = time.time()

        logger.debug(
            f"Rate limiter [{source_key}]: {state.requests_made}/{self.requests_per_minute} requests"
        )

    def record_success(self, source_key: str) -> None:
        """Record a successful request, reset backoff."""
        state = self._get_state(source_key)
        state.retry_count = 0
        state.current_backoff = 0.0

    def record_failure(self, source_key: str) -> float:
        """
        Record a failed request, calculate backoff.
        Returns the backoff time in seconds.
        """
        state = self._get_state(source_key)
        state.retry_count += 1

        if state.retry_count > self.max_retries:
            raise RateLimitError(
                f"Max retries ({self.max_retries}) exceeded for {source_key}", retry_after=None
            )

        # Exponential backoff: base^retry_count
        state.current_backoff = self.backoff_base**state.retry_count

        logger.warning(
            f"Rate limiter [{source_key}]: Retry {state.retry_count}/{self.max_retries}, "
            f"backoff {state.current_backoff:.2f}s"
        )

        return state.current_backoff

    def wait_if_needed(self, source_key: str) -> None:
        """Synchronously wait if rate limit requires."""
        wait_time = self.check_rate_limit(source_key)
        if wait_time > 0:
            logger.info(f"Rate limit reached for {source_key}, waiting {wait_time:.2f}s")
            time.sleep(wait_time)

    async def async_wait_if_needed(self, source_key: str) -> None:
        """Asynchronously wait if rate limit requires."""
        wait_time = self.check_rate_limit(source_key)
        if wait_time > 0:
            logger.info(f"Rate limit reached for {source_key}, waiting {wait_time:.2f}s")
            await asyncio.sleep(wait_time)

    def get_stats(self, source_key: str) -> Dict[str, Any]:
        """Get rate limiter statistics for a source."""
        state = self._get_state(source_key)
        return {
            "source_key": source_key,
            "requests_made": state.requests_made,
            "requests_limit": self.requests_per_minute,
            "retry_count": state.retry_count,
            "current_backoff": state.current_backoff,
            "window_remaining_seconds": max(0, 60 - (time.time() - state.window_start)),
        }


def with_rate_limit(source_key: str, rate_limiter: RateLimiter = None):  # type: ignore[assignment]
    """Decorator for rate-limited functions."""
    limiter = rate_limiter or RateLimiter()

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            limiter.wait_if_needed(source_key)
            limiter.record_request(source_key)

            try:
                result = func(*args, **kwargs)
                limiter.record_success(source_key)
                return result
            except Exception as e:
                backoff = limiter.record_failure(source_key)
                time.sleep(backoff)
                raise

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            await limiter.async_wait_if_needed(source_key)
            limiter.record_request(source_key)

            try:
                result = await func(*args, **kwargs)
                limiter.record_success(source_key)
                return result
            except Exception as e:
                backoff = limiter.record_failure(source_key)
                await asyncio.sleep(backoff)
                raise

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper

    return decorator


# Global rate limiter instance
global_rate_limiter = RateLimiter()
