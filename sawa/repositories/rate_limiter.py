"""Rate limiting for API calls.

This module provides rate limiters for controlling the rate of API calls
to external providers. The primary implementation is a token bucket
algorithm.

Usage:
    limiter = TokenBucket(rate=5.0, capacity=10.0)  # 5 req/sec, burst of 10

    async def make_request():
        await limiter.acquire()  # Wait for permission
        # Make the API call
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Protocol


class RateLimiter(Protocol):
    """Protocol for rate limiters.

    Any rate limiter implementation must provide an async acquire() method
    that blocks until a request is allowed.
    """

    async def acquire(self) -> None:
        """Acquire permission to make a request.

        This method blocks until the rate limiter allows the request.
        """
        ...


@dataclass
class TokenBucket:
    """Token bucket rate limiter.

    The token bucket algorithm adds tokens at a fixed rate up to a maximum
    capacity. Each request consumes one token. If no tokens are available,
    the request waits until a token becomes available.

    This allows for bursting (up to capacity) while maintaining a
    long-term average rate.

    Attributes:
        rate: Tokens added per second (requests per second)
        capacity: Maximum tokens (burst capacity)

    Example:
        # 5 requests per second with burst of 10
        limiter = TokenBucket(rate=5.0, capacity=10.0)

        # In an async context:
        await limiter.acquire()
        response = await make_api_call()
    """

    rate: float  # tokens per second
    capacity: float = 10.0  # max burst

    # Private state (initialized in __post_init__)
    _tokens: float = field(init=False, repr=False)
    _last_update: float = field(init=False, repr=False)
    _lock: asyncio.Lock = field(init=False, repr=False)

    def __post_init__(self) -> None:
        """Initialize internal state."""
        self._tokens = self.capacity
        self._last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until a token is available.

        This method is safe to call concurrently from multiple coroutines.
        """
        async with self._lock:
            while True:
                # Refill tokens based on time elapsed
                now = time.monotonic()
                elapsed = now - self._last_update
                self._tokens = min(
                    self.capacity,
                    self._tokens + elapsed * self.rate,
                )
                self._last_update = now

                if self._tokens >= 1:
                    # Have a token, consume it
                    self._tokens -= 1
                    return

                # Need to wait for a token
                wait_time = (1 - self._tokens) / self.rate
                await asyncio.sleep(wait_time)

    @property
    def available_tokens(self) -> float:
        """Get approximate number of available tokens (not thread-safe)."""
        now = time.monotonic()
        elapsed = now - self._last_update
        return min(self.capacity, self._tokens + elapsed * self.rate)


class NoOpRateLimiter:
    """Rate limiter that doesn't limit.

    Use this when rate limiting is not needed (e.g., database access).
    """

    async def acquire(self) -> None:
        """Immediately returns without waiting."""
        pass


class FixedDelayRateLimiter:
    """Simple rate limiter with fixed delay between requests.

    This is simpler than TokenBucket but doesn't allow bursting.

    Attributes:
        delay_seconds: Minimum delay between requests
    """

    def __init__(self, delay_seconds: float = 0.2) -> None:
        """Initialize with delay.

        Args:
            delay_seconds: Seconds to wait between requests
        """
        self.delay_seconds = delay_seconds
        self._last_request: float = 0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until enough time has passed since last request."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request
            if elapsed < self.delay_seconds:
                await asyncio.sleep(self.delay_seconds - elapsed)
            self._last_request = time.monotonic()


class SyncRateLimiter:
    """Synchronous rate limiter for non-async code.

    Uses a simple delay between requests without async.

    Attributes:
        requests_per_second: Maximum requests per second
    """

    def __init__(self, requests_per_second: float = 5.0) -> None:
        """Initialize with rate limit.

        Args:
            requests_per_second: Maximum requests per second
        """
        self.min_interval = 1.0 / requests_per_second
        self._last_request: float = 0

    def acquire(self) -> None:
        """Wait until enough time has passed since last request."""
        now = time.monotonic()
        elapsed = now - self._last_request
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last_request = time.monotonic()
