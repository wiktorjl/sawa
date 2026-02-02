"""In-memory LRU cache for repository data.

This module provides a thread-safe LRU cache with TTL (time-to-live)
support. It is used by API repositories to cache responses and reduce
API calls.

Usage:
    cache = InMemoryCache(max_size=1000, default_ttl_seconds=300)

    # Store a value
    cache.set("prices:AAPL:2024-01-01:2024-01-31", prices)

    # Retrieve a value (returns None if expired or not found)
    cached = cache.get("prices:AAPL:2024-01-01:2024-01-31")

    # Invalidate by pattern
    cache.invalidate("prices:AAPL")  # Removes all AAPL price entries
"""

import time
from collections import OrderedDict
from dataclasses import dataclass
from threading import Lock
from typing import Any, Generic, TypeVar

T = TypeVar("T")


@dataclass
class CacheEntry(Generic[T]):
    """Cache entry with TTL.

    Attributes:
        value: The cached value
        expires_at: Unix timestamp when entry expires
    """

    value: T
    expires_at: float

    @property
    def is_expired(self) -> bool:
        """Check if entry has expired."""
        return time.time() > self.expires_at


class InMemoryCache:
    """Thread-safe LRU cache with TTL.

    This cache uses an OrderedDict to maintain LRU order. When the cache
    reaches capacity, the least recently used entries are evicted.

    Thread Safety:
        All operations are protected by a lock, making this cache safe
        for use in multi-threaded environments.

    Attributes:
        max_size: Maximum number of entries
        default_ttl: Default time-to-live in seconds
    """

    def __init__(
        self,
        max_size: int = 1000,
        default_ttl_seconds: float = 300,
    ) -> None:
        """Initialize cache.

        Args:
            max_size: Maximum number of entries
            default_ttl_seconds: Default time-to-live in seconds (5 minutes)
        """
        self.max_size = max_size
        self.default_ttl = default_ttl_seconds
        self._cache: OrderedDict[str, CacheEntry[Any]] = OrderedDict()
        self._lock = Lock()

    def get(self, key: str) -> Any | None:
        """Get value from cache if present and not expired.

        Args:
            key: Cache key

        Returns:
            Cached value, or None if not found or expired
        """
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return None

            if entry.is_expired:
                del self._cache[key]
                return None

            # Move to end (most recently used)
            self._cache.move_to_end(key)
            return entry.value

    def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: float | None = None,
    ) -> None:
        """Set value in cache.

        Args:
            key: Cache key
            value: Value to cache
            ttl_seconds: Optional TTL override (uses default if not provided)
        """
        ttl = ttl_seconds if ttl_seconds is not None else self.default_ttl

        with self._lock:
            # Remove if exists (will be re-added at end)
            if key in self._cache:
                del self._cache[key]

            # Evict oldest if at capacity
            while len(self._cache) >= self.max_size:
                self._cache.popitem(last=False)

            expires_at = time.time() + ttl
            self._cache[key] = CacheEntry(value, expires_at)

    def clear(self) -> None:
        """Clear all entries."""
        with self._lock:
            self._cache.clear()

    def invalidate(self, pattern: str) -> int:
        """Invalidate keys matching pattern (simple substring match).

        Args:
            pattern: Substring to match in keys

        Returns:
            Number of entries removed
        """
        with self._lock:
            keys_to_remove = [k for k in self._cache if pattern in k]
            for key in keys_to_remove:
                del self._cache[key]
            return len(keys_to_remove)

    def __len__(self) -> int:
        """Return number of entries (including expired)."""
        with self._lock:
            return len(self._cache)

    def stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dictionary with size, max_size, and default_ttl
        """
        with self._lock:
            return {
                "size": len(self._cache),
                "max_size": self.max_size,
                "default_ttl": self.default_ttl,
            }


class NullCache:
    """No-op cache implementation.

    Use this when caching is disabled. All operations are no-ops
    and get() always returns None.

    Usage:
        cache = NullCache() if not config.cache_enabled else InMemoryCache()
    """

    def get(self, key: str) -> None:
        """Always returns None."""
        return None

    def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: float | None = None,
    ) -> None:
        """No-op."""
        pass

    def clear(self) -> None:
        """No-op."""
        pass

    def invalidate(self, pattern: str) -> int:
        """Always returns 0."""
        return 0

    def __len__(self) -> int:
        """Always returns 0."""
        return 0

    def stats(self) -> dict[str, Any]:
        """Return empty stats."""
        return {"size": 0, "max_size": 0, "default_ttl": 0, "enabled": False}
