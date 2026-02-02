"""Tests for repository cache."""

import time

from sp500_tools.repositories.cache import InMemoryCache, NullCache


class TestInMemoryCache:
    """Tests for InMemoryCache."""

    def test_set_and_get(self) -> None:
        """Test basic set and get operations."""
        cache = InMemoryCache()
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"

    def test_get_missing_key(self) -> None:
        """Test getting a non-existent key."""
        cache = InMemoryCache()
        assert cache.get("nonexistent") is None

    def test_overwrite_key(self) -> None:
        """Test overwriting an existing key."""
        cache = InMemoryCache()
        cache.set("key1", "value1")
        cache.set("key1", "value2")
        assert cache.get("key1") == "value2"

    def test_ttl_expiration(self) -> None:
        """Test that entries expire after TTL."""
        cache = InMemoryCache(default_ttl_seconds=0.1)
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"

        time.sleep(0.15)  # Wait for TTL to expire
        assert cache.get("key1") is None

    def test_custom_ttl(self) -> None:
        """Test setting a custom TTL for a specific entry."""
        cache = InMemoryCache(default_ttl_seconds=10)
        cache.set("key1", "value1", ttl_seconds=0.1)

        time.sleep(0.15)
        assert cache.get("key1") is None

    def test_lru_eviction(self) -> None:
        """Test LRU eviction when cache is full."""
        cache = InMemoryCache(max_size=3)
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.set("key3", "value3")

        # Access key1 to make it most recently used
        cache.get("key1")

        # Add key4, should evict key2 (least recently used)
        cache.set("key4", "value4")

        assert cache.get("key1") == "value1"
        assert cache.get("key2") is None  # Evicted
        assert cache.get("key3") == "value3"
        assert cache.get("key4") == "value4"

    def test_clear(self) -> None:
        """Test clearing the cache."""
        cache = InMemoryCache()
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.clear()

        assert cache.get("key1") is None
        assert cache.get("key2") is None
        assert len(cache) == 0

    def test_invalidate_pattern(self) -> None:
        """Test invalidating entries by pattern."""
        cache = InMemoryCache()
        cache.set("prices:AAPL:2024-01-01", [1, 2, 3])
        cache.set("prices:AAPL:2024-01-02", [4, 5, 6])
        cache.set("prices:MSFT:2024-01-01", [7, 8, 9])
        cache.set("companies:AAPL", {"name": "Apple"})

        # Invalidate all AAPL prices
        count = cache.invalidate("prices:AAPL")
        assert count == 2

        assert cache.get("prices:AAPL:2024-01-01") is None
        assert cache.get("prices:AAPL:2024-01-02") is None
        assert cache.get("prices:MSFT:2024-01-01") == [7, 8, 9]
        assert cache.get("companies:AAPL") == {"name": "Apple"}

    def test_len(self) -> None:
        """Test getting cache size."""
        cache = InMemoryCache()
        assert len(cache) == 0

        cache.set("key1", "value1")
        cache.set("key2", "value2")
        assert len(cache) == 2

    def test_stats(self) -> None:
        """Test getting cache statistics."""
        cache = InMemoryCache(max_size=100, default_ttl_seconds=300)
        cache.set("key1", "value1")
        cache.set("key2", "value2")

        stats = cache.stats()
        assert stats["size"] == 2
        assert stats["max_size"] == 100
        assert stats["default_ttl"] == 300

    def test_complex_values(self) -> None:
        """Test storing complex values."""
        cache = InMemoryCache()

        # List
        cache.set("list", [1, 2, 3])
        assert cache.get("list") == [1, 2, 3]

        # Dict
        cache.set("dict", {"a": 1, "b": 2})
        assert cache.get("dict") == {"a": 1, "b": 2}

        # Nested
        cache.set("nested", {"list": [1, 2], "dict": {"x": "y"}})
        assert cache.get("nested") == {"list": [1, 2], "dict": {"x": "y"}}


class TestNullCache:
    """Tests for NullCache (no-op implementation)."""

    def test_get_always_returns_none(self) -> None:
        """Test that get always returns None."""
        cache = NullCache()
        cache.set("key1", "value1")
        assert cache.get("key1") is None

    def test_set_is_noop(self) -> None:
        """Test that set does nothing."""
        cache = NullCache()
        cache.set("key1", "value1")
        cache.set("key2", "value2", ttl_seconds=60)
        # No errors, just no storage

    def test_clear_is_noop(self) -> None:
        """Test that clear does nothing."""
        cache = NullCache()
        cache.clear()  # Should not raise

    def test_invalidate_returns_zero(self) -> None:
        """Test that invalidate returns 0."""
        cache = NullCache()
        assert cache.invalidate("pattern") == 0

    def test_len_returns_zero(self) -> None:
        """Test that len returns 0."""
        cache = NullCache()
        cache.set("key1", "value1")
        assert len(cache) == 0

    def test_stats(self) -> None:
        """Test getting stats shows disabled."""
        cache = NullCache()
        stats = cache.stats()
        assert stats["enabled"] is False
        assert stats["size"] == 0
