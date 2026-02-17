"""Tests for support & resistance level detection tool."""

import sys
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

_mock_db = MagicMock()
_mock_db.execute_query = MagicMock(return_value=[])
sys.modules.setdefault("mcp_server.database", _mock_db)

from mcp_server.tools.support_resistance import (  # noqa: E402
    _calculate_pivot_strength,
    _cluster_levels,
    _count_touches,
    _find_last_touch,
    _pivot_point_levels,
    _volume_levels,
    calculate_support_resistance_levels,
)


def _make_prices(
    n: int = 30,
    base: float = 100.0,
    spread: float = 2.0,
    start_date: date | None = None,
) -> list[dict]:
    """Generate synthetic OHLCV price data for testing."""
    if start_date is None:
        start_date = date.today() - timedelta(days=n)
    prices = []
    for i in range(n):
        d = start_date + timedelta(days=i)
        close = base + (i % 10) * 0.5 - 2.0
        prices.append({
            "date": d,
            "open": close - 0.3,
            "high": close + spread / 2,
            "low": close - spread / 2,
            "close": close,
            "volume": 1_000_000 + i * 10_000,
        })
    return prices


class TestCountTouches:
    """Tests for _count_touches helper."""

    def test_exact_touch(self):
        prices = _make_prices(10, base=100.0, spread=2.0)
        level = 100.0
        count = _count_touches(prices, level)
        assert count >= 1

    def test_no_touches_far_level(self):
        prices = _make_prices(10, base=100.0, spread=2.0)
        count = _count_touches(prices, 200.0)
        assert count == 0

    def test_all_prices_touch(self):
        prices = _make_prices(5, base=100.0, spread=200.0)
        count = _count_touches(prices, 100.0, tolerance_pct=0.01)
        assert count == 5


class TestFindLastTouch:
    """Tests for _find_last_touch helper."""

    def test_finds_most_recent(self):
        prices = _make_prices(10, base=100.0, spread=10.0)
        result = _find_last_touch(prices, 100.0)
        assert result is not None
        assert result == str(prices[-1]["date"])

    def test_returns_none_for_no_touch(self):
        prices = _make_prices(10, base=100.0, spread=2.0)
        result = _find_last_touch(prices, 500.0)
        assert result is None


class TestPivotPointLevels:
    """Tests for pivot point method."""

    def test_returns_levels(self):
        prices = _make_prices(30)
        levels = _pivot_point_levels(prices, max_levels=5)
        assert len(levels) <= 5
        assert len(levels) > 0

    def test_pivot_formula(self):
        prices = [{
            "date": date.today(),
            "open": 100.0,
            "high": 110.0,
            "low": 90.0,
            "close": 105.0,
            "volume": 1_000_000,
        }]
        levels = _pivot_point_levels(prices, max_levels=5)
        prices_by_label = {lv["label"]: lv["price"] for lv in levels}

        pp = (110.0 + 90.0 + 105.0) / 3.0
        assert abs(prices_by_label["pivot"] - pp) < 0.01
        assert abs(prices_by_label["resistance_1"] - (2 * pp - 90.0)) < 0.01
        assert abs(prices_by_label["support_1"] - (2 * pp - 110.0)) < 0.01
        assert abs(prices_by_label["resistance_2"] - (pp + 20.0)) < 0.01
        assert abs(prices_by_label["support_2"] - (pp - 20.0)) < 0.01

    def test_level_has_required_fields(self):
        prices = _make_prices(10)
        levels = _pivot_point_levels(prices, max_levels=5)
        for level in levels:
            assert "price" in level
            assert "strength" in level
            assert "test_count" in level
            assert "last_touch" in level
            assert "label" in level

    def test_strength_bounded(self):
        prices = _make_prices(30)
        levels = _pivot_point_levels(prices, max_levels=5)
        for level in levels:
            assert 0.0 <= level["strength"] <= 1.0


class TestClusterLevels:
    """Tests for price clustering method."""

    def test_finds_clusters(self):
        prices = _make_prices(50, base=100.0, spread=2.0)
        levels = _cluster_levels(prices, max_levels=5)
        assert isinstance(levels, list)
        for level in levels:
            assert "price" in level
            assert "strength" in level
            assert level["label"] == "cluster"

    def test_no_clusters_in_flat_prices(self):
        """Flat prices (zero range) should return empty."""
        prices = [{
            "date": date.today() - timedelta(days=i),
            "open": 100.0,
            "high": 100.0,
            "low": 100.0,
            "close": 100.0,
            "volume": 1_000_000,
        } for i in range(10)]
        levels = _cluster_levels(prices, max_levels=5)
        assert levels == []

    def test_max_levels_respected(self):
        prices = _make_prices(100, spread=5.0)
        levels = _cluster_levels(prices, max_levels=3)
        assert len(levels) <= 3

    def test_empty_prices(self):
        levels = _cluster_levels([], max_levels=5)
        assert levels == []


class TestVolumeLevels:
    """Tests for volume-based method."""

    def test_returns_volume_nodes(self):
        prices = _make_prices(50)
        levels = _volume_levels(prices, max_levels=5)
        assert isinstance(levels, list)
        for level in levels:
            assert "price" in level
            assert level["label"] == "volume_node"

    def test_strength_is_relative(self):
        prices = _make_prices(50)
        levels = _volume_levels(prices, max_levels=5)
        if levels:
            assert levels[0]["strength"] == 1.0

    def test_empty_prices(self):
        levels = _volume_levels([], max_levels=5)
        assert levels == []

    def test_flat_prices_returns_empty(self):
        prices = [{
            "date": date.today() - timedelta(days=i),
            "open": 50.0,
            "high": 50.0,
            "low": 50.0,
            "close": 50.0,
            "volume": 1_000_000,
        } for i in range(10)]
        levels = _volume_levels(prices, max_levels=5)
        assert levels == []


class TestCalculatePivotStrength:
    """Tests for pivot strength scoring."""

    def test_pivot_highest_base(self):
        assert _calculate_pivot_strength("pivot", 0) > _calculate_pivot_strength("support_2", 0)

    def test_touch_bonus(self):
        assert _calculate_pivot_strength("pivot", 5) > _calculate_pivot_strength("pivot", 0)

    def test_max_capped_at_one(self):
        assert _calculate_pivot_strength("pivot", 100) <= 1.0


class TestCalculateSupportResistanceLevels:
    """Tests for the main entry point function."""

    @patch("mcp_server.tools.support_resistance._fetch_prices")
    def test_pivot_method(self, mock_fetch):
        mock_fetch.return_value = _make_prices(30)
        result = calculate_support_resistance_levels("AAPL", method="pivot")
        assert result["ticker"] == "AAPL"
        assert result["method"] == "pivot"
        assert result["current_price"] is not None
        assert len(result["levels"]) > 0

    @patch("mcp_server.tools.support_resistance._fetch_prices")
    def test_cluster_method(self, mock_fetch):
        mock_fetch.return_value = _make_prices(60, spread=5.0)
        result = calculate_support_resistance_levels("MSFT", method="cluster")
        assert result["method"] == "cluster"
        assert isinstance(result["levels"], list)

    @patch("mcp_server.tools.support_resistance._fetch_prices")
    def test_volume_method(self, mock_fetch):
        mock_fetch.return_value = _make_prices(60, spread=5.0)
        result = calculate_support_resistance_levels("GOOGL", method="volume")
        assert result["method"] == "volume"
        assert isinstance(result["levels"], list)

    @patch("mcp_server.tools.support_resistance._fetch_prices")
    def test_no_data(self, mock_fetch):
        mock_fetch.return_value = []
        result = calculate_support_resistance_levels("ZZZZ")
        assert result["current_price"] is None
        assert result["levels"] == []
        assert "error" in result

    @patch("mcp_server.tools.support_resistance._fetch_prices")
    def test_ticker_normalized(self, mock_fetch):
        mock_fetch.return_value = _make_prices(30)
        result = calculate_support_resistance_levels("aapl", method="pivot")
        assert result["ticker"] == "AAPL"

    def test_invalid_method(self):
        with pytest.raises(ValueError, match="Invalid method"):
            calculate_support_resistance_levels("AAPL", method="invalid")

    @patch("mcp_server.tools.support_resistance._fetch_prices")
    def test_levels_have_type(self, mock_fetch):
        mock_fetch.return_value = _make_prices(30)
        result = calculate_support_resistance_levels("AAPL", method="pivot")
        for level in result["levels"]:
            assert level["type"] in ("support", "resistance")

    @patch("mcp_server.tools.support_resistance._fetch_prices")
    def test_levels_sorted_by_strength(self, mock_fetch):
        mock_fetch.return_value = _make_prices(30)
        result = calculate_support_resistance_levels("AAPL", method="pivot")
        strengths = [lv["strength"] for lv in result["levels"]]
        assert strengths == sorted(strengths, reverse=True)

    @patch("mcp_server.tools.support_resistance._fetch_prices")
    def test_max_levels_respected(self, mock_fetch):
        mock_fetch.return_value = _make_prices(60, spread=5.0)
        result = calculate_support_resistance_levels("AAPL", max_levels=2, method="pivot")
        assert len(result["levels"]) <= 2

    @patch("mcp_server.tools.support_resistance._fetch_prices")
    def test_lookback_clamped(self, mock_fetch):
        mock_fetch.return_value = _make_prices(10)
        result = calculate_support_resistance_levels("AAPL", lookback_days=1, method="pivot")
        assert result["lookback_days"] == 5

        result = calculate_support_resistance_levels("AAPL", lookback_days=9999, method="pivot")
        assert result["lookback_days"] == 500
