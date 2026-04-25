"""Tests for flexible stock screener filter support."""

from typing import Any

from psycopg import sql

from mcp_server.tools import screener


def test_valid_filters_are_registry_keys() -> None:
    """The public filter set should not drift away from executable SQL specs."""
    assert screener.VALID_FILTERS == set(screener.FILTER_SPECS)


def test_filter_expression_uses_registry_aliases() -> None:
    """Filter expression aliases should cover every advertised filter."""
    assert screener._get_filter_expression("price_change_1d") == "change_1d"
    assert screener._get_filter_expression("price_change_ytd") == "change_ytd"

    for filter_name in (
        "sma_5",
        "ema_12",
        "bb_middle",
        "obv",
        "volume_sma_20",
        "daily_range_pct",
        "high_52w_pct",
        "low_52w_pct",
    ):
        assert screener._get_filter_expression(filter_name) == filter_name


def test_sort_alias_accepts_filter_names_and_output_aliases() -> None:
    """Sorting can use either external filter names or returned output aliases."""
    assert screener._get_sort_alias("price_change_1d") == "change_1d"
    assert screener._get_sort_alias("change_1d") == "change_1d"
    assert screener._get_sort_alias("sma_5") == "sma_5"
    assert screener._get_sort_alias("unknown") == "market_cap"


def test_screen_stocks_builds_conditions_for_previously_missing_filters(monkeypatch) -> None:
    """Previously advertised filters should produce query predicates instead of being skipped."""
    captured: dict[str, Any] = {}

    def fake_execute_query(
        query: str | sql.Composable,
        params: dict[str, Any] | None = None,
        validate: bool = True,
    ) -> list[dict[str, Any]]:
        captured["query"] = query
        captured["params"] = params
        captured["validate"] = validate
        return [{"ticker": "AAPL"}]

    monkeypatch.setattr(screener, "execute_query", fake_execute_query)

    result = screener.screen_stocks(
        {
            "sma_5": [1, None],
            "ema_12": [1, None],
            "bb_middle": [1, None],
            "obv": [0, None],
            "volume_sma_20": [1, None],
            "daily_range_pct": [1, None],
            "high_52w_pct": [-10, None],
            "low_52w_pct": [0, None],
            "pe_ratio": [0, None],
        },
        sort_by="price_change_1d",
        limit=5,
    )

    assert result == [{"ticker": "AAPL"}]
    assert captured["params"] == {
        "limit": 5,
        "f0_min": 1,
        "f1_min": 1,
        "f2_min": 1,
        "f3_min": 0,
        "f4_min": 1,
        "f5_min": 1,
        "f6_min": -10,
        "f7_min": 0,
        "f8_min": 0,
    }

    query_repr = repr(captured["query"])
    assert "mv_52week_extremes" in query_repr
    assert "latest_52w" in query_repr
    assert "latest_ratios" in query_repr
    assert "daily_range_pct" in query_repr
    assert "high_52w_pct" in query_repr


def test_get_52week_extremes_uses_latest_available_snapshot(monkeypatch) -> None:
    """52-week lookups should not require the materialized view date to equal price date."""
    captured: dict[str, Any] = {}

    def fake_execute_query(
        query: str | sql.Composable,
        params: dict[str, Any] | None = None,
        validate: bool = True,
    ) -> list[dict[str, Any]]:
        captured["query"] = query
        captured["params"] = params
        captured["validate"] = validate
        return []

    monkeypatch.setattr(screener, "execute_query", fake_execute_query)

    result = screener.get_52week_extremes(include_fundamentals=True, limit=3)

    assert result == []
    assert captured["params"] == {
        "threshold": 2.0,
        "neg_threshold": -2.0,
        "limit": 3,
    }
    query_repr = repr(captured["query"])
    assert "latest_52w" in query_repr
    assert "latest_ratios" in query_repr
    assert "NULLIF(e.high_52w, 0)" in query_repr
