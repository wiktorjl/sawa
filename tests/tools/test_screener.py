"""Tests for flexible stock screener filter support."""

from datetime import date
from typing import Any

from psycopg import sql

from mcp_server.tools import screener

FAKE_DATE_REFS = {
    "latest": date(2026, 6, 12),
    "prev_day": date(2026, 6, 11),
    "week_ago": date(2026, 6, 5),
    "month_ago": date(2026, 5, 13),
    "ytd_start": date(2026, 1, 2),
}


def test_valid_filters_are_registry_keys() -> None:
    """The public filter set should not drift away from executable SQL specs."""
    assert screener.VALID_FILTERS == set(screener.FILTER_SPECS)


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
    monkeypatch.setattr(screener, "get_price_date_refs", lambda: dict(FAKE_DATE_REFS))

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
        **FAKE_DATE_REFS,
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


def test_detect_crossovers_composes_executable_sql(monkeypatch) -> None:
    """Regression: the final SELECT once aliased a table as the reserved word
    CROSS and compared sp/ti columns outside their scope, so every call failed
    server-side while mocked tests passed."""
    captured: list[tuple[Any, Any]] = []

    def fake_execute_query(
        query: str | sql.Composable,
        params: dict[str, Any] | None = None,
        validate: bool = True,
    ) -> list[dict[str, Any]]:
        captured.append((query, params))
        if isinstance(query, str) and "SELECT DISTINCT date" in query:
            return [{"date": date(2026, 6, day)} for day in (11, 10, 9, 8, 5, 4)]
        return []

    monkeypatch.setattr(screener, "execute_query", fake_execute_query)
    monkeypatch.setattr(screener, "get_price_date_refs", lambda: dict(FAKE_DATE_REFS))

    result = screener.detect_crossovers(lookback_days=5)

    assert result == []
    query, params = captured[-1]
    assert params["latest"] == FAKE_DATE_REFS["latest"]
    assert params["recent_dates"][0] == FAKE_DATE_REFS["latest"]
    assert len(params["recent_dates"]) == 6

    query_repr = repr(query)
    assert "crossovers xo" in query_repr
    assert "cross.ticker" not in query_repr
    assert "close >= sma_value" in query_repr


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
    monkeypatch.setattr(screener, "get_price_date_refs", lambda: dict(FAKE_DATE_REFS))

    result = screener.get_52week_extremes(include_fundamentals=True, limit=3)

    assert result == []
    assert captured["params"] == {
        "latest": FAKE_DATE_REFS["latest"],
        "prev_day": FAKE_DATE_REFS["prev_day"],
        "threshold": 2.0,
        "neg_threshold": -2.0,
        "limit": 3,
    }
    query_repr = repr(captured["query"])
    assert "latest_52w" in query_repr
    assert "latest_ratios" in query_repr
    assert "NULLIF(e.high_52w, 0)" in query_repr


def _invalid_placeholders(text: str) -> list[str]:
    """Return any bare '%' that is not a valid psycopg placeholder.

    psycopg's client-side parsing only accepts %s, %b, %t, %(name)s, or an
    escaped %%. A stray '%' (e.g. inside a SQL comment) raises at execute time
    but is invisible to mocked tests, so check the composed SQL statically.
    """
    bad: list[str] = []
    i = 0
    while i < len(text):
        if text[i] == "%":
            nxt = text[i + 1 : i + 2]
            if nxt == "%":
                i += 2
                continue
            if nxt not in ("s", "b", "t", "("):
                bad.append(text[i : i + 6])
        i += 1
    return bad


def test_detect_crossovers_forward_fills_live_sma(monkeypatch) -> None:
    """Same-day crosses must survive. The live (today) row has no
    technical_indicators row yet (TA is computed EOD), so the query LEFT JOINs
    and carries the latest EOD SMA forward instead of inner-joining the row
    away. Also guards against a stray '%' in the composed SQL: that raises
    server-side via psycopg placeholder parsing but passes a mocked execute."""
    captured: dict[str, Any] = {}

    def fake_execute_query(
        query: str | sql.Composable,
        params: dict[str, Any] | None = None,
        validate: bool = True,
    ) -> list[dict[str, Any]]:
        if isinstance(query, str) and "SELECT DISTINCT date" in query:
            return [{"date": date(2026, 6, day)} for day in (11, 10, 9, 8, 5, 4)]
        captured["query"] = query
        captured["params"] = params
        return []

    monkeypatch.setattr(screener, "execute_query", fake_execute_query)
    monkeypatch.setattr(screener, "get_price_date_refs", lambda: dict(FAKE_DATE_REFS))

    result = screener.detect_crossovers(sma_period=50, lookback_days=5)
    assert result == []

    rendered = captured["query"].as_string(None)

    # Live row is retained (LEFT JOIN) and its missing SMA is forward-filled.
    assert "LEFT JOIN technical_indicators" in rendered
    assert "COALESCE(" in rendered
    assert 'LAG(ti."sma_50")' in rendered
    # Positivity guards moved out to act on the forward-filled value.
    assert "sma_value > 0" in rendered
    assert "prev_sma > 0" in rendered
    # The old guard that excluded the live row must be gone.
    assert "ti.date = ANY" not in rendered

    # Regression: a stray '%' (the kind that hid in a SQL comment) would make
    # psycopg reject the statement at execute time.
    assert _invalid_placeholders(rendered) == []
