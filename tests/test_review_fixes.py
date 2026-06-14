"""Regression tests for review-driven bug fixes."""

import json
from datetime import date
from decimal import Decimal

import pytest

from mcp_server.tools import market_data
from mcp_server.validation import validate_tool_arguments
from sawa.repositories.database import (
    DatabaseRatiosRepository,
    DatabaseTechnicalIndicatorsRepository,
)


def test_candlestick_days_validation_allows_tool_contract() -> None:
    args = validate_tool_arguments(
        "detect_candlestick_patterns",
        {"ticker": "aapl", "days": 120},
    )

    assert args["ticker"] == "AAPL"
    assert args["days"] == 120


def test_generic_days_validation_still_caps_live_price() -> None:
    try:
        validate_tool_arguments("get_live_price", {"ticker": "AAPL", "days": 120})
    except ValueError as exc:
        assert "max 30" in str(exc)
    else:
        raise AssertionError("expected get_live_price days validation to fail")


def test_intraday_requires_exactly_one_ticker_input() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        validate_tool_arguments("get_intraday_bars", {})

    with pytest.raises(ValueError, match="exactly one"):
        validate_tool_arguments(
            "get_intraday_bars",
            {"ticker": "AAPL", "tickers": ["MSFT"]},
        )

    args = validate_tool_arguments("get_intraday_bars", {"ticker": "aapl"})
    assert args["ticker"] == "AAPL"


def test_index_validation_allows_future_codes_and_rejects_legacy_code() -> None:
    args = validate_tool_arguments("screen_stocks", {"index": "Future_Index_1"})
    assert args["index"] == "future_index_1"

    with pytest.raises(ValueError, match="nasdaq_listed"):
        validate_tool_arguments("screen_stocks", {"index": "nasdaq5000"})


def test_enum_validation_rejects_silent_defaulting() -> None:
    with pytest.raises(ValueError, match="Invalid direction"):
        validate_tool_arguments("get_top_movers", {"direction": "winnerz"})


def test_intraday_multi_ticker_limit_is_applied_per_ticker(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_execute_query(query: str, params: dict[str, object]):
        captured["query"] = query
        captured["params"] = params
        return []

    monkeypatch.setattr(market_data, "execute_query", fake_execute_query)

    market_data.get_intraday_bars(tickers=["AAPL", "MSFT"], date="2026-05-15", limit=3)

    query = captured["query"]
    assert isinstance(query, str)
    assert "ROW_NUMBER() OVER" in query
    assert "PARTITION BY ticker" in query
    # Rank DESC so the most-recent `limit` bars per ticker are kept (not the
    # oldest), then re-order ASC for display.
    assert "ORDER BY timestamp DESC" in query
    assert "ORDER BY ticker, timestamp ASC" in query
    assert "WHERE rn <= %(limit)s" in query
    assert "timestamp AT TIME ZONE 'America/New_York'" in query
    assert "TIME '09:30:00'" in query
    assert "TIME '16:00:00'" in query
    assert captured["params"] == {
        "tickers": ["AAPL", "MSFT"],
        "date": "2026-05-15",
        "limit": 3,
    }


@pytest.mark.asyncio
async def test_execute_query_tool_passes_params_and_returns_envelope(monkeypatch) -> None:
    pytest.importorskip("dotenv")
    pytest.importorskip("mcp")

    import mcp_server.database as mcp_database
    import mcp_server.server as mcp_server

    captured: dict[str, object] = {}

    def fake_execute_query(query: str, params: dict[str, object] | None = None):
        captured["query"] = query
        captured["params"] = params
        return [{"ticker": params["ticker"] if params else None}]

    monkeypatch.setattr(mcp_server, "execute_query", fake_execute_query)
    monkeypatch.setattr(mcp_database, "log_execute_query", lambda query, params=None: None)

    response = await mcp_server.call_tool(
        "execute_query",
        {
            "sql": "SELECT %(ticker)s AS ticker",
            "params": {"ticker": "AAPL"},
        },
    )

    assert captured == {
        "query": "SELECT %(ticker)s AS ticker",
        "params": {"ticker": "AAPL"},
    }

    payload = json.loads(response[0].text)
    assert payload["data"] == [{"ticker": "AAPL"}]
    assert payload["chart"] is None
    assert payload["warnings"] == []
    assert payload["metadata"]["tool"] == "execute_query"
    assert payload["metadata"]["schema_version"] == "sawa.mcp.tool_response.v1"


def test_database_ratio_mapping_uses_schema_column_names() -> None:
    repo = DatabaseRatiosRepository("postgresql://unused")

    ratio = repo._row_to_ratio({
        "ticker": "AAPL",
        "date": date(2026, 5, 15),
        "price_to_earnings": Decimal("31.2"),
        "price_to_book": Decimal("12.4"),
        "price_to_sales": Decimal("8.1"),
        "return_on_equity": Decimal("0.42"),
        "return_on_assets": Decimal("0.21"),
        "current": Decimal("1.5"),
        "quick": Decimal("1.2"),
    })

    assert ratio.pe_ratio == Decimal("31.2")
    assert ratio.pb_ratio == Decimal("12.4")
    assert ratio.ps_ratio == Decimal("8.1")
    assert ratio.roe == Decimal("0.42")
    assert ratio.roa == Decimal("0.21")
    assert ratio.current_ratio == Decimal("1.5")
    assert ratio.quick_ratio == Decimal("1.2")

    # These columns do not exist in financial_ratios, so the reader must
    # not map them; the corresponding fields stay None rather than being
    # read from a non-existent column.
    assert ratio.peg_ratio is None
    assert ratio.profit_margin is None
    assert ratio.operating_margin is None
    assert ratio.debt_to_assets is None
    assert ratio.asset_turnover is None
    assert ratio.inventory_turnover is None


def test_database_technical_indicator_mapping_returns_newer_fields() -> None:
    repo = DatabaseTechnicalIndicatorsRepository("postgresql://unused")

    indicators = repo._row_to_indicators({
        "ticker": "AAPL",
        "date": date(2026, 5, 15),
        "bb_width_pct": Decimal("0.08"),
        "adx_14": Decimal("27.5"),
        "dollar_volume_sma_20": Decimal("123456789.12"),
    })

    assert indicators.bb_width_pct == Decimal("0.08")
    assert indicators.adx_14 == Decimal("27.5")
    assert indicators.dollar_volume_sma_20 == Decimal("123456789.12")
