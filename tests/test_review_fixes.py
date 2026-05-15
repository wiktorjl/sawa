"""Regression tests for review-driven bug fixes."""

from datetime import date
from decimal import Decimal

from mcp_server.validation import validate_tool_arguments
from mcp_server.tools import market_data
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
    assert "WHERE rn <= %(limit)s" in query
    assert captured["params"] == {
        "tickers": ["AAPL", "MSFT"],
        "date": "2026-05-15",
        "limit": 3,
    }


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
