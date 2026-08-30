"""Focused regressions for database repository/schema contracts.

Every query test replaces ``_get_connection``; this module never opens a real
database connection.
"""

from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from sawa.repositories import database
from sawa.repositories.database import (
    DatabaseFundamentalRepository,
    DatabaseNewsRepository,
    DatabaseTechnicalIndicatorsRepository,
)


class _RecordingCursor:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.executions: list[tuple[str, object]] = []

    def __enter__(self) -> "_RecordingCursor":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, query: str, params: object = None) -> None:
        self.executions.append((query, params))

    def fetchall(self) -> list[dict[str, Any]]:
        return self.rows


class _RecordingConnection:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.cursor_instance = _RecordingCursor(rows or [])

    def __enter__(self) -> "_RecordingConnection":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def cursor(self, **kwargs: object) -> _RecordingCursor:
        return self.cursor_instance


def _fundamental_row(**values: object) -> dict[str, object]:
    return {
        "ticker": "AAPL",
        "period_end": date(2026, 6, 30),
        "timeframe": "quarterly",
        "fiscal_year": 2026,
        "fiscal_quarter": 3,
        **values,
    }


def test_income_mapper_uses_exact_schema_columns_and_preserves_zero() -> None:
    repo = DatabaseFundamentalRepository("postgresql://unused")
    income = repo._row_to_income(
        _fundamental_row(
            net_income_loss_attributable_common_shareholders=Decimal("0"),
            consolidated_net_income_loss=Decimal("999"),
            basic_earnings_per_share=Decimal("1.23"),
            diluted_earnings_per_share=Decimal("1.11"),
            net_income=Decimal("888"),
            basic_eps=Decimal("8.88"),
            diluted_eps=Decimal("7.77"),
        )
    )

    assert income.net_income == Decimal("0")
    assert income.basic_eps == Decimal("1.23")
    assert income.diluted_eps == Decimal("1.11")


def test_income_mapper_falls_back_to_consolidated_net_income() -> None:
    repo = DatabaseFundamentalRepository("postgresql://unused")
    income = repo._row_to_income(
        _fundamental_row(
            net_income_loss_attributable_common_shareholders=None,
            consolidated_net_income_loss="42.5",
        )
    )

    assert income.net_income == Decimal("42.5")


def test_balance_mapper_uses_exact_schema_columns() -> None:
    repo = DatabaseFundamentalRepository("postgresql://unused")
    balance = repo._row_to_balance(
        _fundamental_row(
            long_term_debt_and_capital_lease_obligations="125.5",
            retained_earnings_deficit="-18.25",
            long_term_debt="999",
            retained_earnings="888",
        )
    )

    assert balance.long_term_debt == Decimal("125.5")
    assert balance.retained_earnings == Decimal("-18.25")


def test_cashflow_mapper_uses_exact_schema_columns_without_deriving_fcf() -> None:
    repo = DatabaseFundamentalRepository("postgresql://unused")
    cashflow = repo._row_to_cashflow(
        _fundamental_row(
            net_cash_from_operating_activities="400",
            purchase_of_property_plant_and_equipment="-75",
            dividends="-20",
            operating_cash_flow="999",
            capital_expenditure="888",
            dividends_paid="777",
            free_cash_flow="325",
        )
    )

    assert cashflow.operating_cash_flow == Decimal("400")
    assert cashflow.capital_expenditure == Decimal("-75")
    assert cashflow.dividends_paid == Decimal("-20")
    assert cashflow.free_cash_flow is None


@pytest.mark.parametrize(
    ("method_name", "args", "expected_params"),
    [
        ("_get_news_sync", ("aapl", 17, 9), ("AAPL", 9, 17)),
        ("_get_sentiment_summary_sync", ("msft", 12), ("MSFT", 12)),
    ],
)
def test_news_queries_use_parameter_safe_interval_arithmetic(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
    args: tuple[object, ...],
    expected_params: tuple[object, ...],
) -> None:
    connection = _RecordingConnection()
    monkeypatch.setattr(database, "_get_connection", lambda unused: connection)
    repo = DatabaseNewsRepository("postgresql://unused")

    getattr(repo, method_name)(*args)

    query, params = connection.cursor_instance.executions[0]
    assert "NOW() - (%s * INTERVAL '1 day')" in query
    assert "INTERVAL '%s days'" not in query
    assert params == expected_params


def test_new_indicator_filters_are_accepted_and_queried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = _RecordingConnection()
    monkeypatch.setattr(database, "_get_connection", lambda unused: connection)
    repo = DatabaseTechnicalIndicatorsRepository("postgresql://unused")

    results = repo._screen_sync(
        {
            "adx_14": (20, None),
            "bb_width_pct": (None, 0.1),
            "dollar_volume_sma_20": (1_000_000, 5_000_000),
        },
        target_date=None,
        index=None,
        limit=25,
    )

    assert results == []
    query, params = connection.cursor_instance.executions[0]
    assert "adx_14 >= %s" in query
    assert "bb_width_pct <= %s" in query
    assert "dollar_volume_sma_20 BETWEEN %s AND %s" in query
    assert params == [20, 0.1, 1_000_000, 5_000_000, 25]


def test_unknown_indicators_are_sorted_and_rejected_before_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection_calls = 0

    def fail_if_connected(unused: str) -> _RecordingConnection:
        nonlocal connection_calls
        connection_calls += 1
        raise AssertionError("database connection should not be opened")

    monkeypatch.setattr(database, "_get_connection", fail_if_connected)
    repo = DatabaseTechnicalIndicatorsRepository("postgresql://unused")

    with pytest.raises(
        ValueError,
        match=r"^Unknown technical indicators: alpha_unknown, zeta_unknown$",
    ):
        repo._screen_sync(
            {
                "zeta_unknown": (1, None),
                "rsi_14": (30, 70),
                "alpha_unknown": (None, 2),
            },
            target_date=None,
            index=None,
            limit=100,
        )

    assert connection_calls == 0
