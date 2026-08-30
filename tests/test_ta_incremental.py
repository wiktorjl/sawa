"""Incremental TA state and persistence-boundary tests."""

from datetime import date
from decimal import Decimal
from typing import Any

from sawa.daily import (
    _effective_ta_recompute_from,
    _new_ta_rows,
    _ta_rows_to_persist,
)
from sawa.database.ta_load import get_cumulative_indicator_seed
from sawa.domain.technical_indicators import TechnicalIndicators


class _SeedCursor:
    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self.row = row
        self.query = ""
        self.params: tuple[Any, ...] = ()

    def __enter__(self) -> "_SeedCursor":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, query: str, params: tuple[Any, ...]) -> None:
        self.query = query
        self.params = params

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.row


class _SeedConnection:
    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self.cursor_instance = _SeedCursor(row)

    def cursor(self) -> _SeedCursor:
        return self.cursor_instance


def test_cumulative_seed_uses_strict_pre_window_cutoff() -> None:
    first_fetched_date = date(2026, 8, 28)
    conn = _SeedConnection(
        (Decimal("12345.67"), 321, -99, Decimal("42.50"))
    )

    seed = get_cumulative_indicator_seed(conn, "aapl", first_fetched_date)

    assert "date < %s" in conn.cursor_instance.query
    assert conn.cursor_instance.params == ("AAPL", first_fetched_date)
    assert seed.vwap_numerator == Decimal("12345.67")
    assert seed.cumulative_volume == 321
    assert seed.obv == -99
    assert seed.previous_close == Decimal("42.50")


def test_incremental_daily_filter_only_keeps_dates_after_last_ta() -> None:
    rows = [
        TechnicalIndicators("AAPL", date(2026, 8, 27)),
        TechnicalIndicators("AAPL", date(2026, 8, 28)),
        TechnicalIndicators("AAPL", date(2026, 8, 29)),
    ]

    assert _new_ta_rows(rows, date(2026, 8, 28)) == rows[2:]
    assert _new_ta_rows(rows, None) == rows


def test_forced_historical_replay_overwrites_ta_even_when_latest_ta_unchanged() -> None:
    rows = [
        TechnicalIndicators("AAPL", date(2026, 8, 26)),
        TechnicalIndicators("AAPL", date(2026, 8, 27)),
        TechnicalIndicators("AAPL", date(2026, 8, 28)),
        TechnicalIndicators("AAPL", date(2026, 8, 29)),
    ]

    selected = _ta_rows_to_persist(
        rows,
        last_ta=date(2026, 8, 29),
        recompute_from=date(2026, 8, 27),
    )

    assert selected == rows[1:]


def test_effective_recompute_preserves_bootstrap_and_stale_ta_gaps() -> None:
    repaired = date(2026, 8, 20)

    # A ticker without any TA must still bootstrap its complete history.
    assert _effective_ta_recompute_from(None, repaired) is None
    # If TA lags the repair window, fill from the first missing TA date.
    assert _effective_ta_recompute_from(date(2026, 8, 10), repaired) == date(
        2026, 8, 11
    )
    # If a price correction predates the latest TA, overwrite from the repair.
    assert _effective_ta_recompute_from(date(2026, 8, 29), repaired) == repaired
