"""Intraday bar-size identity and loader regressions."""

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import pytest

from sawa.database.intraday_load import load_intraday_bars


class _Cursor:
    def __init__(self) -> None:
        self.calls: list[tuple[object, tuple[Any, ...]]] = []

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, query: object, params: tuple[Any, ...]) -> None:
        self.calls.append((query, params))


class _Connection:
    def __init__(self) -> None:
        self.cursor_value = _Cursor()
        self.commits = 0

    def cursor(self) -> _Cursor:
        return self.cursor_value

    def commit(self) -> None:
        self.commits += 1


def _bar(size: int) -> dict[str, Any]:
    return {
        "ticker": "AAPL",
        "timestamp": datetime(2026, 8, 28, 14, 30, tzinfo=timezone.utc),
        "open": 100,
        "high": 102,
        "low": 99,
        "close": 101,
        "volume": 1000,
        "bar_size_minutes": size,
    }


def test_loader_keeps_same_timestamp_at_two_bar_sizes() -> None:
    conn = _Connection()

    inserted = load_intraday_bars(
        conn, [_bar(5), _bar(15)], logging.getLogger(__name__)  # type: ignore[arg-type]
    )

    assert inserted == 2
    assert conn.commits == 1
    assert [call[1][-3] for call in conn.cursor_value.calls] == [5, 15]
    assert [call[1][-2] for call in conn.cursor_value.calls] == [5, 15]
    assert [call[1][-1] for call in conn.cursor_value.calls] == [31, 32767]
    query_text = str(conn.cursor_value.calls[0][0])
    assert "ON CONFLICT (ticker, timestamp, bar_size_minutes)" in query_text


def test_loader_rejects_unsupported_size_before_opening_cursor() -> None:
    conn = _Connection()

    with pytest.raises(ValueError, match="bar_size_minutes"):
        load_intraday_bars(
            conn, [_bar(7)], logging.getLogger(__name__)  # type: ignore[arg-type]
        )

    assert conn.cursor_value.calls == []
    assert conn.commits == 0


def test_loader_rejects_impossible_source_minute_count_before_cursor() -> None:
    conn = _Connection()
    bar = _bar(5)
    bar["source_minute_count"] = 6

    with pytest.raises(ValueError, match="source_minute_count"):
        load_intraday_bars(
            conn, [bar], logging.getLogger(__name__)  # type: ignore[arg-type]
        )

    assert conn.cursor_value.calls == []
    assert conn.commits == 0


def test_loader_rejects_inconsistent_minute_mask_before_cursor() -> None:
    conn = _Connection()
    bar = _bar(5)
    bar["source_minute_count"] = 1
    bar["source_minute_mask"] = 0b100000

    with pytest.raises(ValueError, match="source_minute_mask"):
        load_intraday_bars(
            conn, [bar], logging.getLogger(__name__)  # type: ignore[arg-type]
        )

    assert conn.cursor_value.calls == []
    assert conn.commits == 0


def test_loader_rejects_huge_integer_as_value_error_before_cursor() -> None:
    conn = _Connection()
    bar = _bar(5)
    bar["open"] = 10**1000

    with pytest.raises(ValueError, match="NUMERIC"):
        load_intraday_bars(
            conn, [bar], logging.getLogger(__name__)  # type: ignore[arg-type]
        )

    assert conn.cursor_value.calls == []
    assert conn.commits == 0


def test_loader_rejects_price_that_rounds_to_zero_before_cursor() -> None:
    conn = _Connection()
    bar = _bar(5)
    bar.update(
        open=0.000000001,
        high=0.000000002,
        low=0.000000001,
        close=0.000000001,
    )

    with pytest.raises(ValueError, match="NUMERIC"):
        load_intraday_bars(
            conn, [bar], logging.getLogger(__name__)  # type: ignore[arg-type]
        )

    assert conn.cursor_value.calls == []
    assert conn.commits == 0


def test_loader_rejects_price_that_rounds_up_past_numeric_max() -> None:
    conn = _Connection()
    bar = _bar(5)
    too_close = Decimal("999999999999.999999995")
    bar.update(open=too_close, high=too_close, low=too_close, close=too_close)

    with pytest.raises(ValueError, match="NUMERIC"):
        load_intraday_bars(
            conn, [bar], logging.getLogger(__name__)  # type: ignore[arg-type]
        )

    assert conn.cursor_value.calls == []
    assert conn.commits == 0


def test_upsert_uses_more_complete_authoritative_snapshot() -> None:
    conn = _Connection()
    corrected = _bar(5)
    corrected["source_minute_count"] = 5

    load_intraday_bars(
        conn, [corrected], logging.getLogger(__name__)  # type: ignore[arg-type]
    )

    query_text = str(conn.cursor_value.calls[0][0])
    assert "EXCLUDED.source_minute_mask |" in query_text
    assert "THEN EXCLUDED.high" in query_text
    assert "THEN EXCLUDED.volume" in query_text
    assert "GREATEST(stock_prices_intraday.high" not in query_text
