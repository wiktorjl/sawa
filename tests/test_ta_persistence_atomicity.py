"""Offline regressions for truthful and atomic TA persistence."""

from __future__ import annotations

import logging
from copy import deepcopy
from datetime import date
from typing import Any
from unittest import mock

import psycopg
import pytest

from sawa import daily, ta_backfill
from sawa.database.ta_load import (
    TechnicalIndicatorWriteError,
    load_technical_indicators,
)
from sawa.domain.technical_indicators import TechnicalIndicators


class _WriteCursor:
    def __init__(self, conn: _TransactionalConnection) -> None:
        self.conn = conn
        self.rowcount = 0

    def __enter__(self) -> _WriteCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, query: Any, params: Any = None) -> None:
        text = query.as_string() if hasattr(query, "as_string") else str(query)
        if text.startswith("DELETE FROM technical_indicators"):
            tickers = set(params[0])
            before = len(self.conn.pending)
            self.conn.pending = [row for row in self.conn.pending if row[0] not in tickers]
            self.rowcount = before - len(self.conn.pending)
            return
        if text.startswith("INSERT INTO \"technical_indicators\""):
            self.conn.insert_attempts += 1
            if self.conn.insert_attempts == self.conn.fail_on_insert:
                raise psycopg.IntegrityError("forced replacement failure")
            assert params is not None
            key = (params[0], params[1], params[2])
            self.conn.pending.append(key)
            self.rowcount = 1
            return
        raise AssertionError(f"unexpected SQL: {text}")


class _TransactionalConnection:
    """Small transaction model; it never opens a real database connection."""

    def __init__(
        self,
        rows: list[tuple[str, date, object]],
        *,
        fail_on_insert: int,
    ) -> None:
        self.committed = deepcopy(rows)
        self.pending = deepcopy(rows)
        self.fail_on_insert = fail_on_insert
        self.insert_attempts = 0
        self.commits = 0
        self.rollbacks = 0

    def __enter__(self) -> _TransactionalConnection:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        if exc_type is None:
            self.commit()
        else:
            self.rollback()

    def cursor(self) -> _WriteCursor:
        return _WriteCursor(self)

    def commit(self) -> None:
        self.committed = deepcopy(self.pending)
        self.commits += 1

    def rollback(self) -> None:
        self.pending = deepcopy(self.committed)
        self.rollbacks += 1


def _indicator(day: int) -> TechnicalIndicators:
    return TechnicalIndicators(
        ticker="AAPL",
        date=date(2026, 8, day),
    )


def test_row_write_failure_rolls_back_and_raises() -> None:
    old = [("AAPL", date(2026, 8, 1), "old")]
    conn = _TransactionalConnection(old, fail_on_insert=2)

    with pytest.raises(TechnicalIndicatorWriteError, match="AAPL/2026-08-03"):
        load_technical_indicators(conn, [_indicator(2), _indicator(3)])

    assert conn.committed == old
    assert conn.pending == old
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_failed_atomic_recompute_preserves_preexisting_rows(monkeypatch) -> None:
    """A failure after DELETE rolls the deletion and prior inserts back."""
    old = [
        ("AAPL", date(2026, 7, 30), "old-1"),
        ("AAPL", date(2026, 7, 31), "old-2"),
    ]
    conn = _TransactionalConnection(old, fail_on_insert=2)
    monkeypatch.setattr(ta_backfill.psycopg, "connect", lambda _url: conn)
    monkeypatch.setattr(
        ta_backfill,
        "get_prices_for_ticker",
        lambda *_args, **_kwargs: [
            {"date": date(2026, 8, 2)},
            {"date": date(2026, 8, 3)},
        ],
    )
    monkeypatch.setattr(
        ta_backfill,
        "calculate_indicators_for_ticker",
        lambda *_args, **_kwargs: [_indicator(2), _indicator(3)],
    )
    ta_backfill._init_worker("offline-test", replace_existing=True)

    result = ta_backfill._process_ticker("AAPL")

    assert result["count"] == 0
    assert "technical indicator write failed" in result["error"]
    assert conn.committed == old
    assert conn.pending == old
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_skipped_calculation_date_never_deletes_old_series(monkeypatch) -> None:
    old = [
        ("AAPL", date(2026, 7, 30), "old-1"),
        ("AAPL", date(2026, 7, 31), "old-2"),
    ]
    conn = _TransactionalConnection(old, fail_on_insert=999)
    prices = [
        {"date": date(2026, 8, 2)},
        {"date": date(2026, 8, 3)},
    ]
    monkeypatch.setattr(ta_backfill.psycopg, "connect", lambda _url: conn)
    monkeypatch.setattr(
        ta_backfill,
        "get_prices_for_ticker",
        lambda *_args, **_kwargs: prices,
    )
    # Simulate ta_engine isolating a validation error by skipping one date.
    monkeypatch.setattr(
        ta_backfill,
        "calculate_indicators_for_ticker",
        lambda *_args, **_kwargs: [_indicator(2)],
    )
    ta_backfill._init_worker("offline-test", replace_existing=True)

    result = ta_backfill._process_ticker("AAPL")

    assert result["count"] == 0
    assert "incomplete or mismatched series" in result["error"]
    assert conn.committed == old
    assert conn.pending == old
    assert conn.insert_attempts == 0
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_successful_atomic_recompute_commits_delete_and_replacement(monkeypatch) -> None:
    old = [("AAPL", date(2026, 7, 31), "old")]
    conn = _TransactionalConnection(old, fail_on_insert=999)
    monkeypatch.setattr(ta_backfill.psycopg, "connect", lambda _url: conn)
    monkeypatch.setattr(
        ta_backfill,
        "get_prices_for_ticker",
        lambda *_args, **_kwargs: [
            {"date": date(2026, 8, 2)},
            {"date": date(2026, 8, 3)},
        ],
    )
    monkeypatch.setattr(
        ta_backfill,
        "calculate_indicators_for_ticker",
        lambda *_args, **_kwargs: [_indicator(2), _indicator(3)],
    )
    ta_backfill._init_worker("offline-test", replace_existing=True)

    result = ta_backfill._process_ticker("AAPL")

    assert result["count"] == 2
    assert result["deleted"] == 1
    assert conn.committed == [
        ("AAPL", date(2026, 8, 2), None),
        ("AAPL", date(2026, 8, 3), None),
    ]
    assert conn.commits == 1
    assert conn.rollbacks == 0


class _MetadataCursor:
    def __enter__(self) -> _MetadataCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, *_args: object, **_kwargs: object) -> None:
        return None

    def fetchone(self) -> tuple[bool]:
        return (True,)


class _MetadataConnection:
    def __enter__(self) -> _MetadataConnection:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def cursor(self) -> _MetadataCursor:
        return _MetadataCursor()


def test_backfill_is_unsuccessful_when_any_ticker_fails(monkeypatch) -> None:
    monkeypatch.setattr(
        ta_backfill.psycopg,
        "connect",
        lambda _url: _MetadataConnection(),
    )
    monkeypatch.setattr(ta_backfill, "get_ta_count", lambda _conn: 17)

    def process(ticker: str) -> dict[str, Any]:
        if ticker == "FAIL":
            return {"ticker": ticker, "count": 0, "error": "write failed", "time": 0}
        return {"ticker": ticker, "count": 2, "deleted": 1, "time": 0}

    monkeypatch.setattr(ta_backfill, "_process_ticker", process)

    stats = ta_backfill.run_ta_backfill(
        "offline-test",
        tickers=["GOOD", "FAIL"],
        workers=1,
        log=logging.getLogger(__name__),
    )

    assert stats["success"] is False
    assert stats["tickers_succeeded"] == 1
    assert stats["tickers_failed"] == 1
    assert stats["ticker_errors"] == [{"ticker": "FAIL", "error": "write failed"}]


def test_recompute_requests_atomic_replace_and_surfaces_failure(monkeypatch) -> None:
    run = mock.Mock(
        return_value={
            "success": False,
            "tickers_failed": 1,
            "ticker_errors": [{"ticker": "AAPL", "error": "write failed"}],
            "deleted": 0,
        }
    )
    monkeypatch.setattr(ta_backfill, "run_ta_backfill", run)

    stats = ta_backfill.recompute_ta_for_tickers(
        "offline-test",
        ["aapl", "AAPL"],
        log=logging.getLogger(__name__),
    )

    assert stats["success"] is False
    assert stats["ticker_errors"][0]["ticker"] == "AAPL"
    assert run.call_args.kwargs["tickers"] == ["AAPL"]
    assert run.call_args.kwargs["replace_existing"] is True


def test_daily_split_heal_turns_nested_recompute_failure_into_error() -> None:
    with mock.patch(
        "sawa.corporate_actions.run_corporate_actions_update",
        return_value={
            "success": True,
            "splits_loaded": 1,
            "split_tickers": ["AAPL"],
        },
    ), mock.patch(
        "sawa.split_adjust.refresh_split_adjusted_prices",
        return_value={"success": True, "prices_updated": 500},
    ), mock.patch(
        "sawa.ta_backfill.recompute_ta_for_tickers",
        return_value={"success": False, "tickers_failed": 1},
    ):
        stats: dict[str, Any] = {}
        with pytest.raises(RuntimeError, match="split TA recompute failed"):
            daily._heal_splits_in_window(
                "offline-key",
                "offline-db",
                date(2026, 8, 1),
                logging.getLogger(__name__),
                stats,
            )

    assert stats["split_heal"]["ta_recompute"]["success"] is False
