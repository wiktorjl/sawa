"""Offline regressions for atomic stock-character persistence and status."""

from __future__ import annotations

import logging
from datetime import date
from types import SimpleNamespace
from typing import Any

from sawa import stock_character_batch
from sawa.database import stock_character as stock_character_db
from sawa.domain.stock_character import CharacterFlag

RUN_DATE = date(2026, 8, 28)


class _Cursor:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection
        self.results: list[tuple[object, ...]] = []

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, statement: object, params: object = None, **_kwargs: object) -> None:
        rendered = str(statement)
        if "FROM stock_prices" in rendered:
            self.results = [(RUN_DATE, 1, 2, 0.5, 1.5, 100)]
        elif "DELETE FROM public.stock_character_flags" in rendered:
            ticker, run_date = params  # type: ignore[misc]
            self.connection.working_flags = {
                identity
                for identity in self.connection.working_flags
                if identity[:2] != (ticker, run_date)
            }
        elif "INSERT INTO" in rendered and "stock_character_flags" in rendered:
            values = params  # type: ignore[assignment]
            self.connection.working_flags.add((values[0], values[1], values[2]))
        elif "SELECT flag FROM public.stock_character_flags" in rendered:
            ticker, run_date = params  # type: ignore[misc]
            self.results = [
                (flag,)
                for stored_ticker, stored_date, flag in sorted(
                    self.connection.working_flags
                )
                if (stored_ticker, stored_date) == (ticker, run_date)
            ]

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.results


class _Connection:
    def __init__(
        self,
        initial_flags: set[tuple[str, date, str]] | None = None,
    ) -> None:
        self.commits = 0
        self.rollbacks = 0
        self.committed_flags = set(initial_flags or set())
        self.working_flags = set(self.committed_flags)

    def __enter__(self) -> _Connection:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _traceback: object,
    ) -> None:
        if exc_type is not None:
            self.rollback()

    def cursor(self) -> _Cursor:
        return _Cursor(self)

    def commit(self) -> None:
        self.commits += 1
        self.committed_flags = set(self.working_flags)

    def rollback(self) -> None:
        self.rollbacks += 1
        self.working_flags = set(self.committed_flags)


def _analysis(
    *,
    flags: list[CharacterFlag] | None = None,
    scorecard_count: int | None = None,
    scorecard_flags: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    computed_flags = flags or []
    return {
        "classification": SimpleNamespace(
            ticker="AAPL",
            run_date=RUN_DATE,
            character="trend",
            confidence=0.9,
        ),
        "baseline": object(),
        "flags": computed_flags,
        "scorecard": SimpleNamespace(
            flag_count=(len(computed_flags) if scorecard_count is None else scorecard_count),
            flags=(
                tuple(flag.flag for flag in computed_flags)
                if scorecard_flags is None
                else scorecard_flags
            ),
        ),
    }


def _prepare_worker(monkeypatch, conn: _Connection, result: dict[str, Any]) -> None:
    monkeypatch.setattr(stock_character_batch.psycopg, "connect", lambda _url: conn)
    monkeypatch.setattr(stock_character_batch, "analyze_stock", lambda *_args: result)
    stock_character_batch._init_worker("offline-test", {}, RUN_DATE)


def test_ticker_write_count_mismatch_rolls_back_all_artifacts(monkeypatch) -> None:
    conn = _Connection()
    flag = CharacterFlag("AAPL", RUN_DATE, "FLAG_X")
    _prepare_worker(monkeypatch, conn, _analysis(flags=[flag]))
    monkeypatch.setattr(stock_character_batch, "load_classification", lambda *_a, **_k: 1)
    monkeypatch.setattr(stock_character_batch, "load_baseline", lambda *_a, **_k: 0)
    monkeypatch.setattr(stock_character_batch, "replace_flags", lambda *_a, **_k: 1)
    monkeypatch.setattr(stock_character_batch, "load_scorecard", lambda *_a, **_k: 1)

    result = stock_character_batch._process_ticker("AAPL")

    assert result["classified"] is False
    assert "baseline persisted only 0/1" in result["error"]
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_zero_flags_is_a_successful_exact_write(monkeypatch) -> None:
    old_identity = ("AAPL", RUN_DATE, "FLAG_X")
    conn = _Connection({old_identity})
    _prepare_worker(monkeypatch, conn, _analysis(flags=[]))
    monkeypatch.setattr(stock_character_batch, "load_classification", lambda *_a, **_k: 1)
    monkeypatch.setattr(stock_character_batch, "load_baseline", lambda *_a, **_k: 1)
    monkeypatch.setattr(stock_character_batch, "load_scorecard", lambda *_a, **_k: 1)

    result = stock_character_batch._process_ticker("AAPL")

    assert result["classified"] is True
    assert conn.commits == 1
    assert conn.rollbacks == 0
    assert conn.committed_flags == set()


def test_rerun_replaces_old_flags_with_exact_new_set(monkeypatch) -> None:
    old_identity = ("AAPL", RUN_DATE, "OLD_FLAG")
    conn = _Connection({old_identity})
    new_flags = [
        CharacterFlag("AAPL", RUN_DATE, "NEW_A"),
        CharacterFlag("AAPL", RUN_DATE, "NEW_B"),
    ]
    _prepare_worker(monkeypatch, conn, _analysis(flags=new_flags))
    monkeypatch.setattr(stock_character_batch, "load_classification", lambda *_a, **_k: 1)
    monkeypatch.setattr(stock_character_batch, "load_baseline", lambda *_a, **_k: 1)
    monkeypatch.setattr(stock_character_batch, "load_scorecard", lambda *_a, **_k: 1)

    result = stock_character_batch._process_ticker("AAPL")

    assert result["classified"] is True
    assert conn.committed_flags == {
        ("AAPL", RUN_DATE, "NEW_A"),
        ("AAPL", RUN_DATE, "NEW_B"),
    }


def test_flag_postverification_mismatch_rolls_back_old_set(monkeypatch) -> None:
    old_identity = ("AAPL", RUN_DATE, "OLD_FLAG")
    conn = _Connection({old_identity})
    new_flag = CharacterFlag("AAPL", RUN_DATE, "NEW_FLAG")
    _prepare_worker(monkeypatch, conn, _analysis(flags=[new_flag]))
    monkeypatch.setattr(stock_character_batch, "load_classification", lambda *_a, **_k: 1)
    monkeypatch.setattr(stock_character_batch, "load_baseline", lambda *_a, **_k: 1)
    monkeypatch.setattr(stock_character_db, "load_flags", lambda *_a, **_k: 1)
    monkeypatch.setattr(stock_character_batch, "load_scorecard", lambda *_a, **_k: 1)

    result = stock_character_batch._process_ticker("AAPL")

    assert result["classified"] is False
    assert "verification failed" in result["error"]
    assert conn.committed_flags == {old_identity}
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_later_scorecard_failure_rolls_back_flag_delete(monkeypatch) -> None:
    old_identity = ("AAPL", RUN_DATE, "OLD_FLAG")
    conn = _Connection({old_identity})
    _prepare_worker(monkeypatch, conn, _analysis(flags=[]))
    monkeypatch.setattr(stock_character_batch, "load_classification", lambda *_a, **_k: 1)
    monkeypatch.setattr(stock_character_batch, "load_baseline", lambda *_a, **_k: 1)

    def fail_scorecard(*_args: object, **_kwargs: object) -> int:
        raise RuntimeError("scorecard write failed")

    monkeypatch.setattr(stock_character_batch, "load_scorecard", fail_scorecard)

    result = stock_character_batch._process_ticker("AAPL")

    assert result["classified"] is False
    assert conn.committed_flags == {old_identity}
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_scorecard_flag_count_must_match_computed_set(monkeypatch) -> None:
    conn = _Connection()
    flag = CharacterFlag("AAPL", RUN_DATE, "FLAG_X")
    _prepare_worker(
        monkeypatch,
        conn,
        _analysis(flags=[flag], scorecard_count=0),
    )

    result = stock_character_batch._process_ticker("AAPL")

    assert result["classified"] is False
    assert "scorecard flag_count" in result["error"]
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_scorecard_flag_names_must_match_computed_set(monkeypatch) -> None:
    conn = _Connection()
    flag = CharacterFlag("AAPL", RUN_DATE, "FLAG_X")
    _prepare_worker(
        monkeypatch,
        conn,
        _analysis(flags=[flag], scorecard_flags=("DIFFERENT_FLAG",)),
    )

    result = stock_character_batch._process_ticker("AAPL")

    assert result["classified"] is False
    assert "flag identities" in result["error"]
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_ticker_error_is_typed_and_redacted(monkeypatch) -> None:
    conn = _Connection()
    _prepare_worker(monkeypatch, conn, _analysis())
    secret = "character-secret"

    def fail(*_args: object, **_kwargs: object) -> int:
        raise RuntimeError(f"postgresql://user:{secret}@db/sawa")

    monkeypatch.setattr(stock_character_batch, "load_classification", fail)

    result = stock_character_batch._process_ticker("AAPL")

    assert result["classified"] is False
    assert result["error"].startswith("RuntimeError:")
    assert secret not in result["error"]
    assert conn.rollbacks == 1


def test_empty_batch_is_unsuccessful_without_division_by_zero(monkeypatch) -> None:
    monkeypatch.setattr(
        stock_character_batch.psycopg,
        "connect",
        lambda _url: _Connection(),
    )
    monkeypatch.setattr(stock_character_batch, "_fetch_benchmark_prices", lambda _url: {})

    stats = stock_character_batch.run_stock_character_batch(
        "offline-test",
        tickers=[],
        workers=1,
        log=logging.getLogger(__name__),
    )

    assert stats["success"] is False
    assert stats["total"] == 0


def test_batch_is_unsuccessful_when_any_ticker_errors(monkeypatch) -> None:
    monkeypatch.setattr(
        stock_character_batch.psycopg,
        "connect",
        lambda _url: _Connection(),
    )
    monkeypatch.setattr(stock_character_batch, "_fetch_benchmark_prices", lambda _url: {})
    monkeypatch.setattr(
        stock_character_batch,
        "_process_ticker",
        lambda ticker: (
            {"ticker": ticker, "classified": True, "character": "trend", "time": 0}
            if ticker == "GOOD"
            else {"ticker": ticker, "classified": False, "error": "write failed", "time": 0}
        ),
    )

    stats = stock_character_batch.run_stock_character_batch(
        "offline-test",
        tickers=["GOOD", "FAIL"],
        workers=1,
        log=logging.getLogger(__name__),
    )

    assert stats["success"] is False
    assert stats["degraded"] is True
    assert stats["errors"] == 1
    assert stats["ticker_errors"] == [{"ticker": "FAIL", "error": "write failed"}]


def test_batch_deduplicates_normalized_explicit_tickers(monkeypatch) -> None:
    monkeypatch.setattr(
        stock_character_batch.psycopg,
        "connect",
        lambda _url: _Connection(),
    )
    monkeypatch.setattr(stock_character_batch, "_fetch_benchmark_prices", lambda _url: {})
    processed: list[str] = []

    def process(ticker: str) -> dict[str, Any]:
        processed.append(ticker)
        return {
            "ticker": ticker,
            "classified": True,
            "character": "trend",
            "time": 0,
        }

    monkeypatch.setattr(stock_character_batch, "_process_ticker", process)

    stats = stock_character_batch.run_stock_character_batch(
        "offline-test",
        tickers=["aapl", "AAPL", "msft", "MSFT"],
        workers=1,
        log=logging.getLogger(__name__),
    )

    assert stats["success"] is True
    assert stats["total"] == 2
    assert processed == ["AAPL", "MSFT"]
