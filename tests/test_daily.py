import logging
import math
from datetime import date
from decimal import Decimal
from typing import Any
from unittest import mock

import pytest

from sawa import daily
from sawa.daily import (
    _heal_splits_in_window,
    _is_valid_price_row,
    _last_date_coverage,
    fetch_prices_via_api,
    insert_prices,
    refresh_52week_extremes_if_needed,
)
from sawa.database.intraday_load import cleanup_today_intraday_data


class FakeCursor:
    def __init__(self, rows: list[Any]) -> None:
        self.rows = rows
        self.statements: list[str] = []
        self.params: list[Any] = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, query: str, params: Any = None) -> None:
        self.statements.append(query)
        if params is not None:
            self.params.append(params)

    def fetchone(self) -> Any:
        return self.rows.pop(0)


class FakeConnection:
    def __init__(self, rows: list[Any]) -> None:
        self.cursor_obj = FakeCursor(rows)
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> FakeCursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def test_refresh_52week_extremes_when_stale() -> None:
    conn = FakeConnection(
        [
            ("mv_52week_extremes",),
            (date(2026, 4, 24), date(2026, 2, 25)),
        ]
    )

    refreshed = refresh_52week_extremes_if_needed(conn, logging.getLogger(__name__))

    assert refreshed is True
    assert conn.commits == 1
    assert conn.cursor_obj.statements[-1] == "REFRESH MATERIALIZED VIEW mv_52week_extremes"


def test_refresh_52week_extremes_skips_when_current() -> None:
    conn = FakeConnection(
        [
            ("mv_52week_extremes",),
            (date(2026, 4, 24), date(2026, 4, 24)),
        ]
    )

    refreshed = refresh_52week_extremes_if_needed(conn, logging.getLogger(__name__))

    assert refreshed is False
    assert conn.commits == 0
    assert "REFRESH MATERIALIZED VIEW mv_52week_extremes" not in conn.cursor_obj.statements


def test_last_date_coverage_returns_latest_and_baseline() -> None:
    conn = FakeConnection([(5457, 10274)])
    last_date = date(2026, 5, 15)

    latest, baseline = _last_date_coverage(conn, last_date)

    assert (latest, baseline) == (5457, 10274)
    # Both placeholders bound to the same date (latest subquery + prior_dates filter).
    assert conn.cursor_obj.params == [(last_date, last_date)]


def test_last_date_coverage_handles_empty_rows() -> None:
    conn = FakeConnection([(None, None)])

    assert _last_date_coverage(conn, date(2026, 5, 15)) == (0, 0)


def _price_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "ticker": "AAPL",
        "date": "2026-06-12",
        "open": 100,
        "high": 102,
        "low": 99,
        "close": 101,
        "volume": 100,
    }
    row.update(overrides)
    return row


def test_price_validation_rejects_nonfinite_and_inconsistent_ohlc() -> None:
    assert _is_valid_price_row(_price_row()) is True
    for invalid in (
        _price_row(open=math.nan),
        _price_row(high=math.inf),
        _price_row(low=-math.inf),
        _price_row(open=103),
        _price_row(close=98),
        _price_row(volume=1.5),
        _price_row(open=True),
        _price_row(volume=True),
        _price_row(open="100"),
        _price_row(open=Decimal("999999999999.999999995")),
    ):
        assert _is_valid_price_row(invalid) is False


def test_all_invalid_rows_open_no_cursor_and_commit_nothing() -> None:
    conn = FakeConnection([])

    inserted = insert_prices(
        conn,
        [_price_row(open=math.nan), _price_row(open=103)],
        logging.getLogger(__name__),
    )

    assert inserted == 0
    assert conn.cursor_obj.statements == []
    assert conn.commits == 0


def test_fetch_rejects_malformed_rows_before_reporting_fetched() -> None:
    class MalformedClient:
        def get(self, *args: object, **kwargs: object) -> dict[str, Any]:
            return {
                "results": [
                    {
                        "t": 1781265600000,
                        "o": 1,
                        "h": 2,
                        "l": 1,
                        "c": 2,
                        "v": 100,
                    },
                    {
                        "t": 1781352000000,
                        "o": math.nan,
                        "h": 2,
                        "l": 1,
                        "c": 2,
                        "v": 100,
                    },
                    42,
                ]
            }

    stats: dict[str, Any] = {}
    prices = fetch_prices_via_api(
        MalformedClient(),  # type: ignore[arg-type]
        ["AAPL"],
        "2026-06-12",
        "2026-06-13",
        logging.getLogger(__name__),
        stats=stats,
    )

    assert len(prices) == 1
    assert stats == {
        "requested_symbols": 1,
        "succeeded_symbols": 1,
        "failed_symbols": 0,
        "failed_tickers": [],
        "empty_tickers": [],
        "provider_price_rows": 3,
        "invalid_price_rows": 2,
    }


def test_fetch_rejects_bool_and_out_of_range_timestamps() -> None:
    class TimestampClient:
        def get(self, *args: object, **kwargs: object) -> dict[str, Any]:
            base = {"o": 1, "h": 2, "l": 1, "c": 2, "v": 100}
            return {
                "results": [
                    {**base, "t": True},
                    {**base, "t": 1749700800000},
                    {**base, "t": 1781265600000},
                ]
            }

    stats: dict[str, Any] = {}
    prices = fetch_prices_via_api(
        TimestampClient(),  # type: ignore[arg-type]
        ["AAPL"],
        "2026-06-12",
        "2026-06-12",
        logging.getLogger(__name__),
        stats=stats,
    )

    assert [price["date"] for price in prices] == ["2026-06-12"]
    assert stats == {
        "requested_symbols": 1,
        "succeeded_symbols": 1,
        "failed_symbols": 0,
        "failed_tickers": [],
        "empty_tickers": [],
        "provider_price_rows": 3,
        "invalid_price_rows": 2,
    }


def test_heal_splits_adjusts_and_recomputes_when_split_in_window() -> None:
    with mock.patch(
        "sawa.corporate_actions.run_corporate_actions_update",
        return_value={"success": True, "splits_loaded": 1, "split_tickers": ["KLAC"]},
    ), mock.patch(
        "sawa.split_adjust.refresh_split_adjusted_prices",
        return_value={"success": True, "prices_updated": 500},
    ) as madj, mock.patch(
        "sawa.ta_backfill.recompute_ta_for_tickers",
        return_value={"success": True, "deleted": 200, "indicators_calculated": 210},
    ) as mrec:
        stats: dict[str, Any] = {}
        _heal_splits_in_window(
            "k", "db", date(2026, 6, 10), logging.getLogger(__name__), stats
        )

    assert madj.called
    assert mrec.call_args.kwargs["tickers"] == ["KLAC"]
    assert stats["split_heal"]["ta_recompute"]["indicators_calculated"] == 210


def test_heal_splits_stops_before_ta_when_adjustment_is_incomplete() -> None:
    with mock.patch(
        "sawa.corporate_actions.run_corporate_actions_update",
        return_value={"success": True, "splits_loaded": 1, "split_tickers": ["KLAC"]},
    ), mock.patch(
        "sawa.split_adjust.refresh_split_adjusted_prices",
        return_value={
            "success": False,
            "tickers_requested": 1,
            "tickers_adjusted": 0,
        },
    ), mock.patch("sawa.ta_backfill.recompute_ta_for_tickers") as recompute:
        with pytest.raises(RuntimeError, match="incomplete result"):
            _heal_splits_in_window(
                "k", "db", date(2026, 6, 10), logging.getLogger(__name__), {}
            )

    recompute.assert_not_called()


def test_price_insert_later_batch_failure_rolls_back_every_row() -> None:
    class FailingCursor:
        def __init__(self, conn: "FailingConnection") -> None:
            self.conn = conn

        def __enter__(self) -> "FailingCursor":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, _query: object, params: object = None) -> None:
            self.conn.attempts += 1
            if self.conn.attempts == 1001:
                raise RuntimeError("forced later-batch failure")
            self.conn.pending.append(params)

    class FailingConnection:
        def __init__(self) -> None:
            self.pending: list[object] = []
            self.committed: list[object] = []
            self.attempts = 0
            self.commits = 0
            self.rollbacks = 0

        def cursor(self) -> "FailingCursor":
            return FailingCursor(self)

        def commit(self) -> None:
            self.committed = list(self.pending)
            self.commits += 1

        def rollback(self) -> None:
            self.pending = list(self.committed)
            self.rollbacks += 1

    conn = FailingConnection()
    rows = [_price_row(date=f"2026-01-{index:02d}") for index in range(1, 1002)]

    with pytest.raises(RuntimeError, match="later-batch"):
        insert_prices(conn, rows, logging.getLogger(__name__))

    assert conn.committed == []
    assert conn.pending == []
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_heal_splits_noop_when_no_split_in_window() -> None:
    with mock.patch(
        "sawa.corporate_actions.run_corporate_actions_update",
        return_value={"success": True, "splits_loaded": 0, "split_tickers": []},
    ), mock.patch("sawa.split_adjust.refresh_split_adjusted_prices") as madj, mock.patch(
        "sawa.ta_backfill.recompute_ta_for_tickers"
    ) as mrec:
        stats: dict[str, Any] = {}
        _heal_splits_in_window(
            "k", "db", date(2026, 6, 10), logging.getLogger(__name__), stats
        )

    assert not madj.called
    assert not mrec.called
    assert stats["split_heal"] == {"splits_loaded": 0}


def _daily_psycopg_conn() -> Any:
    class Cur:
        rowcount = 0

        def __enter__(self) -> "Cur":
            return self

        def __exit__(self, *a: object) -> None:
            return None

        def execute(self, *a: object, **k: object) -> None:
            return None

        def fetchone(self) -> Any:
            return (True,)

        def fetchall(self) -> list[Any]:
            return []

    class Conn:
        def __enter__(self) -> "Conn":
            return self

        def __exit__(self, *a: object) -> None:
            return None

        def cursor(self) -> Cur:
            return Cur()

        def commit(self) -> None:
            return None

        def rollback(self) -> None:
            return None

    return Conn()


def test_news_only_cannot_also_skip_news() -> None:
    with mock.patch.object(daily, "PolygonClient") as client, pytest.raises(
        ValueError, match="cannot also skip news"
    ):
        daily.run_daily(
            api_key="k",
            database_url="offline-db",
            skip_news=True,
            skip_prices=True,
            skip_ta=True,
            news_only=True,
        )

    client.assert_not_called()


def test_empty_aapl_probe_does_not_skip_universe_fetch(monkeypatch) -> None:
    """An empty get_trading_days (e.g. AAPL halt) must not skip all symbols."""

    class GateClient:
        def __init__(self, *a: object, **k: object) -> None:
            pass

        def get_trading_days(self, *a: object, **k: object) -> list[str]:
            return []  # empty AAPL proxy bar

        def get(self, *a: object, **k: object) -> dict[str, Any]:
            return {
                "results": [
                    {
                        "t": 1781265600000,
                        "o": 1,
                        "h": 2,
                        "l": 1,
                        "c": 2,
                        "v": 100,
                    }
                ]
            }

    monkeypatch.delenv("FRED_API_KEY", raising=False)
    with mock.patch("psycopg.connect", return_value=_daily_psycopg_conn()), mock.patch.object(
        daily, "PolygonClient", GateClient
    ), mock.patch.object(daily, "SyncRateLimiter"), mock.patch.object(
        daily, "get_symbols_from_db", return_value=["MSFT", "GOOG"]
    ), mock.patch.object(daily, "get_last_date", return_value=date(2026, 6, 11)), mock.patch.object(
        daily, "_last_date_coverage", return_value=(0, 0)
    ), mock.patch.object(
        daily, "get_market_date", return_value=date(2026, 6, 12)
    ), mock.patch.object(
        daily, "is_after_market_close", return_value=True
    ), mock.patch.object(daily, "insert_prices", return_value=2) as mins, mock.patch.object(
        daily, "refresh_52week_extremes_if_needed", return_value=False
    ), mock.patch(
        "sawa.corporate_actions.run_corporate_actions_update",
        return_value={"success": True, "splits_loaded": 0, "split_tickers": []},
    ), mock.patch.object(daily, "get_notifier"), mock.patch.object(daily, "alert_missing_api_key"):
        stats = daily.run_daily(
            api_key="k",
            database_url="db",
            skip_news=True,
            skip_ta=True,
            skip_market_internals=True,
            logger=logging.getLogger(__name__),
        )

    # Both non-AAPL symbols fetched + inserted despite the empty proxy probe.
    assert mins.called
    assert stats["prices_fetched"] == 2
    assert stats["success"] is True


def test_total_symbol_fetch_outage_is_failed_and_degraded(monkeypatch) -> None:
    class FailingClient:
        def __init__(self, *a: object, **k: object) -> None:
            pass

        def get_trading_days(self, *a: object, **k: object) -> list[str]:
            return ["2026-06-12"]

        def get(self, *a: object, **k: object) -> dict[str, Any]:
            raise RuntimeError("provider unavailable")

    monkeypatch.delenv("FRED_API_KEY", raising=False)
    with mock.patch("psycopg.connect", return_value=_daily_psycopg_conn()), mock.patch.object(
        daily, "PolygonClient", FailingClient
    ), mock.patch.object(daily, "SyncRateLimiter"), mock.patch.object(
        daily, "get_symbols_from_db", return_value=["MSFT", "GOOG"]
    ), mock.patch.object(
        daily, "get_last_date", return_value=date(2026, 6, 11)
    ), mock.patch.object(
        daily, "_last_date_coverage", return_value=(0, 0)
    ), mock.patch.object(
        daily, "get_market_date", return_value=date(2026, 6, 12)
    ), mock.patch.object(
        daily, "is_after_market_close", return_value=True
    ), mock.patch.object(
        daily, "insert_prices", return_value=0
    ), mock.patch.object(
        daily, "refresh_52week_extremes_if_needed", return_value=False
    ), mock.patch(
        "sawa.corporate_actions.run_corporate_actions_update",
        return_value={"success": True, "splits_loaded": 0, "split_tickers": []},
    ), mock.patch.object(daily, "get_notifier"):
        stats = daily.run_daily(
            api_key="k",
            database_url="test-only",
            skip_news=True,
            skip_ta=True,
            skip_market_internals=True,
            logger=logging.getLogger(__name__),
        )

    assert stats["fetch_errors"] == 2
    assert stats["prices_error"] == "all 2 symbol price requests failed"
    assert stats["degraded"] is True
    assert stats["success"] is False
    assert "price fetch failed" in stats["degraded_reasons"]


def test_partial_symbol_fetch_failure_is_visible_but_nonfatal(monkeypatch) -> None:
    class PartiallyFailingClient:
        def __init__(self, *a: object, **k: object) -> None:
            pass

        def get_trading_days(self, *a: object, **k: object) -> list[str]:
            return ["2026-06-12"]

        def get(self, *a: object, **k: object) -> dict[str, Any]:
            ticker = k["path_params"]["ticker"]
            if ticker == "GOOG":
                raise RuntimeError("isolated ticker failure")
            return {
                "results": [
                    {
                        "t": 1781265600000,
                        "o": 1,
                        "h": 2,
                        "l": 1,
                        "c": 2,
                        "v": 100,
                    }
                ]
            }

    monkeypatch.delenv("FRED_API_KEY", raising=False)
    with mock.patch("psycopg.connect", return_value=_daily_psycopg_conn()), mock.patch.object(
        daily, "PolygonClient", PartiallyFailingClient
    ), mock.patch.object(daily, "SyncRateLimiter"), mock.patch.object(
        daily, "get_symbols_from_db", return_value=["MSFT", "GOOG"]
    ), mock.patch.object(
        daily, "get_last_date", return_value=date(2026, 6, 11)
    ), mock.patch.object(
        daily, "_last_date_coverage", return_value=(99, 100)
    ), mock.patch.object(
        daily, "get_market_date", return_value=date(2026, 6, 12)
    ), mock.patch.object(
        daily, "is_after_market_close", return_value=True
    ), mock.patch.object(
        daily, "insert_prices", return_value=1
    ), mock.patch.object(
        daily, "_heal_splits_in_window"
    ), mock.patch.object(
        daily, "refresh_52week_extremes_if_needed", return_value=False
    ), mock.patch.object(daily, "get_notifier"):
        stats = daily.run_daily(
            api_key="k",
            database_url="test-only",
            skip_news=True,
            skip_ta=True,
            skip_market_internals=True,
            logger=logging.getLogger(__name__),
        )

    assert stats["fetch_errors"] == 1
    assert stats["prices_fetched"] == 1
    assert stats["degraded"] is True
    assert stats["success"] is True
    assert stats["fatal_reasons"] == []
    assert stats["degraded_reasons"] == ["price fetch failed for 1/2 symbols"]


def test_next_run_retries_full_stale_ticker_window_after_long_catchup(
    monkeypatch,
) -> None:
    class CalendarClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

        def get_trading_days(self, start: str, end: str) -> list[str]:
            return [end]

    fetch_calls: list[tuple[list[str], str, str, dict[str, str] | None]] = []

    def prices(
        client: object,
        symbols: list[str],
        start: str,
        end: str,
        logger: object,
        rate_limiter: object,
        stats: dict[str, Any],
        start_dates: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        fetch_calls.append((symbols, start, end, start_dates))
        if len(fetch_calls) == 1:
            stats["fetch_errors"] = 1
        return [_price_row(ticker="MSFT", date=end)]

    current_last = [date(2026, 6, 1)]
    current_market = [date(2026, 7, 1)]
    watermark_results = iter(
        [
            {"MSFT": date(2026, 6, 1), "GOOG": date(2026, 6, 1)},
            {"MSFT": date(2026, 7, 1), "GOOG": date(2026, 6, 1)},
        ]
    )
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    with mock.patch("psycopg.connect", return_value=_daily_psycopg_conn()), mock.patch.object(
        daily, "PolygonClient", CalendarClient
    ), mock.patch.object(daily, "SyncRateLimiter"), mock.patch.object(
        daily, "get_symbols_from_db", return_value=["MSFT", "GOOG"]
    ), mock.patch.object(
        daily, "get_last_date", side_effect=lambda *_args: current_last[0]
    ), mock.patch.object(
        daily, "_last_date_coverage", return_value=(2, 2)
    ), mock.patch.object(
        daily, "_symbol_price_watermarks", side_effect=lambda *_: next(watermark_results)
    ), mock.patch.object(
        daily, "get_market_date", side_effect=lambda: current_market[0]
    ), mock.patch.object(
        daily, "is_after_market_close", return_value=True
    ), mock.patch.object(
        daily, "fetch_prices_via_api", side_effect=prices
    ), mock.patch.object(
        daily, "insert_prices", return_value=1
    ), mock.patch.object(
        daily, "refresh_52week_extremes_if_needed", return_value=False
    ), mock.patch(
        "sawa.corporate_actions.run_corporate_actions_update",
        return_value={"success": True, "splits_loaded": 0, "split_tickers": []},
    ), mock.patch.object(daily, "get_notifier"), mock.patch.object(
        daily, "alert_missing_api_key"
    ):
        first = daily.run_daily(
            api_key="k",
            database_url="test-only",
            skip_news=True,
            skip_ta=True,
            skip_market_internals=True,
            logger=logging.getLogger(__name__),
        )
        current_last[0] = date(2026, 7, 1)
        current_market[0] = date(2026, 7, 2)
        second = daily.run_daily(
            api_key="k",
            database_url="test-only",
            skip_news=True,
            skip_ta=True,
            skip_market_internals=True,
            logger=logging.getLogger(__name__),
        )

    assert first["fetch_errors"] == 1
    assert first["success"] is True
    assert second["success"] is True
    assert fetch_calls == [
        (
            ["MSFT", "GOOG"],
            "2026-05-18",
            "2026-07-01",
            {"MSFT": "2026-05-18", "GOOG": "2026-05-18"},
        ),
        (
            ["MSFT", "GOOG"],
            "2026-06-01",
            "2026-07-02",
            {"MSFT": "2026-06-17", "GOOG": "2026-06-01"},
        ),
    ]


def test_stale_price_repair_widens_split_lookup_and_recomputes_ta_per_ticker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class CalendarClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

        def get_trading_days(self, _start: str, end: str) -> list[str]:
            return [end]

    class Indicator:
        def __init__(self, ticker: str, value_date: date) -> None:
            self.ticker = ticker
            self.date = value_date

    fetched = [
        _price_row(ticker="STALE", date="2026-05-02"),
        _price_row(ticker="CURRENT", date="2026-07-02"),
    ]
    price_starts: dict[str, date | None] = {}
    persisted: dict[str, list[date]] = {}

    def fetch_prices(*args: object, **kwargs: object) -> list[dict[str, Any]]:
        stats = kwargs["stats"]
        stats.update(
            requested_symbols=2,
            succeeded_symbols=2,
            failed_symbols=0,
            failed_tickers=[],
            empty_tickers=[],
        )
        return fetched

    def get_prices(
        _conn: object,
        ticker: str,
        start_date: date | None = None,
    ) -> list[dict[str, Any]]:
        price_starts[ticker] = start_date
        return [{"date": start_date or date(2020, 1, 1)}]

    def calculate(
        ticker: str,
        _prices: list[dict[str, Any]],
        *_args: object,
    ) -> list[Indicator]:
        if ticker == "STALE":
            return [
                Indicator(ticker, date(2026, 5, 2)),
                Indicator(ticker, date(2026, 7, 2)),
            ]
        return [
            Indicator(ticker, date(2026, 6, 15)),
            Indicator(ticker, date(2026, 7, 2)),
        ]

    def load_ta(
        _conn: object,
        indicators: list[Indicator],
        _logger: object,
    ) -> int:
        persisted[indicators[0].ticker] = [row.date for row in indicators]
        return len(indicators)

    monkeypatch.delenv("FRED_API_KEY", raising=False)
    with (
        mock.patch("psycopg.connect", return_value=_daily_psycopg_conn()),
        mock.patch.object(daily, "PolygonClient", CalendarClient),
        mock.patch.object(daily, "SyncRateLimiter"),
        mock.patch.object(daily, "get_symbols_from_db", return_value=["STALE", "CURRENT"]),
        mock.patch.object(daily, "get_last_date", return_value=date(2026, 7, 1)),
        mock.patch.object(daily, "_last_date_coverage", return_value=(2, 2)),
        mock.patch.object(
            daily,
            "_symbol_price_watermarks",
            return_value={
                "STALE": date(2026, 5, 1),
                "CURRENT": date(2026, 7, 1),
            },
        ),
        mock.patch.object(daily, "get_market_date", return_value=date(2026, 7, 2)),
        mock.patch.object(daily, "is_after_market_close", return_value=True),
        mock.patch.object(daily, "fetch_prices_via_api", side_effect=fetch_prices),
        mock.patch.object(daily, "insert_prices", return_value=2),
        mock.patch.object(daily, "_heal_splits_in_window") as heal,
        mock.patch.object(daily, "refresh_52week_extremes_if_needed", return_value=False),
        mock.patch(
            "sawa.calculation.ta_engine.get_required_lookback_days",
            return_value=5,
        ),
        mock.patch(
            "sawa.calculation.ta_engine.calculate_indicators_for_ticker",
            side_effect=calculate,
        ),
        mock.patch(
            "sawa.database.ta_load.get_last_ta_date",
            return_value=date(2026, 7, 1),
        ),
        mock.patch(
            "sawa.database.ta_load.get_prices_for_ticker",
            side_effect=get_prices,
        ),
        mock.patch("sawa.database.ta_load.get_cumulative_indicator_seed"),
        mock.patch(
            "sawa.database.ta_load.load_technical_indicators",
            side_effect=load_ta,
        ),
        mock.patch.object(daily, "get_notifier"),
    ):
        stats = daily.run_daily(
            api_key="k",
            database_url="test-only",
            skip_news=True,
            skip_market_internals=True,
            logger=logging.getLogger(__name__),
        )

    assert stats["success"] is True
    assert heal.call_args.args[2] == date(2026, 5, 1)
    assert price_starts == {
        "STALE": date(2026, 4, 27),
        "CURRENT": date(2026, 6, 27),
    }
    assert persisted == {
        "STALE": [date(2026, 5, 2), date(2026, 7, 2)],
        "CURRENT": [date(2026, 7, 2)],
    }


def test_forced_historical_partial_failure_is_fatal(monkeypatch) -> None:
    class CalendarClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

        def get_trading_days(self, *args: object) -> list[str]:
            return ["2026-06-12"]

    def partial_prices(*args: object, **kwargs: object) -> list[dict[str, Any]]:
        kwargs["stats"]["fetch_errors"] = 1
        return [_price_row(ticker="MSFT")]

    monkeypatch.delenv("FRED_API_KEY", raising=False)
    with mock.patch("psycopg.connect", return_value=_daily_psycopg_conn()), mock.patch.object(
        daily, "PolygonClient", CalendarClient
    ), mock.patch.object(daily, "SyncRateLimiter"), mock.patch.object(
        daily, "get_symbols_from_db", return_value=["MSFT", "GOOG"]
    ), mock.patch.object(
        daily, "get_last_date", return_value=date(2026, 6, 11)
    ), mock.patch.object(
        daily, "get_market_date", return_value=date(2026, 6, 12)
    ), mock.patch.object(
        daily, "_last_date_coverage", return_value=(99, 100)
    ), mock.patch.object(
        daily, "is_after_market_close", return_value=True
    ), mock.patch.object(
        daily, "fetch_prices_via_api", side_effect=partial_prices
    ), mock.patch.object(
        daily, "insert_prices", return_value=1
    ), mock.patch.object(
        daily, "refresh_52week_extremes_if_needed", return_value=False
    ), mock.patch(
        "sawa.corporate_actions.run_corporate_actions_update",
        return_value={"success": True, "splits_loaded": 0, "split_tickers": []},
    ), mock.patch.object(daily, "get_notifier"), mock.patch.object(
        daily, "alert_missing_api_key"
    ):
        stats = daily.run_daily(
            api_key="k",
            database_url="test-only",
            force_from_date=date(2026, 6, 1),
            skip_news=True,
            skip_ta=True,
            skip_market_internals=True,
            logger=logging.getLogger(__name__),
        )

    assert stats["prices_error"] == (
        "forced historical update failed for 1/2 symbol price requests"
    )
    assert stats["success"] is False


def test_split_self_heal_failure_is_explicitly_degraded(monkeypatch) -> None:
    class CalendarClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

        def get_trading_days(self, *args: object) -> list[str]:
            return ["2026-06-12"]

    monkeypatch.delenv("FRED_API_KEY", raising=False)
    with mock.patch("psycopg.connect", return_value=_daily_psycopg_conn()), mock.patch.object(
        daily, "PolygonClient", CalendarClient
    ), mock.patch.object(daily, "SyncRateLimiter"), mock.patch.object(
        daily, "get_symbols_from_db", return_value=["MSFT"]
    ), mock.patch.object(
        daily, "get_last_date", return_value=date(2026, 6, 11)
    ), mock.patch.object(
        daily, "_last_date_coverage", return_value=(1, 1)
    ), mock.patch.object(
        daily, "get_market_date", return_value=date(2026, 6, 12)
    ), mock.patch.object(
        daily, "is_after_market_close", return_value=True
    ), mock.patch.object(
        daily, "fetch_prices_via_api", return_value=[_price_row(ticker="MSFT")]
    ), mock.patch.object(
        daily, "insert_prices", return_value=1
    ), mock.patch.object(
        daily, "_heal_splits_in_window", side_effect=RuntimeError("split repair failed")
    ), mock.patch.object(
        daily, "refresh_52week_extremes_if_needed", return_value=False
    ), mock.patch.object(daily, "get_notifier"):
        stats = daily.run_daily(
            api_key="k",
            database_url="test-only",
            skip_news=True,
            skip_ta=True,
            skip_market_internals=True,
            logger=logging.getLogger(__name__),
        )

    assert stats["success"] is True
    assert stats["degraded"] is True
    assert stats["split_heal_error"] == "RuntimeError: split repair failed"
    assert "split adjustment/TA self-heal failed" in stats["degraded_reasons"]


def test_empty_price_result_on_reported_trading_day_is_fatal(monkeypatch) -> None:
    class EmptyClient:
        def __init__(self, *a: object, **k: object) -> None:
            pass

        def get_trading_days(self, *a: object, **k: object) -> list[str]:
            return ["2026-06-12"]

        def get(self, *a: object, **k: object) -> dict[str, Any]:
            return {"results": []}

    monkeypatch.delenv("FRED_API_KEY", raising=False)
    with mock.patch("psycopg.connect", return_value=_daily_psycopg_conn()), mock.patch.object(
        daily, "PolygonClient", EmptyClient
    ), mock.patch.object(daily, "SyncRateLimiter"), mock.patch.object(
        daily, "get_symbols_from_db", return_value=["MSFT"]
    ), mock.patch.object(
        daily, "get_last_date", return_value=date(2026, 6, 11)
    ), mock.patch.object(
        daily, "_last_date_coverage", return_value=(0, 0)
    ), mock.patch.object(
        daily, "get_market_date", return_value=date(2026, 6, 12)
    ), mock.patch.object(
        daily, "is_after_market_close", return_value=True
    ), mock.patch.object(
        daily, "insert_prices", return_value=0
    ), mock.patch.object(
        daily, "refresh_52week_extremes_if_needed", return_value=False
    ), mock.patch.object(daily, "get_notifier"):
        stats = daily.run_daily(
            api_key="k",
            database_url="test-only",
            skip_news=True,
            skip_ta=True,
            skip_market_internals=True,
            logger=logging.getLogger(__name__),
        )

    assert stats["prices_error"].startswith("no price records returned")
    assert stats["success"] is False
    assert stats["fatal_reasons"] == ["required price update failed"]


def test_forced_run_fails_when_every_provider_price_row_is_malformed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class MalformedClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

        def get_trading_days(self, *args: object, **kwargs: object) -> list[str]:
            return []

        def get(self, *args: object, **kwargs: object) -> dict[str, Any]:
            return {
                "results": [
                    {"t": True, "o": 1, "h": 2, "l": 1, "c": 2, "v": 100}
                ]
            }

    monkeypatch.delenv("FRED_API_KEY", raising=False)
    with (
        mock.patch("psycopg.connect", return_value=_daily_psycopg_conn()),
        mock.patch.object(daily, "PolygonClient", MalformedClient),
        mock.patch.object(daily, "SyncRateLimiter"),
        mock.patch.object(daily, "get_symbols_from_db", return_value=["MSFT"]),
        mock.patch.object(daily, "get_last_date", return_value=date(2026, 6, 11)),
        mock.patch.object(daily, "_last_date_coverage", return_value=(1, 1)),
        mock.patch.object(daily, "get_market_date", return_value=date(2026, 6, 12)),
        mock.patch.object(daily, "is_after_market_close", return_value=True),
        mock.patch.object(daily, "insert_prices", return_value=0),
        mock.patch.object(daily, "refresh_52week_extremes_if_needed", return_value=False),
        mock.patch.object(daily, "get_notifier"),
    ):
        stats = daily.run_daily(
            api_key="k",
            database_url="offline-db",
            force_from_date=date(2026, 6, 12),
            skip_news=True,
            skip_ta=True,
            skip_market_internals=True,
            logger=logging.getLogger(__name__),
        )

    assert stats["invalid_price_rows"] == 1
    assert stats["prices_error"].startswith("every provider price row was malformed")
    assert stats["success"] is False


@pytest.mark.parametrize("persisted", [0, 1])
def test_low_persisted_fraction_on_trading_day_is_fatal(
    monkeypatch: pytest.MonkeyPatch,
    persisted: int,
) -> None:
    class ValidClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

        def get_trading_days(self, *args: object, **kwargs: object) -> list[str]:
            return ["2026-06-12"]

        def get(self, *args: object, **kwargs: object) -> dict[str, Any]:
            return {
                "results": [
                    {
                        "t": 1781265600000,
                        "o": 1,
                        "h": 2,
                        "l": 1,
                        "c": 2,
                        "v": 100,
                    },
                    {
                        "t": 1781265600000,
                        "o": 2,
                        "h": 3,
                        "l": 2,
                        "c": 3,
                        "v": 200,
                    },
                ]
            }

    monkeypatch.delenv("FRED_API_KEY", raising=False)
    price_conn = _daily_psycopg_conn()
    price_conn.commit = mock.Mock()
    price_conn.rollback = mock.Mock()
    with mock.patch(
        "psycopg.connect", return_value=price_conn
    ), mock.patch.object(daily, "PolygonClient", ValidClient), mock.patch.object(
        daily, "SyncRateLimiter"
    ), mock.patch.object(
        daily, "get_symbols_from_db", return_value=["AAPL"]
    ), mock.patch.object(
        daily, "get_last_date", return_value=date(2026, 6, 11)
    ), mock.patch.object(
        daily, "_last_date_coverage", return_value=(0, 0)
    ), mock.patch.object(
        daily, "get_market_date", return_value=date(2026, 6, 12)
    ), mock.patch.object(
        daily, "is_after_market_close", return_value=True
    ), mock.patch.object(
        daily, "insert_prices", return_value=persisted
    ) as insert, mock.patch.object(
        daily, "refresh_52week_extremes_if_needed", return_value=False
        ), mock.patch.object(
            daily, "get_notifier"
        ), mock.patch(
            "sawa.corporate_actions.run_corporate_actions_update",
            return_value={"success": True, "splits_loaded": 0, "split_tickers": []},
        ):
        stats = daily.run_daily(
            api_key="k",
            database_url="test-only",
            skip_news=True,
            skip_ta=True,
            skip_market_internals=True,
            logger=logging.getLogger(__name__),
        )

    assert stats["prices_fetched"] == 2
    assert stats["prices_inserted"] == 0
    assert stats["prices_error"].startswith("persisted only")
    assert stats["success"] is False
    assert stats["fatal_reasons"] == ["required price update failed"]
    insert.assert_called_once()
    assert insert.call_args.kwargs["commit"] is False
    price_conn.commit.assert_not_called()
    price_conn.rollback.assert_called_once()


def test_all_empty_ordinary_overlap_with_failed_calendar_probe_is_fatal(
    monkeypatch,
) -> None:
    """A 14-day ordinary overlap cannot be mistaken for a one-day holiday."""

    class AmbiguousEmptyClient:
        def __init__(self, *a: object, **k: object) -> None:
            pass

        def get_trading_days(self, *a: object, **k: object) -> list[str]:
            raise RuntimeError("calendar probe unavailable")

        def get(self, *a: object, **k: object) -> dict[str, Any]:
            return {"results": []}

    monkeypatch.delenv("FRED_API_KEY", raising=False)
    with mock.patch(
        "psycopg.connect", return_value=_daily_psycopg_conn()
    ), mock.patch.object(daily, "PolygonClient", AmbiguousEmptyClient), mock.patch.object(
        daily, "SyncRateLimiter"
    ), mock.patch.object(
        daily, "get_symbols_from_db", return_value=["MSFT"]
    ), mock.patch.object(
        daily, "get_last_date", return_value=date(2026, 6, 11)
    ), mock.patch.object(
        daily, "_last_date_coverage", return_value=(0, 0)
    ), mock.patch.object(
        daily, "get_market_date", return_value=date(2026, 6, 12)
    ), mock.patch.object(
        daily, "is_after_market_close", return_value=True
    ), mock.patch.object(
        daily, "insert_prices", return_value=0
    ), mock.patch.object(
        daily, "refresh_52week_extremes_if_needed", return_value=False
    ), mock.patch(
        "sawa.corporate_actions.run_corporate_actions_update"
    ) as split_update, mock.patch.object(
        daily, "get_notifier"
    ):
        stats = daily.run_daily(
            api_key="k",
            database_url="test-only",
            skip_news=True,
            skip_ta=True,
            skip_market_internals=True,
            logger=logging.getLogger(__name__),
        )

    assert stats["prices_fetched"] == 0
    assert stats["trading_days_error"] == "RuntimeError: calendar probe unavailable"
    assert stats["prices_error"].startswith("all 1 symbol price requests returned no rows")
    assert stats["degraded"] is True
    assert stats["success"] is False
    assert stats["fatal_reasons"] == ["required price update failed"]
    split_update.assert_not_called()


def test_intraday_cleanup_only_targets_tickers_with_eod_rows() -> None:
    class CleanupCursor:
        rowcount = 7

        def __init__(self) -> None:
            self.query = ""
            self.params: Any = None

        def __enter__(self) -> "CleanupCursor":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, query: Any, params: Any = None) -> None:
            self.query = query.as_string()
            self.params = params

    class CleanupConnection:
        def __init__(self) -> None:
            self.cursor_obj = CleanupCursor()
            self.commits = 0

        def cursor(self) -> CleanupCursor:
            return self.cursor_obj

        def commit(self) -> None:
            self.commits += 1

    conn = CleanupConnection()
    market_date = date(2026, 6, 12)

    deleted = cleanup_today_intraday_data(conn, market_date, logging.getLogger(__name__))

    assert deleted == 7
    assert conn.commits == 1
    assert "EXISTS" in conn.cursor_obj.query
    assert "sp.ticker = spi.ticker" in conn.cursor_obj.query
    assert conn.cursor_obj.params == (market_date, market_date)
