import logging
from datetime import date
from typing import Any
from unittest import mock

from sawa import daily
from sawa.daily import (
    _heal_splits_in_window,
    _last_date_coverage,
    refresh_52week_extremes_if_needed,
)


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

    def cursor(self) -> FakeCursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.commits += 1


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


def test_heal_splits_adjusts_and_recomputes_when_split_in_window() -> None:
    with mock.patch(
        "sawa.corporate_actions.run_corporate_actions_update",
        return_value={"splits_loaded": 1, "split_tickers": ["KLAC"]},
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


def test_heal_splits_noop_when_no_split_in_window() -> None:
    with mock.patch(
        "sawa.corporate_actions.run_corporate_actions_update",
        return_value={"splits_loaded": 0, "split_tickers": []},
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


def test_empty_aapl_probe_does_not_skip_universe_fetch(monkeypatch) -> None:
    """An empty get_trading_days (e.g. AAPL halt) must not skip all symbols."""

    class GateClient:
        def __init__(self, *a: object, **k: object) -> None:
            pass

        def get_trading_days(self, *a: object, **k: object) -> list[str]:
            return []  # empty AAPL proxy bar

        def get(self, *a: object, **k: object) -> dict[str, Any]:
            return {"results": [{"t": 1749700800000, "o": 1, "h": 2, "l": 1, "c": 2, "v": 100}]}

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
        return_value={"splits_loaded": 0, "split_tickers": []},
    ), mock.patch.object(daily, "get_notifier"), mock.patch.object(daily, "alert_missing_api_key"):
        stats = daily.run_daily(
            api_key="k",
            database_url="db",
            skip_news=True,
            skip_ta=True,
            logger=logging.getLogger(__name__),
        )

    # Both non-AAPL symbols fetched + inserted despite the empty proxy probe.
    assert mins.called
    assert stats["prices_fetched"] == 2
    assert stats["success"] is True
