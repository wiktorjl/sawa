"""Regression tests for split-adjusted price refresh."""

import logging
from datetime import date
from unittest import mock

from sawa import split_adjust


class _FakeCursor:
    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, *args: object, **kwargs: object) -> None:
        return None

    def fetchone(self):  # earliest price date probe
        return (None,)

    def fetchall(self):
        return []


class _FakeConn:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return _FakeCursor()

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def test_explicit_tickers_are_deduplicated() -> None:
    """A ticker passed multiple times (one per split row) is fetched once."""
    captured: dict[str, list[str]] = {}

    def fake_earliest(conn: object, tickers: list[str]):
        captured["tickers"] = tickers
        return None  # short-circuits before the network fetch

    with mock.patch.object(split_adjust, "psycopg") as mpg, mock.patch.object(
        split_adjust, "PolygonClient"
    ), mock.patch.object(split_adjust, "SyncRateLimiter"), mock.patch.object(
        split_adjust, "get_earliest_price_date", side_effect=fake_earliest
    ):
        mpg.connect.return_value = _FakeConn()
        stats = split_adjust.refresh_split_adjusted_prices(
            api_key="k",
            database_url="db",
            tickers=["SMX", "SMX", "KLAC", "SMX"],
            logger=logging.getLogger(__name__),
        )

    # Deduped, order preserved.
    assert captured["tickers"] == ["SMX", "KLAC"]
    assert stats["success"] is False
    assert stats["tickers_requested"] == 2
    assert "no existing price data" in stats["error"]


def test_provider_outage_is_unsuccessful_and_writes_nothing() -> None:
    def failed_fetch(*_args: object, **kwargs: object) -> list[dict]:
        kwargs["stats"].update(
            {
                "requested_symbols": 1,
                "succeeded_symbols": 0,
                "failed_symbols": 1,
                "failed_tickers": ["SMX"],
                "empty_tickers": [],
                "provider_price_rows": 0,
            }
        )
        return []

    with mock.patch.object(split_adjust, "psycopg") as mpg, mock.patch.object(
        split_adjust, "PolygonClient"
    ), mock.patch.object(split_adjust, "SyncRateLimiter"), mock.patch.object(
        split_adjust, "get_earliest_price_date", return_value=date(2025, 1, 1)
    ), mock.patch.object(
        split_adjust,
        "get_existing_price_dates",
        return_value={"SMX": {date(2026, 8, 28)}},
    ), mock.patch.object(
        split_adjust, "fetch_prices_via_api", side_effect=failed_fetch
    ), mock.patch.object(split_adjust, "insert_prices") as insert:
        mpg.connect.return_value = _FakeConn()
        stats = split_adjust.refresh_split_adjusted_prices(
            api_key="k",
            database_url="db",
            tickers=["SMX"],
            logger=logging.getLogger(__name__),
        )

    assert stats["success"] is False
    assert stats["provider"]["failed_symbols"] == 1
    insert.assert_not_called()


def test_short_persistence_is_unsuccessful() -> None:
    price = {
        "ticker": "SMX",
        "date": "2026-08-28",
        "open": 1,
        "high": 1,
        "low": 1,
        "close": 1,
        "volume": 1,
    }

    def successful_fetch(*_args: object, **kwargs: object) -> list[dict]:
        kwargs["stats"].update(
            {
                "requested_symbols": 1,
                "succeeded_symbols": 1,
                "failed_symbols": 0,
                "failed_tickers": [],
                "empty_tickers": [],
                "provider_price_rows": 1,
            }
        )
        return [price]

    with mock.patch.object(split_adjust, "psycopg") as mpg, mock.patch.object(
        split_adjust, "PolygonClient"
    ), mock.patch.object(split_adjust, "SyncRateLimiter"), mock.patch.object(
        split_adjust, "get_earliest_price_date", return_value=date(2025, 1, 1)
    ), mock.patch.object(
        split_adjust,
        "get_existing_price_dates",
        return_value={"SMX": {date(2026, 8, 28)}},
    ), mock.patch.object(
        split_adjust, "fetch_prices_via_api", side_effect=successful_fetch
    ), mock.patch.object(split_adjust, "insert_prices", return_value=0) as insert:
        conn = _FakeConn()
        mpg.connect.return_value = conn
        stats = split_adjust.refresh_split_adjusted_prices(
            api_key="k",
            database_url="db",
            tickers=["SMX"],
            logger=logging.getLogger(__name__),
        )

    assert stats["success"] is False
    assert stats["prices_updated"] == 0
    assert "only 0/1" in stats["error"]
    assert conn.commits == 0
    assert conn.rollbacks == 1
    assert insert.call_args.kwargs["commit"] is False


def test_exact_persistence_commits_once() -> None:
    price = {
        "ticker": "SMX",
        "date": "2026-08-28",
        "open": 1,
        "high": 1,
        "low": 1,
        "close": 1,
        "volume": 1,
    }

    def successful_fetch(*_args: object, **kwargs: object) -> list[dict]:
        kwargs["stats"].update(
            {
                "requested_symbols": 1,
                "succeeded_symbols": 1,
                "failed_symbols": 0,
                "failed_tickers": [],
                "empty_tickers": [],
                "provider_price_rows": 1,
            }
        )
        return [price]

    with mock.patch.object(split_adjust, "psycopg") as mpg, mock.patch.object(
        split_adjust, "PolygonClient"
    ), mock.patch.object(split_adjust, "SyncRateLimiter"), mock.patch.object(
        split_adjust, "get_earliest_price_date", return_value=date(2025, 1, 1)
    ), mock.patch.object(
        split_adjust,
        "get_existing_price_dates",
        return_value={"SMX": {date(2026, 8, 28)}},
    ), mock.patch.object(
        split_adjust, "fetch_prices_via_api", side_effect=successful_fetch
    ), mock.patch.object(split_adjust, "insert_prices", return_value=1) as insert:
        conn = _FakeConn()
        mpg.connect.return_value = conn
        stats = split_adjust.refresh_split_adjusted_prices(
            api_key="k",
            database_url="db",
            tickers=["SMX"],
            logger=logging.getLogger(__name__),
        )

    assert stats["success"] is True
    assert conn.commits == 1
    assert conn.rollbacks == 0
    assert insert.call_args.kwargs["commit"] is False


def test_persistence_exception_rolls_back_and_does_not_commit() -> None:
    price = {
        "ticker": "SMX",
        "date": "2026-08-28",
        "open": 1,
        "high": 1,
        "low": 1,
        "close": 1,
        "volume": 1,
    }

    def successful_fetch(*_args: object, **kwargs: object) -> list[dict]:
        kwargs["stats"].update(
            {
                "requested_symbols": 1,
                "succeeded_symbols": 1,
                "failed_symbols": 0,
                "failed_tickers": [],
                "empty_tickers": [],
                "provider_price_rows": 1,
            }
        )
        return [price]

    with mock.patch.object(split_adjust, "psycopg") as mpg, mock.patch.object(
        split_adjust, "PolygonClient"
    ), mock.patch.object(split_adjust, "SyncRateLimiter"), mock.patch.object(
        split_adjust, "get_earliest_price_date", return_value=date(2025, 1, 1)
    ), mock.patch.object(
        split_adjust,
        "get_existing_price_dates",
        return_value={"SMX": {date(2026, 8, 28)}},
    ), mock.patch.object(
        split_adjust, "fetch_prices_via_api", side_effect=successful_fetch
    ), mock.patch.object(
        split_adjust,
        "insert_prices",
        side_effect=RuntimeError("persistence failed"),
    ) as insert:
        conn = _FakeConn()
        mpg.connect.return_value = conn
        stats = split_adjust.refresh_split_adjusted_prices(
            api_key="k",
            database_url="db",
            tickers=["SMX"],
            logger=logging.getLogger(__name__),
        )

    assert stats["success"] is False
    assert conn.commits == 0
    assert conn.rollbacks == 1
    assert insert.call_args.kwargs["commit"] is False


def test_missing_middle_history_date_prevents_any_write() -> None:
    prices = [
        {
            "ticker": "SMX",
            "date": day,
            "open": 1,
            "high": 1,
            "low": 1,
            "close": 1,
            "volume": 1,
        }
        for day in ("2026-08-26", "2026-08-28")
    ]

    def incomplete_fetch(*_args: object, **kwargs: object) -> list[dict]:
        kwargs["stats"].update(
            {
                "requested_symbols": 1,
                "succeeded_symbols": 1,
                "failed_symbols": 0,
                "failed_tickers": [],
                "empty_tickers": [],
                "provider_price_rows": 2,
            }
        )
        return prices

    with mock.patch.object(split_adjust, "psycopg") as mpg, mock.patch.object(
        split_adjust, "PolygonClient"
    ), mock.patch.object(split_adjust, "SyncRateLimiter"), mock.patch.object(
        split_adjust, "get_earliest_price_date", return_value=date(2026, 8, 26)
    ), mock.patch.object(
        split_adjust,
        "get_existing_price_dates",
        return_value={
            "SMX": {date(2026, 8, 26), date(2026, 8, 27), date(2026, 8, 28)}
        },
    ), mock.patch.object(
        split_adjust, "fetch_prices_via_api", side_effect=incomplete_fetch
    ), mock.patch.object(split_adjust, "insert_prices") as insert:
        mpg.connect.return_value = _FakeConn()
        stats = split_adjust.refresh_split_adjusted_prices(
            api_key="k",
            database_url="db",
            tickers=["SMX"],
            logger=logging.getLogger(__name__),
        )

    assert stats["success"] is False
    assert stats["missing_existing_price_dates"] == 1
    assert stats["missing_existing_price_date_samples"] == ["SMX/2026-08-27"]
    insert.assert_not_called()


def test_history_older_than_provider_window_still_adjusts_what_it_can() -> None:
    """The provider serves a rolling window; older rows can never be re-based.

    Treating that as incompleteness aborted the entire adjustment, so nothing
    was re-based and the series stayed discontinuous at the split rather than
    at the unreachable horizon. Rows the provider cannot serve are reported and
    left untouched; the adjustable range is written.
    """
    prices = [
        {
            "ticker": "RCON",
            "date": day,
            "open": 1,
            "high": 1,
            "low": 1,
            "close": 1,
            "volume": 1,
        }
        for day in ("2026-08-26", "2026-08-27", "2026-08-28")
    ]

    def horizon_limited_fetch(*_args: object, **kwargs: object) -> list[dict]:
        kwargs["stats"].update(
            {
                "requested_symbols": 1,
                "succeeded_symbols": 1,
                "failed_symbols": 0,
                "failed_tickers": [],
                "empty_tickers": [],
                "provider_price_rows": 3,
            }
        )
        return prices

    with mock.patch.object(split_adjust, "psycopg") as mpg, mock.patch.object(
        split_adjust, "PolygonClient"
    ), mock.patch.object(split_adjust, "SyncRateLimiter"), mock.patch.object(
        split_adjust, "get_earliest_price_date", return_value=date(2021, 2, 18)
    ), mock.patch.object(
        split_adjust,
        "get_existing_price_dates",
        return_value={
            "RCON": {
                date(2021, 2, 18),   # predates the provider window
                date(2021, 3, 1),    # predates the provider window
                date(2026, 8, 26),
                date(2026, 8, 27),
                date(2026, 8, 28),
            }
        },
    ), mock.patch.object(
        split_adjust, "fetch_prices_via_api", side_effect=horizon_limited_fetch
    ), mock.patch.object(split_adjust, "insert_prices", return_value=3) as insert:
        mpg.connect.return_value = _FakeConn()
        stats = split_adjust.refresh_split_adjusted_prices(
            api_key="k",
            database_url="db",
            tickers=["RCON"],
            logger=logging.getLogger(__name__),
        )

    assert stats["success"] is True
    assert stats["pre_horizon_dates_not_adjusted"] == 2
    assert stats["provider_history_horizon"] == "2026-08-26"
    assert "missing_existing_price_dates" not in stats
    insert.assert_called_once()


def test_no_blacklist_constant_remains() -> None:
    """ADTX is no longer silently excluded from split adjustment."""
    import sawa.utils.constants as constants

    assert not hasattr(constants, "SPLIT_ADJUST_BLACKLIST")
