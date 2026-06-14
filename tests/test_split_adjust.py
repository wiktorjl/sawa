"""Regression tests for split-adjusted price refresh."""

import logging
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
    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return _FakeCursor()

    def commit(self) -> None:
        return None


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
    assert stats["success"] is True


def test_no_blacklist_constant_remains() -> None:
    """ADTX is no longer silently excluded from split adjustment."""
    import sawa.utils.constants as constants

    assert not hasattr(constants, "SPLIT_ADJUST_BLACKLIST")
