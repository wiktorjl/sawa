"""Regression tests for split-adjusted price refresh."""

import logging
from datetime import date
from unittest import mock

import pytest

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


def test_unresolved_old_history_is_visible_and_never_blindly_rebased() -> None:
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
    ), mock.patch.object(
        split_adjust, "known_identity_conflict", return_value=None
    ), mock.patch.object(split_adjust, "insert_prices", return_value=3) as insert:
        mpg.connect.return_value = _FakeConn()
        stats = split_adjust.refresh_split_adjusted_prices(
            api_key="k",
            database_url="db",
            tickers=["RCON"],
            logger=logging.getLogger(__name__),
        )

    assert stats["success"] is False
    assert stats["degraded"] is True
    assert stats["pre_horizon_dates_not_adjusted"] == 2
    assert stats["provider_history_horizon"] == "2026-08-26"
    assert "missing_existing_price_dates" not in stats
    insert.assert_not_called()


def test_no_blacklist_constant_remains() -> None:
    """ADTX is no longer silently excluded from split adjustment."""
    import sawa.utils.constants as constants

    assert not hasattr(constants, "SPLIT_ADJUST_BLACKLIST")


# --- Pre-horizon re-basing ---------------------------------------------------
#
# NVDA as it stood after coldstart: the flat-file bars were as-traded, and a
# REST refresh could only serve dates from 2021-02-18 on. The provider's own
# re-basing of that boundary (593.16 -> 14.829, a 4:1 then a 10:1 split) is the
# ratio the three older bars needed.

from decimal import Decimal  # noqa: E402
from fractions import Fraction  # noqa: E402

_HORIZON = date(2021, 2, 18)
_PROBES = [
    _HORIZON,
    date(2021, 2, 19),
    date(2021, 2, 22),
    date(2021, 2, 23),
    date(2021, 2, 24),
]
_TAIL = [date(2021, 2, 12), date(2021, 2, 16), date(2021, 2, 17)]


def _fetched_rows() -> dict[date, dict]:
    return {
        day: {
            "ticker": "NVDA",
            "date": day.isoformat(),
            "open": 14.729,
            "high": 14.8733,
            "low": 14.575,
            "close": 14.829,
            "volume": 234080800,
        }
        for day in _PROBES
    }


def _stored_rows(*, tail_close: str = "596.24", boundary_scale: str = "40") -> dict[date, dict]:
    scale = Decimal(boundary_scale)
    stored = {
        day: {
            "open": Decimal("14.729") * scale,
            "high": Decimal("14.8733") * scale,
            "low": Decimal("14.575") * scale,
            "close": Decimal("14.829") * scale,
            "volume": Decimal("234080800") / scale,
        }
        for day in _PROBES
    }
    stored[_TAIL[-1]] = {
        "open": Decimal("606.84"),
        "high": Decimal("608.9407"),
        "low": Decimal("591.2"),
        "close": Decimal(tail_close),
        "volume": Decimal("6761910"),
    }
    for day in _TAIL[:-1]:
        stored[day] = dict(stored[_TAIL[-1]])
    return stored


def _plan(stored: dict[date, dict], unapplied: list | None = None):
    with mock.patch.object(
        split_adjust, "get_stored_price_rows", return_value=stored
    ), mock.patch.object(
        split_adjust, "get_unapplied_splits_in_range", return_value=unapplied or []
    ), mock.patch.object(
        split_adjust, "recorded_basis_factor", side_effect=lambda _c, _t, _d, factor: factor
    ):
        return split_adjust.plan_pre_horizon_rebases(
            object(),
            fetched_by_ticker={"NVDA": _fetched_rows()},
            existing_dates={"NVDA": set(_PROBES) | set(_TAIL)},
            unreachable_by_ticker={"NVDA": list(_TAIL)},
        )


def test_infer_basis_factor_snaps_a_consistent_provider_ratio() -> None:
    factor = split_adjust.infer_basis_factor(_stored_rows(), _fetched_rows(), _PROBES)
    assert factor == Fraction(40)


def test_infer_basis_factor_rejects_probes_that_disagree() -> None:
    stored = _stored_rows()
    drifted_close = stored[_PROBES[2]]["close"] * Decimal("1.1")
    stored[_PROBES[2]] = {**stored[_PROBES[2]], "close": drifted_close}
    assert split_adjust.infer_basis_factor(stored, _fetched_rows(), _PROBES) is None


def test_infer_basis_factor_needs_every_probe_stored() -> None:
    stored = _stored_rows()
    del stored[_PROBES[1]]
    assert split_adjust.infer_basis_factor(stored, _fetched_rows(), _PROBES) is None
    assert split_adjust.infer_basis_factor(stored, _fetched_rows(), []) is None


def test_infer_basis_factor_keeps_a_measured_ratio_that_is_not_a_simple_fraction() -> None:
    # A compounded reverse split whose ratio has no small denominator.
    stored = {_HORIZON: {"open": Decimal("1"), "close": Decimal("1")}}
    fetched = {_HORIZON: {"open": 28250, "close": 28250}}
    factor = split_adjust.infer_basis_factor(stored, fetched, [_HORIZON])
    assert factor is not None
    assert abs(float(factor) * 28250 - 1) < 1e-9


def test_plan_rebases_a_tail_that_shares_the_boundary_basis() -> None:
    plans, already, skipped = _plan(_stored_rows())
    assert plans == [
        split_adjust.PreHorizonRebase(
            ticker="NVDA", before=_HORIZON, factor=Fraction(40), expected_rows=3
        )
    ]
    assert already == []
    assert skipped == {}


def test_plan_reports_a_tail_already_on_the_provider_basis() -> None:
    plans, already, skipped = _plan(_stored_rows(boundary_scale="1", tail_close="14.906"))
    assert plans == []
    assert already == ["NVDA"]
    assert skipped == {}


@pytest.mark.parametrize("historical_step", ["1.44", "1.3032", "0.7625", "0.725"])
def test_adjusted_tail_allows_large_historical_market_moves(historical_step: str) -> None:
    stored = _stored_rows(boundary_scale="1", tail_close="14.906")
    stored[_TAIL[0]]["close"] = stored[_TAIL[1]]["close"] / Decimal(historical_step)

    plans, already, skipped = _plan(stored)

    # A past daily move is not evidence for a new factor. Matching boundary
    # prices and no unapplied ledger splits require no changes to this tail.
    assert plans == []
    assert already == ["NVDA"]
    assert skipped == {}


@pytest.mark.parametrize("historical_step", ["0.5", "2"])
def test_adjusted_tail_still_rejects_unexplained_twofold_steps(historical_step: str) -> None:
    stored = _stored_rows(boundary_scale="1", tail_close="14.906")
    stored[_TAIL[0]]["close"] = stored[_TAIL[1]]["close"] / Decimal(historical_step)

    plans, already, skipped = _plan(stored)

    assert plans == []
    assert already == []
    assert "discontinuity" in skipped["NVDA"]


def test_provider_boundary_keeps_tight_continuity_despite_wider_historical_screen() -> None:
    plans, already, skipped = _plan(_stored_rows(boundary_scale="1", tail_close="20"))

    assert plans == []
    assert already == []
    assert "different basis than the boundary" in skipped["NVDA"]


def test_plan_skips_a_tail_that_is_discontinuous_with_the_boundary() -> None:
    # Boundary rows are as-traded (593.16) but the tail was already re-based
    # (14.906): applying the boundary ratio would push it 40x too low.
    plans, already, skipped = _plan(_stored_rows(tail_close="14.906"))
    assert plans == []
    assert already == []
    assert "discontinuity" in skipped["NVDA"]


def test_plan_skips_when_the_provider_ratio_is_not_consistent() -> None:
    stored = _stored_rows()
    stored[_PROBES[0]] = {**stored[_PROBES[0]], "open": Decimal("1")}
    plans, already, skipped = _plan(stored)
    assert plans == []
    assert "not one consistent ratio" in skipped["NVDA"]


def test_plan_also_applies_splits_whose_raw_step_is_still_inside_the_tail() -> None:
    """APH: a 2:1 in 2021-03 sits inside the as-traded tail; the boundary ratio
    (its 2024 2:1) alone would leave the rows before 2021-03-05 still 2x high."""
    inside = date(2021, 3, 5)
    plans, already, skipped = _plan(_stored_rows(), unapplied=[(inside, Fraction(2), 1)])
    assert plans == [
        split_adjust.PreHorizonRebase(
            ticker="NVDA", before=_HORIZON, factor=Fraction(40), expected_rows=3
        ),
        split_adjust.PreHorizonRebase(
            ticker="NVDA", before=inside, factor=Fraction(2), expected_rows=1
        ),
    ]
    assert already == [] and skipped == {}


def test_plan_applies_an_inside_split_even_when_the_boundary_ratio_is_one() -> None:
    inside = date(2021, 3, 5)
    plans, already, skipped = _plan(
        _stored_rows(boundary_scale="1", tail_close="14.906"),
        unapplied=[(inside, Fraction(1, 10), 2)],
    )
    assert plans == [
        split_adjust.PreHorizonRebase(
            ticker="NVDA", before=inside, factor=Fraction(1, 10), expected_rows=2
        )
    ]
    assert already == []


def test_raw_split_jump_present_picks_the_closer_basis() -> None:
    # As-traded 2:1: the close halves on the execution date.
    assert split_adjust.raw_split_jump_present(Decimal("120.08"), Decimal("61.56"), Fraction(2))
    # Already adjusted: the close barely moves.
    assert not split_adjust.raw_split_jump_present(Decimal("60.1"), Decimal("61.56"), Fraction(2))
    # As-traded 1:10 reverse split: the close jumps tenfold.
    assert split_adjust.raw_split_jump_present(Decimal("0.5"), Decimal("4.8"), Fraction(1, 10))
    assert not split_adjust.raw_split_jump_present(Decimal("0"), Decimal("4.8"), Fraction(1, 10))


def test_plan_skips_when_nothing_stored_overlaps_the_provider_window() -> None:
    with mock.patch.object(split_adjust, "get_stored_price_rows", return_value={}):
        plans, already, skipped = split_adjust.plan_pre_horizon_rebases(
            object(),
            fetched_by_ticker={"NVDA": _fetched_rows()},
            existing_dates={"NVDA": set(_TAIL)},
            unreachable_by_ticker={"NVDA": list(_TAIL)},
        )
    assert plans == []
    assert "no stored date overlaps" in skipped["NVDA"]


class _RecordingCursor:
    def __init__(self, rowcount: int) -> None:
        self.rowcount = rowcount
        self.calls: list[tuple[object, object]] = []

    def __enter__(self) -> "_RecordingCursor":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, statement: object, params: object = None) -> None:
        self.calls.append((statement, params))


def test_rebase_rows_before_date_divides_prices_and_scales_volume() -> None:
    cursor = _RecordingCursor(rowcount=3)
    conn = mock.MagicMock()
    conn.cursor.return_value = cursor

    updated = split_adjust.rebase_rows_before_date(conn, "NVDA", _HORIZON, Fraction(40))

    assert updated == 3
    statement, params = cursor.calls[0]
    assert "UPDATE stock_prices" in str(statement)
    assert "round(open * %(den)s / %(num)s, 8)" in str(statement)
    assert "round(volume * %(num)s / %(den)s)::bigint" in str(statement)
    assert "date < %(before)s" in str(statement)
    assert params == {
        "ticker": "NVDA",
        "before": _HORIZON,
        "num": Decimal("40"),
        "den": Decimal("1"),
    }


def _refresh_with_pre_horizon_tail(*, rebase_rowcount: int) -> tuple[dict, _FakeConn, mock.Mock]:
    fetched = list(_fetched_rows().values())

    def horizon_limited_fetch(*_args: object, **kwargs: object) -> list[dict]:
        kwargs["stats"].update(
            {
                "requested_symbols": 1,
                "succeeded_symbols": 1,
                "failed_symbols": 0,
                "failed_tickers": [],
                "empty_tickers": [],
                "provider_price_rows": len(fetched),
            }
        )
        return fetched

    conn = _FakeConn()
    with mock.patch.object(split_adjust, "psycopg") as mpg, mock.patch.object(
        split_adjust, "PolygonClient"
    ), mock.patch.object(split_adjust, "SyncRateLimiter"), mock.patch.object(
        split_adjust, "get_earliest_price_date", return_value=_TAIL[0]
    ), mock.patch.object(
        split_adjust,
        "get_existing_price_dates",
        return_value={"NVDA": set(_PROBES) | set(_TAIL)},
    ), mock.patch.object(
        split_adjust, "get_stored_price_rows", return_value=_stored_rows()
    ), mock.patch.object(
        split_adjust, "get_unapplied_splits_in_range", return_value=[]
    ), mock.patch.object(
        split_adjust, "recorded_basis_factor", side_effect=lambda _c, _t, _d, factor: factor
    ), mock.patch.object(
        split_adjust, "fetch_prices_via_api", side_effect=horizon_limited_fetch
    ), mock.patch.object(
        split_adjust, "insert_prices", return_value=len(fetched)
    ), mock.patch.object(
        split_adjust, "rebase_rows_before_date", return_value=rebase_rowcount
    ) as rebase:
        mpg.connect.return_value = conn
        stats = split_adjust.refresh_split_adjusted_prices(
            api_key="k",
            database_url="db",
            tickers=["NVDA"],
            logger=logging.getLogger(__name__),
        )
    return stats, conn, rebase


def test_refresh_rebases_the_pre_horizon_tail_in_the_upsert_transaction() -> None:
    stats, conn, rebase = _refresh_with_pre_horizon_tail(rebase_rowcount=3)

    assert stats["success"] is True
    rebase.assert_called_once_with(conn, "NVDA", _HORIZON, Fraction(40))
    assert stats["pre_horizon_dates_rebased"] == 3
    assert stats["pre_horizon_dates_already_adjusted"] == 0
    assert stats["pre_horizon_dates_not_adjusted"] == 0
    assert "pre_horizon_rebase_skipped" not in stats
    assert stats["provider_history_horizon"] == _HORIZON.isoformat()
    assert conn.commits == 1
    assert conn.rollbacks == 0


def test_refresh_rolls_back_when_the_rebase_touches_an_unexpected_row_count() -> None:
    stats, conn, rebase = _refresh_with_pre_horizon_tail(rebase_rowcount=2)

    assert stats["success"] is False
    assert "touched 2/3" in stats["error"]
    rebase.assert_called_once()
    assert conn.commits == 0
    assert conn.rollbacks == 1


@pytest.mark.parametrize("older_close", ["14.90", "298.12"])
def test_mixed_older_price_bases_are_not_scaled_by_the_boundary_ratio(older_close: str) -> None:
    stored = _stored_rows()
    stored[_TAIL[0]] = {**stored[_TAIL[0]], "close": Decimal(older_close)}
    plans, already, skipped = _plan(stored)
    assert plans == [] and already == []
    assert "discontinuity" in skipped["NVDA"]


@pytest.mark.parametrize(
    ("ledger", "measured", "expected"),
    [
        ([(1, 10), (1, 4)], Fraction(400001, 10000), Fraction(40)),
        ([(2, 3)], Fraction(3, 2), Fraction(3, 2)),
        ([(2, 3)], Fraction(7, 5), None),
        ([], Fraction(40), None),
        ([(1000, 1001)], Fraction(1), None),  # two indistinguishable candidates
    ],
)
def test_boundary_rebase_requires_one_exact_recorded_split_basis(
    ledger: list[tuple], measured: Fraction, expected: Fraction | None,
) -> None:
    conn = mock.MagicMock()
    conn.cursor.return_value.__enter__.return_value.fetchall.return_value = ledger
    assert split_adjust.recorded_basis_factor(conn, "NVDA", _HORIZON, measured) == expected


def test_small_split_and_real_price_move_are_not_guessed_from_proximity() -> None:
    with pytest.raises(ValueError, match="uniquely establish"):
        split_adjust.raw_split_jump_present(Decimal("10"), Decimal("8"), Fraction(3, 2))


def test_known_reused_ticker_is_rejected_before_price_fetch_or_writes() -> None:
    client = mock.Mock()
    client.get_single.side_effect = [
        {"ticker": "DFNS", "cik": "0001777946"},
        {"ticker": "DFNS", "cik": "0001787518"},
    ]
    conn = _FakeConn()
    with (
        mock.patch.object(split_adjust.psycopg, "connect", return_value=conn),
        mock.patch.object(split_adjust, "PolygonClient", return_value=client),
        mock.patch.object(split_adjust, "SyncRateLimiter"),
        mock.patch.object(split_adjust, "get_earliest_price_date", return_value=date(2021, 2, 18)),
        mock.patch.object(split_adjust, "get_existing_price_dates", return_value={
            "DFNS": {date(2021, 2, 18), date(2026, 9, 25)},
        }),
        mock.patch.object(split_adjust, "fetch_prices_via_api") as fetch,
        mock.patch.object(split_adjust, "insert_prices") as insert,
    ):
        stats = split_adjust.refresh_split_adjusted_prices(
            "key", "unused", ["DFNS"], logger=logging.getLogger(__name__),
        )
    assert stats["success"] is False
    assert stats["identity_conflicts"]["DFNS"]["old_cik"] == "1777946"
    assert "ticker reuse" in stats["error"]
    fetch.assert_not_called()
    insert.assert_not_called()
    assert conn.commits == 0


def test_identity_check_accepts_same_cik_with_different_zero_padding() -> None:
    client = mock.Mock()
    client.get_single.side_effect = [{"cik": "0001787518"}, {"cik": "1787518"}]
    assert split_adjust.known_identity_conflict(
        client, "DFNS", {date(2026, 2, 9), date(2026, 9, 25)}, mock.Mock(),
    ) is None


def test_split_repair_refreshes_same_date_extrema() -> None:
    with mock.patch.object(split_adjust, "refresh_52week_extremes_if_needed") as refresh:
        stats, conn, _ = _refresh_with_pre_horizon_tail(rebase_rowcount=3)
    assert stats["success"] is True
    assert refresh.call_args.args[0] is conn
    assert refresh.call_args.kwargs == {"prices_changed": True}


def test_failed_extrema_refresh_rolls_back_price_repair() -> None:
    with mock.patch.object(
        split_adjust, "refresh_52week_extremes_if_needed", side_effect=RuntimeError("view failed"),
    ):
        stats, conn, _ = _refresh_with_pre_horizon_tail(rebase_rowcount=3)
    assert stats["success"] is False
    assert stats["prices_updated"] == 0
    assert stats["pre_horizon_dates_rebased"] == 0
    assert conn.commits == 0
    assert conn.rollbacks == 1
