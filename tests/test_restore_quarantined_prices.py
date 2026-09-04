"""Decision logic of scripts/restore_quarantined_prices.py.

The archive holds rows on a stale split basis. They come back only when exactly
one re-basing built from the recorded splits makes the series continuous.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest import mock

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "restore_quarantined_prices.py"
_spec = importlib.util.spec_from_file_location("restore_quarantined_prices", _SCRIPT)
assert _spec is not None and _spec.loader is not None
restore = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = restore
_spec.loader.exec_module(restore)


def _conn(*, archived: list[tuple], boundary: tuple | None, splits: list[tuple]) -> mock.Mock:
    """Route the script's three queries to canned rows."""

    def execute(query: str, params: tuple = ()) -> mock.Mock:
        result = mock.Mock()
        if "stock_prices_unadjustable_archive" in query:
            result.fetchall.return_value = archived
        elif "FROM stock_splits" in query:
            result.fetchall.return_value = splits
        elif "FROM stock_prices" in query:
            result.fetchone.return_value = boundary
        else:  # pragma: no cover - guards against new queries slipping by
            raise AssertionError(query)
        return result

    conn = mock.Mock()
    conn.execute.side_effect = execute
    return conn


def _row(day: date, close: str, volume: int = 1_000_000) -> tuple:
    price = Decimal(close)
    return (day, price, price, price, price, volume)


# NVDA as it stood: three as-traded bars (4:1 then 10:1 still to apply) ahead of
# a boundary the provider had already re-based.
NVDA_ARCHIVE = [
    _row(date(2021, 2, 12), "598.45", 9_336_943),
    _row(date(2021, 2, 16), "613.21", 8_018_308),
    _row(date(2021, 2, 17), "596.24", 6_761_910),
]
NVDA_SPLITS = [
    ("NVDA", date(2021, 7, 20), 1, 4),
    ("NVDA", date(2024, 6, 10), 1, 10),
]


def test_as_traded_rows_are_restored_by_every_later_split() -> None:
    conn = _conn(
        archived=NVDA_ARCHIVE,
        boundary=(date(2021, 2, 18), Decimal("14.829")),
        splits=NVDA_SPLITS,
    )
    decision = restore.decide(
        conn, "NVDA", 3, date(2021, 2, 12), date(2021, 2, 17), date(2021, 2, 18)
    )

    assert decision.restore is not None
    assert decision.reason.startswith("as-traded")
    closes = [row["close"] for row in decision.restore.rows]
    assert closes == [Decimal("14.96125000"), Decimal("15.33025000"), Decimal("14.90600000")]
    assert decision.restore.rows[0]["volume"] == 373_477_720
    assert Decimal("1.00") < decision.restore.boundary_ratio < Decimal("1.01")


def test_partly_rebased_rows_take_only_the_most_recent_splits() -> None:
    """An earlier refresh already applied the 1:8; only the later 1:10 is missing."""
    archived = [_row(date(2021, 3, 1), "50"), _row(date(2021, 3, 2), "52")]
    conn = _conn(
        archived=archived,
        boundary=(date(2021, 3, 3), Decimal("515")),
        splits=[("XYZ", date(2024, 1, 2), 8, 1), ("XYZ", date(2026, 5, 4), 10, 1)],
    )
    decision = restore.decide(conn, "XYZ", 2, date(2021, 3, 1), date(2021, 3, 2), date(2021, 3, 3))

    assert decision.restore is not None
    assert decision.reason == "last 1 split(s), ratio 1/10"
    assert [row["close"] for row in decision.restore.rows] == [
        Decimal("500.00000000"),
        Decimal("520.00000000"),
    ]
    assert decision.restore.rows[0]["volume"] == 100_000


def test_unexplained_older_segment_stays_archived_while_the_newer_one_returns() -> None:
    """The 10x step inside the archive splits it; only the newer segment is explained."""
    archived = [
        _row(date(2021, 3, 1), "5"),
        _row(date(2021, 3, 2), "50"),
        _row(date(2021, 3, 3), "51"),
    ]
    conn = _conn(
        archived=archived,
        boundary=(date(2021, 3, 4), Decimal("510")),
        splits=[("XYZ", date(2026, 5, 4), 10, 1)],
    )
    decision = restore.decide(conn, "XYZ", 3, date(2021, 3, 1), date(2021, 3, 3), date(2021, 3, 4))

    assert decision.restore is not None
    assert [row["date"] for row in decision.restore.rows] == [date(2021, 3, 2), date(2021, 3, 3)]
    assert decision.reason.startswith(
        "as-traded (1 recorded split(s)); 1 older row(s) left archived"
    )
    assert "boundary ratio 0.100" in decision.reason


def test_rows_without_a_matching_split_stay_archived() -> None:
    archived = [_row(date(2021, 3, 1), "20"), _row(date(2021, 3, 2), "20.5")]
    conn = _conn(
        archived=archived,
        boundary=(date(2021, 3, 3), Decimal("6")),
        splits=[],
    )
    decision = restore.decide(conn, "TAL", 2, date(2021, 3, 1), date(2021, 3, 2), date(2021, 3, 3))

    assert decision.restore is None
    assert decision.reason == "no recorded split after the archived rows"


def test_boundary_that_does_not_fit_any_candidate_stays_archived() -> None:
    """GE: boundary rows still as-traded, so no combination of recorded splits lines up."""
    archived = [_row(date(2021, 7, 29), "12.9"), _row(date(2021, 7, 30), "12.95")]
    conn = _conn(
        archived=archived,
        boundary=(date(2021, 8, 2), Decimal("100.6")),
        splits=[
            ("GE", date(2021, 8, 2), 8, 1),
            ("GE", date(2023, 1, 4), 1000, 1281),
            ("GE", date(2024, 4, 2), 1000, 1253),
        ],
    )
    decision = restore.decide(conn, "GE", 2, date(2021, 7, 29), date(2021, 7, 30), date(2021, 8, 2))

    assert decision.restore is None
    assert decision.reason.startswith("no continuous re-basing")
    assert "boundary ratio" in decision.reason


def test_missing_boundary_row_stays_archived() -> None:
    conn = _conn(archived=NVDA_ARCHIVE, boundary=None, splits=NVDA_SPLITS)
    decision = restore.decide(
        conn, "NVDA", 3, date(2021, 2, 12), date(2021, 2, 17), date(2021, 2, 18)
    )
    assert decision.restore is None
    assert decision.reason == "no stored row after the cutoff"


def test_mixed_basis_archive_is_restored_segment_by_segment() -> None:
    """Newer segment lacks only the 1:10; the older one is fully as-traded (1:8 too)."""
    archived = [
        _row(date(2021, 3, 1), "5"),  # as-traded: needs x8 then x10
        _row(date(2021, 3, 2), "5.2"),
        _row(date(2021, 3, 3), "41"),  # already x8: needs x10 only
        _row(date(2021, 3, 4), "42"),
    ]
    conn = _conn(
        archived=archived,
        boundary=(date(2021, 3, 5), Decimal("415")),
        splits=[("XYZ", date(2024, 1, 2), 8, 1), ("XYZ", date(2026, 5, 4), 10, 1)],
    )
    decision = restore.decide(conn, "XYZ", 4, date(2021, 3, 1), date(2021, 3, 4), date(2021, 3, 5))

    assert decision.restore is not None
    assert [row["close"] for row in decision.restore.rows] == [
        Decimal("400.00000000"),
        Decimal("416.00000000"),
        Decimal("410.00000000"),
        Decimal("420.00000000"),
    ]
    assert decision.reason == "last 1 split(s), ratio 1/10 then as-traded (2 recorded split(s))"


def test_segment_restoration_stops_at_the_first_unexplained_segment() -> None:
    archived = [
        _row(date(2021, 3, 1), "0.9"),  # no recorded split explains this one
        _row(date(2021, 3, 2), "1.0"),
        _row(date(2021, 3, 3), "41"),
        _row(date(2021, 3, 4), "42"),
    ]
    conn = _conn(
        archived=archived,
        boundary=(date(2021, 3, 5), Decimal("415")),
        splits=[("XYZ", date(2026, 5, 4), 10, 1)],
    )
    decision = restore.decide(conn, "XYZ", 4, date(2021, 3, 1), date(2021, 3, 4), date(2021, 3, 5))

    assert decision.restore is not None
    assert [row["date"] for row in decision.restore.rows] == [date(2021, 3, 3), date(2021, 3, 4)]
    assert "2 older row(s) left archived" in decision.reason


def test_as_is_restores_unchanged_only_without_a_later_split() -> None:
    archived = [_row(date(2021, 7, 21), "20"), _row(date(2021, 7, 22), "20.52")]
    conn = _conn(archived=archived, boundary=(date(2021, 7, 23), Decimal("6")), splits=[])
    decision = restore.decide(
        conn, "TAL", 2, date(2021, 7, 21), date(2021, 7, 22), date(2021, 7, 23), as_is=True
    )
    assert decision.restore is not None
    assert [row["close"] for row in decision.restore.rows] == [Decimal("20"), Decimal("20.52")]
    assert "step at cutoff 3.420" in decision.reason

    conn = _conn(
        archived=archived,
        boundary=(date(2021, 7, 23), Decimal("6")),
        splits=[("TAL", date(2024, 1, 2), 3, 1)],
    )
    refused = restore.decide(
        conn, "TAL", 2, date(2021, 7, 21), date(2021, 7, 22), date(2021, 7, 23), as_is=True
    )
    assert refused.restore is None
    assert refused.reason.startswith("refused --as-is: 1 recorded split(s)")
