import logging
from datetime import date
from typing import Any

from sawa.daily import _last_date_coverage, refresh_52week_extremes_if_needed


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
