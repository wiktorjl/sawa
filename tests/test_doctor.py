from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from sawa.doctor import (
    _backup_checks,
    format_checks,
    run_doctor_on_connection,
    summarize_checks,
)
from sawa.utils.market_hours import ET


@pytest.fixture(autouse=True)
def _no_host_backup_dir(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Keep the backup check off this host's real /data/db-backups.

    Without this the doctor tests would pass or fail according to whether the
    machine running them happens to hold a recent database backup.
    """
    monkeypatch.setenv("SAWA_BACKUP_DIR", str(tmp_path / "absent"))


class FakeCursor:
    def __init__(self, conn: FakeConnection) -> None:
        self.conn = conn
        self.query = ""
        self.params: tuple[Any, ...] = ()

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, query: str, params: tuple[Any, ...] = ()) -> None:
        self.query = query
        self.params = params

    def fetchone(self) -> tuple[Any, ...]:
        return self.conn.fetchone(self.query, self.params)

    def fetchall(self) -> list[tuple[Any, ...]]:
        return []


class FakeConnection:
    def __init__(
        self,
        *,
        tables: set[str],
        active_count: int = 100,
        latest_price_date: date | None = date(2026, 5, 14),
        price_tickers: int = 100,
        price_rows: int = 1000,
        latest_price_tickers: int = 100,
        latest_price_rows: int = 100,
        recent_baseline_tickers: int = 100,
        bad_latest_rows: int = 0,
        latest_news: datetime | None = None,
        null_sic: int = 0,
        null_mcap: int = 0,
        economy_latest: date | None = date(2026, 5, 14),
        character_run: date | None = None,
        character_tickers: int = 100,
        corporate_action_rows: int = 5,
        quarterly_latest: date | None = None,
        quarterly_rows: int = 1000,
        post_split_checked: int = 0,
        post_split_flagged: int = 0,
        post_split_worst: str = "",
        market_latest: tuple[date | None, date | None, date | None] | None = None,
        extremes_latest: date | None = None,
    ) -> None:
        self.queries: list[str] = []
        self.tables = tables
        self.active_count = active_count
        self.null_sic = null_sic
        self.null_mcap = null_mcap
        self.latest_price_date = latest_price_date
        self.price_tickers = price_tickers
        self.price_rows = price_rows
        self.latest_price_tickers = latest_price_tickers
        self.latest_price_rows = latest_price_rows
        self.recent_baseline_tickers = recent_baseline_tickers
        self.bad_latest_rows = bad_latest_rows
        self.latest_news = latest_news or datetime(2026, 5, 14, tzinfo=timezone.utc)
        self.economy_latest = economy_latest
        self.character_run = character_run or date(2026, 5, 14)
        self.character_tickers = character_tickers
        self.corporate_action_rows = corporate_action_rows
        self.quarterly_latest = quarterly_latest or date(2026, 5, 14)
        self.quarterly_rows = quarterly_rows
        self.post_split_checked = post_split_checked
        self.post_split_flagged = post_split_flagged
        self.post_split_worst = post_split_worst
        self.market_latest = market_latest or (
            latest_price_date,
            latest_price_date,
            latest_price_date,
        )
        self.extremes_latest = extremes_latest or latest_price_date

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def fetchone(self, query: str, params: tuple[Any, ...]) -> tuple[Any, ...]:
        compact = " ".join(query.split())
        self.queries.append(compact)

        if "to_regclass" in compact:
            table = str(params[0]).removeprefix("public.")
            return (table if table in self.tables else None,)

        if "FILTER (WHERE sic_code IS NULL)" in compact:
            return (self.active_count, self.null_sic, self.null_mcap)

        if "COUNT(*) FROM companies WHERE active = true" in compact:
            return (self.active_count,)

        if "WITH recent_dates AS" in compact:
            return (self.recent_baseline_tickers,)

        if "eligible_character_prices" in compact:
            return (self.character_tickers, self.active_count)

        if (
            "SELECT COUNT(DISTINCT sp.ticker), COUNT(*) FROM stock_prices sp" in compact
            and "JOIN companies" in compact
            and "sp.date = (SELECT MAX(date)" in compact
        ):
            return (self.latest_price_tickers, self.latest_price_rows)

        if "FROM stock_prices" in compact and "COUNT(DISTINCT" in compact:
            if "JOIN companies" in compact and "sp.date = (SELECT MAX(date)" in compact:
                return (self.latest_price_tickers, self.latest_price_rows)
            return (self.latest_price_date, self.price_tickers, self.price_rows)

        if "malformed OHLCV" in compact:
            return (self.bad_latest_rows,)

        if "open IS NULL" in compact:
            return (self.bad_latest_rows,)

        # Post-split TA recompute check: returns (checked, flagged, worst).
        # Matched before the generic technical_indicators branch because its
        # CTE also references FROM technical_indicators.
        if "recent_splits" in compact and "stored_sma_50" in compact:
            return (
                self.post_split_checked,
                self.post_split_flagged,
                self.post_split_worst,
            )

        if "FROM technical_indicators" in compact:
            return (self.latest_price_date, self.price_tickers)

        if "FROM market_internals" in compact or "FROM public.market_internals" in compact:
            return self.market_latest

        if "FROM news_articles" in compact:
            return (self.latest_news, 25)

        if "FROM mv_52week_extremes" in compact:
            return (self.latest_price_date, self.extremes_latest)

        if "FROM stock_character_classification" in compact:
            return (self.character_run,)

        if compact.startswith("SELECT MAX(date) FROM") and any(
            t in compact
            for t in (
                "treasury_yields",
                "inflation",
                "inflation_expectations",
                "labor_market",
            )
        ):
            return (self.economy_latest,)

        if compact.startswith("SELECT COUNT(*) FROM") and (
            "stock_splits" in compact or "dividends" in compact
        ):
            return (self.corporate_action_rows,)

        if "SELECT MAX(period_end), COUNT(*)" in compact or (
            "SELECT MAX(date), COUNT(*)" in compact
            and "financial_ratios" in compact
        ):
            return (self.quarterly_latest, self.quarterly_rows)

        raise AssertionError(f"Unexpected query: {compact}")


def test_doctor_daily_passes_when_latest_price_coverage_is_good() -> None:
    conn = FakeConnection(
        tables={
            "companies",
            "stock_prices",
            "technical_indicators",
            "news_articles",
            "market_internals",
            "treasury_yields",
            "stock_prices_live",
            "mv_52week_extremes",
        }
    )

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 14))
    summary = summarize_checks(checks)

    assert summary["success"] is True
    assert summary["failed"] == 0
    assert any(c.name == "stock_prices.latest_coverage" for c in checks)
    assert any(c.name == "mv_52week_extremes.freshness" for c in checks)


def test_doctor_stops_at_missing_required_schema() -> None:
    conn = FakeConnection(tables={"companies", "stock_prices"})

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 15))
    summary = summarize_checks(checks)

    assert summary["success"] is False
    assert summary["failed"] == 6
    assert [c.name for c in checks] == [
        "schema.companies",
        "schema.stock_prices",
        "schema.technical_indicators",
        "schema.news_articles",
        "schema.market_internals",
        "schema.treasury_yields",
        "schema.stock_prices_live",
        "schema.mv_52week_extremes",
    ]


def test_doctor_fails_when_latest_price_coverage_is_too_low() -> None:
    conn = FakeConnection(
        tables={
            "companies",
            "stock_prices",
            "technical_indicators",
            "news_articles",
            "market_internals",
            "treasury_yields",
            "stock_prices_live",
            "mv_52week_extremes",
        },
        latest_price_tickers=80,
        latest_price_rows=80,
        recent_baseline_tickers=100,
    )

    checks = run_doctor_on_connection(
        conn,
        job="daily",
        today=date(2026, 5, 15),
        min_coverage=0.95,
    )
    failed = {c.name: c for c in checks if c.status == "FAIL"}

    assert "stock_prices.latest_coverage" in failed
    assert failed["stock_prices.latest_coverage"].observed == 80
    assert "expected at least 95" in failed["stock_prices.latest_coverage"].message
    assert "stock_prices.latest_rows" not in failed
    assert summarize_checks(checks)["success"] is False


def test_latest_rows_does_not_duplicate_latest_coverage_failure() -> None:
    conn = FakeConnection(
        tables={
            "companies",
            "stock_prices",
            "technical_indicators",
            "news_articles",
            "market_internals",
            "treasury_yields",
            "stock_prices_live",
            "mv_52week_extremes",
        },
        active_count=5889,
        recent_baseline_tickers=5889,
        latest_price_tickers=5004,
        latest_price_rows=5004,
        market_latest=(date(2026, 5, 15), date(2026, 5, 15), date(2026, 5, 14)),
    )

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 15))
    by_name = {c.name: c for c in checks}

    assert by_name["stock_prices.latest_rows"].status == "PASS"
    assert by_name["stock_prices.latest_rows"].observed == 5004
    assert by_name["stock_prices.latest_coverage"].status == "FAIL"
    assert "expected at least 5006" in by_name["stock_prices.latest_coverage"].message


def test_doctor_uses_recent_daily_baseline_not_broad_reference_index() -> None:
    conn = FakeConnection(
        tables={
            "companies",
            "stock_prices",
            "technical_indicators",
            "news_articles",
            "market_internals",
            "treasury_yields",
            "stock_prices_live",
            "mv_52week_extremes",
            "indices",
            "index_constituents",
        },
        active_count=10401,
        price_tickers=10392,
        price_rows=10000,
        recent_baseline_tickers=5004,
        latest_price_tickers=5004,
        latest_price_rows=5004,
        market_latest=(date(2026, 5, 15), date(2026, 5, 15), date(2026, 5, 14)),
    )

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 15))
    by_name = {c.name: c for c in checks}

    assert by_name["stock_prices.expected_universe"].observed == 5004
    assert "recent daily stock_prices baseline" in by_name["stock_prices.expected_universe"].message
    assert by_name["stock_prices.latest_coverage"].status == "PASS"
    assert not any("index_constituents" in query for query in conn.queries)
    assert summarize_checks(checks)["success"] is True


def test_same_day_news_is_not_marked_stale() -> None:
    conn = FakeConnection(
        tables={
            "companies",
            "stock_prices",
            "technical_indicators",
            "news_articles",
            "market_internals",
            "treasury_yields",
            "stock_prices_live",
            "mv_52week_extremes",
        },
        latest_news=datetime(2026, 5, 15, 21, 0, tzinfo=timezone.utc),
    )

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 15))
    by_name = {c.name: c for c in checks}

    assert by_name["news_articles.recent"].status == "PASS"


def test_doctor_checks_each_market_internal_series_freshness() -> None:
    conn = FakeConnection(
        tables={
            "companies",
            "stock_prices",
            "technical_indicators",
            "news_articles",
            "market_internals",
            "treasury_yields",
            "stock_prices_live",
            "mv_52week_extremes",
        },
        market_latest=(date(2026, 5, 15), date(2026, 5, 1), None),
    )

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 15))
    by_name = {check.name: check for check in checks}

    assert by_name["market_internals.vix.latest_date"].status == "PASS"
    assert by_name["market_internals.vix3m.latest_date"].status == "FAIL"
    assert by_name["market_internals.hy_spread.latest_date"].status == "FAIL"


def test_future_market_internal_dates_do_not_pass_freshness() -> None:
    conn = FakeConnection(
        tables={
            "companies",
            "stock_prices",
            "technical_indicators",
            "news_articles",
            "market_internals",
            "treasury_yields",
            "stock_prices_live",
            "mv_52week_extremes",
        },
        market_latest=(date(2026, 5, 16), date(2026, 5, 16), date(2026, 5, 16)),
    )

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 15))
    by_name = {check.name: check for check in checks}

    assert by_name["market_internals.vix.latest_date"].status == "FAIL"
    assert by_name["market_internals.vix3m.latest_date"].status == "FAIL"
    assert by_name["market_internals.hy_spread.latest_date"].status == "FAIL"


def test_future_stock_price_date_does_not_pass_freshness() -> None:
    conn = FakeConnection(
        tables=_DAILY_TABLES,
        latest_price_date=date(2026, 5, 16),
    )

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 15))
    by_name = {check.name: check for check in checks}

    assert by_name["stock_prices.latest_date"].status == "FAIL"


def test_future_materialized_view_date_does_not_pass_freshness() -> None:
    conn = FakeConnection(
        tables=_DAILY_TABLES,
        latest_price_date=date(2026, 5, 15),
        extremes_latest=date(2026, 5, 16),
    )

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 15))
    by_name = {check.name: check for check in checks}

    assert by_name["stock_prices.latest_date"].status == "PASS"
    assert by_name["mv_52week_extremes.freshness"].status == "WARN"


_DAILY_TABLES = {
    "companies",
    "stock_prices",
    "technical_indicators",
    "news_articles",
    "market_internals",
    "treasury_yields",
    "stock_prices_live",
    "mv_52week_extremes",
}

_WEEKLY_TABLES = {
    "companies",
    "stock_prices",
    "treasury_yields",
    "inflation",
    "inflation_expectations",
    "labor_market",
    "stock_splits",
    "dividends",
    "stock_character_classification",
    "stock_character_scorecard",
}

_QUARTERLY_TABLES = {
    "companies",
    "stock_prices",
    "financial_ratios",
    "balance_sheets",
    "income_statements",
    "cash_flows",
}


def test_doctor_weekly_passes_when_cadence_is_fresh() -> None:
    conn = FakeConnection(
        tables=_WEEKLY_TABLES,
        latest_price_date=date(2026, 5, 14),
        economy_latest=date(2026, 5, 13),
        character_run=date(2026, 5, 12),
        character_tickers=100,
    )

    checks = run_doctor_on_connection(conn, job="weekly", today=date(2026, 5, 15))
    by_name = {c.name: c for c in checks}

    assert summarize_checks(checks)["success"] is True
    assert by_name["treasury_yields.latest_date"].status == "PASS"
    assert by_name["stock_character_classification.latest_run"].status == "PASS"


def test_doctor_weekly_fails_on_stale_treasury_and_character() -> None:
    # Fresh stock_prices (daily feed) but a stale weekly cadence must still flip
    # the exit code: treasury_yields and the character run are FAIL-capable.
    conn = FakeConnection(
        tables=_WEEKLY_TABLES,
        latest_price_date=date(2026, 5, 14),
        economy_latest=date(2026, 1, 1),
        character_run=date(2026, 1, 1),
    )

    checks = run_doctor_on_connection(conn, job="weekly", today=date(2026, 5, 15))
    failed = {c.name for c in checks if c.status == "FAIL"}

    assert summarize_checks(checks)["success"] is False
    assert "treasury_yields.latest_date" in failed
    assert "stock_character_classification.latest_run" in failed
    # Slow series and coverage stay WARN, not FAIL.
    assert "inflation.latest_date" not in failed
    assert "labor_market.latest_date" not in failed
    assert "stock_character_classification.coverage" not in failed


def test_doctor_quarterly_fails_on_stale_fundamentals() -> None:
    conn = FakeConnection(
        tables=_QUARTERLY_TABLES,
        latest_price_date=date(2026, 5, 14),
        quarterly_latest=date(2025, 1, 1),
        quarterly_rows=1000,
    )

    checks = run_doctor_on_connection(conn, job="quarterly", today=date(2026, 5, 15))
    failed = {c.name for c in checks if c.status == "FAIL"}

    assert summarize_checks(checks)["success"] is False
    assert "financial_ratios.latest_date" in failed
    assert "balance_sheets.latest_date" in failed
    assert "income_statements.latest_date" in failed
    assert "cash_flows.latest_date" in failed


def test_doctor_quarterly_passes_when_fundamentals_fresh() -> None:
    conn = FakeConnection(
        tables=_QUARTERLY_TABLES,
        latest_price_date=date(2026, 5, 14),
        quarterly_latest=date(2026, 4, 5),
        quarterly_rows=1000,
    )

    checks = run_doctor_on_connection(conn, job="quarterly", today=date(2026, 5, 15))

    assert summarize_checks(checks)["success"] is True


def test_post_split_ta_check_flags_uncomputed_tickers() -> None:
    conn = FakeConnection(
        tables=_DAILY_TABLES | {"stock_splits"},
        post_split_checked=150,
        post_split_flagged=3,
        post_split_worst="KLAC, XXII, AERT",
        market_latest=(date(2026, 5, 15), date(2026, 5, 15), date(2026, 5, 14)),
    )

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 15))
    by_name = {c.name: c for c in checks}

    check = by_name["technical_indicators.post_split_recompute"]
    assert check.status == "WARN"
    assert check.observed == 3
    assert "KLAC" in check.message
    # Detection-gap signal stays a WARN so the live DB (which has split
    # tickers awaiting recompute) does not flip the exit code prematurely.
    assert summarize_checks(checks)["success"] is True


def test_post_split_ta_check_passes_when_recomputed() -> None:
    conn = FakeConnection(
        tables=_DAILY_TABLES | {"stock_splits"},
        post_split_checked=150,
        post_split_flagged=0,
        post_split_worst="",
    )

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 15))
    by_name = {c.name: c for c in checks}

    check = by_name["technical_indicators.post_split_recompute"]
    assert check.status == "PASS"
    assert check.observed == 0


def test_post_split_ta_check_skipped_without_stock_splits_table() -> None:
    conn = FakeConnection(tables=_DAILY_TABLES)

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 15))

    assert not any(
        c.name == "technical_indicators.post_split_recompute" for c in checks
    )


def test_format_checks_includes_summary_counts() -> None:
    conn = FakeConnection(
        tables={
            "companies",
            "stock_prices",
            "technical_indicators",
            "news_articles",
            "market_internals",
            "treasury_yields",
            "stock_prices_live",
            "mv_52week_extremes",
        },
        latest_price_date=date(2026, 4, 1),
    )

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 15))
    output = format_checks(checks)

    assert "Database Doctor" in output
    assert "Summary:" in output
    assert "stock_prices.latest_date" in output


def _write_backup(directory: Path, name: str, *, days_old: float, size: int = 4096) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_bytes(b"\0" * size)
    stamp = time.time() - days_old * 86400
    os.utime(path, (stamp, stamp))
    return path


def test_backup_check_is_omitted_when_the_host_keeps_no_backups(tmp_path: Path) -> None:
    """A developer machine or CI runner is not running backups, so there is nothing to report."""
    os.environ["SAWA_BACKUP_DIR"] = str(tmp_path / "nonexistent")

    assert _backup_checks() == []


def test_recent_backup_passes(tmp_path: Path) -> None:
    _write_backup(tmp_path, "postgres_backup_20260904_000633.tar", days_old=1)
    os.environ["SAWA_BACKUP_DIR"] = str(tmp_path)

    checks = _backup_checks()

    assert len(checks) == 1
    assert checks[0].name == "backup.freshness"
    assert checks[0].status == "PASS"
    assert "postgres_backup_20260904_000633.tar" in checks[0].message


def test_stale_backup_fails_rather_than_warns(tmp_path: Path) -> None:
    """The 14-week silent failure is exactly what this check exists to catch."""
    _write_backup(tmp_path, "postgres_backup_20260524_010001.tar", days_old=102)
    os.environ["SAWA_BACKUP_DIR"] = str(tmp_path)

    checks = _backup_checks()

    assert checks[0].status == "FAIL"
    assert checks[0].observed == 102


def test_empty_backup_directory_fails(tmp_path: Path) -> None:
    (tmp_path / "backups").mkdir()
    os.environ["SAWA_BACKUP_DIR"] = str(tmp_path / "backups")

    checks = _backup_checks()

    assert checks[0].status == "FAIL"
    assert checks[0].observed == "none"


def test_freshness_uses_the_newest_archive(tmp_path: Path) -> None:
    _write_backup(tmp_path, "postgres_backup_20260405_010001.tar", days_old=150)
    _write_backup(tmp_path, "postgres_backup_20260904_000633.tar", days_old=2)
    os.environ["SAWA_BACKUP_DIR"] = str(tmp_path)

    checks = _backup_checks()

    assert checks[0].status == "PASS"
    assert checks[0].observed == 2


def test_a_partial_dump_is_not_counted_as_a_backup(tmp_path: Path) -> None:
    """The script writes .part first and renames on success; .part must not qualify."""
    _write_backup(tmp_path, "postgres_backup_20260904_000633.tar.part", days_old=0)
    os.environ["SAWA_BACKUP_DIR"] = str(tmp_path)

    checks = _backup_checks()

    assert checks[0].status == "FAIL"
    assert checks[0].observed == "none"


# ── Trading-calendar news freshness and the watchdog job ─────────────────────

def _et(year: int, month: int, day: int, hour: int, minute: int = 0) -> datetime:
    return ET.localize(datetime(year, month, day, hour, minute))


@pytest.mark.parametrize("job", ["daily", "watchdog"])
def test_daily_cadence_requires_treasury_table(job: str) -> None:
    conn = FakeConnection(tables=_DAILY_TABLES - {"treasury_yields"})

    checks = run_doctor_on_connection(conn, job=job, today=date(2026, 5, 15))

    assert [c.name for c in checks if c.status == "FAIL"] == ["schema.treasury_yields"]
    assert summarize_checks(checks)["success"] is False


@pytest.mark.parametrize(
    ("job", "now", "session", "treasury_floor"),
    [
        ("daily", _et(2026, 9, 25, 18), date(2026, 9, 25), date(2026, 9, 24)),
        ("watchdog", _et(2026, 9, 28, 8, 30), date(2026, 9, 25), date(2026, 9, 24)),
        # Labor Day is not a completed trading session for publication lag.
        ("daily", _et(2026, 9, 8, 18), date(2026, 9, 8), date(2026, 9, 4)),
        ("watchdog", _et(2026, 9, 8, 8, 30), date(2026, 9, 4), date(2026, 9, 3)),
    ],
)
@pytest.mark.parametrize("state", ["publication_lag", "same_session", "stale", "empty", "future"])
def test_treasury_daily_cadence_allows_only_one_session_publication_lag(
    job: str, now: datetime, session: date, treasury_floor: date, state: str,
    scheduler_state: Path, tmp_path: Path,
) -> None:
    from sawa.utils.market_hours import previous_trading_day

    latest = {
        "publication_lag": treasury_floor,
        "same_session": session,
        "stale": previous_trading_day(treasury_floor),
        "empty": None,
        "future": now.date() + timedelta(days=1),
    }[state]
    conn = FakeConnection(
        tables=_DAILY_TABLES,
        latest_price_date=session,
        economy_latest=latest,
        latest_news=now,
    )
    proc = tmp_path / "proc"
    proc.mkdir()

    checks = run_doctor_on_connection(
        conn, job=job, now=now, scheduler_state_dir=scheduler_state, proc=proc
    )
    check = next(c for c in checks if c.name == "treasury_yields.latest_date")

    assert check.status == ("PASS" if state in {"publication_lag", "same_session"} else "FAIL")
    assert check.observed == latest
    assert str(treasury_floor) in check.expected
    assert "one completed trading session" in check.expected


def test_treasury_publication_lag_does_not_weaken_same_session_cboe_check() -> None:
    conn = FakeConnection(
        tables=_DAILY_TABLES,
        latest_price_date=date(2026, 9, 25),
        economy_latest=date(2026, 9, 24),
        market_latest=(date(2026, 9, 24), date(2026, 9, 24), date(2026, 9, 24)),
    )

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 9, 25))
    by_name = {c.name: c for c in checks}

    assert by_name["treasury_yields.latest_date"].status == "PASS"
    assert by_name["market_internals.vix.latest_date"].status == "FAIL"
    assert by_name["market_internals.vix3m.latest_date"].status == "FAIL"


@pytest.mark.parametrize("job", ["all", "coldstart"])
def test_combined_doctor_uses_only_strict_daily_treasury_check(job: str) -> None:
    conn = FakeConnection(
        tables=_DAILY_TABLES | _WEEKLY_TABLES | _QUARTERLY_TABLES,
        latest_price_date=date(2026, 5, 15),
        economy_latest=date(2026, 5, 13),
    )

    checks = run_doctor_on_connection(conn, job=job, today=date(2026, 5, 15))
    treasury = [c for c in checks if c.name == "treasury_yields.latest_date"]

    assert len(treasury) == 1
    assert treasury[0].status == "FAIL"
    assert summarize_checks(checks)["success"] is False


def test_news_four_days_stale_warns_but_does_not_fail_the_daily_doctor() -> None:
    # Friday evening post-job doctor: session 05-15 expected, news floor 05-12.
    # The post-job check is WARN-only so a provider news outage cannot withhold
    # daily_done and re-run the daily on every evening tick.
    conn = FakeConnection(
        tables=_DAILY_TABLES,
        latest_news=datetime(2026, 5, 11, 20, 0, tzinfo=timezone.utc),
        market_latest=(date(2026, 5, 15), date(2026, 5, 15), date(2026, 5, 14)),
    )

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 15))
    by_name = {c.name: c for c in checks}

    assert by_name["news_articles.recent"].status == "WARN"
    assert summarize_checks(checks)["success"] is True


def test_news_three_days_behind_the_session_still_passes_post_job() -> None:
    conn = FakeConnection(
        tables=_DAILY_TABLES,
        latest_news=datetime(2026, 5, 12, 20, 0, tzinfo=timezone.utc),
    )

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 15))
    by_name = {c.name: c for c in checks}

    assert by_name["news_articles.recent"].status == "PASS"


def test_news_three_days_stale_fails_the_watchdog(scheduler_state: Path) -> None:
    # Tuesday 09-15 08:30 ET: expected session Monday 09-14, strict floor 09-12.
    now = _et(2026, 9, 15, 8, 30)
    _fresh_state(scheduler_state, now, date(2026, 9, 14), "2026-W38")
    conn = FakeConnection(
        tables=_DAILY_TABLES,
        latest_price_date=date(2026, 9, 14),
        latest_news=datetime(2026, 9, 11, 20, 0, tzinfo=timezone.utc),
        market_latest=(date(2026, 9, 14), date(2026, 9, 14), date(2026, 9, 11)),
    )

    checks = run_doctor_on_connection(conn, job="watchdog", now=now)
    by_name = {c.name: c for c in checks}

    assert by_name["news_articles.recent"].status == "FAIL"
    assert summarize_checks(checks)["success"] is False


def test_news_floor_follows_the_trading_calendar_over_a_long_weekend() -> None:
    # Tuesday 2026-09-08 08:30 ET after Labor Day: latest session is Friday
    # 09-04, so news from Wednesday 09-02 is still acceptable.
    conn = FakeConnection(
        tables=_DAILY_TABLES,
        latest_price_date=date(2026, 9, 4),
        latest_news=datetime(2026, 9, 2, 20, 0, tzinfo=timezone.utc),
    )

    checks = run_doctor_on_connection(conn, job="daily", now=_et(2026, 9, 8, 8, 30))
    by_name = {c.name: c for c in checks}

    assert by_name["news_articles.recent"].status == "PASS"


def test_same_evening_news_after_utc_midnight_is_not_future_dated() -> None:
    # 21:30 ET on 05-15 is 01:30 UTC on 05-16: judged in ET, it is today's news.
    conn = FakeConnection(
        tables=_DAILY_TABLES,
        latest_news=datetime(2026, 5, 16, 1, 30, tzinfo=timezone.utc),
    )

    checks = run_doctor_on_connection(conn, job="daily", today=date(2026, 5, 15))
    by_name = {c.name: c for c in checks}

    assert by_name["news_articles.recent"].status == "PASS"


@pytest.fixture
def scheduler_state(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    state = tmp_path / "scheduler"
    state.mkdir()
    monkeypatch.setenv("SAWA_SCHEDULER_STATE_DIR", str(state))
    return state


def _write_log(state: Path, *lines: str) -> None:
    (state / "scheduler.log").write_text("\n".join(lines) + "\n")


def _fresh_state(state: Path, now: datetime, expected_eod: date, week: str) -> None:
    tick = now - timedelta(minutes=10)
    _write_log(
        state,
        f"[{tick:%Y-%m-%d %H:%M:%S} ET] Scheduler tick — market: closed, time: {tick:%H:%M} ET",
        f"[{tick:%Y-%m-%d %H:%M:%S} ET] No action needed",
    )
    (state / f"daily_done_{expected_eod:%Y-%m-%d}").touch()
    (state / f"weekly_done_{week}").touch()


def test_watchdog_passes_on_a_healthy_morning(scheduler_state: Path) -> None:
    now = _et(2026, 9, 15, 8, 30)  # Tuesday; expected session Monday 09-14
    _fresh_state(scheduler_state, now, date(2026, 9, 14), "2026-W38")
    conn = FakeConnection(
        tables=_DAILY_TABLES,
        latest_price_date=date(2026, 9, 14),
        economy_latest=date(2026, 9, 11),
        latest_news=datetime(2026, 9, 14, 22, 0, tzinfo=timezone.utc),
        market_latest=(date(2026, 9, 14), date(2026, 9, 14), date(2026, 9, 11)),
    )

    checks = run_doctor_on_connection(conn, job="watchdog", now=now)
    by_name = {c.name: c for c in checks}

    assert summarize_checks(checks)["success"] is True, format_checks(checks)
    assert by_name["stock_prices.latest_date"].status == "PASS"
    assert by_name["technical_indicators.latest_date"].status == "PASS"
    assert by_name["market_internals.hy_spread.latest_date"].status == "PASS"
    assert by_name["scheduler.tick_freshness"].status == "PASS"
    assert by_name["scheduler.daily_done"].status == "PASS"
    assert by_name["scheduler.weekly_done"].status == "PASS"
    assert by_name["scheduler.intraday_orphan"].status == "PASS"


def test_watchdog_fails_one_session_behind(scheduler_state: Path) -> None:
    # Data stopped Friday 09-11; Tuesday 09-15 morning expects Monday 09-14.
    now = _et(2026, 9, 15, 8, 30)
    _fresh_state(scheduler_state, now, date(2026, 9, 14), "2026-W38")
    conn = FakeConnection(
        tables=_DAILY_TABLES,
        latest_price_date=date(2026, 9, 11),
        latest_news=datetime(2026, 9, 12, 20, 0, tzinfo=timezone.utc),
    )

    checks = run_doctor_on_connection(conn, job="watchdog", now=now)
    failed = {c.name for c in checks if c.status == "FAIL"}

    assert "stock_prices.latest_date" in failed
    assert "technical_indicators.latest_date" in failed
    assert "market_internals.vix.latest_date" in failed
    # News gets two days of slack behind the expected session (09-14), so
    # 09-12 news still passes; the price/TA/internals checks carry the alert.
    assert "news_articles.recent" not in failed


def test_watchdog_monday_morning_expects_friday_and_last_weeks_flag(
    scheduler_state: Path,
) -> None:
    now = _et(2026, 9, 14, 8, 30)  # Monday
    _fresh_state(scheduler_state, now, date(2026, 9, 11), "2026-W37")
    conn = FakeConnection(
        tables=_DAILY_TABLES,
        latest_price_date=date(2026, 9, 11),
        latest_news=datetime(2026, 9, 13, 20, 0, tzinfo=timezone.utc),
    )

    checks = run_doctor_on_connection(conn, job="watchdog", now=now)
    by_name = {c.name: c for c in checks}

    assert by_name["stock_prices.latest_date"].status == "PASS"
    assert by_name["scheduler.daily_done"].status == "PASS"
    assert by_name["scheduler.weekly_done"].status == "PASS"


def test_watchdog_reports_the_2026_09_04_outage(scheduler_state: Path, tmp_path: Path) -> None:
    now = _et(2026, 9, 15, 8, 30)
    _write_log(
        scheduler_state,
        "[2026-09-04 14:15:02 ET] Scheduler tick — market: open, time: 14:15 ET",
        "[2026-09-04 14:15:02 ET] No action needed",
        "[2026-09-04 14:30:01 ET] ERROR: could not safely parse .env",
        "[2026-09-15 08:15:01 ET] ERROR: could not safely parse .env",
    )
    (scheduler_state / "daily_done_2026-09-03").touch()
    (scheduler_state / "weekly_done_2026-W36").touch()
    # A fake /proc with a live `sawa intraday` process.
    proc = tmp_path / "proc"
    (proc / "210102").mkdir(parents=True)
    (proc / "210102" / "cmdline").write_bytes(
        b"/x/.venv/bin/python\0/x/.venv/bin/sawa\0intraday\0--log-dir\0/x/logs\0"
    )
    (scheduler_state / "intraday.pid").write_text("210102 39973489\n")
    (scheduler_state / "intraday_start_time").write_text("2026-09-04 09:30 ET\n")
    conn = FakeConnection(
        tables=_DAILY_TABLES,
        latest_price_date=date(2026, 9, 3),
        latest_news=datetime(2026, 9, 3, 20, 55, tzinfo=timezone.utc),
    )

    # The integrated path with a controlled /proc: nothing here depends on
    # the host's real process table.
    checks = run_doctor_on_connection(
        conn, job="watchdog", now=now, scheduler_state_dir=scheduler_state, proc=proc
    )
    by_name = {c.name: c for c in checks}

    assert by_name["news_articles.recent"].status == "FAIL"
    assert by_name["stock_prices.latest_date"].status == "FAIL"
    tick = by_name["scheduler.tick_freshness"]
    assert tick.status == "FAIL"
    assert "2026-09-04 14:15 ET" in tick.message
    assert "could not safely parse .env" in tick.message
    assert by_name["scheduler.daily_done"].status == "FAIL"
    assert "daily_done_2026-09-14" in by_name["scheduler.daily_done"].message
    assert by_name["scheduler.weekly_done"].status == "FAIL"
    assert "weekly_done_2026-W38" in by_name["scheduler.weekly_done"].message
    orphan = by_name["scheduler.intraday_orphan"]
    assert orphan.status == "FAIL"
    assert "210102" in orphan.message and "2026-09-04 09:30 ET" in orphan.message


def test_watchdog_tolerates_a_stale_tick_while_a_job_holds_the_lock(
    scheduler_state: Path, tmp_path: Path
) -> None:
    from sawa import doctor as doctor_module

    now = _et(2026, 9, 15, 18, 40)
    tick = now - timedelta(hours=1, minutes=40)
    _write_log(
        scheduler_state,
        f"[{tick:%Y-%m-%d %H:%M:%S} ET] Scheduler tick — market: closed, time: {tick:%H:%M} ET",
        f"[{tick:%Y-%m-%d %H:%M:%S} ET] Starting sawa daily...",
    )
    proc = tmp_path / "proc"
    (proc / "4242").mkdir(parents=True)
    (proc / "4242" / "cmdline").write_bytes(b"/x/.venv/bin/python\0/x/.venv/bin/sawa\0daily\0")

    checks = doctor_module._scheduler_state_checks(
        now_et=now, expected_eod=date(2026, 9, 14), state_dir=scheduler_state, proc=proc
    )
    by_name = {c.name: c for c in checks}

    assert by_name["scheduler.tick_freshness"].status == "PASS"
    assert "sawa daily is running" in by_name["scheduler.tick_freshness"].message


def test_intraday_during_the_session_is_not_an_orphan(
    scheduler_state: Path, tmp_path: Path,
) -> None:
    from sawa import doctor as doctor_module

    now = _et(2026, 9, 15, 11, 0)
    _fresh_state(scheduler_state, now, date(2026, 9, 14), "2026-W38")
    proc = tmp_path / "proc"
    (proc / "77").mkdir(parents=True)
    (proc / "77" / "cmdline").write_bytes(b"/x/.venv/bin/sawa\0intraday\0")
    (scheduler_state / "intraday.pid").write_text("77 1\n")

    checks = doctor_module._scheduler_state_checks(
        now_et=now, expected_eod=date(2026, 9, 14), state_dir=scheduler_state, proc=proc
    )
    by_name = {c.name: c for c in checks}

    assert by_name["scheduler.intraday_orphan"].status == "PASS"


def test_watchdog_fails_when_the_state_directory_is_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("SAWA_SCHEDULER_STATE_DIR", str(tmp_path / "nowhere"))
    conn = FakeConnection(tables=_DAILY_TABLES, latest_price_date=date(2026, 9, 14))

    checks = run_doctor_on_connection(conn, job="watchdog", now=_et(2026, 9, 15, 8, 30))
    by_name = {c.name: c for c in checks}

    assert by_name["scheduler.state_dir"].status == "FAIL"
