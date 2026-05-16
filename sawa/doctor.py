"""Database doctor checks for scheduled Sawa jobs."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from math import ceil
from typing import Any, Literal

import psycopg

from sawa.utils.logging import setup_logging
from sawa.utils.market_hours import get_market_date

DoctorJob = Literal["all", "daily", "weekly", "quarterly", "coldstart"]
Severity = Literal["info", "warn", "fail"]


@dataclass(frozen=True)
class DoctorCheck:
    """Single doctor check result."""

    name: str
    status: Literal["PASS", "WARN", "FAIL"]
    message: str
    observed: Any = None
    expected: Any = None


@dataclass(frozen=True)
class PriceUniverse:
    """Universe used to judge stock price completeness."""

    source: Literal["active_companies", "recent_daily_baseline"]
    label: str
    count: int


def _fetchone(conn: Any, query: str, params: tuple[Any, ...] | None = None) -> tuple[Any, ...]:
    with conn.cursor() as cur:
        cur.execute(query, params or ())
        row = cur.fetchone()
    return tuple(row or ())


def _table_exists(conn: Any, table_name: str) -> bool:
    row = _fetchone(conn, "SELECT to_regclass(%s)", (f"public.{table_name}",))
    return bool(row and row[0])


def _status(ok: bool, severity: Severity) -> Literal["PASS", "WARN", "FAIL"]:
    if ok:
        return "PASS"
    return "FAIL" if severity == "fail" else "WARN"


def _check(
    name: str,
    ok: bool,
    message: str,
    *,
    severity: Severity = "fail",
    observed: Any = None,
    expected: Any = None,
) -> DoctorCheck:
    return DoctorCheck(
        name=name,
        status=_status(ok, severity),
        message=message,
        observed=observed,
        expected=expected,
    )


def _days_old(value: date | datetime | None, today: date) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        value = value.date()
    return (today - value).days


def _within_days(value: date | datetime | None, today: date, max_days: int) -> bool:
    age = _days_old(value, today)
    return age is not None and age <= max_days


def _required_coverage_count(expected_count: int, min_coverage: float) -> int:
    return ceil(expected_count * min_coverage)


def _coverage_ok(count: int, expected_count: int, min_coverage: float) -> bool:
    if expected_count <= 0:
        return False
    return count >= _required_coverage_count(expected_count, min_coverage)


def _required_tables_checks(conn: Any, job: DoctorJob) -> list[DoctorCheck]:
    tables = ["companies", "stock_prices"]
    if job in {"all", "daily", "coldstart"}:
        tables.extend(
            [
                "technical_indicators",
                "news_articles",
                "market_internals",
                "stock_prices_live",
                "mv_52week_extremes",
            ]
        )
    if job in {"all", "weekly", "coldstart"}:
        tables.extend(
            [
                "treasury_yields",
                "inflation",
                "inflation_expectations",
                "labor_market",
                "stock_splits",
                "dividends",
                "stock_character_classification",
                "stock_character_scorecard",
            ]
        )
    if job in {"all", "quarterly", "coldstart"}:
        tables.extend(["financial_ratios", "balance_sheets", "income_statements", "cash_flows"])

    checks: list[DoctorCheck] = []
    for table in dict.fromkeys(tables):
        exists = _table_exists(conn, table)
        checks.append(
            _check(
                f"schema.{table}",
                exists,
                f"{table} table exists" if exists else f"{table} table is missing",
                severity="fail",
                observed=exists,
                expected=True,
            )
        )
    return checks


def _active_company_count(conn: Any) -> int:
    row = _fetchone(conn, "SELECT COUNT(*) FROM companies WHERE active = true")
    return int(row[0] or 0)


def _price_universe(conn: Any, active_count: int) -> PriceUniverse:
    """Pick the best available universe for stock price completeness checks."""
    row = _fetchone(
        conn,
        """
        WITH recent_dates AS (
            SELECT date
            FROM stock_prices
            WHERE date < (SELECT MAX(date) FROM stock_prices)
            GROUP BY date
            ORDER BY date DESC
            LIMIT 10
        ),
        daily_counts AS (
            SELECT COUNT(DISTINCT sp.ticker) AS ticker_count
            FROM stock_prices sp
            JOIN companies c ON c.ticker = sp.ticker
            WHERE c.active = true
              AND sp.date IN (SELECT date FROM recent_dates)
            GROUP BY sp.date
        )
        SELECT COALESCE(MAX(ticker_count), 0)
        FROM daily_counts
        """,
    )
    count = int(row[0] or 0)
    if count > 0:
        return PriceUniverse(
            "recent_daily_baseline",
            "recent daily stock_prices baseline",
            count,
        )

    return PriceUniverse("active_companies", "active companies", active_count)


def _universe_total_price_tickers(conn: Any, universe: PriceUniverse) -> int:
    if universe.source == "recent_daily_baseline":
        return universe.count

    row = _fetchone(
        conn,
        """
        SELECT COUNT(DISTINCT sp.ticker)
        FROM stock_prices sp
        JOIN companies c ON c.ticker = sp.ticker
        WHERE c.active = true
        """,
    )
    return int(row[0] or 0)


def _universe_latest_price_counts(conn: Any, universe: PriceUniverse) -> tuple[int, int]:
    if universe.source == "recent_daily_baseline":
        return _fetchone(
            conn,
            """
            SELECT COUNT(DISTINCT sp.ticker), COUNT(*)
            FROM stock_prices sp
            JOIN companies c ON c.ticker = sp.ticker
            WHERE c.active = true
              AND sp.date = (SELECT MAX(date) FROM stock_prices)
            """,
        )

    return _fetchone(
        conn,
        """
        SELECT COUNT(DISTINCT sp.ticker), COUNT(*)
        FROM stock_prices sp
        JOIN companies c ON c.ticker = sp.ticker
        WHERE c.active = true
          AND sp.date = (SELECT MAX(date) FROM stock_prices)
        """,
    )


def _price_checks(
    conn: Any,
    *,
    active_count: int,
    today: date,
    min_coverage: float,
    max_staleness_days: int,
) -> list[DoctorCheck]:
    universe = _price_universe(conn, active_count)
    checks: list[DoctorCheck] = [
        _check(
            "companies.active_count",
            active_count > 0,
            f"{active_count} active companies",
            observed=active_count,
            expected="> 0",
        )
    ]
    checks.append(
        _check(
            "stock_prices.expected_universe",
            universe.count > 0,
            f"using {universe.label} as price universe ({universe.count} tickers)",
            observed=universe.count,
            expected="> 0",
        )
    )

    latest_date, _raw_ticker_count, row_count = _fetchone(
        conn,
        """
        SELECT MAX(date), COUNT(DISTINCT ticker), COUNT(*)
        FROM stock_prices
        """,
    )
    latest_age = _days_old(latest_date, today)
    checks.append(
        _check(
            "stock_prices.latest_date",
            latest_age is not None and latest_age <= max_staleness_days,
            (
                f"latest stock_prices date is {latest_date}"
                if latest_date is not None
                else "stock_prices has no rows"
            ),
            observed=latest_date,
            expected=f"within {max_staleness_days} days of {today}",
        )
    )
    total_universe_tickers = _universe_total_price_tickers(conn, universe)
    checks.append(
        _check(
            "stock_prices.total_tickers",
            _coverage_ok(total_universe_tickers, universe.count, min_coverage),
            (
                "stock_prices has historical rows for "
                f"{total_universe_tickers}/{universe.count} {universe.label}"
            ),
            observed=total_universe_tickers,
            expected=f">= {min_coverage:.0%} of {universe.label}",
        )
    )
    checks.append(
        _check(
            "stock_prices.total_rows",
            int(row_count or 0) >= universe.count,
            f"stock_prices has {row_count or 0} total rows",
            observed=int(row_count or 0),
            expected=f">= price universe count ({universe.count})",
        )
    )

    if latest_date is None:
        return checks

    latest_ticker_count, latest_row_count = _universe_latest_price_counts(conn, universe)
    expected_latest = _required_coverage_count(universe.count, min_coverage)
    checks.append(
        _check(
            "stock_prices.latest_coverage",
            _coverage_ok(int(latest_ticker_count or 0), universe.count, min_coverage),
            (
                f"latest stock_prices date has {latest_ticker_count or 0}/"
                f"{universe.count} {universe.label}; expected at least {expected_latest}"
            ),
            observed=int(latest_ticker_count or 0),
            expected=f">= {expected_latest}",
        )
    )
    checks.append(
        _check(
            "stock_prices.latest_rows",
            int(latest_row_count or 0) > 0,
            (
                f"latest stock_prices date has {latest_row_count or 0} rows "
                f"for active tickers"
            ),
            severity="warn",
            observed=int(latest_row_count or 0),
            expected="> 0",
        )
    )

    bad_latest_rows = _fetchone(
        conn,
        """
        SELECT COUNT(*)
        FROM stock_prices
        WHERE date = (SELECT MAX(date) FROM stock_prices)
          AND (
              open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL
              OR open <= 0 OR high <= 0 OR low <= 0 OR close <= 0
              OR high < low OR high < open OR high < close
              OR low > open OR low > close
              OR volume IS NULL OR volume < 0
          )
        """,
    )[0]
    checks.append(
        _check(
            "stock_prices.latest_ohlcv_sanity",
            int(bad_latest_rows or 0) == 0,
            f"{bad_latest_rows or 0} malformed OHLCV rows on latest stock_prices date",
            observed=int(bad_latest_rows or 0),
            expected=0,
        )
    )
    return checks


def _daily_checks(
    conn: Any,
    *,
    active_count: int,
    today: date,
    min_coverage: float,
) -> list[DoctorCheck]:
    checks: list[DoctorCheck] = []

    if _table_exists(conn, "technical_indicators"):
        latest_ta, ta_tickers = _fetchone(
            conn,
            """
            SELECT MAX(date), COUNT(DISTINCT ticker)
            FROM technical_indicators
            """,
        )
        checks.append(
            _check(
                "technical_indicators.latest_date",
                _within_days(latest_ta, today, 10),
                f"latest technical_indicators date is {latest_ta}",
                severity="warn",
                observed=latest_ta,
                expected=f"within 10 days of {today}",
            )
        )
        checks.append(
            _check(
                "technical_indicators.coverage",
                _coverage_ok(int(ta_tickers or 0), active_count, min_coverage),
                f"technical_indicators covers {ta_tickers or 0}/{active_count} active tickers",
                severity="warn",
                observed=int(ta_tickers or 0),
                expected=f">= {min_coverage:.0%} of active companies",
            )
        )

    if _table_exists(conn, "market_internals"):
        latest_market = _fetchone(conn, "SELECT MAX(date) FROM market_internals")[0]
        checks.append(
            _check(
                "market_internals.latest_date",
                _within_days(latest_market, today, 14),
                f"latest market_internals date is {latest_market}",
                severity="warn",
                observed=latest_market,
                expected=f"within 14 days of {today}",
            )
        )

    if _table_exists(conn, "news_articles"):
        latest_news, news_rows = _fetchone(
            conn,
            "SELECT MAX(published_utc), COUNT(*) FROM news_articles",
        )
        checks.append(
            _check(
                "news_articles.recent",
                _within_days(latest_news, today, 14),
                f"latest news article is {latest_news}; total rows={news_rows or 0}",
                severity="warn",
                observed=latest_news,
                expected=f"within 14 days of {today}",
            )
        )

    if _table_exists(conn, "mv_52week_extremes"):
        price_latest, extremes_latest = _fetchone(
            conn,
            """
            SELECT
                (SELECT MAX(date) FROM stock_prices),
                (SELECT MAX(date) FROM mv_52week_extremes)
            """,
        )
        checks.append(
            _check(
                "mv_52week_extremes.freshness",
                price_latest is not None
                and extremes_latest is not None
                and extremes_latest >= price_latest,
                (
                    "mv_52week_extremes latest date is "
                    f"{extremes_latest}; stock_prices latest date is {price_latest}"
                ),
                severity="warn",
                observed=extremes_latest,
                expected=f">= {price_latest}",
            )
        )

    return checks


def _weekly_checks(
    conn: Any,
    *,
    active_count: int,
    today: date,
    min_coverage: float,
) -> list[DoctorCheck]:
    checks: list[DoctorCheck] = []

    economy_thresholds = {
        "treasury_yields": 21,
        "inflation": 120,
        "inflation_expectations": 120,
        "labor_market": 120,
    }
    for table, max_age in economy_thresholds.items():
        if not _table_exists(conn, table):
            continue
        latest = _fetchone(conn, f"SELECT MAX(date) FROM {table}")[0]
        checks.append(
            _check(
                f"{table}.latest_date",
                _within_days(latest, today, max_age),
                f"latest {table} date is {latest}",
                severity="warn",
                observed=latest,
                expected=f"within {max_age} days of {today}",
            )
        )

    if _table_exists(conn, "stock_character_classification"):
        latest_run, classified = _fetchone(
            conn,
            """
            SELECT MAX(run_date), COUNT(DISTINCT ticker)
            FROM stock_character_classification
            """,
        )
        checks.append(
            _check(
                "stock_character_classification.latest_run",
                _within_days(latest_run, today, 21),
                f"latest stock character run is {latest_run}",
                severity="warn",
                observed=latest_run,
                expected=f"within 21 days of {today}",
            )
        )
        checks.append(
            _check(
                "stock_character_classification.coverage",
                _coverage_ok(int(classified or 0), active_count, min_coverage),
                f"stock character covers {classified or 0}/{active_count} active tickers",
                severity="warn",
                observed=int(classified or 0),
                expected=f">= {min_coverage:.0%} of active companies",
            )
        )

    for table in ("stock_splits", "dividends"):
        if _table_exists(conn, table):
            rows = _fetchone(conn, f"SELECT COUNT(*) FROM {table}")[0]
            checks.append(
                _check(
                    f"{table}.readable",
                    rows is not None,
                    f"{table} is readable with {rows or 0} rows",
                    severity="warn",
                    observed=int(rows or 0),
                    expected="query succeeds",
                )
            )

    return checks


def _quarterly_checks(conn: Any, *, today: date) -> list[DoctorCheck]:
    checks: list[DoctorCheck] = []
    date_columns = {
        "financial_ratios": "date",
        "balance_sheets": "period_end",
        "income_statements": "period_end",
        "cash_flows": "period_end",
    }
    for table, date_column in date_columns.items():
        if not _table_exists(conn, table):
            continue
        latest = _fetchone(conn, f"SELECT MAX({date_column}), COUNT(*) FROM {table}")
        latest_date = latest[0]
        rows = int(latest[1] or 0)
        checks.append(
            _check(
                f"{table}.latest_date",
                rows > 0
                and latest_date is not None
                and _within_days(latest_date, today, 210),
                f"latest {table} date is {latest_date}; total rows={rows}",
                severity="warn",
                observed=latest_date,
                expected=f"within 210 days of {today}",
            )
        )
    return checks


def run_doctor_on_connection(
    conn: Any,
    *,
    job: DoctorJob = "all",
    today: date | None = None,
    min_coverage: float = 0.85,
    max_staleness_days: int = 5,
) -> list[DoctorCheck]:
    """Run doctor checks against an existing database connection."""
    today = today or get_market_date()
    checks = _required_tables_checks(conn, job)

    blocking_schema_failures = [c for c in checks if c.status == "FAIL"]
    if blocking_schema_failures:
        return checks

    active_count = _active_company_count(conn)
    checks.extend(
        _price_checks(
            conn,
            active_count=active_count,
            today=today,
            min_coverage=min_coverage,
            max_staleness_days=max_staleness_days,
        )
    )

    if job in {"all", "daily", "coldstart"}:
        checks.extend(
            _daily_checks(
                conn,
                active_count=active_count,
                today=today,
                min_coverage=min_coverage,
            )
        )

    if job in {"all", "weekly", "coldstart"}:
        checks.extend(
            _weekly_checks(
                conn,
                active_count=active_count,
                today=today,
                min_coverage=min_coverage,
            )
        )

    if job in {"all", "quarterly", "coldstart"}:
        checks.extend(_quarterly_checks(conn, today=today))

    return checks


def summarize_checks(checks: list[DoctorCheck]) -> dict[str, Any]:
    """Summarize check counts for run stats and notifications."""
    return {
        "success": not any(c.status == "FAIL" for c in checks),
        "checks": len(checks),
        "passed": sum(c.status == "PASS" for c in checks),
        "warnings": sum(c.status == "WARN" for c in checks),
        "failed": sum(c.status == "FAIL" for c in checks),
    }


def format_checks(checks: list[DoctorCheck]) -> str:
    """Format doctor checks as a compact table."""
    lines = ["", "Database Doctor", ""]
    lines.append(f"{'Status':<6} {'Check':<45} Message")
    lines.append("-" * 96)
    for check in checks:
        lines.append(f"{check.status:<6} {check.name:<45} {check.message}")
    summary = summarize_checks(checks)
    lines.append("")
    lines.append(
        "Summary: "
        f"{summary['passed']} passed, {summary['warnings']} warnings, "
        f"{summary['failed']} failed"
    )
    return "\n".join(lines)


def run_doctor(
    database_url: str,
    *,
    job: DoctorJob = "all",
    min_coverage: float = 0.85,
    max_staleness_days: int = 5,
    today: date | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """Run database doctor checks and return summary stats."""
    logger = logger or setup_logging(run_name="doctor")
    with psycopg.connect(database_url) as conn:
        checks = run_doctor_on_connection(
            conn,
            job=job,
            today=today,
            min_coverage=min_coverage,
            max_staleness_days=max_staleness_days,
        )

    logger.info(format_checks(checks))
    summary = summarize_checks(checks)
    summary["job"] = job
    summary["results"] = [
        {
            "name": c.name,
            "status": c.status,
            "message": c.message,
            "observed": c.observed,
            "expected": c.expected,
        }
        for c in checks
    ]
    return summary


__all__ = [
    "DoctorCheck",
    "DoctorJob",
    "format_checks",
    "run_doctor",
    "run_doctor_on_connection",
    "summarize_checks",
]
