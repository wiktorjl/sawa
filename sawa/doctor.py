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


def _fetchone(
    conn: Any,
    query: str,
    params: tuple[Any, ...] | dict[str, Any] | None = None,
) -> tuple[Any, ...]:
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

    # Completeness of the companies dimension itself. Heavy NULL population in
    # sic_code/market_cap silently degrades sector bucketing and market-cap
    # sorted tools, and was previously unmonitored.
    if active_count > 0:
        total, null_sic, null_mcap = _fetchone(
            conn,
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE sic_code IS NULL),
                   COUNT(*) FILTER (WHERE market_cap IS NULL)
            FROM companies
            WHERE active = true
            """,
        )
        total = int(total or 0)
        if total > 0:
            sic_frac = int(null_sic or 0) / total
            mcap_frac = int(null_mcap or 0) / total
            checks.append(
                _check(
                    "companies.attribute_completeness",
                    sic_frac <= 0.30 and mcap_frac <= 0.30,
                    f"active companies missing sic_code={null_sic or 0}/{total} "
                    f"({sic_frac:.0%}), market_cap={null_mcap or 0}/{total} ({mcap_frac:.0%})",
                    severity="warn",
                    observed=f"sic_null={sic_frac:.0%}, mcap_null={mcap_frac:.0%}",
                    expected="<= 30% NULL on each",
                )
            )
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


# Post-split TA staleness detection. The daily TA loop only appends rows for
# date > last_ta, and split_adjust rewrites historical stock_prices without
# touching technical_indicators, so a recently-split ticker keeps TA computed
# from pre-adjustment prices — off by ~the split ratio — while latest_date and
# coverage still look current. We catch this by recomputing sma_50 from the
# adjusted prices and comparing it to the stored value.
#
# sma_50 (not sma_5) is the signal: it stays contaminated for ~50 trading days
# after a split, whereas the short sma_5 window self-heals within ~5 sessions.
# 60 calendar days back from the latest price date covers that window with
# margin. The 2% tolerance sits in a wide empirical gap — split-contaminated
# tickers diverge >=4% (live: 0.045..8.75) while correctly-recomputed split
# tickers land <1% off (nearest healthy ~0.6%), so the check is not flaky.
_POST_SPLIT_TA_WINDOW_DAYS = 60
_POST_SPLIT_TA_TOLERANCE = 0.02
_POST_SPLIT_TA_QUERY = """
    WITH recent_splits AS (
        SELECT ticker, MAX(execution_date) AS exec_date
        FROM stock_splits
        WHERE execution_date >= (SELECT MAX(date) FROM stock_prices) - %(window)s
        GROUP BY ticker
    ),
    latest_ta AS (
        SELECT DISTINCT ON (ti.ticker)
               ti.ticker, ti.date AS ta_date, ti.sma_50 AS stored_sma_50
        FROM technical_indicators ti
        JOIN recent_splits rs ON rs.ticker = ti.ticker
        ORDER BY ti.ticker, ti.date DESC
    ),
    price_sma AS (
        SELECT w.ticker,
               AVG(w.close) AS price_sma_50,
               COUNT(*) AS n,
               MAX(w.maxdate) AS price_date
        FROM (
            SELECT ticker, close,
                   MAX(date) OVER (PARTITION BY ticker) AS maxdate,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
            FROM stock_prices
            WHERE ticker IN (SELECT ticker FROM recent_splits)
        ) w
        WHERE w.rn <= 50
        GROUP BY w.ticker
    ),
    compared AS (
        SELECT lt.ticker,
               ABS(lt.stored_sma_50 / ps.price_sma_50 - 1) AS rel_diff
        FROM latest_ta lt
        JOIN price_sma ps ON ps.ticker = lt.ticker
        WHERE ps.n >= 50
          AND lt.ta_date = ps.price_date
          AND lt.stored_sma_50 IS NOT NULL
          AND lt.stored_sma_50 > 0
          AND ps.price_sma_50 > 0
    )
    SELECT
        COUNT(*) AS checked,
        COUNT(*) FILTER (WHERE rel_diff > %(tolerance)s) AS flagged,
        COALESCE(
            string_agg(ticker, ', ' ORDER BY rel_diff DESC)
                FILTER (WHERE rel_diff > %(tolerance)s),
            ''
        ) AS worst
    FROM compared
"""


def _post_split_ta_check(conn: Any) -> DoctorCheck:
    """Flag recently-split tickers whose stored TA was never recomputed."""
    checked, flagged, worst = _fetchone(
        conn,
        _POST_SPLIT_TA_QUERY,
        {
            "window": _POST_SPLIT_TA_WINDOW_DAYS,
            "tolerance": _POST_SPLIT_TA_TOLERANCE,
        },
    )
    checked = int(checked or 0)
    flagged = int(flagged or 0)
    worst_list = str(worst or "")
    # Cap the offender list so the message stays compact.
    sample = ", ".join(worst_list.split(", ")[:8]) if worst_list else ""
    if flagged > 0:
        message = (
            f"{flagged}/{checked} recently-split tickers have stored sma_50 "
            f">{_POST_SPLIT_TA_TOLERANCE:.0%} off the price-derived value "
            f"(TA not recomputed after split): {sample}"
        )
    else:
        message = (
            f"all {checked} recently-split tickers have stored sma_50 within "
            f"{_POST_SPLIT_TA_TOLERANCE:.0%} of the price-derived value"
        )
    return _check(
        "technical_indicators.post_split_recompute",
        flagged == 0,
        message,
        severity="warn",
        observed=flagged,
        expected=0,
    )


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
        # NEW dates get TA every daily run, so chronic staleness/coverage loss
        # is a real failure the scheduler should catch and retry — not a silent
        # WARN. Threshold allows for a long weekend. NOTE: the daily path only
        # appends TA for date > last_ta; it does NOT recompute historical rows,
        # so these two checks alone stay green even when a split has rewritten
        # the underlying prices (see technical_indicators.post_split_recompute
        # below, which guards that case).
        checks.append(
            _check(
                "technical_indicators.latest_date",
                _within_days(latest_ta, today, 4),
                f"latest technical_indicators date is {latest_ta}",
                severity="fail",
                observed=latest_ta,
                expected=f"within 4 days of {today}",
            )
        )
        checks.append(
            _check(
                "technical_indicators.coverage",
                _coverage_ok(int(ta_tickers or 0), active_count, min_coverage),
                f"technical_indicators covers {ta_tickers or 0}/{active_count} active tickers",
                severity="fail",
                observed=int(ta_tickers or 0),
                expected=f">= {min_coverage:.0%} of active companies",
            )
        )

        if _table_exists(conn, "stock_splits"):
            checks.append(_post_split_ta_check(conn))

    if _table_exists(conn, "market_internals"):
        latest_market = _fetchone(conn, "SELECT MAX(date) FROM market_internals")[0]
        # Refreshed by every daily run (FRED + same-day CBOE); promote to FAIL
        # so a silently-skipped internals step is caught and retried.
        checks.append(
            _check(
                "market_internals.latest_date",
                _within_days(latest_market, today, 5),
                f"latest market_internals date is {latest_market}",
                severity="fail",
                observed=latest_market,
                expected=f"within 5 days of {today}",
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

    # treasury_yields is a daily-cadence series but currently refreshed only by
    # the weekly job, so it can lag up to a week. Threshold tightened from 21 to
    # 8 days so a *missed* weekly run (which would push it past a week) is
    # surfaced instead of hidden. (Follow-up: move it to the daily refresh.)
    #
    # treasury_yields is the fastest-cadence weekly series, so promote it to FAIL
    # so a silently-skipped/failed weekly economy pull flips the exit code and
    # the scheduler retries — the same "success on failure" guard the daily
    # TA/internals checks already carry. The slower inflation/labor series stay
    # WARN on their long thresholds (they genuinely refresh infrequently).
    economy_thresholds: dict[str, tuple[int, Severity]] = {
        "treasury_yields": (8, "fail"),
        "inflation": (120, "warn"),
        "inflation_expectations": (120, "warn"),
        "labor_market": (120, "warn"),
    }
    for table, (max_age, severity) in economy_thresholds.items():
        if not _table_exists(conn, table):
            continue
        latest = _fetchone(conn, f"SELECT MAX(date) FROM {table}")[0]
        checks.append(
            _check(
                f"{table}.latest_date",
                _within_days(latest, today, max_age),
                f"latest {table} date is {latest}",
                severity=severity,
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
        # The character classification is the headline weekly artifact; a run
        # that silently fails or is skipped leaves stale classifications served
        # to MCP tools with no alert. Promote freshness to FAIL (the 21-day
        # threshold still tolerates a single missed Saturday) so a stale weekly
        # cadence flips the exit code; coverage stays WARN.
        checks.append(
            _check(
                "stock_character_classification.latest_run",
                _within_days(latest_run, today, 21),
                f"latest stock character run is {latest_run}",
                severity="fail",
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
        # Fundamentals freshness is FAIL-capable: if a reporting season is
        # missed or the quarterly pull silently fails, success=not any FAIL must
        # flip the exit code so the run is surfaced/retried rather than served
        # stale indefinitely. The 210-day window is deliberately loose (a single
        # missed quarter still passes) — it only fires on a genuinely stale or
        # empty fundamentals table.
        checks.append(
            _check(
                f"{table}.latest_date",
                rows > 0
                and latest_date is not None
                and _within_days(latest_date, today, 210),
                f"latest {table} date is {latest_date}; total rows={rows}",
                severity="fail",
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
