"""Database doctor checks for scheduled Sawa jobs."""

from __future__ import annotations

import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from datetime import time as dt_time
from math import ceil
from pathlib import Path
from typing import Any, Literal

import psycopg

from sawa.utils.logging import setup_logging
from sawa.utils.market_hours import (
    ET,
    expected_latest_eod_date,
    is_trading_day,
    previous_trading_day,
)

DoctorJob = Literal["all", "daily", "weekly", "quarterly", "coldstart", "watchdog"]
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
    # A future-dated row is not "extra fresh": it is corrupt or clock-skewed
    # data and must fail the freshness check instead of masking stale feeds.
    return age is not None and 0 <= age <= max_days


def _market_date_of(value: date | datetime | None) -> date | None:
    """Calendar date of ``value`` in market (ET) time.

    published_utc rows written at 21:00 ET land on the next UTC date; judging
    them by their UTC date would call same-evening news "future-dated".
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(ET)
        return value.date()
    return value


def _required_coverage_count(expected_count: int, min_coverage: float) -> int:
    return ceil(expected_count * min_coverage)


def _coverage_ok(count: int, expected_count: int, min_coverage: float) -> bool:
    if expected_count <= 0:
        return False
    return count >= _required_coverage_count(expected_count, min_coverage)


def _required_tables_checks(conn: Any, job: DoctorJob) -> list[DoctorCheck]:
    tables = ["companies", "stock_prices"]
    if job in {"all", "daily", "coldstart", "watchdog"}:
        tables.extend(
            [
                "technical_indicators",
                "news_articles",
                "market_internals",
                "treasury_yields",
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
    checks.append(
        _check(
            "stock_prices.latest_date",
            _within_days(latest_date, today, max_staleness_days),
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


_BACKUP_DIR_ENV = "SAWA_BACKUP_DIR"
_DEFAULT_BACKUP_DIR = "/data/db-backups"
_BACKUP_GLOB = "postgres_backup_*.tar"
# Backups run weekly, so 10 days leaves room for one missed run before alerting.
_BACKUP_MAX_AGE_DAYS = 10


def _backup_checks(*, now: float | None = None) -> list[DoctorCheck]:
    """Fail when the newest database backup has gone stale.

    The weekly backup failed silently on 14 consecutive runs because nothing
    downstream ever looked at its result: the script wrote to a log file, and
    a non-zero exit reached no one. Checking the artifact here means a stopped
    backup — or a stopped cron — surfaces through the same alerting path as
    every other doctor failure, within a day rather than a quarter.

    Hosts with no backup directory (a developer machine, CI) are not running
    backups at all, so the check does not apply and is omitted entirely rather
    than reported as a failure.

    Age is elapsed time, not a difference of calendar dates: the file carries a
    UTC timestamp while the rest of the doctor works in market (ET) dates, and
    subtracting one from the other reports a backup written this evening as
    minus one day old.
    """
    backup_dir = Path(os.environ.get(_BACKUP_DIR_ENV) or _DEFAULT_BACKUP_DIR)
    if not backup_dir.is_dir():
        return []

    try:
        backups = sorted(
            backup_dir.glob(_BACKUP_GLOB),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
    except OSError as exc:
        return [
            _check(
                "backup.freshness",
                False,
                f"cannot read backup directory {backup_dir}: {exc.strerror}",
                severity="fail",
                observed="unreadable",
                expected=f"a readable {backup_dir}",
            )
        ]

    expected = f"an archive newer than {_BACKUP_MAX_AGE_DAYS} days"
    if not backups:
        return [
            _check(
                "backup.freshness",
                False,
                f"no {_BACKUP_GLOB} archive in {backup_dir}",
                severity="fail",
                observed="none",
                expected=expected,
            )
        ]

    newest = backups[0]
    stat = newest.stat()
    reference = time.time() if now is None else now
    age_days = max(0, int((reference - stat.st_mtime) // 86400))
    size_gb = stat.st_size / 1024**3
    return [
        _check(
            "backup.freshness",
            age_days <= _BACKUP_MAX_AGE_DAYS,
            f"newest backup is {newest.name} ({size_gb:.1f} GB), {age_days} day(s) old",
            severity="fail",
            observed=age_days,
            expected=expected,
        )
    ]


# News is a seven-day feed re-pulled by every daily run, so its newest article
# should trail the latest expected EOD session by at most a couple of days.
# The watchdog (calendar-strict) FAILS after two days of lag. The post-job
# daily doctor only WARNS, with an extra day of slack: sawa/daily.py makes the
# news step non-fatal on purpose, and a doctor FAIL there withholds daily_done
# and re-runs the whole daily on every evening tick — a provider news outage
# must not turn into a retry loop against the price provider.
_NEWS_MAX_LAG_DAYS_STRICT = 2
_NEWS_MAX_LAG_DAYS_POST_JOB = 3


def _daily_checks(
    conn: Any,
    *,
    active_count: int,
    today: date,
    min_coverage: float,
    expected_eod: date,
    calendar_strict: bool = False,
    internals_strict: bool = False,
) -> list[DoctorCheck]:
    """Daily-cadence freshness checks.

    ``expected_eod`` is the most recent session whose data should be present.
    With ``calendar_strict`` (the watchdog job) the TA and market-internals
    thresholds collapse to that session instead of the weekend-sized calendar
    windows the post-job doctor uses, so a single missed session is caught the
    next morning.
    """
    checks: list[DoctorCheck] = []
    eod_lag = max(0, (today - expected_eod).days)
    ta_max_days = eod_lag if calendar_strict else 4

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
                _within_days(latest_ta, today, ta_max_days),
                f"latest technical_indicators date is {latest_ta}",
                severity="fail",
                observed=latest_ta,
                expected=f"within {ta_max_days} days of {today}",
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
        latest_values = _fetchone(
            conn,
            """
            SELECT
                MAX(date) FILTER (WHERE vix IS NOT NULL),
                MAX(date) FILTER (WHERE vix3m IS NOT NULL),
                MAX(date) FILTER (WHERE hy_spread IS NOT NULL)
            FROM public.market_internals
            """,
        )
        # A fresh value in one column must not mask a failed/stale independent
        # provider series in another column.
        # FRED publishes the HY spread one business day late, so it is
        # legitimately one session behind VIX/VIX3M on a healthy run.
        internals_floor = {
            "vix": expected_eod,
            "vix3m": expected_eod,
            "hy_spread": previous_trading_day(expected_eod),
        }
        for field, latest_value in zip(
            ("vix", "vix3m", "hy_spread"), latest_values, strict=True
        ):
            max_days = (
                max(0, (today - internals_floor[field]).days)
                if calendar_strict or internals_strict else 5
            )
            checks.append(
                _check(
                    f"market_internals.{field}.latest_date",
                    _within_days(latest_value, today, max_days),
                    f"latest market_internals.{field} date is {latest_value}",
                    severity="fail",
                    observed=latest_value,
                    expected=f"within {max_days} days of {today}",
                )
            )

    if _table_exists(conn, "treasury_yields"):
        latest_treasury = _fetchone(
            conn, "SELECT MAX(date) FROM public.treasury_yields"
        )[0]
        # Unlike CBOE's same-session settlement, Treasury observations may be
        # published one completed trading session late. Use the trading
        # calendar so weekends/holidays do not consume that publication lag.
        treasury_floor = previous_trading_day(expected_eod)
        treasury_max_days = max(0, (today - treasury_floor).days)
        checks.append(
            _check(
                "treasury_yields.latest_date",
                _within_days(latest_treasury, today, treasury_max_days),
                f"latest treasury_yields date is {latest_treasury}",
                severity="fail",
                observed=latest_treasury,
                expected=f"on or after {treasury_floor} and not after {today} "
                "(one completed trading session publication lag)",
            )
        )

    if _table_exists(conn, "news_articles"):
        latest_news, news_rows = _fetchone(
            conn,
            "SELECT MAX(published_utc), COUNT(*) FROM news_articles",
        )
        news_lag = _NEWS_MAX_LAG_DAYS_STRICT if calendar_strict else _NEWS_MAX_LAG_DAYS_POST_JOB
        news_floor = expected_eod - timedelta(days=news_lag)
        news_date = _market_date_of(latest_news)
        checks.append(
            _check(
                "news_articles.recent",
                news_date is not None and news_floor <= news_date <= today,
                f"latest news article is {latest_news}; total rows={news_rows or 0}",
                severity="fail" if calendar_strict else "warn",
                observed=latest_news,
                expected=f"published on or after {news_floor} (latest session {expected_eod})",
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
        extremes_age = _days_old(extremes_latest, today)
        checks.append(
            _check(
                "mv_52week_extremes.freshness",
                price_latest is not None
                and extremes_latest is not None
                and extremes_latest == price_latest
                and extremes_age is not None
                and extremes_age >= 0,
                (
                    "mv_52week_extremes latest date is "
                    f"{extremes_latest}; stock_prices latest date is {price_latest}"
                ),
                severity="warn",
                observed=extremes_latest,
                expected=f"equal to {price_latest} and not future-dated",
            )
        )

    return checks


def _weekly_checks(
    conn: Any,
    *,
    active_count: int,
    today: date,
    min_coverage: float,
    include_treasury: bool = True,
) -> list[DoctorCheck]:
    checks: list[DoctorCheck] = []

    # Treasury now refreshes daily and the daily/watchdog checks enforce its
    # one-session publication lag. Keep this older eight-day guard for the
    # standalone weekly economy job; combined all/coldstart checks already use
    # the stricter daily guard and must not emit a duplicate weaker result.
    # Slow inflation/labor series stay WARN on their long release cadences.
    economy_thresholds: dict[str, tuple[int, Severity]] = {
        "treasury_yields": (8, "fail"),
        "inflation": (120, "warn"),
        "inflation_expectations": (120, "warn"),
        "labor_market": (120, "warn"),
    }
    for table, (max_age, severity) in economy_thresholds.items():
        if table == "treasury_yields" and not include_treasury:
            continue
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
        latest_run = _fetchone(
            conn,
            """
            SELECT MAX(sc.run_date)
            FROM stock_character_classification sc
            JOIN companies c ON c.ticker = sc.ticker AND c.active = true
            """,
        )[0]
        classified, eligible = 0, 0
        if latest_run is not None:
            from sawa.calculation.stock_character_config import MIN_HISTORY_DAYS

            source_date = previous_trading_day(latest_run + timedelta(days=1))
            classified, eligible = _fetchone(
                conn,
                """
                WITH eligible_character_prices AS (
                    SELECT sp.ticker
                    FROM stock_prices sp
                    JOIN companies c ON c.ticker = sp.ticker AND c.active = true
                    WHERE sp.date <= %s
                    GROUP BY sp.ticker
                    HAVING MAX(sp.date) = %s AND COUNT(*) >= %s
                )
                SELECT COUNT(sc.ticker), COUNT(ep.ticker)
                FROM eligible_character_prices ep
                LEFT JOIN stock_character_classification sc
                    ON sc.ticker = ep.ticker AND sc.run_date = %s
                """,
                (latest_run, source_date, MIN_HISTORY_DAYS, latest_run),
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
                bool(eligible) and _coverage_ok(int(classified or 0), int(eligible), min_coverage),
                f"latest stock character run covers {classified or 0}/{eligible} "
                "eligible active tickers with current source prices",
                severity="warn",
                observed=int(classified or 0),
                expected=f">= {min_coverage:.0%} of eligible active companies on latest run",
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


# ── Scheduler liveness (watchdog job) ────────────────────────────────────────
#
# Every check above reads the database, so all of them are downstream of the
# scheduler that fills it. These read the scheduler's own state directory and
# fail when the scheduler itself has stopped ticking, stopped marking jobs
# done, or left the intraday streamer running outside the session — the
# conditions that produced the silent 2026-09-04..09-15 outage.

_SCHEDULER_STATE_DIR_ENV = "SAWA_SCHEDULER_STATE_DIR"
_SCHEDULER_LOG_LINE = re.compile(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) ET\] (.*)$")
_SCHEDULER_LOG_TAIL_BYTES = 4 * 1024 * 1024
# The cron cadence is 15 minutes; a running daily/weekly holds the lock for up
# to ~2.5 h and logs no tick, so the age limit only applies when no job is live.
_SCHEDULER_TICK_MAX_AGE = timedelta(minutes=45)
_SCHEDULER_JOBS = ("daily", "weekly", "coldstart", "quarterly")
_INTRADAY_SESSION_START = dt_time(9, 0)
_INTRADAY_SESSION_END = dt_time(16, 30)


def _scheduler_state_dir() -> Path:
    configured = os.environ.get(_SCHEDULER_STATE_DIR_ENV)
    return Path(configured) if configured else Path.home() / ".sawa" / "scheduler"


def _tail_text(path: Path, max_bytes: int = _SCHEDULER_LOG_TAIL_BYTES) -> str:
    with path.open("rb") as handle:
        handle.seek(0, os.SEEK_END)
        size = handle.tell()
        handle.seek(max(0, size - max_bytes))
        return handle.read().decode("utf-8", errors="replace")


def _sawa_subcommand(pid_dir: Path) -> str | None:
    """Subcommand of a ``sawa`` CLI process, read from /proc/<pid>/cmdline."""
    try:
        argv = (pid_dir / "cmdline").read_bytes().split(b"\0")
    except OSError:
        return None
    for previous, argument in zip(argv, argv[1:]):
        if previous.rsplit(b"/", 1)[-1] == b"sawa":
            return argument.decode("utf-8", errors="replace")
    return None


def _running_scheduler_job(proc: Path = Path("/proc")) -> str | None:
    """Name of a scheduler-driven sawa job currently running, if any."""
    try:
        entries = list(proc.iterdir())
    except OSError:
        return None
    for entry in entries:
        if not entry.name.isdigit():
            continue
        subcommand = _sawa_subcommand(entry)
        if subcommand in _SCHEDULER_JOBS:
            return subcommand
    return None


def _intraday_process_alive(pid: int, proc: Path = Path("/proc")) -> bool:
    return _sawa_subcommand(proc / str(pid)) == "intraday"


def _scheduler_state_checks(
    *,
    now_et: datetime,
    expected_eod: date,
    state_dir: Path | None = None,
    proc: Path = Path("/proc"),
) -> list[DoctorCheck]:
    state_dir = state_dir or _scheduler_state_dir()
    checks: list[DoctorCheck] = []
    if not state_dir.is_dir():
        return [
            _check(
                "scheduler.state_dir",
                False,
                f"scheduler state directory {state_dir} does not exist",
                severity="fail",
                observed=str(state_dir),
                expected="the directory market_scheduler.sh writes to",
            )
        ]

    # 1. The scheduler is still ticking (setup_env succeeded recently).
    log_path = state_dir / "scheduler.log"
    last_tick: datetime | None = None
    last_error: tuple[datetime, str] | None = None
    if log_path.is_file():
        for line in _tail_text(log_path).splitlines():
            match = _SCHEDULER_LOG_LINE.match(line)
            if not match:
                continue
            stamp = ET.localize(datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S"))
            message = match.group(2)
            if message.startswith("Scheduler tick"):
                last_tick, last_error = stamp, None
            elif message.startswith("ERROR:"):
                last_error = (stamp, message)
    running_job = _running_scheduler_job(proc)
    if last_tick is None:
        tick_ok, tick_message = False, f"no 'Scheduler tick' line in {log_path}"
    else:
        age = now_et - last_tick
        age_minutes = int(age.total_seconds() // 60)
        tick_ok = age <= _SCHEDULER_TICK_MAX_AGE or running_job is not None
        tick_message = f"last scheduler tick {last_tick:%Y-%m-%d %H:%M ET} ({age_minutes} min ago)"
        if running_job:
            tick_message += f"; sawa {running_job} is running"
    if last_error is not None:
        tick_message += (
            f"; newest error after that tick: {last_error[1]!r} at "
            f"{last_error[0]:%Y-%m-%d %H:%M ET} (see ~/.sawa/scheduler/cron.log)"
        )
    checks.append(
        _check(
            "scheduler.tick_freshness",
            tick_ok,
            tick_message,
            severity="fail",
            observed=last_tick,
            expected=f"a tick within {int(_SCHEDULER_TICK_MAX_AGE.total_seconds() // 60)} min",
        )
    )

    # 2. The daily job completed (doctor passed) for the latest expected session.
    daily_flag = state_dir / f"daily_done_{expected_eod:%Y-%m-%d}"
    checks.append(
        _check(
            "scheduler.daily_done",
            daily_flag.is_file(),
            (
                f"{daily_flag.name} present"
                if daily_flag.is_file()
                else f"{daily_flag.name} missing: no completed daily for session {expected_eod}"
            ),
            severity="fail",
            observed=daily_flag.is_file(),
            expected=f"{daily_flag.name} in {state_dir}",
        )
    )

    # 3. The weekly job completed for the current ISO week (it runs on the
    #    first closed-market evening of the week, i.e. Monday); on Monday the
    #    previous week's flag is the one that must exist.
    reference_day = now_et.date()
    if reference_day.isoweekday() == 1:
        reference_day -= timedelta(days=7)
    iso_year, iso_week, _ = reference_day.isocalendar()
    weekly_flag = state_dir / f"weekly_done_{iso_year}-W{iso_week:02d}"
    checks.append(
        _check(
            "scheduler.weekly_done",
            weekly_flag.is_file(),
            (
                f"{weekly_flag.name} present"
                if weekly_flag.is_file()
                else f"{weekly_flag.name} missing: no completed weekly for ISO week {iso_week}"
            ),
            severity="fail",
            observed=weekly_flag.is_file(),
            expected=f"{weekly_flag.name} in {state_dir}",
        )
    )

    # 4. No intraday streamer left running outside the regular session.
    pid_file = state_dir / "intraday.pid"
    pid: int | None = None
    if pid_file.is_file():
        first_field = pid_file.read_text().split()[:1]
        if first_field and first_field[0].isdigit():
            pid = int(first_field[0])
    alive = pid is not None and _intraday_process_alive(pid, proc)
    in_session_window = (
        is_trading_day(now_et.date())
        and _INTRADAY_SESSION_START <= now_et.time() <= _INTRADAY_SESSION_END
    )
    started = ""
    start_file = state_dir / "intraday_start_time"
    if alive and start_file.is_file():
        started = f", started {start_file.read_text().strip()}"
    checks.append(
        _check(
            "scheduler.intraday_orphan",
            not alive or in_session_window,
            (
                f"intraday PID {pid} is running outside the session window{started}"
                if alive and not in_session_window
                else (
                    f"intraday PID {pid} running during the session"
                    if alive else "no live intraday process"
                )
            ),
            severity="fail",
            observed=pid if alive else None,
            expected="no intraday process outside 09:00-16:30 ET on trading days",
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
    now: datetime | None = None,
    scheduler_state_dir: Path | None = None,
    proc: Path = Path("/proc"),
) -> list[DoctorCheck]:
    """Run doctor checks against an existing database connection.

    ``now`` fixes the wall clock (tests, replays). When only ``today`` is
    given the clock is pinned to the end of that market day, so a trading day
    is expected to be fully loaded — the situation after a post-job doctor.
    ``scheduler_state_dir`` and ``proc`` are only read by the watchdog job
    (defaults: ~/.sawa/scheduler or $SAWA_SCHEDULER_STATE_DIR, and /proc).
    """
    if now is not None:
        now_et = now.astimezone(ET)
    elif today is not None:
        now_et = ET.localize(datetime.combine(today, dt_time(23, 59)))
    else:
        now_et = datetime.now(ET)
    today = today or now_et.date()
    # Post-job doctors run right after the 17:00 ET daily, so from 17:00 the
    # current session is expected. The watchdog runs on its own clock (mornings)
    # and must not expect today's session before the daily has had time to
    # finish, so it waits until 20:00 ET.
    settled_hour = 20 if job == "watchdog" else 17
    expected_eod = expected_latest_eod_date(now_et, settled_hour=settled_hour)
    if job == "watchdog":
        # Stock prices must reach the latest expected session exactly; the
        # --max-staleness-days flag is deliberately ignored for this job.
        max_staleness_days = max(0, (today - expected_eod).days)

    checks = _required_tables_checks(conn, job)

    blocking_schema_failures = [c for c in checks if c.status == "FAIL"]
    if blocking_schema_failures:
        return checks

    checks.extend(_backup_checks())

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

    if job in {"all", "daily", "coldstart", "watchdog"}:
        checks.extend(
            _daily_checks(
                conn,
                active_count=active_count,
                today=today,
                min_coverage=min_coverage,
                expected_eod=expected_eod,
                calendar_strict=job == "watchdog",
                internals_strict=job in {"daily", "watchdog"},
            )
        )

    if job == "watchdog":
        checks.extend(
            _scheduler_state_checks(
                now_et=now_et,
                expected_eod=expected_eod,
                state_dir=scheduler_state_dir,
                proc=proc,
            )
        )

    if job in {"all", "weekly", "coldstart"}:
        checks.extend(
            _weekly_checks(
                conn,
                active_count=active_count,
                today=today,
                min_coverage=min_coverage,
                include_treasury=job == "weekly",
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
    # Dict values render as "name=message" pairs in the failure notification,
    # so the push says *which* checks failed instead of only how many.
    failures = {c.name: c.message for c in checks if c.status == "FAIL"}
    if failures:
        summary["failures"] = failures
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
