"""
Corporate actions: Download and store stock splits, dividends, and earnings.

Purpose: Update splits, dividends, and earnings tables from Polygon API.
Re-entrant: Safe to run multiple times (upsert on unique constraints).
"""

import logging
from datetime import date, timedelta
from typing import Any

import psycopg

from sawa.api import PolygonClient
from sawa.domain.corporate_actions import (
    Dividend,
    Earnings,
    SplitAdjuster,
    StockSplit,
    is_unrepresentable_split_ratio,
)
from sawa.repositories.rate_limiter import SyncRateLimiter
from sawa.utils import setup_logging
from sawa.utils.constants import DEFAULT_API_RATE_LIMIT
from sawa.utils.security import redact_sensitive_text
from sawa.utils.symbols import validate_ticker


class ActionPersistenceResult(int):
    """Int-compatible exact persistence outcome for one action artifact."""

    source_rows: int
    rejected_rows: int
    persisted_tickers: tuple[str, ...]

    def __new__(
        cls,
        persisted: int,
        *,
        source_rows: int,
        rejected_rows: int = 0,
        persisted_tickers: list[str] | None = None,
    ) -> "ActionPersistenceResult":
        obj = int.__new__(cls, persisted)
        obj.source_rows = source_rows
        obj.rejected_rows = rejected_rows
        obj.persisted_tickers = tuple(dict.fromkeys(persisted_tickers or []))
        return obj

    @property
    def fully_persisted(self) -> bool:
        return int(self) == self.source_rows and self.rejected_rows == 0

    def summary(self) -> dict[str, Any]:
        return {
            "source_rows": self.source_rows,
            # Writes are still pending when the runner builds this result. The
            # transaction outcome below fills in committed_rows truthfully.
            "attempted_rows": int(self),
            "committed_rows": 0,
            "rejected_rows": self.rejected_rows,
        }


def get_active_tickers(conn) -> list[str]:
    """Get list of active tickers from companies table."""
    with conn.cursor() as cur:
        cur.execute("SELECT ticker FROM companies WHERE active = true ORDER BY ticker")
        return [row[0] for row in cur.fetchall()]


def get_split_adjuster(conn) -> SplitAdjuster:
    """Build a ``SplitAdjuster`` from every split recorded in ``stock_splits``.

    Flat-file bars are as-traded, so any loader that writes them into the
    split-adjusted ``stock_prices`` series must re-base them with the splits
    the registry knows about; an incomplete registry leaves bars before an
    unrecorded split on the wrong basis.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ticker, execution_date, split_from, split_to FROM stock_splits"
        )
        return SplitAdjuster.from_rows(cur.fetchall())


def load_splits(
    conn,
    splits: list[StockSplit],
    logger: logging.Logger,
    *,
    commit: bool = True,
) -> ActionPersistenceResult:
    """Load stock splits into database using upsert."""
    if not splits:
        return ActionPersistenceResult(0, source_rows=0)

    insert_sql = """
        INSERT INTO stock_splits (ticker, execution_date, split_from, split_to)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (ticker, execution_date) DO UPDATE SET
            split_from = EXCLUDED.split_from,
            split_to = EXCLUDED.split_to
    """

    loaded = 0
    rejected = 0
    persisted_tickers: list[str] = []
    with conn.cursor() as cur:
        for split in splits:
            try:
                cur.execute("SAVEPOINT row_insert")
                cur.execute(insert_sql, split.to_tuple())
                cur.execute("RELEASE SAVEPOINT row_insert")
                loaded += 1
                persisted_tickers.append(split.ticker)
            except psycopg.errors.ForeignKeyViolation:
                # Skip splits for tickers not in companies table. Roll back only
                # this row's savepoint so prior inserts in the batch survive.
                cur.execute("ROLLBACK TO SAVEPOINT row_insert")
                cur.execute("RELEASE SAVEPOINT row_insert")
                rejected += 1
                logger.debug(f"Skipping split for unknown ticker: {split.ticker}")
                continue

    if commit:
        conn.commit()
    return ActionPersistenceResult(
        loaded,
        source_rows=len(splits),
        rejected_rows=rejected,
        persisted_tickers=persisted_tickers,
    )


def load_dividends(
    conn,
    dividends: list[Dividend],
    logger: logging.Logger,
    *,
    commit: bool = True,
) -> ActionPersistenceResult:
    """Load dividends into database using upsert."""
    if not dividends:
        return ActionPersistenceResult(0, source_rows=0)

    insert_sql = """
        INSERT INTO dividends (
            ticker, ex_dividend_date, record_date, pay_date,
            cash_amount, declaration_date, dividend_type, frequency
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (
            ticker,
            ex_dividend_date,
            (COALESCE(dividend_type, ''::character varying))
        ) DO UPDATE SET
            record_date = EXCLUDED.record_date,
            pay_date = EXCLUDED.pay_date,
            cash_amount = EXCLUDED.cash_amount,
            declaration_date = EXCLUDED.declaration_date,
            frequency = EXCLUDED.frequency
    """

    loaded = 0
    rejected = 0
    with conn.cursor() as cur:
        for div in dividends:
            try:
                cur.execute("SAVEPOINT row_insert")
                cur.execute(insert_sql, div.to_tuple())
                cur.execute("RELEASE SAVEPOINT row_insert")
                loaded += 1
            except psycopg.errors.ForeignKeyViolation:
                # Skip dividends for tickers not in companies table. Roll back
                # only this row's savepoint so prior inserts survive.
                cur.execute("ROLLBACK TO SAVEPOINT row_insert")
                cur.execute("RELEASE SAVEPOINT row_insert")
                rejected += 1
                logger.debug(f"Skipping dividend for unknown ticker: {div.ticker}")
                continue

    if commit:
        conn.commit()
    return ActionPersistenceResult(
        loaded,
        source_rows=len(dividends),
        rejected_rows=rejected,
    )


def load_earnings(
    conn,
    earnings: list[Earnings],
    logger: logging.Logger,
    *,
    commit: bool = True,
) -> ActionPersistenceResult:
    """Load earnings into database using upsert.

    Aligned to the migrated earnings schema (migration 19 swapped the unique
    constraint to (ticker, report_date); migration 20 dropped revenue_estimate
    and added surprise_pct), so the upsert keys on report_date rather than the
    no-longer-present fiscal-period constraint.
    """
    if not earnings:
        return ActionPersistenceResult(0, source_rows=0)

    insert_sql = """
        INSERT INTO earnings (
            ticker, report_date, fiscal_quarter, fiscal_year,
            timing, eps_estimate, eps_actual, revenue_actual, surprise_pct
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, report_date) DO UPDATE SET
            fiscal_quarter = EXCLUDED.fiscal_quarter,
            fiscal_year = EXCLUDED.fiscal_year,
            timing = EXCLUDED.timing,
            eps_estimate = EXCLUDED.eps_estimate,
            eps_actual = EXCLUDED.eps_actual,
            revenue_actual = EXCLUDED.revenue_actual,
            surprise_pct = EXCLUDED.surprise_pct
    """

    loaded = 0
    rejected = 0
    with conn.cursor() as cur:
        for earn in earnings:
            # Skip if missing report_date (the unique-constraint key).
            if not earn.report_date:
                logger.debug(f"Skipping earnings without report_date: {earn.ticker}")
                rejected += 1
                continue
            try:
                cur.execute("SAVEPOINT row_insert")
                cur.execute(insert_sql, earn.to_tuple())
                cur.execute("RELEASE SAVEPOINT row_insert")
                loaded += 1
            except psycopg.errors.ForeignKeyViolation:
                # Roll back only this row's savepoint so prior inserts survive.
                cur.execute("ROLLBACK TO SAVEPOINT row_insert")
                cur.execute("RELEASE SAVEPOINT row_insert")
                rejected += 1
                logger.debug(f"Skipping earnings for unknown ticker: {earn.ticker}")
                continue

    if commit:
        conn.commit()
    return ActionPersistenceResult(
        loaded,
        source_rows=len(earnings),
        rejected_rows=rejected,
    )


def run_corporate_actions_update(
    api_key: str,
    database_url: str,
    start_date: date | None = None,
    tickers: list[str] | None = None,
    include_splits: bool = True,
    include_dividends: bool = True,
    include_earnings: bool = False,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Download and store corporate actions (splits, dividends, earnings) from Polygon.

    Args:
        api_key: Polygon API key
        database_url: PostgreSQL connection URL
        start_date: Fetch data from this date (default: 1 year ago)
        tickers: List of tickers to fetch (default: all active)
        include_splits: Whether to fetch splits
        include_dividends: Whether to fetch dividends
        include_earnings: Whether to fetch earnings (experimental, Polygon ticker-events
            API currently only returns ticker_change events, not earnings)
        dry_run: If True, show what would be done without writing
        logger: Logger instance

    Returns:
        Statistics dictionary with counts
    """
    logger = logger or setup_logging()
    stats: dict[str, Any] = {
        "success": False,
        "splits_fetched": 0,
        "splits_loaded": 0,
        "dividends_fetched": 0,
        "dividends_loaded": 0,
        "earnings_fetched": 0,
        "earnings_loaded": 0,
        "split_tickers": [],
        "errors": [],
    }

    if not any((include_splits, include_dividends, include_earnings)):
        raise ValueError("at least one corporate-action feed must be requested")
    full_universe_requested = tickers is None
    if tickers is not None:
        if not tickers:
            raise ValueError("explicit corporate-action ticker list is empty")
        tickers = sorted({validate_ticker(ticker) for ticker in tickers})

    default_window_requested = start_date is None
    end_date = date.today()
    # Default start date: 1 year ago
    if start_date is None:
        start_date = end_date - timedelta(days=365)
    if start_date > end_date:
        raise ValueError("corporate-action start date cannot be in the future")

    start_str = start_date.isoformat()
    end_str = end_date.isoformat()
    history_days = (end_date - start_date).days
    # The normal weekly refresh queries both global feeds over their default
    # full-universe annual window. Both being empty is not credible. Explicit
    # ticker investigations and the daily short split-heal window can
    # legitimately have no matching actions and must remain successful.
    full_refresh_activity_required = bool(
        full_universe_requested
        and default_window_requested
        and include_splits
        and include_dividends
    )
    stats["feed_expectations"] = {
        "splits_nonempty_required": full_refresh_activity_required,
        "dividends_nonempty_required": full_refresh_activity_required,
        "history_days": history_days,
    }

    logger.info(f"Corporate actions update: {start_str} to {end_str}")

    # Initialize API client
    client = PolygonClient(api_key, logger)
    rate_limiter = SyncRateLimiter(DEFAULT_API_RATE_LIMIT)

    with psycopg.connect(database_url) as conn:
        # Get tickers if not provided
        if tickers is None:
            tickers = get_active_tickers(conn)
            logger.info(f"Found {len(tickers)} active tickers")
        if not tickers:
            raise ValueError("no active tickers resolved for corporate actions")
        ticker_set = set(tickers)

        def _tracked_only(
            records: list[Any],
            label: str,
        ) -> list[Any]:
            """Drop provider records for tickers we do not track, before parsing.

            Polygon's corporate-action feeds cover instruments outside our
            universe: structured-product identifiers (VIIT0142, MSTR0263),
            fund share classes, and money-market funds quoting sub-cent
            distributions that NUMERIC(10,4) cannot hold. Every one of those is
            discarded by the ticker filter below anyway — but parsing them
            first let a single unrepresentable value abort the entire batch, so
            nothing at all was loaded.
            """
            kept = [
                record
                for record in records
                if isinstance(record, dict)
                and str(record.get("ticker", "")).strip().upper() in ticker_set
            ]
            dropped = len(records) - len(kept)
            if dropped:
                logger.info(f"  Ignored {dropped} {label} for untracked tickers")
            return kept

        # Fetch and load splits
        if include_splits:
            logger.info("Fetching stock splits...")
            rate_limiter.acquire()
            raw_splits = client.get_splits(execution_date_gte=start_str, execution_date_lte=end_str)
            if not isinstance(raw_splits, list):
                raise ValueError("split provider returned a non-list response")
            stats["splits_fetched"] = len(raw_splits)
            logger.info(f"  Found {len(raw_splits)} splits")

            if raw_splits and not dry_run:
                # Fund reorganizations arrive on this endpoint with fractional
                # ratios the integer schema cannot hold. Skip only those, so a
                # tracked ticker's real split still loads.
                raw_splits = _tracked_only(raw_splits, "split(s)")
                fractional = [s for s in raw_splits if is_unrepresentable_split_ratio(s)]
                if fractional:
                    stats["splits_fractional_skipped"] = len(fractional)
                    logger.warning(
                        f"  Skipped {len(fractional)} split(s) with non-integer "
                        "share ratios (fund reorganizations)"
                    )
                splits = [
                    StockSplit.from_polygon(s)
                    for s in raw_splits
                    if not is_unrepresentable_split_ratio(s)
                ]
                if any(
                    split.execution_date < start_date
                    or split.execution_date > end_date
                    for split in splits
                ):
                    raise ValueError(
                        "split provider returned an execution date outside the "
                        "requested window"
                    )
                # Filter to known tickers
                splits = [s for s in splits if s.ticker in ticker_set]
                stats["splits_eligible"] = len(splits)
                split_result = load_splits(conn, splits, logger, commit=False)
                stats["splits_loaded"] = int(split_result)
                stats["splits_persistence"] = split_result.summary()
                # Only persisted split rows may trigger price/TA repair.
                stats["split_tickers"] = list(split_result.persisted_tickers)
                if not split_result.fully_persisted:
                    stats["errors"].append("split persistence was incomplete")
                logger.info(f"  Loaded {stats['splits_loaded']} splits")
            elif dry_run:
                logger.info("  [DRY RUN] Would load splits")

        # Fetch and load dividends
        if include_dividends:
            logger.info("Fetching dividends...")
            rate_limiter.acquire()
            raw_dividends = client.get_dividends(
                ex_dividend_date_gte=start_str, ex_dividend_date_lte=end_str
            )
            if not isinstance(raw_dividends, list):
                raise ValueError("dividend provider returned a non-list response")
            stats["dividends_fetched"] = len(raw_dividends)
            logger.info(f"  Found {len(raw_dividends)} dividends")

            if raw_dividends and not dry_run:
                raw_dividends = _tracked_only(raw_dividends, "dividend(s)")
                dividends = [Dividend.from_polygon(d) for d in raw_dividends]
                if any(
                    dividend.ex_dividend_date < start_date
                    or dividend.ex_dividend_date > end_date
                    for dividend in dividends
                ):
                    raise ValueError(
                        "dividend provider returned an ex-dividend date outside "
                        "the requested window"
                    )
                # Filter to known tickers
                dividends = [d for d in dividends if d.ticker in ticker_set]
                stats["dividends_eligible"] = len(dividends)
                dividend_result = load_dividends(
                    conn,
                    dividends,
                    logger,
                    commit=False,
                )
                stats["dividends_loaded"] = int(dividend_result)
                stats["dividends_persistence"] = dividend_result.summary()
                if not dividend_result.fully_persisted:
                    stats["errors"].append("dividend persistence was incomplete")
                logger.info(f"  Loaded {stats['dividends_loaded']} dividends")
            elif dry_run:
                logger.info("  [DRY RUN] Would load dividends")

        # Fetch and load earnings (per-ticker API, slower)
        if include_earnings:
            logger.info(f"Fetching earnings for {len(tickers)} tickers...")
            all_earnings: list[Earnings] = []
            earnings_requested = len(tickers)
            earnings_succeeded = 0
            earnings_empty = 0
            earnings_failed = 0
            earnings_rejected_events = 0
            earnings_failures: list[dict[str, str]] = []

            for i, ticker in enumerate(tickers, 1):
                if i % 50 == 0:
                    logger.info(f"  Progress: {i}/{len(tickers)}")

                try:
                    rate_limiter.acquire()
                    events_data = client.get_ticker_events(ticker, event_types=["earnings"])
                    if not isinstance(events_data, dict):
                        raise ValueError("earnings provider returned a non-object response")
                    events = events_data.get("events", [])
                    if not isinstance(events, list):
                        raise ValueError("earnings provider returned a non-list event set")
                    produced = 0
                    for event in events:
                        earn = Earnings.from_polygon_event(ticker, event)
                        if earn:
                            all_earnings.append(earn)
                            produced += 1
                        else:
                            earnings_rejected_events += 1
                    # A response is successful only after its complete event
                    # set parses; this keeps requested == succeeded + failed.
                    earnings_succeeded += 1
                    if produced == 0:
                        earnings_empty += 1
                except Exception as e:
                    earnings_failed += 1
                    safe_error = f"{type(e).__name__}: {redact_sensitive_text(e)}"
                    earnings_failures.append(
                        {"ticker": ticker, "error": safe_error}
                    )
                    logger.debug(f"  {ticker}: {safe_error}")

            stats["earnings_fetched"] = len(all_earnings)
            stats["earnings_requests"] = {
                "requested": earnings_requested,
                "succeeded": earnings_succeeded,
                "empty": earnings_empty,
                "failed": earnings_failed,
                "rejected_events": earnings_rejected_events,
                "failures": earnings_failures,
            }
            logger.info(f"  Found {len(all_earnings)} earnings records")

            if all_earnings and not dry_run:
                earnings_result = load_earnings(
                    conn,
                    all_earnings,
                    logger,
                    commit=False,
                )
                stats["earnings_loaded"] = int(earnings_result)
                stats["earnings_persistence"] = earnings_result.summary()
                if not earnings_result.fully_persisted:
                    stats["errors"].append("earnings persistence was incomplete")
                logger.info(f"  Loaded {stats['earnings_loaded']} earnings")
            elif dry_run:
                logger.info("  [DRY RUN] Would load earnings")

            if earnings_failed:
                stats["errors"].append(
                    f"earnings provider failed for {earnings_failed}/{earnings_requested} tickers"
                )
            if earnings_rejected_events:
                stats["errors"].append(
                    f"earnings provider returned {earnings_rejected_events} unusable event(s)"
                )

        if full_refresh_activity_required and stats["splits_fetched"] == 0:
            stats["errors"].append(
                "split provider returned no rows for the required full-universe "
                "annual refresh"
            )
        if full_refresh_activity_required and stats["dividends_fetched"] == 0:
            stats["errors"].append(
                "dividend provider returned no rows for the required full-universe "
                "annual refresh"
            )

        if stats["errors"]:
            conn.rollback()
            stats["rolled_back"] = True
            stats["splits_loaded"] = 0
            stats["dividends_loaded"] = 0
            stats["earnings_loaded"] = 0
            stats["split_tickers"] = []
        elif not dry_run:
            conn.commit()

        for artifact in ("splits", "dividends", "earnings"):
            persistence = stats.get(f"{artifact}_persistence")
            if isinstance(persistence, dict):
                persistence["committed_rows"] = (
                    persistence.get("attempted_rows", 0)
                    if not stats["errors"] and not dry_run
                    else 0
                )

    stats["degraded"] = bool(stats["errors"])
    stats["success"] = not stats["errors"]
    logger.info("Corporate actions update complete")
    return stats
