"""
Corporate actions: Download and store stock splits and dividends.

Purpose: Update splits and dividends tables from Polygon API.
Re-entrant: Safe to run multiple times (upsert on unique constraints).
"""

import logging
from datetime import date, timedelta
from typing import Any

import psycopg

from sawa.api import PolygonClient
from sawa.domain.corporate_actions import Dividend, StockSplit
from sawa.repositories.rate_limiter import SyncRateLimiter
from sawa.utils import setup_logging
from sawa.utils.constants import DEFAULT_API_RATE_LIMIT


def get_active_tickers(conn) -> list[str]:
    """Get list of active tickers from companies table."""
    with conn.cursor() as cur:
        cur.execute("SELECT ticker FROM companies WHERE active = true ORDER BY ticker")
        return [row[0] for row in cur.fetchall()]


def load_splits(
    conn,
    splits: list[StockSplit],
    logger: logging.Logger,
) -> int:
    """Load stock splits into database using upsert."""
    if not splits:
        return 0

    insert_sql = """
        INSERT INTO stock_splits (ticker, execution_date, split_from, split_to)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (ticker, execution_date) DO UPDATE SET
            split_from = EXCLUDED.split_from,
            split_to = EXCLUDED.split_to
    """

    with conn.cursor() as cur:
        for split in splits:
            try:
                cur.execute(insert_sql, split.to_tuple())
            except psycopg.errors.ForeignKeyViolation:
                # Skip splits for tickers not in companies table
                conn.rollback()
                logger.debug(f"Skipping split for unknown ticker: {split.ticker}")
                continue

    conn.commit()
    return len(splits)


def load_dividends(
    conn,
    dividends: list[Dividend],
    logger: logging.Logger,
) -> int:
    """Load dividends into database using upsert."""
    if not dividends:
        return 0

    insert_sql = """
        INSERT INTO dividends (
            ticker, ex_dividend_date, record_date, pay_date,
            cash_amount, declaration_date, dividend_type, frequency
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, ex_dividend_date, COALESCE(cash_amount, 0)) DO UPDATE SET
            record_date = EXCLUDED.record_date,
            pay_date = EXCLUDED.pay_date,
            declaration_date = EXCLUDED.declaration_date,
            dividend_type = EXCLUDED.dividend_type,
            frequency = EXCLUDED.frequency
    """

    loaded = 0
    with conn.cursor() as cur:
        for div in dividends:
            try:
                cur.execute(insert_sql, div.to_tuple())
                loaded += 1
            except psycopg.errors.ForeignKeyViolation:
                # Skip dividends for tickers not in companies table
                conn.rollback()
                logger.debug(f"Skipping dividend for unknown ticker: {div.ticker}")
                continue

    conn.commit()
    return loaded


def run_corporate_actions_update(
    api_key: str,
    database_url: str,
    start_date: date | None = None,
    tickers: list[str] | None = None,
    include_splits: bool = True,
    include_dividends: bool = True,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Download and store corporate actions (splits, dividends) from Polygon.

    Args:
        api_key: Polygon API key
        database_url: PostgreSQL connection URL
        start_date: Fetch data from this date (default: 1 year ago)
        tickers: List of tickers to fetch (default: all active)
        include_splits: Whether to fetch splits
        include_dividends: Whether to fetch dividends
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
    }

    # Default start date: 1 year ago
    if start_date is None:
        start_date = date.today() - timedelta(days=365)

    start_str = start_date.isoformat()
    end_str = date.today().isoformat()

    logger.info(f"Corporate actions update: {start_str} to {end_str}")

    # Initialize API client
    client = PolygonClient(api_key, logger)
    rate_limiter = SyncRateLimiter(DEFAULT_API_RATE_LIMIT)

    with psycopg.connect(database_url) as conn:
        # Get tickers if not provided
        if tickers is None:
            tickers = get_active_tickers(conn)
            logger.info(f"Found {len(tickers)} active tickers")

        # Fetch and load splits
        if include_splits:
            logger.info("Fetching stock splits...")
            rate_limiter.acquire()
            raw_splits = client.get_splits(execution_date_gte=start_str, execution_date_lte=end_str)
            stats["splits_fetched"] = len(raw_splits)
            logger.info(f"  Found {len(raw_splits)} splits")

            if raw_splits and not dry_run:
                splits = [StockSplit.from_polygon(s) for s in raw_splits]
                # Filter to known tickers
                ticker_set = set(tickers)
                splits = [s for s in splits if s.ticker in ticker_set]
                stats["splits_loaded"] = load_splits(conn, splits, logger)
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
            stats["dividends_fetched"] = len(raw_dividends)
            logger.info(f"  Found {len(raw_dividends)} dividends")

            if raw_dividends and not dry_run:
                dividends = [Dividend.from_polygon(d) for d in raw_dividends]
                # Filter to known tickers
                ticker_set = set(tickers)
                dividends = [d for d in dividends if d.ticker in ticker_set]
                stats["dividends_loaded"] = load_dividends(conn, dividends, logger)
                logger.info(f"  Loaded {stats['dividends_loaded']} dividends")
            elif dry_run:
                logger.info("  [DRY RUN] Would load dividends")

    stats["success"] = True
    logger.info("Corporate actions update complete")
    return stats
