"""
Split-adjusted price refresh: Re-fetch adjusted prices for tickers with recent splits.

Purpose: After stock splits are detected, re-fetch full adjusted price history
from the Polygon REST API and upsert over the stale unadjusted data.
Re-entrant: Safe to run multiple times (upsert by ticker/date).
"""

import logging
from datetime import date, timedelta
from typing import Any

import psycopg

from sawa.api import PolygonClient
from sawa.daily import fetch_prices_via_api, insert_prices
from sawa.repositories.rate_limiter import SyncRateLimiter
from sawa.utils import setup_logging
from sawa.utils.constants import DEFAULT_API_RATE_LIMIT
from sawa.utils.dates import DATE_FORMAT


def get_tickers_with_recent_splits(
    conn,
    since_date: date,
) -> list[str]:
    """Query stock_splits for tickers with splits since the given date."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT ticker FROM stock_splits WHERE execution_date >= %s ORDER BY ticker",
            (since_date,),
        )
        return [row[0] for row in cur.fetchall()]


def get_earliest_price_date(
    conn,
    tickers: list[str],
) -> date | None:
    """Get the earliest price date across the given tickers."""
    if not tickers:
        return None
    with conn.cursor() as cur:
        cur.execute(
            "SELECT MIN(date) FROM stock_prices WHERE ticker = ANY(%s)",
            (tickers,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def refresh_split_adjusted_prices(
    api_key: str,
    database_url: str,
    tickers: list[str] | None = None,
    since: date | None = None,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Re-fetch adjusted price history for tickers with recent stock splits.

    Args:
        api_key: Polygon API key
        database_url: PostgreSQL connection URL
        tickers: Specific tickers to adjust (default: auto-detect from stock_splits)
        since: Only consider splits since this date (default: 1 year ago)
        dry_run: If True, show what would be done without executing
        logger: Logger instance

    Returns:
        Statistics dictionary
    """
    logger = logger or setup_logging()
    stats: dict[str, Any] = {"success": False, "tickers_adjusted": 0, "prices_updated": 0}

    if since is None:
        since = date.today() - timedelta(days=365)

    client = PolygonClient(api_key, logger)
    rate_limiter = SyncRateLimiter(DEFAULT_API_RATE_LIMIT)

    with psycopg.connect(database_url) as conn:
        # Determine which tickers need adjustment
        if tickers is None:
            tickers = get_tickers_with_recent_splits(conn, since)

        if not tickers:
            logger.info("No tickers with recent splits found - nothing to adjust")
            stats["success"] = True
            return stats

        logger.info(f"Found {len(tickers)} ticker(s) with splits to adjust: {', '.join(tickers)}")

        # Get earliest price date to know how far back to fetch
        earliest = get_earliest_price_date(conn, tickers)
        if not earliest:
            logger.warning("No existing price data found for split tickers")
            stats["success"] = True
            return stats

        end_date = date.today()
        start_str = earliest.strftime(DATE_FORMAT)
        end_str = end_date.strftime(DATE_FORMAT)

        logger.info(f"Re-fetching adjusted prices from {start_str} to {end_str}")

        if dry_run:
            logger.info("[DRY RUN] Would re-fetch adjusted prices for:")
            for ticker in tickers:
                logger.info(f"  - {ticker}")
            stats["success"] = True
            stats["dry_run"] = True
            stats["tickers"] = tickers
            return stats

        # Fetch adjusted prices for each ticker
        prices = fetch_prices_via_api(
            client, tickers, start_str, end_str, logger, rate_limiter
        )
        logger.info(f"Fetched {len(prices)} adjusted price records")

        if prices:
            inserted = insert_prices(conn, prices, logger)
            stats["prices_updated"] = inserted
            logger.info(f"Upserted {inserted} adjusted price records")

        stats["tickers_adjusted"] = len(tickers)
        stats["success"] = True

    return stats
