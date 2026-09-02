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
from sawa.utils.security import redact_sensitive_text


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


def get_existing_price_dates(
    conn,
    tickers: list[str],
) -> dict[str, set[date]]:
    """Return every stored date that a full adjusted refresh must replace."""
    dates: dict[str, set[date]] = {ticker: set() for ticker in tickers}
    if not tickers:
        return dates
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ticker, date FROM stock_prices WHERE ticker = ANY(%s)",
            (tickers,),
        )
        for ticker, price_date in cur.fetchall():
            if ticker in dates and isinstance(price_date, date):
                dates[ticker].add(price_date)
    return dates


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
    stats: dict[str, Any] = {
        "success": False,
        "tickers_requested": 0,
        "tickers_adjusted": 0,
        "prices_fetched": 0,
        "prices_updated": 0,
    }

    if since is None:
        since = date.today() - timedelta(days=365)

    client = PolygonClient(api_key, logger)
    rate_limiter = SyncRateLimiter(DEFAULT_API_RATE_LIMIT)

    with psycopg.connect(database_url) as conn:
        # Determine which tickers need adjustment. Dedupe so a ticker that split
        # multiple times in the window isn't re-fetched (full earliest-to-present
        # history) once per split.
        if tickers is None:
            tickers = get_tickers_with_recent_splits(conn, since)
        else:
            # Preserve order, drop duplicates (callers may pass one entry per
            # split row, e.g. weekly's stats['split_tickers']).
            tickers = list(dict.fromkeys(tickers))

        if not tickers:
            logger.info("No tickers with recent splits found - nothing to adjust")
            stats["success"] = True
            return stats

        stats["tickers_requested"] = len(tickers)

        logger.info(f"Found {len(tickers)} ticker(s) with splits to adjust: {', '.join(tickers)}")
        # Expose the resolved ticker list so callers (e.g. the adjust-splits CLI)
        # can recompute technical indicators for exactly the adjusted tickers.
        stats["tickers"] = tickers

        # Get earliest price date to know how far back to fetch
        earliest = get_earliest_price_date(conn, tickers)
        if not earliest:
            logger.warning("No existing price data found for split tickers")
            stats["error"] = "no existing price data found for requested split tickers"
            return stats
        existing_dates = get_existing_price_dates(conn, tickers)
        tickers_without_prices = [ticker for ticker in tickers if not existing_dates[ticker]]
        if tickers_without_prices:
            stats["missing_tickers"] = tickers_without_prices
            stats["error"] = (
                "no existing price history found for "
                f"{len(tickers_without_prices)} requested split ticker(s)"
            )
            logger.warning(stats["error"])
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
        provider_stats: dict[str, Any] = {}
        prices = fetch_prices_via_api(
            client,
            tickers,
            start_str,
            end_str,
            logger,
            rate_limiter,
            stats=provider_stats,
        )
        stats["provider"] = provider_stats
        stats["prices_fetched"] = len(prices)
        logger.info(f"Fetched {len(prices)} adjusted price records")

        fetched_tickers = {str(price.get("ticker", "")).upper() for price in prices}
        missing_tickers = [ticker for ticker in tickers if ticker not in fetched_tickers]
        fetched_dates: set[tuple[str, date]] = set()
        for price in prices:
            ticker = str(price.get("ticker", "")).upper()
            raw_date = price.get("date")
            try:
                price_date = (
                    raw_date if isinstance(raw_date, date) else date.fromisoformat(str(raw_date))
                )
            except ValueError:
                continue
            fetched_dates.add((ticker, price_date))
        # The provider serves a rolling history window (currently five years),
        # so stored rows older than the oldest row it will return can never be
        # re-based. Requiring the provider to cover every stored date therefore
        # failed the whole adjustment for any ticker with a longer history —
        # and failing meant NOTHING was re-based, leaving the series
        # discontinuous at the split instead of at the unreachable horizon.
        # Split the shortfall: a gap inside the window the provider did serve
        # is real incompleteness and still fails; anything older than that
        # window is reported and skipped, and no stored row is deleted.
        earliest_fetched: dict[str, date] = {}
        for ticker, price_date in fetched_dates:
            current = earliest_fetched.get(ticker)
            if current is None or price_date < current:
                earliest_fetched[ticker] = price_date

        missing_existing_dates: list[tuple[str, date]] = []
        unreachable_dates: list[tuple[str, date]] = []
        for ticker, stored_dates in existing_dates.items():
            horizon = earliest_fetched.get(ticker)
            for price_date in stored_dates:
                if (ticker, price_date) in fetched_dates:
                    continue
                if horizon is not None and price_date < horizon:
                    unreachable_dates.append((ticker, price_date))
                else:
                    missing_existing_dates.append((ticker, price_date))
        missing_existing_dates.sort()

        if unreachable_dates:
            horizon_start = min(earliest_fetched.values())
            stats["pre_horizon_dates_not_adjusted"] = len(unreachable_dates)
            stats["provider_history_horizon"] = horizon_start.isoformat()
            logger.warning(
                f"  {len(unreachable_dates)} stored date(s) predate the provider's "
                f"available history (earliest {horizon_start.isoformat()}) and keep "
                "their existing basis; the adjustable range is re-based"
            )

        if missing_tickers or provider_stats.get("failed_symbols") or provider_stats.get(
            "invalid_price_rows"
        ) or missing_existing_dates:
            stats["missing_tickers"] = missing_tickers
            stats["missing_existing_price_dates"] = len(missing_existing_dates)
            stats["missing_existing_price_date_samples"] = [
                f"{ticker}/{price_date.isoformat()}"
                for ticker, price_date in missing_existing_dates[:10]
            ]
            stats["error"] = (
                "adjusted price source was incomplete "
                f"({len(fetched_tickers)}/{len(tickers)} tickers with valid rows; "
                f"{provider_stats.get('failed_symbols', 0)} request failures; "
                f"{provider_stats.get('invalid_price_rows', 0)} invalid rows; "
                f"{len(missing_existing_dates)} stored dates missing)"
            )
            logger.warning(stats["error"])
            return stats

        try:
            inserted = insert_prices(conn, prices, logger, commit=False)
        except Exception as e:
            conn.rollback()
            safe_error = f"{type(e).__name__}: {redact_sensitive_text(e)}"
            stats["error"] = f"adjusted price persistence failed: {safe_error}"
            logger.error(stats["error"])
            return stats
        stats["prices_updated"] = inserted
        logger.info(f"Upserted {inserted} adjusted price records")
        if inserted != len(prices):
            conn.rollback()
            stats["error"] = (
                f"adjusted price persistence wrote only {inserted}/{len(prices)} rows"
            )
            logger.error(stats["error"])
            return stats
        conn.commit()

        stats["tickers_adjusted"] = len(fetched_tickers)
        stats["success"] = True

    return stats
