"""
Daily update: Pull new stock prices and news since last update.

Purpose: Update stock prices and news (fast, daily operation).
Re-entrant: Safe to run multiple times (upsert by ticker/date).
Uses REST API for near real-time data availability.
"""

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from sawa.api import PolygonClient
from sawa.database import get_last_date, get_symbols_from_db
from sawa.database.news import fetch_and_load_news
from sawa.repositories.rate_limiter import SyncRateLimiter
from sawa.utils import setup_logging
from sawa.utils.constants import DEFAULT_API_RATE_LIMIT, DEFAULT_NEWS_DAYS
from sawa.utils.dates import DATE_FORMAT, timestamp_to_date
from sawa.utils.market_hours import is_after_market_close


def fetch_prices_via_api(
    client: PolygonClient,
    symbols: list[str],
    start_date: str,
    end_date: str,
    logger: logging.Logger,
    rate_limiter: SyncRateLimiter | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch prices for all symbols via REST API.

    Returns list of price records ready for database insert.
    """
    all_prices: list[dict[str, Any]] = []

    for i, symbol in enumerate(symbols, 1):
        if i % 50 == 0:
            logger.info(f"  Progress: {i}/{len(symbols)} symbols")

        try:
            if rate_limiter:
                rate_limiter.acquire()

            data = client.get(
                "aggregates",
                path_params={"ticker": symbol, "start": start_date, "end": end_date},
                params={"adjusted": "true", "limit": 50000},
            )

            results = data.get("results", [])
            for r in results:
                if r.get("t"):
                    price_date = timestamp_to_date(r["t"])
                    all_prices.append(
                        {
                            "ticker": symbol,
                            "date": price_date.strftime(DATE_FORMAT),
                            "open": r.get("o"),
                            "high": r.get("h"),
                            "low": r.get("l"),
                            "close": r.get("c"),
                            "volume": r.get("v"),
                        }
                    )

        except Exception as e:
            logger.debug(f"  {symbol}: {e}")

    return all_prices


def insert_prices(
    conn,
    prices: list[dict[str, Any]],
    logger: logging.Logger,
) -> int:
    """Insert prices into database with upsert."""
    if not prices:
        return 0

    query = sql.SQL("""
        INSERT INTO stock_prices (ticker, date, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, date) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume
    """)

    inserted = 0
    batch_size = 1000

    with conn.cursor() as cur:
        for i in range(0, len(prices), batch_size):
            batch = prices[i : i + batch_size]
            for p in batch:
                cur.execute(
                    query,
                    (
                        p["ticker"],
                        p["date"],
                        p["open"],
                        p["high"],
                        p["low"],
                        p["close"],
                        p["volume"],
                    ),
                )
                inserted += 1
            conn.commit()

            if (i + batch_size) % 5000 == 0:
                logger.info(f"  Inserted {min(i + batch_size, len(prices))}/{len(prices)} records")

    return inserted


def run_daily(
    api_key: str,
    database_url: str,
    output_dir: Path | None = None,
    force_from_date: date | None = None,
    skip_news: bool = False,
    skip_ta: bool = False,
    skip_prices: bool = False,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Run daily price, news, and technical indicator update using REST API.

    Args:
        api_key: Polygon/Massive API key
        database_url: PostgreSQL connection URL
        output_dir: Not used (kept for CLI compatibility)
        force_from_date: Optional date to force update from
        skip_news: Skip news update
        skip_ta: Skip technical indicator calculation
        skip_prices: Skip price update (for --news-only mode)
        dry_run: If True, show what would be done without executing
        logger: Logger instance

    Returns:
        Statistics dictionary
    """
    logger = logger or setup_logging()
    stats: dict[str, Any] = {"success": False}

    logger.info("=" * 60)
    logger.info("DAILY UPDATE - Stock Prices & News (API)")
    logger.info("=" * 60)

    # Initialize client and rate limiter
    client = PolygonClient(api_key, logger)
    rate_limiter = SyncRateLimiter(DEFAULT_API_RATE_LIMIT)

    try:
        with psycopg.connect(database_url) as conn:
            # Get last price date
            logger.info("Checking last price date...")
            last_price_date = get_last_date(conn, "stock_prices")

            if force_from_date:
                start_date = force_from_date
                logger.info(f"  Forcing update from: {start_date}")
            elif last_price_date:
                start_date = last_price_date + timedelta(days=1)
                logger.info(f"  Last price date: {last_price_date}")
                logger.info(f"  Starting from: {start_date}")
            else:
                logger.error("No existing price data found. Run coldstart first.")
                return stats

            end_date = date.today()

            # Skip today if market hasn't closed yet (before 5 PM ET)
            # This prevents incomplete data from overriding intraday stream
            if not is_after_market_close():
                end_date = end_date - timedelta(days=1)
                logger.info("Market not yet closed - fetching through yesterday only")
                logger.info(f"  End date: {end_date}")
            else:
                logger.info("Market closed - including today's EOD data")

            start_str = start_date.strftime(DATE_FORMAT)
            end_str = end_date.strftime(DATE_FORMAT)

            # Get symbols
            symbols = get_symbols_from_db(conn)
            if not symbols:
                logger.error("No symbols in database. Run coldstart first.")
                return stats
            logger.info(f"Found {len(symbols)} symbols in database")
            stats["symbols"] = len(symbols)

        # Check if prices need updating
        prices_up_to_date = start_date > end_date or skip_prices
        trading_days: list[str] = []

        if skip_prices:
            logger.info("Skipping prices (--news-only)")
            stats["prices_inserted"] = 0
        elif prices_up_to_date:
            logger.info("Prices already up to date.")
            stats["prices_inserted"] = 0
        else:
            # Get trading days for the update period
            logger.info(f"\nGetting trading days from {start_str} to {end_str}...")
            trading_days = client.get_trading_days(start_str, end_str)
            logger.info(f"  Found {len(trading_days)} trading days")
            stats["trading_days"] = len(trading_days)

        if dry_run:
            logger.info("\n[DRY RUN] Would fetch:")
            if trading_days:
                logger.info(f"  - Prices for {len(symbols)} symbols")
                logger.info(f"  - {len(trading_days)} trading days")
                logger.info(f"  - Date range: {start_str} to {end_str}")
            else:
                logger.info("  - No price updates needed")
            if not skip_news:
                logger.info(f"  - News articles (last {DEFAULT_NEWS_DAYS} days)")
            if not skip_ta:
                logger.info(f"  - Technical indicators for {len(symbols)} symbols")
            stats["success"] = True
            stats["dry_run"] = True
            return stats

        # Fetch and insert prices if there are trading days
        if trading_days:
            logger.info("\nFetching prices via API...")
            prices = fetch_prices_via_api(client, symbols, start_str, end_str, logger, rate_limiter)
            logger.info(f"  Fetched {len(prices)} price records")
            stats["prices_fetched"] = len(prices)

            logger.info("\nInserting prices into database...")
            with psycopg.connect(database_url) as conn:
                inserted = insert_prices(conn, prices, logger)

                # If we just inserted today's EOD, cleanup intraday data for today
                if end_date == date.today():
                    try:
                        from sawa.database.intraday_load import cleanup_today_intraday_data

                        cleanup_today_intraday_data(conn, logger)
                    except ImportError:
                        pass

                # Cleanup old intraday data (>7 days)
                try:
                    from sawa.database.intraday_load import cleanup_old_intraday_data

                    cleanup_old_intraday_data(conn, 7, logger)
                except ImportError:
                    pass

            logger.info(f"  Inserted {inserted} records")
            stats["prices_inserted"] = inserted

        # Fetch and load news (always, unless skipped)
        if not skip_news:
            logger.info(f"\nFetching news (last {DEFAULT_NEWS_DAYS} days)...")
            with psycopg.connect(database_url) as conn:
                news_count = fetch_and_load_news(
                    conn, client, days=DEFAULT_NEWS_DAYS, limit=1000, log=logger
                )
            stats["news"] = news_count
        else:
            logger.info("\nSkipping news (--skip-news)")

        # Calculate technical indicators (always, unless skipped)
        if not skip_ta:
            logger.info("\nCalculating technical indicators...")
            try:
                from sawa.calculation.ta_engine import (
                    calculate_indicators_for_ticker,
                    get_required_lookback_days,
                )
                from sawa.database.ta_load import (
                    get_last_ta_date,
                    get_prices_for_ticker,
                    load_technical_indicators,
                )

                lookback_days = get_required_lookback_days()
                ta_count = 0

                with psycopg.connect(database_url) as conn:
                    # Check if technical_indicators table exists
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables
                                WHERE table_name = 'technical_indicators'
                            )
                        """)
                        row = cur.fetchone()
                        table_exists = row[0] if row else False

                    if not table_exists:
                        logger.warning("  Table 'technical_indicators' does not exist")
                        logger.warning("  Run schema migration or coldstart to create it")
                        stats["ta_skipped"] = "table does not exist"
                    else:
                        for i, ticker in enumerate(symbols, 1):
                            if i % 100 == 0:
                                logger.info(f"  Progress: {i}/{len(symbols)} tickers")

                            # Get last TA date for this ticker
                            last_ta = get_last_ta_date(conn, ticker)

                            # Calculate start date for price fetch (need lookback for warm-up)
                            if last_ta:
                                price_start = last_ta - timedelta(days=lookback_days)
                            else:
                                price_start = None  # Fetch all prices

                            # Fetch prices
                            prices = get_prices_for_ticker(conn, ticker, start_date=price_start)
                            if not prices:
                                continue

                            # Calculate indicators
                            indicators = calculate_indicators_for_ticker(ticker, prices, logger)
                            if not indicators:
                                continue

                            # Filter to only new dates (after last_ta)
                            if last_ta:
                                indicators = [ind for ind in indicators if ind.date > last_ta]

                            if indicators:
                                inserted = load_technical_indicators(conn, indicators, logger)
                                ta_count += inserted

                        stats["ta_calculated"] = ta_count
                        logger.info(f"  Calculated {ta_count} indicator records")

            except ImportError as e:
                logger.warning(f"Skipping TA calculation: {e}")
                logger.warning("  Install ta-lib to enable: pip install TA-Lib")
                stats["ta_skipped"] = "ta-lib not installed"
        else:
            logger.info("\nSkipping technical indicators (--skip-ta)")

        stats["success"] = True
        logger.info("\n" + "=" * 60)
        logger.info("DAILY UPDATE COMPLETE")
        logger.info("=" * 60)
        logger.info(f"  Price records: {stats.get('prices_inserted', 0)}")
        if not skip_news:
            logger.info(f"  News articles: {stats.get('news', 0)}")
        if not skip_ta and "ta_calculated" in stats:
            logger.info(f"  TA indicators: {stats.get('ta_calculated', 0)}")

    except Exception as e:
        logger.error(f"Daily update failed: {e}")
        stats["error"] = str(e)
        raise

    return stats
