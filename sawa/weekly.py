"""
Weekly update: Pull slow-changing data (fundamentals, economy, etc).

Purpose: Update fundamentals, economy data, company overviews, ratios, news.
Re-entrant: Safe to run multiple times (upsert on primary keys).
"""

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from sawa.api import PolygonClient
from sawa.database.load import (
    load_companies,
    load_economy,
    load_fundamentals,
    load_news,
    load_ratios,
)
from sawa.repositories.rate_limiter import SyncRateLimiter
from sawa.utils import setup_logging
from sawa.utils.constants import DEFAULT_API_RATE_LIMIT, DEFAULT_NEWS_DAYS
from sawa.utils.csv_utils import write_csv_auto_fields
from sawa.utils.dates import DATE_FORMAT


def get_last_date(conn, table: str, date_column: str = "date") -> date | None:
    """Get the most recent date from a table."""
    query = sql.SQL("SELECT MAX({}) FROM {}").format(
        sql.Identifier(date_column),
        sql.Identifier(table),
    )
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchone()
        if result and result[0]:
            return result[0]
    return None


def get_symbols_from_db(conn) -> list[str]:
    """Get list of symbols from companies table."""
    with conn.cursor() as cur:
        cur.execute("SELECT ticker FROM companies ORDER BY ticker")
        return [row[0] for row in cur.fetchall()]


def download_fundamentals(
    client: PolygonClient,
    symbols: list[str],
    start_date: str,
    end_date: str,
    output_dir: Path,
    logger: logging.Logger,
    rate_limiter: SyncRateLimiter | None = None,
) -> dict[str, int]:
    """Download fundamentals data."""
    endpoints = ["balance-sheets", "cash-flow", "income-statements"]
    stats: dict[str, int] = {}

    for endpoint in endpoints:
        logger.info(f"Downloading {endpoint}...")
        output_dir.mkdir(parents=True, exist_ok=True)

        all_data: list[dict[str, Any]] = []
        for i, symbol in enumerate(symbols, 1):
            if i % 50 == 0:
                logger.info(f"  Progress: {i}/{len(symbols)}")
            try:
                if rate_limiter:
                    rate_limiter.acquire()
                data = client.get_fundamentals(
                    endpoint, ticker=symbol, start_date=start_date, end_date=end_date
                )
                # Clean up tickers field
                for record in data:
                    if "tickers" in record and isinstance(record["tickers"], list):
                        record["tickers"] = record["tickers"][0] if record["tickers"] else ""
                all_data.extend(data)
            except Exception as e:
                logger.debug(f"  {symbol}: {e}")

        if all_data:
            filepath = output_dir / f"{endpoint.replace('-', '_')}.csv"
            write_csv_auto_fields(filepath, all_data, logger)

        stats[endpoint] = len(all_data)

    return stats


def download_overviews(
    client: PolygonClient,
    symbols: list[str],
    output_dir: Path,
    logger: logging.Logger,
    rate_limiter: SyncRateLimiter | None = None,
) -> int:
    """Download company overviews."""
    logger.info("Downloading company overviews...")
    output_dir.mkdir(parents=True, exist_ok=True)

    overviews: list[dict[str, Any]] = []
    for i, symbol in enumerate(symbols, 1):
        if i % 50 == 0:
            logger.info(f"  Progress: {i}/{len(symbols)}")
        try:
            if rate_limiter:
                rate_limiter.acquire()
            data = client.get_ticker_details(symbol)
            if data:
                # Flatten nested fields
                flat = {k: v for k, v in data.items() if not isinstance(v, dict)}
                if "address" in data and data["address"]:
                    for k, v in data["address"].items():
                        flat[f"address_{k}"] = v
                if "branding" in data and data["branding"]:
                    for k, v in data["branding"].items():
                        flat[f"branding_{k}"] = v
                overviews.append(flat)
        except Exception as e:
            logger.warning(f"  {symbol}: {e}")

    if overviews:
        filepath = output_dir / "overviews.csv"
        write_csv_auto_fields(filepath, overviews, logger)

    return len(overviews)


def download_economy(
    client: PolygonClient,
    start_date: str,
    end_date: str,
    output_dir: Path,
    logger: logging.Logger,
) -> dict[str, int]:
    """Download economy data."""
    endpoints = ["treasury-yields", "inflation", "inflation-expectations", "labor-market"]
    stats: dict[str, int] = {}

    output_dir.mkdir(parents=True, exist_ok=True)

    for endpoint in endpoints:
        logger.info(f"Downloading {endpoint}...")
        try:
            data = client.get_economy_data(endpoint, start_date, end_date)
            if data:
                filepath = output_dir / f"{endpoint.replace('-', '_')}.csv"
                write_csv_auto_fields(filepath, data, logger)
            stats[endpoint] = len(data)
        except Exception as e:
            logger.error(f"  Failed: {e}")
            stats[endpoint] = 0

    return stats


def download_ratios(
    client: PolygonClient,
    symbols: list[str],
    output_dir: Path,
    logger: logging.Logger,
    rate_limiter: SyncRateLimiter | None = None,
) -> int:
    """Download financial ratios."""
    logger.info("Downloading financial ratios...")
    output_dir.mkdir(parents=True, exist_ok=True)

    all_ratios: list[dict[str, Any]] = []
    for i, symbol in enumerate(symbols, 1):
        if i % 50 == 0:
            logger.info(f"  Progress: {i}/{len(symbols)}")
        try:
            if rate_limiter:
                rate_limiter.acquire()
            ratios = client.get_ratios(symbol)
            for r in ratios:
                r["ticker"] = symbol
            all_ratios.extend(ratios)
        except Exception as e:
            logger.warning(f"  {symbol}: {e}")

    if all_ratios:
        filepath = output_dir / "ratios.csv"
        write_csv_auto_fields(filepath, all_ratios, logger)

    return len(all_ratios)


def run_weekly(
    api_key: str,
    database_url: str,
    output_dir: Path,
    skip_fundamentals: bool = False,
    skip_economy: bool = False,
    skip_overviews: bool = False,
    skip_ratios: bool = False,
    skip_news: bool = False,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Run weekly data update.

    Args:
        api_key: Polygon/Massive API key
        database_url: PostgreSQL connection URL
        output_dir: Directory to save downloaded data
        skip_fundamentals: Skip fundamentals update
        skip_economy: Skip economy data update
        skip_overviews: Skip company overviews update
        skip_ratios: Skip financial ratios update
        skip_news: Skip news update
        dry_run: If True, show what would be done without executing
        logger: Logger instance

    Returns:
        Statistics dictionary
    """
    logger = logger or setup_logging()
    stats: dict[str, Any] = {"success": False}

    logger.info("=" * 60)
    logger.info("WEEKLY UPDATE - Fundamentals & Economy")
    logger.info("=" * 60)

    # Initialize client and rate limiter
    client = PolygonClient(api_key, logger)
    rate_limiter = SyncRateLimiter(DEFAULT_API_RATE_LIMIT)

    try:
        with psycopg.connect(database_url) as conn:
            # Get symbols
            symbols = get_symbols_from_db(conn)
            if not symbols:
                logger.error("No symbols in database. Run coldstart first.")
                return stats
            logger.info(f"Found {len(symbols)} symbols in database")
            stats["symbols"] = len(symbols)

            # Get last dates for incremental updates
            last_fundamental_date = get_last_date(conn, "balance_sheets", "period_end")
            last_treasury_date = get_last_date(conn, "treasury_yields")

            # Calculate date range (default: last 90 days for fundamentals)
            end_date = date.today()
            if last_fundamental_date:
                fund_start = last_fundamental_date - timedelta(days=30)  # Overlap for safety
            else:
                fund_start = end_date - timedelta(days=365)  # 1 year if no data

            if last_treasury_date:
                econ_start = last_treasury_date
            else:
                econ_start = end_date - timedelta(days=365)

            fund_start_str = fund_start.strftime(DATE_FORMAT)
            econ_start_str = econ_start.strftime(DATE_FORMAT)
            end_str = end_date.strftime(DATE_FORMAT)

            logger.info(f"Fundamentals date range: {fund_start_str} to {end_str}")
            logger.info(f"Economy date range: {econ_start_str} to {end_str}")

        if dry_run:
            logger.info("\n[DRY RUN] Would update:")
            if not skip_overviews:
                logger.info(f"  - Company overviews for {len(symbols)} symbols")
            if not skip_fundamentals:
                logger.info(f"  - Fundamentals from {fund_start_str}")
            if not skip_ratios:
                logger.info(f"  - Financial ratios for {len(symbols)} symbols")
            if not skip_economy:
                logger.info(f"  - Economy data from {econ_start_str}")
            if not skip_news:
                logger.info(f"  - News articles (last {DEFAULT_NEWS_DAYS} days)")
            stats["success"] = True
            stats["dry_run"] = True
            return stats

        step = 1
        total_steps = 5 - sum([skip_overviews, skip_fundamentals, skip_ratios, skip_economy, skip_news])

        # Step: Update company overviews
        if not skip_overviews:
            logger.info(f"\n[{step}/{total_steps}] Updating company overviews...")
            step += 1
            overview_count = download_overviews(
                client, symbols, output_dir / "overviews", logger, rate_limiter
            )
            stats["overviews"] = overview_count
            # Load into database
            with psycopg.connect(database_url) as conn:
                load_companies(conn, output_dir / "overviews" / "overviews.csv", logger)

        # Step: Update fundamentals
        if not skip_fundamentals:
            logger.info(f"\n[{step}/{total_steps}] Updating fundamentals...")
            step += 1
            fund_stats = download_fundamentals(
                client, symbols, fund_start_str, end_str,
                output_dir / "fundamentals", logger, rate_limiter
            )
            stats["fundamentals"] = fund_stats
            # Load into database
            with psycopg.connect(database_url) as conn:
                load_fundamentals(conn, output_dir / "fundamentals", logger)

        # Step: Update ratios
        if not skip_ratios:
            logger.info(f"\n[{step}/{total_steps}] Updating financial ratios...")
            step += 1
            ratio_count = download_ratios(
                client, symbols, output_dir / "ratios", logger, rate_limiter
            )
            stats["ratios"] = ratio_count
            # Load into database
            with psycopg.connect(database_url) as conn:
                load_ratios(conn, output_dir / "ratios" / "ratios.csv", logger)

        # Step: Update economy data
        if not skip_economy:
            logger.info(f"\n[{step}/{total_steps}] Updating economy data...")
            step += 1
            econ_stats = download_economy(
                client, econ_start_str, end_str, output_dir / "economy", logger
            )
            stats["economy"] = econ_stats
            # Load into database
            with psycopg.connect(database_url) as conn:
                load_economy(conn, output_dir / "economy", logger)

        # Step: Update news
        if not skip_news:
            logger.info(f"\n[{step}/{total_steps}] Updating news articles...")
            step += 1
            with psycopg.connect(database_url) as conn:
                news_count = load_news(
                    conn, client, symbols, days=DEFAULT_NEWS_DAYS, log=logger
                )
            stats["news"] = news_count

        stats["success"] = True
        logger.info("\n" + "=" * 60)
        logger.info("WEEKLY UPDATE COMPLETE")
        logger.info("=" * 60)

        if "overviews" in stats:
            logger.info(f"  Overviews: {stats['overviews']}")
        if "fundamentals" in stats:
            logger.info(f"  Fundamentals: {sum(stats['fundamentals'].values())} records")
        if "ratios" in stats:
            logger.info(f"  Ratios: {stats['ratios']}")
        if "economy" in stats:
            logger.info(f"  Economy: {sum(stats['economy'].values())} records")
        if "news" in stats:
            logger.info(f"  News: {stats['news']} articles")

    except Exception as e:
        logger.error(f"Weekly update failed: {e}")
        stats["error"] = str(e)
        raise

    return stats
