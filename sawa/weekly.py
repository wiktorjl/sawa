"""
Weekly update: Pull economy data, overviews, news, and corporate actions.

Purpose: Update data that changes frequently (economy, news, corporate actions).
Re-entrant: Safe to run multiple times (upsert on primary keys).
"""

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import psycopg

from sawa.api import PolygonClient
from sawa.corporate_actions import run_corporate_actions_update
from sawa.database import get_last_date, get_symbols_from_db
from sawa.database.load import load_companies, load_economy, load_news
from sawa.repositories.rate_limiter import SyncRateLimiter
from sawa.utils import setup_logging
from sawa.utils.constants import DEFAULT_API_RATE_LIMIT, DEFAULT_NEWS_DAYS
from sawa.utils.csv_utils import write_csv_auto_fields
from sawa.utils.dates import DATE_FORMAT


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


def run_weekly(
    api_key: str,
    database_url: str,
    output_dir: Path,
    skip_economy: bool = False,
    skip_overviews: bool = False,
    skip_news: bool = False,
    skip_corporate_actions: bool = False,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Run weekly data update.

    Args:
        api_key: Polygon/Massive API key
        database_url: PostgreSQL connection URL
        output_dir: Directory to save downloaded data
        skip_economy: Skip economy data update
        skip_overviews: Skip company overviews update
        skip_news: Skip news update
        skip_corporate_actions: Skip corporate actions (splits/dividends) update
        dry_run: If True, show what would be done without executing
        logger: Logger instance

    Returns:
        Statistics dictionary
    """
    logger = logger or setup_logging()
    stats: dict[str, Any] = {"success": False}

    logger.info("=" * 60)
    logger.info("WEEKLY UPDATE - Economy & Corporate Actions")
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

            # Get last date for incremental updates
            last_treasury_date = get_last_date(conn, "treasury_yields")

            # Calculate date range
            end_date = date.today()
            if last_treasury_date:
                econ_start = last_treasury_date
            else:
                econ_start = end_date - timedelta(days=365)

            econ_start_str = econ_start.strftime(DATE_FORMAT)
            end_str = end_date.strftime(DATE_FORMAT)

            logger.info(f"Economy date range: {econ_start_str} to {end_str}")

        if dry_run:
            logger.info("\n[DRY RUN] Would update:")
            if not skip_overviews:
                logger.info(f"  - Company overviews for {len(symbols)} symbols")
            if not skip_economy:
                logger.info(f"  - Economy data from {econ_start_str}")
            if not skip_news:
                logger.info(f"  - News articles (last {DEFAULT_NEWS_DAYS} days)")
            if not skip_corporate_actions:
                logger.info("  - Corporate actions (splits, dividends)")
            stats["success"] = True
            stats["dry_run"] = True
            return stats

        step = 1
        total_steps = 4 - sum(
            [
                skip_overviews,
                skip_economy,
                skip_news,
                skip_corporate_actions,
            ]
        )

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
                news_count = load_news(conn, client, symbols, days=DEFAULT_NEWS_DAYS, log=logger)
            stats["news"] = news_count

        # Step: Update corporate actions (splits, dividends)
        if not skip_corporate_actions:
            logger.info(f"\n[{step}/{total_steps}] Updating corporate actions...")
            step += 1
            ca_stats = run_corporate_actions_update(
                api_key=api_key,
                database_url=database_url,
                dry_run=False,
                logger=logger,
            )
            stats["corporate_actions"] = ca_stats

        stats["success"] = True
        logger.info("\n" + "=" * 60)
        logger.info("WEEKLY UPDATE COMPLETE")
        logger.info("=" * 60)

        if "overviews" in stats:
            logger.info(f"  Overviews: {stats['overviews']}")
        if "economy" in stats:
            logger.info(f"  Economy: {sum(stats['economy'].values())} records")
        if "news" in stats:
            logger.info(f"  News: {stats['news']} articles")
        if "corporate_actions" in stats:
            ca = stats["corporate_actions"]
            logger.info(
                f"  Corporate actions: {ca.get('splits_loaded', 0)} splits, "
                f"{ca.get('dividends_loaded', 0)} dividends"
            )

    except Exception as e:
        logger.error(f"Weekly update failed: {e}")
        stats["error"] = str(e)
        raise

    return stats
