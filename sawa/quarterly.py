"""
Quarterly update: Pull fundamentals data (balance sheets, income, cash flow, ratios).

Purpose: Update financial statements and ratios that are released quarterly.
Re-entrant: Safe to run multiple times (upsert on primary keys).
"""

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from sawa.api import PolygonClient
from sawa.database.load import load_fundamentals, load_ratios
from sawa.repositories.rate_limiter import SyncRateLimiter
from sawa.utils import setup_logging
from sawa.utils.constants import DEFAULT_API_RATE_LIMIT
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
    """Download fundamentals data (balance sheets, cash flow, income statements)."""
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


def run_quarterly(
    api_key: str,
    database_url: str,
    output_dir: Path,
    skip_fundamentals: bool = False,
    skip_ratios: bool = False,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Run quarterly fundamentals update.

    Args:
        api_key: Polygon/Massive API key
        database_url: PostgreSQL connection URL
        output_dir: Directory to save downloaded data
        skip_fundamentals: Skip fundamentals update
        skip_ratios: Skip financial ratios update
        dry_run: If True, show what would be done without executing
        logger: Logger instance

    Returns:
        Statistics dictionary
    """
    logger = logger or setup_logging()
    stats: dict[str, Any] = {"success": False}

    logger.info("=" * 60)
    logger.info("QUARTERLY UPDATE - Fundamentals & Ratios")
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
            last_fundamental_date = get_last_date(conn, "balance_sheets", "period_end")

            # Calculate date range
            end_date = date.today()
            if last_fundamental_date:
                fund_start = last_fundamental_date - timedelta(days=30)  # Overlap for safety
            else:
                fund_start = end_date - timedelta(days=365)  # 1 year if no data

            fund_start_str = fund_start.strftime(DATE_FORMAT)
            end_str = end_date.strftime(DATE_FORMAT)

            logger.info(f"Fundamentals date range: {fund_start_str} to {end_str}")

        if dry_run:
            logger.info("\n[DRY RUN] Would update:")
            if not skip_fundamentals:
                logger.info(f"  - Fundamentals from {fund_start_str}")
            if not skip_ratios:
                logger.info(f"  - Financial ratios for {len(symbols)} symbols")
            stats["success"] = True
            stats["dry_run"] = True
            return stats

        step = 1
        total_steps = 2 - sum([skip_fundamentals, skip_ratios])

        # Step: Update fundamentals
        if not skip_fundamentals:
            logger.info(f"\n[{step}/{total_steps}] Updating fundamentals...")
            step += 1
            fund_stats = download_fundamentals(
                client,
                symbols,
                fund_start_str,
                end_str,
                output_dir / "fundamentals",
                logger,
                rate_limiter,
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

        stats["success"] = True
        logger.info("\n" + "=" * 60)
        logger.info("QUARTERLY UPDATE COMPLETE")
        logger.info("=" * 60)

        if "fundamentals" in stats:
            total = sum(int(v) for v in stats["fundamentals"].values())
            logger.info(f"  Fundamentals: {total} records")
        if "ratios" in stats:
            logger.info(f"  Ratios: {stats['ratios']}")

    except Exception as e:
        logger.error(f"Quarterly update failed: {e}")
        stats["error"] = str(e)
        raise

    return stats
