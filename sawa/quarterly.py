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

from sawa.api import PolygonClient
from sawa.database import get_last_date, get_symbols_from_db
from sawa.database.load import load_fundamentals, load_ratios
from sawa.repositories.rate_limiter import SyncRateLimiter
from sawa.utils import setup_logging
from sawa.utils.constants import DEFAULT_API_RATE_LIMIT
from sawa.utils.csv_utils import write_csv_auto_fields
from sawa.utils.dates import DATE_FORMAT


def download_fundamentals(
    client: PolygonClient,
    symbols: list[str],
    start_date: str | None,
    end_date: str,
    output_dir: Path,
    logger: logging.Logger,
    rate_limiter: SyncRateLimiter | None = None,
    filing_date_gte: str | None = None,
) -> dict[str, int]:
    """Download fundamentals data (balance sheets, cash flow, income statements).

    The incremental quarterly pull filters on filing_date_gte (when reports
    became available) so late filings and restatements of older periods are
    captured; start_date (period_end.gte) is used only for a full backfill.
    """
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
                    endpoint,
                    ticker=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    filing_date_gte=filing_date_gte,
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

            # Get last date for incremental updates. Anchor on MAX(filing_date)
            # (when reports became available), NOT MAX(period_end) (when the
            # fiscal period closed): filtering on period_end silently skips
            # late/amended filers, non-calendar fiscal years, and restatements
            # of older quarters whose period_end predates the global max.
            last_filing_date = get_last_date(conn, "balance_sheets", "filing_date")

            # Calculate date range
            end_date = date.today()
            end_str = end_date.strftime(DATE_FORMAT)

            # filing_date_gte drives the incremental pull; fund_start_str
            # (period_end.gte) is only used for the full-backfill cold path.
            fund_start_str: str | None
            if last_filing_date:
                # Widen overlap to 120 days to also recapture amended filings.
                filing_gte = last_filing_date - timedelta(days=120)
                filing_gte_str = filing_gte.strftime(DATE_FORMAT)
                fund_start_str = None
                logger.info(
                    f"Fundamentals incremental window: filing_date >= {filing_gte_str} "
                    f"(period_end <= {end_str})"
                )
            else:
                # No data yet: full backfill anchored on period_end.
                fund_start = end_date - timedelta(days=365)
                fund_start_str = fund_start.strftime(DATE_FORMAT)
                filing_gte_str = None
                logger.info(f"Fundamentals date range: {fund_start_str} to {end_str}")

        if dry_run:
            logger.info("\n[DRY RUN] Would update:")
            if not skip_fundamentals:
                window = (
                    f"filing_date >= {filing_gte_str}"
                    if filing_gte_str
                    else f"period_end >= {fund_start_str}"
                )
                logger.info(f"  - Fundamentals ({window})")
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
                filing_date_gte=filing_gte_str,
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
