"""
Technical indicator backfill: Calculate TA for all historical data.

Purpose: Full backfill of technical indicators for all stocks.
Re-entrant: Safe to run multiple times (upsert by ticker/date).
Supports parallel processing with multiprocessing.Pool.
"""

import logging
import time
from multiprocessing import Pool
from typing import Any

import psycopg

from sawa.calculation.ta_engine import calculate_indicators_for_ticker
from sawa.database.ta_load import (
    get_prices_for_ticker,
    get_ta_count,
    get_tickers_with_prices,
    load_technical_indicators,
)
from sawa.utils import setup_logging

logger = logging.getLogger(__name__)


# Global database URL for worker processes
_db_url: str = ""


def _init_worker(db_url: str) -> None:
    """Initialize worker process with database URL."""
    global _db_url
    _db_url = db_url


def _process_ticker(ticker: str) -> dict[str, Any]:
    """Process a single ticker (worker function for multiprocessing).

    Args:
        ticker: Stock symbol to process

    Returns:
        Dict with ticker, count of indicators calculated, and timing
    """
    start = time.time()
    log = logging.getLogger(f"worker.{ticker}")

    try:
        with psycopg.connect(_db_url) as conn:
            # Fetch all prices for ticker
            prices = get_prices_for_ticker(conn, ticker)
            if not prices:
                return {"ticker": ticker, "count": 0, "error": "no prices", "time": 0}

            # Calculate indicators
            indicators = calculate_indicators_for_ticker(ticker, prices, log)
            if not indicators:
                return {"ticker": ticker, "count": 0, "error": "calculation failed", "time": 0}

            # Insert into database
            inserted = load_technical_indicators(conn, indicators, log)

            elapsed = time.time() - start
            return {"ticker": ticker, "count": inserted, "time": elapsed}

    except Exception as e:
        elapsed = time.time() - start
        return {"ticker": ticker, "count": 0, "error": str(e), "time": elapsed}


def estimate_backfill_time(
    database_url: str,
    sample_size: int = 10,
    log: logging.Logger | None = None,
) -> dict[str, Any]:
    """Estimate total backfill time based on sample run.

    Args:
        database_url: PostgreSQL connection URL
        sample_size: Number of tickers to test
        log: Logger instance

    Returns:
        Dict with timing estimates
    """
    log = log or logger

    with psycopg.connect(database_url) as conn:
        tickers = get_tickers_with_prices(conn)

    if not tickers:
        return {"error": "no tickers found"}

    test_tickers = tickers[:sample_size]
    log.info(f"Testing on {len(test_tickers)} tickers...")

    # Run sample calculations
    _init_worker(database_url)
    start = time.time()
    results = [_process_ticker(t) for t in test_tickers]
    elapsed = time.time() - start

    # Calculate stats
    successful = [r for r in results if "error" not in r]
    per_ticker = elapsed / len(test_tickers) if test_tickers else 0
    total_estimated = per_ticker * len(tickers)

    return {
        "sample_size": len(test_tickers),
        "sample_time_seconds": round(elapsed, 2),
        "per_ticker_seconds": round(per_ticker, 3),
        "total_tickers": len(tickers),
        "estimated_sequential_seconds": round(total_estimated, 1),
        "estimated_sequential_minutes": round(total_estimated / 60, 1),
        "estimated_4_workers_minutes": round(total_estimated / 4 / 60, 1),
        "estimated_8_workers_minutes": round(total_estimated / 8 / 60, 1),
        "successful_samples": len(successful),
        "failed_samples": len(results) - len(successful),
    }


def run_ta_backfill(
    database_url: str,
    tickers: list[str] | None = None,
    workers: int = 4,
    dry_run: bool = False,
    estimate_only: bool = False,
    log: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Run full technical indicator backfill.

    Args:
        database_url: PostgreSQL connection URL
        tickers: List of tickers to process (None = all with prices)
        workers: Number of parallel workers
        dry_run: If True, show what would be done without executing
        estimate_only: If True, only run time estimation
        log: Logger instance

    Returns:
        Statistics dictionary
    """
    log = log or setup_logging()
    stats: dict[str, Any] = {"success": False}

    log.info("=" * 60)
    log.info("TECHNICAL INDICATORS BACKFILL")
    log.info("=" * 60)

    # Estimate only mode
    if estimate_only:
        log.info("\nRunning time estimation...")
        estimate = estimate_backfill_time(database_url, sample_size=10, log=log)
        stats.update(estimate)
        stats["success"] = True

        log.info("\nEstimation Results:")
        log.info(f"  Sample size: {estimate.get('sample_size', 0)} tickers")
        log.info(f"  Per-ticker time: {estimate.get('per_ticker_seconds', 0):.3f}s")
        log.info(f"  Total tickers: {estimate.get('total_tickers', 0)}")
        log.info("\nEstimated total time:")
        log.info(f"  Sequential: {estimate.get('estimated_sequential_minutes', 0):.1f} minutes")
        log.info(f"  4 workers: {estimate.get('estimated_4_workers_minutes', 0):.1f} minutes")
        log.info(f"  8 workers: {estimate.get('estimated_8_workers_minutes', 0):.1f} minutes")
        return stats

    try:
        with psycopg.connect(database_url) as conn:
            # Check if technical_indicators table exists
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = 'technical_indicators'
                    )
                """)
                result = cur.fetchone()
                table_exists = result[0] if result else False

            if not table_exists:
                log.error("Table 'technical_indicators' does not exist.")
                log.error("Run the schema migration first:")
                log.error("  psql $DATABASE_URL -f sqlschema/11_technical_indicators.sql")
                log.error("Or re-run coldstart with --schema-only to update schema.")
                return stats

            # Get tickers to process
            if tickers:
                all_tickers = [t.upper() for t in tickers]
                log.info(f"Processing {len(all_tickers)} specified tickers")
            else:
                all_tickers = get_tickers_with_prices(conn)
                log.info(f"Found {len(all_tickers)} tickers with price data")

            if not all_tickers:
                log.error("No tickers found. Run coldstart first.")
                return stats

            stats["tickers"] = len(all_tickers)

            # Get current TA count
            current_count = get_ta_count(conn)
            log.info(f"Current technical indicator records: {current_count}")

        # Dry run mode
        if dry_run:
            log.info("\n[DRY RUN] Would calculate indicators for:")
            log.info(f"  - {len(all_tickers)} tickers")
            log.info(f"  - Using {workers} workers")
            sample = all_tickers[:10]
            log.info(f"  - Sample: {', '.join(sample)}{'...' if len(all_tickers) > 10 else ''}")
            stats["success"] = True
            stats["dry_run"] = True
            return stats

        # Process tickers
        log.info(f"\nProcessing {len(all_tickers)} tickers with {workers} workers...")
        start_time = time.time()

        if workers > 1:
            # Parallel processing
            with Pool(
                processes=workers,
                initializer=_init_worker,
                initargs=(database_url,),
            ) as pool:
                results = []
                for i, result in enumerate(pool.imap_unordered(_process_ticker, all_tickers)):
                    results.append(result)
                    if (i + 1) % 50 == 0:
                        log.info(f"  Progress: {i + 1}/{len(all_tickers)}")
        else:
            # Sequential processing
            _init_worker(database_url)
            results = []
            for i, ticker in enumerate(all_tickers):
                result = _process_ticker(ticker)
                results.append(result)
                if (i + 1) % 50 == 0:
                    log.info(f"  Progress: {i + 1}/{len(all_tickers)}")

        elapsed = time.time() - start_time

        # Compile stats
        total_inserted = sum(r.get("count", 0) for r in results)
        errors = [r for r in results if "error" in r]

        stats["indicators_calculated"] = total_inserted
        stats["elapsed_seconds"] = round(elapsed, 2)
        stats["elapsed_minutes"] = round(elapsed / 60, 2)
        stats["tickers_processed"] = len(results)
        stats["tickers_failed"] = len(errors)

        log.info("\n" + "=" * 60)
        log.info("BACKFILL COMPLETE")
        log.info("=" * 60)
        log.info(f"  Tickers processed: {len(results)}")
        log.info(f"  Indicators calculated: {total_inserted}")
        log.info(f"  Time: {stats['elapsed_minutes']:.1f} minutes")
        log.info(f"  Rate: {len(results) / elapsed:.1f} tickers/second")

        if errors:
            log.warning(f"  Errors: {len(errors)}")
            for err in errors[:5]:
                log.warning(f"    {err['ticker']}: {err.get('error', 'unknown')}")
            if len(errors) > 5:
                log.warning(f"    ... and {len(errors) - 5} more")

        stats["success"] = True

    except Exception as e:
        log.error(f"Backfill failed: {e}")
        stats["error"] = str(e)
        raise

    return stats
