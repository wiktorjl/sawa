"""Stock character classification batch runner.

Runs the full Stage 1-4 pipeline across all tickers using multiprocessing.
Follows the same pattern as ta_backfill.py.
"""

import logging
import time
from multiprocessing import Pool
from typing import Any, cast

import psycopg

from sawa.calculation.stock_character_scorecard import analyze_stock
from sawa.database.stock_character import (
    load_baseline,
    load_classification,
    load_flags,
    load_scorecard,
)
from sawa.database.ta_load import get_tickers_with_prices

logger = logging.getLogger(__name__)

# Globals shared across worker processes
_db_url: str = ""
_benchmark_prices: dict[str, list[dict[str, Any]]] = {}
_run_date: Any = None


def _init_worker(
    db_url: str,
    benchmark_prices: dict[str, list[dict[str, Any]]],
    run_date: Any,
) -> None:
    """Initialize worker process with shared data."""
    global _db_url, _benchmark_prices, _run_date
    _db_url = db_url
    _benchmark_prices = benchmark_prices
    _run_date = run_date


def _process_ticker(ticker: str) -> dict[str, Any]:
    """Process a single ticker (worker function)."""
    start = time.time()

    try:
        with psycopg.connect(_db_url) as conn:
            # Fetch prices for this ticker only
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT date, open, high, low, close, volume "
                    "FROM stock_prices WHERE ticker = %s ORDER BY date ASC",
                    (ticker,),
                )
                rows = cur.fetchall()

            if not rows:
                return {"ticker": ticker, "classified": False, "error": "no prices", "time": 0}

            prices = [
                {"date": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5]}
                for r in rows
            ]

            result = analyze_stock(ticker, prices, _benchmark_prices, _run_date)

            if result is None:
                elapsed = time.time() - start
                return {"ticker": ticker, "classified": False, "time": elapsed}

            # Persist all stages
            load_classification(conn, result["classification"])
            load_baseline(conn, result["baseline"])
            load_flags(conn, result["flags"])
            load_scorecard(conn, result["scorecard"])

            elapsed = time.time() - start
            return {
                "ticker": ticker,
                "classified": True,
                "character": result["classification"].character,
                "confidence": result["classification"].confidence,
                "flags": result["scorecard"].flag_count,
                "time": elapsed,
            }

    except Exception as e:
        elapsed = time.time() - start
        return {"ticker": ticker, "classified": False, "error": str(e), "time": elapsed}


def _fetch_benchmark_prices(db_url: str) -> dict[str, list[dict[str, Any]]]:
    """Fetch benchmark prices once for all workers."""
    benchmarks = {}
    with psycopg.connect(db_url) as conn:
        for sym in ("SPY", "GLD", "TLT"):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT date, open, high, low, close, volume "
                    "FROM stock_prices WHERE ticker = %s ORDER BY date ASC",
                    (sym,),
                )
                benchmarks[sym] = [
                    {"date": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5]}
                    for r in cur.fetchall()
                ]
    return benchmarks


def run_stock_character_batch(
    database_url: str,
    tickers: list[str] | None = None,
    workers: int = 4,
    run_date: Any = None,
    log: logging.Logger | None = None,
) -> dict[str, Any]:
    """Run stock character classification across all tickers.

    Args:
        database_url: PostgreSQL connection URL
        tickers: Specific tickers to process (None = all)
        workers: Number of parallel workers
        run_date: Classification date (defaults to today)
        log: Logger instance

    Returns:
        Statistics dictionary
    """
    from datetime import date

    log = log or logger
    run_date = run_date or date.today()

    log.info("=" * 60)
    log.info("STOCK CHARACTER CLASSIFICATION BATCH")
    log.info("=" * 60)

    # Get tickers
    with psycopg.connect(database_url) as conn:
        if tickers:
            all_tickers = [t.upper() for t in tickers]
        else:
            all_tickers = get_tickers_with_prices(conn)

    log.info(f"Tickers to process: {len(all_tickers)}")
    log.info(f"Workers: {workers}")

    # Fetch benchmarks once
    log.info("Fetching benchmark prices (SPY, GLD, TLT)...")
    benchmark_prices = _fetch_benchmark_prices(database_url)
    for sym, prices in benchmark_prices.items():
        log.info(f"  {sym}: {len(prices)} days")

    # Process
    log.info(f"\nProcessing {len(all_tickers)} tickers...")
    start_time = time.time()
    results: list[dict[str, Any]] = []

    if workers > 1:
        with Pool(
            processes=workers,
            initializer=_init_worker,
            initargs=(database_url, benchmark_prices, run_date),
        ) as pool:
            for i, res in enumerate(pool.imap_unordered(_process_ticker, all_tickers)):
                results.append(cast(dict[str, Any], res))
                if (i + 1) % 200 == 0:
                    classified = sum(1 for r in results if r.get("classified"))
                    log.info(f"  Progress: {i + 1}/{len(all_tickers)} ({classified} classified)")
    else:
        _init_worker(database_url, benchmark_prices, run_date)
        for i, ticker in enumerate(all_tickers):
            results.append(_process_ticker(ticker))
            if (i + 1) % 200 == 0:
                classified = sum(1 for r in results if r.get("classified"))
                log.info(f"  Progress: {i + 1}/{len(all_tickers)} ({classified} classified)")

    elapsed = time.time() - start_time

    # Stats
    classified = [r for r in results if r.get("classified")]
    errors = [r for r in results if r.get("error")]
    unclassifiable = [r for r in results if not r.get("classified") and not r.get("error")]

    # Character breakdown
    char_counts: dict[str, int] = {}
    for r in classified:
        c = r.get("character", "unknown")
        char_counts[c] = char_counts.get(c, 0) + 1

    log.info("\n" + "=" * 60)
    log.info("BATCH COMPLETE")
    log.info("=" * 60)
    log.info(f"  Total tickers:    {len(all_tickers)}")
    log.info(f"  Classified:       {len(classified)} ({100*len(classified)/len(all_tickers):.1f}%)")
    log.info(f"  Unclassifiable:   {len(unclassifiable)} ({100*len(unclassifiable)/len(all_tickers):.1f}%)")
    log.info(f"  Errors:           {len(errors)}")
    log.info(f"  Time:             {elapsed:.1f}s ({elapsed/60:.1f} min)")
    log.info(f"  Rate:             {len(all_tickers)/elapsed:.1f} tickers/sec")
    log.info(f"\n  Character breakdown:")
    for char, count in sorted(char_counts.items()):
        log.info(f"    {char:15s} {count:5d}")

    if errors:
        log.warning(f"\n  First 5 errors:")
        for err in errors[:5]:
            log.warning(f"    {err['ticker']}: {err.get('error', 'unknown')}")

    return {
        "success": True,
        "total": len(all_tickers),
        "classified": len(classified),
        "unclassifiable": len(unclassifiable),
        "errors": len(errors),
        "elapsed_seconds": round(elapsed, 1),
        "character_counts": char_counts,
    }
