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
    StockCharacterWriteError,
    load_baseline,
    load_classification,
    load_scorecard,
    replace_flags,
)
from sawa.database.ta_load import get_tickers_with_prices
from sawa.utils.market_hours import get_market_date
from sawa.utils.security import redact_sensitive_text
from sawa.utils.symbols import validate_ticker

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
                {
                    "date": r[0],
                    "open": r[1],
                    "high": r[2],
                    "low": r[3],
                    "close": r[4],
                    "volume": r[5],
                }
                for r in rows
            ]

            result = analyze_stock(ticker, prices, _benchmark_prices, _run_date)

            if result is None:
                elapsed = time.time() - start
                return {"ticker": ticker, "classified": False, "time": elapsed}

            classification = result["classification"]
            flags = result["flags"]
            scorecard = result["scorecard"]
            computed_flag_names = tuple(sorted(flag.flag for flag in flags))
            if scorecard.flag_count != len(flags):
                raise StockCharacterWriteError(
                    "scorecard flag_count does not match the computed flag set: "
                    f"{scorecard.flag_count}/{len(flags)}"
                )
            if tuple(sorted(scorecard.flags)) != computed_flag_names:
                raise StockCharacterWriteError(
                    "scorecard flag identities do not match the computed flag set"
                )

            # All four artifacts are one logical per-ticker transaction. The
            # loaders neither commit nor suppress errors in strict mode.
            writes = (
                (
                    "classification",
                    load_classification(
                        conn,
                        classification,
                        commit=False,
                        strict=True,
                    ),
                    1,
                ),
                (
                    "baseline",
                    load_baseline(
                        conn,
                        result["baseline"],
                        commit=False,
                        strict=True,
                    ),
                    1,
                ),
                (
                    "flags",
                    replace_flags(
                        conn,
                        classification.ticker,
                        classification.run_date,
                        flags,
                        commit=False,
                        strict=True,
                    ),
                    len(flags),
                ),
                (
                    "scorecard",
                    load_scorecard(
                        conn,
                        scorecard,
                        commit=False,
                        strict=True,
                    ),
                    1,
                ),
            )
            for artifact, persisted, expected in writes:
                if persisted != expected:
                    raise StockCharacterWriteError(
                        f"{artifact} persisted only {persisted}/{expected} rows"
                    )
            conn.commit()

            elapsed = time.time() - start
            return {
                "ticker": ticker,
                "classified": True,
                "character": classification.character,
                "confidence": classification.confidence,
                "flags": scorecard.flag_count,
                "time": elapsed,
            }

    except Exception as e:
        elapsed = time.time() - start
        safe_error = f"{type(e).__name__}: {redact_sensitive_text(e)}"
        return {
            "ticker": ticker,
            "classified": False,
            "error": safe_error,
            "time": elapsed,
        }


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
                    {
                        "date": r[0],
                        "open": r[1],
                        "high": r[2],
                        "low": r[3],
                        "close": r[4],
                        "volume": r[5],
                    }
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
        run_date: Classification date (defaults to the current market date)
        log: Logger instance

    Returns:
        Statistics dictionary
    """
    log = log or logger
    # Stamp the market date, not the host's calendar date. The box runs UTC, so
    # a weekly finishing after 20:00 ET recorded run_date as tomorrow; the
    # doctor compares freshness against get_market_date() and correctly rejects
    # a future-dated row as clock skew, failing right after a healthy run.
    run_date = run_date or get_market_date()

    log.info("=" * 60)
    log.info("STOCK CHARACTER CLASSIFICATION BATCH")
    log.info("=" * 60)

    # Get tickers
    with psycopg.connect(database_url) as conn:
        if tickers is not None:
            all_tickers = list(dict.fromkeys(validate_ticker(t) for t in tickers))
        else:
            all_tickers = list(
                dict.fromkeys(validate_ticker(t) for t in get_tickers_with_prices(conn))
            )

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
                    classified_count = sum(1 for r in results if r.get("classified"))
                    log.info(
                        f"  Progress: {i + 1}/{len(all_tickers)} "
                        f"({classified_count} classified)"
                    )
    else:
        _init_worker(database_url, benchmark_prices, run_date)
        for i, ticker in enumerate(all_tickers):
            results.append(_process_ticker(ticker))
            if (i + 1) % 200 == 0:
                classified_count = sum(1 for r in results if r.get("classified"))
                log.info(
                    f"  Progress: {i + 1}/{len(all_tickers)} "
                    f"({classified_count} classified)"
                )

    elapsed = time.time() - start_time

    # Stats
    classified_results = [r for r in results if r.get("classified")]
    errors = [r for r in results if r.get("error")]
    unclassifiable = [r for r in results if not r.get("classified") and not r.get("error")]

    # Character breakdown
    char_counts: dict[str, int] = {}
    for r in classified_results:
        c = r.get("character", "unknown")
        char_counts[c] = char_counts.get(c, 0) + 1

    log.info("\n" + "=" * 60)
    log.info("BATCH COMPLETE")
    log.info("=" * 60)
    log.info(f"  Total tickers:    {len(all_tickers)}")
    total = len(all_tickers)
    classified_percent = 100 * len(classified_results) / total if total else 0.0
    unclassifiable_percent = 100 * len(unclassifiable) / total if total else 0.0
    log.info(
        f"  Classified:       {len(classified_results)} ({classified_percent:.1f}%)"
    )
    log.info(
        f"  Unclassifiable:   {len(unclassifiable)} ({unclassifiable_percent:.1f}%)"
    )
    log.info(f"  Errors:           {len(errors)}")
    log.info(f"  Time:             {elapsed:.1f}s ({elapsed/60:.1f} min)")
    rate = total / elapsed if elapsed > 0 else 0.0
    log.info(f"  Rate:             {rate:.1f} tickers/sec")
    log.info("\n  Character breakdown:")
    for char, count in sorted(char_counts.items()):
        log.info(f"    {char:15s} {count:5d}")

    if errors:
        log.warning("\n  First 5 errors:")
        for err in errors[:5]:
            log.warning(f"    {err['ticker']}: {err.get('error', 'unknown')}")

    return {
        "success": total > 0 and bool(classified_results) and not errors,
        "degraded": bool(errors),
        # Surfaced so an operator can see which date the run stamped; a
        # mismatch with the market date is what made doctor fail after a
        # healthy weekly.
        "run_date": run_date,
        "total": total,
        "classified": len(classified_results),
        "unclassifiable": len(unclassifiable),
        "errors": len(errors),
        "elapsed_seconds": round(elapsed, 1),
        "character_counts": char_counts,
        "ticker_errors": [
            {"ticker": item.get("ticker"), "error": item.get("error")}
            for item in errors
        ],
    }
