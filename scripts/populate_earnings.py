#!/usr/bin/env python3
"""
Populate earnings table from Yahoo Finance.

Downloads historical and upcoming earnings dates, EPS estimates/actuals,
and surprise percentages for all active tickers ordered by market cap.

Designed for stealth: uses random delays between requests to avoid
rate limiting. Safe to run for extended periods (12-24 hours for full universe).

Usage:
    python scripts/populate_earnings.py                  # All active tickers
    python scripts/populate_earnings.py --limit 100      # First 100 by market cap
    python scripts/populate_earnings.py --resume TSLA    # Resume from ticker
    python scripts/populate_earnings.py --dry-run        # Preview without DB writes
    python scripts/populate_earnings.py --min-delay 3 --max-delay 8  # Custom delays
"""

import argparse
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import yfinance as yf
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration defaults
# ---------------------------------------------------------------------------
DEFAULT_MIN_DELAY = 2.0    # seconds
DEFAULT_MAX_DELAY = 6.0    # seconds
EARNINGS_LIMIT = 20        # yfinance limit param (~5 years of quarterly data)
BATCH_COMMIT_SIZE = 25     # commit to DB every N tickers
PROGRESS_LOG_INTERVAL = 10 # log progress every N tickers

# Longer pauses injected periodically to look more human
LONG_PAUSE_EVERY = (40, 80)        # random range: pause every N tickers
LONG_PAUSE_DURATION = (30.0, 90.0) # random range: pause duration in seconds

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("populate_earnings")

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
shutdown_requested = False


def handle_signal(signum, frame):
    global shutdown_requested
    if shutdown_requested:
        logger.warning("Force quit")
        sys.exit(1)
    shutdown_requested = True
    logger.info("Shutdown requested, finishing current ticker...")


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# ---------------------------------------------------------------------------
# State file for resumption
# ---------------------------------------------------------------------------
STATE_FILE = Path(__file__).parent / ".earnings_state"


def save_state(ticker: str, processed: int, loaded: int, errors: int):
    STATE_FILE.write_text(
        f"{ticker}\n{processed}\n{loaded}\n{errors}\n{datetime.now(timezone.utc).isoformat()}\n"
    )


def load_state() -> str | None:
    if STATE_FILE.exists():
        lines = STATE_FILE.read_text().strip().split("\n")
        if lines:
            logger.info(f"Found state file: last ticker={lines[0]}, written {lines[4] if len(lines) > 4 else '?'}")
            return lines[0]
    return None


def clear_state():
    if STATE_FILE.exists():
        STATE_FILE.unlink()


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def get_tickers_by_marketcap(conn: psycopg.Connection) -> list[tuple[str, float | None]]:
    """Get active tickers ordered by market cap descending (nulls last)."""
    cur = conn.execute(
        """
        SELECT ticker, market_cap
        FROM companies
        WHERE active = true
        ORDER BY market_cap DESC NULLS LAST, ticker ASC
        """
    )
    return cur.fetchall()


def infer_timing(earnings_dt) -> str | None:
    """Infer BMO/AMC from the earnings datetime hour."""
    try:
        hour = earnings_dt.hour
        if hour < 12:
            return "BMO"
        elif hour >= 16:
            return "AMC"
        else:
            return "DMH"
    except Exception:
        return None


def fetch_earnings(ticker: str) -> list[dict] | None:
    """Fetch earnings dates from yfinance for a single ticker."""
    try:
        t = yf.Ticker(ticker)
        df = t.get_earnings_dates(limit=EARNINGS_LIMIT)
        if df is None or df.empty:
            return None

        records = []
        for dt_idx, row in df.iterrows():
            report_date = dt_idx.date() if hasattr(dt_idx, "date") else None
            if report_date is None:
                continue

            records.append({
                "ticker": ticker,
                "report_date": report_date,
                "timing": infer_timing(dt_idx),
                "eps_estimate": None if (hasattr(row["EPS Estimate"], "__class__") and row["EPS Estimate"] != row["EPS Estimate"]) else float(row["EPS Estimate"]),
                "eps_actual": None if (hasattr(row["Reported EPS"], "__class__") and row["Reported EPS"] != row["Reported EPS"]) else float(row["Reported EPS"]),
                "surprise_pct": None if (hasattr(row["Surprise(%)"], "__class__") and row["Surprise(%)"] != row["Surprise(%)"]) else float(row["Surprise(%)"]),
            })
        return records
    except Exception as e:
        logger.debug(f"  {ticker}: {e}")
        return None


def upsert_earnings(conn: psycopg.Connection, records: list[dict]) -> int:
    """Upsert earnings records into the database. Returns count of rows affected."""
    if not records:
        return 0

    sql = """
        INSERT INTO earnings (ticker, report_date, timing, eps_estimate, eps_actual, surprise_pct, updated_at)
        VALUES (%(ticker)s, %(report_date)s, %(timing)s, %(eps_estimate)s, %(eps_actual)s, %(surprise_pct)s, NOW())
        ON CONFLICT (ticker, report_date) DO UPDATE SET
            timing = COALESCE(EXCLUDED.timing, earnings.timing),
            eps_estimate = COALESCE(EXCLUDED.eps_estimate, earnings.eps_estimate),
            eps_actual = COALESCE(EXCLUDED.eps_actual, earnings.eps_actual),
            surprise_pct = COALESCE(EXCLUDED.surprise_pct, earnings.surprise_pct),
            updated_at = NOW()
    """
    count = 0
    for rec in records:
        try:
            conn.execute(sql, rec)
            count += 1
        except Exception as e:
            logger.debug(f"  Upsert failed for {rec['ticker']} {rec['report_date']}: {e}")
    return count


def random_delay(min_delay: float, max_delay: float):
    """Sleep for a random duration with slight jitter."""
    delay = random.uniform(min_delay, max_delay)
    # Add occasional micro-jitter to look less uniform
    delay += random.gauss(0, 0.3)
    delay = max(0.5, delay)  # never less than 0.5s
    time.sleep(delay)


def run(
    conn: psycopg.Connection,
    limit: int | None = None,
    resume_ticker: str | None = None,
    auto_resume: bool = False,
    dry_run: bool = False,
    min_delay: float = DEFAULT_MIN_DELAY,
    max_delay: float = DEFAULT_MAX_DELAY,
):
    global shutdown_requested

    tickers = get_tickers_by_marketcap(conn)
    total = len(tickers)
    logger.info(f"Found {total} active tickers ordered by market cap")

    # Handle resume
    if auto_resume and not resume_ticker:
        resume_ticker = load_state()

    start_idx = 0
    if resume_ticker:
        resume_ticker = resume_ticker.upper()
        for i, (t, _) in enumerate(tickers):
            if t == resume_ticker:
                start_idx = i + 1  # start AFTER the last completed ticker
                break
        if start_idx == 0:
            logger.warning(f"Resume ticker {resume_ticker} not found, starting from beginning")
        else:
            logger.info(f"Resuming after {resume_ticker} (index {start_idx}/{total})")

    if limit:
        tickers = tickers[start_idx : start_idx + limit]
    else:
        tickers = tickers[start_idx:]

    logger.info(f"Processing {len(tickers)} tickers (delay: {min_delay}-{max_delay}s)")
    if dry_run:
        logger.info("DRY RUN - no database writes")

    processed = 0
    loaded = 0
    errors = 0
    batch_records = 0
    next_long_pause = random.randint(*LONG_PAUSE_EVERY)
    start_time = time.monotonic()

    for i, (ticker, mcap) in enumerate(tickers, 1):
        if shutdown_requested:
            logger.info(f"Shutting down after {processed} tickers, {loaded} records loaded")
            save_state(ticker, processed, loaded, errors)
            break

        records = fetch_earnings(ticker)
        if records is None:
            logger.debug(f"  [{start_idx + i}/{total}] {ticker}: no data")
            errors += 1
        else:
            count = len(records)
            if not dry_run:
                upserted = upsert_earnings(conn, records)
                loaded += upserted
                batch_records += upserted
            else:
                loaded += count

            if count > 0:
                mcap_f = float(mcap) if mcap else 0
                mcap_str = f"${mcap_f/1e9:.1f}B" if mcap_f > 1e9 else f"${mcap_f/1e6:.0f}M" if mcap_f else "N/A"
                logger.debug(f"  [{start_idx + i}/{total}] {ticker} ({mcap_str}): {count} earnings")

        processed += 1

        # Commit in batches
        if not dry_run and batch_records > 0 and processed % BATCH_COMMIT_SIZE == 0:
            conn.commit()
            batch_records = 0
            save_state(ticker, processed, loaded, errors)

        # Progress log
        if processed % PROGRESS_LOG_INTERVAL == 0:
            elapsed = time.monotonic() - start_time
            rate = processed / elapsed * 3600 if elapsed > 0 else 0
            eta_hours = (len(tickers) - processed) / rate if rate > 0 else 0
            logger.info(
                f"Progress: {start_idx + processed}/{total} tickers | "
                f"{loaded} records | {errors} errors | "
                f"{rate:.0f}/hr | ETA: {eta_hours:.1f}h"
            )

        # Inject long pauses periodically
        if processed >= next_long_pause:
            pause = random.uniform(*LONG_PAUSE_DURATION)
            logger.info(f"  Long pause: {pause:.0f}s")
            time.sleep(pause)
            next_long_pause = processed + random.randint(*LONG_PAUSE_EVERY)

        # Random delay between requests
        random_delay(min_delay, max_delay)

    # Final commit
    if not dry_run:
        conn.commit()
        save_state(ticker if tickers else "", processed, loaded, errors)

    elapsed = time.monotonic() - start_time
    logger.info(
        f"Done: {processed} tickers, {loaded} records loaded, "
        f"{errors} errors, {elapsed/3600:.1f} hours elapsed"
    )

    if not shutdown_requested:
        clear_state()
        logger.info("Run complete, state file cleared")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")

    parser = argparse.ArgumentParser(
        description="Populate earnings table from Yahoo Finance"
    )
    parser.add_argument("--limit", type=int, help="Max tickers to process")
    parser.add_argument("--resume", type=str, metavar="TICKER", help="Resume after this ticker")
    parser.add_argument("--auto-resume", action="store_true", help="Auto-resume from state file")
    parser.add_argument("--dry-run", action="store_true", help="Preview without DB writes")
    parser.add_argument("--min-delay", type=float, default=DEFAULT_MIN_DELAY, help=f"Min delay between requests (default: {DEFAULT_MIN_DELAY}s)")
    parser.add_argument("--max-delay", type=float, default=DEFAULT_MAX_DELAY, help=f"Max delay between requests (default: {DEFAULT_MAX_DELAY}s)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Debug logging")
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        logger.error("DATABASE_URL not set")
        sys.exit(1)

    with psycopg.connect(db_url) as conn:
        run(
            conn,
            limit=args.limit,
            resume_ticker=args.resume,
            auto_resume=args.auto_resume,
            dry_run=args.dry_run,
            min_delay=args.min_delay,
            max_delay=args.max_delay,
        )


if __name__ == "__main__":
    main()
