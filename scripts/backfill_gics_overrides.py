#!/usr/bin/env python3
"""
Backfill ticker-level GICS overrides from yfinance.

After ``scripts/backfill_sic_codes.py`` re-runs Polygon for every
``sic_code IS NULL`` row, the residue is mostly foreign ADRs (~300
tickers) — Polygon simply doesn't carry SIC for many non-US issuers.
yfinance, however, exposes Yahoo's sector/industry classification for
nearly every ADR.

This script fetches ``yf.Ticker(t).info`` for each unsectorized
active ADR (and any active CS still missing SIC), maps Yahoo's
12-bucket sector vocabulary onto GICS sector names, and writes the
result into ``gics_overrides`` with ``source='yfinance'``. After this
ships, ``get_gics_sector()`` returns a real GICS sector for all
active ADRs instead of falling back to the SIC description.

Throttling mimics ``scripts/populate_earnings.py``: random 2–6s delays
between requests + occasional 30–90s long pauses every ~50 tickers,
because yfinance soft-rate-limits aggressively.

Usage:
    python scripts/backfill_gics_overrides.py
    python scripts/backfill_gics_overrides.py --limit 50         # smoke test
    python scripts/backfill_gics_overrides.py --types ADRC       # ADRs only
    python scripts/backfill_gics_overrides.py --resume AAAU      # continue
    python scripts/backfill_gics_overrides.py --dry-run          # no DB writes

Requires:
    DATABASE_URL env var
    yfinance (already a transitive dep via scripts/populate_earnings.py)
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import signal
import sys
import time
from pathlib import Path

import psycopg
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sawa.utils import setup_logging  # noqa: E402

# yfinance's `info["sector"]` uses a different vocabulary than GICS.
# Yahoo categorizes equities into ~12 sectors that map to GICS with
# only label differences (Yahoo "Technology" = GICS "Information
# Technology", Yahoo "Healthcare" = GICS "Health Care", etc.).
YFINANCE_TO_GICS_SECTOR: dict[str, str] = {
    "basic materials":         "Materials",
    "communication services":  "Communication Services",
    "consumer cyclical":       "Consumer Discretionary",
    "consumer defensive":      "Consumer Staples",
    "energy":                  "Energy",
    "financial":               "Financials",
    "financial services":      "Financials",
    "healthcare":              "Health Care",
    "industrials":             "Industrials",
    "real estate":             "Real Estate",
    "technology":              "Information Technology",
    "utilities":               "Utilities",
}

DEFAULT_MIN_DELAY = 2.0
DEFAULT_MAX_DELAY = 6.0
BATCH_COMMIT_SIZE = 25
PROGRESS_LOG_INTERVAL = 25
LONG_PAUSE_EVERY = (40, 80)
LONG_PAUSE_DURATION = (30.0, 90.0)

STATE_FILE = Path(__file__).parent / ".gics_overrides_backfill_state"

shutdown_requested = False


def _handle_signal(signum, frame):
    global shutdown_requested
    if shutdown_requested:
        sys.exit(1)
    shutdown_requested = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def _load_targets(
    conn: psycopg.Connection,
    types: tuple[str, ...],
    resume_after: str | None,
    limit: int | None,
) -> list[tuple[str, str]]:
    """Return ``[(ticker, type), ...]`` for active tickers with NULL sic_code
    that don't yet have a row in ``gics_overrides``."""
    type_placeholders = ", ".join(["%s"] * len(types))
    args: list = list(types)
    where = [
        "c.active = true",
        "c.sic_code IS NULL",
        f"c.type IN ({type_placeholders})",
        "NOT EXISTS (SELECT 1 FROM gics_overrides o WHERE o.ticker = c.ticker)",
    ]
    if resume_after:
        where.append("c.ticker > %s")
        args.append(resume_after)
    sql = (
        "SELECT c.ticker, c.type FROM companies c WHERE "
        + " AND ".join(where)
        + " ORDER BY c.ticker"
    )
    if limit:
        sql += f" LIMIT {int(limit)}"
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return [(row[0], row[1]) for row in cur.fetchall()]


def _save_state(ticker: str, filled: int, skipped: int, errors: int) -> None:
    STATE_FILE.write_text(
        f"last_ticker={ticker}\nfilled={filled}\nskipped={skipped}\nerrors={errors}\n"
    )


def _fetch_yfinance_sector(ticker: str) -> tuple[str | None, str | None]:
    """Return (yahoo_sector, yahoo_industry) or (None, None) on any failure.

    Wrapping every yfinance call in a try/except because Yahoo's
    backend periodically returns 404, 429, or HTML pages that yfinance
    surfaces as various exception types — none of which should kill a
    multi-hour backfill.
    """
    try:
        info = yf.Ticker(ticker).info or {}
    except Exception:
        return None, None
    return info.get("sector"), info.get("industry")


def _map_to_gics(yahoo_sector: str | None) -> str | None:
    """Translate a yfinance sector string to a GICS sector name."""
    if not yahoo_sector:
        return None
    return YFINANCE_TO_GICS_SECTOR.get(yahoo_sector.strip().lower())


def _random_delay(min_delay: float, max_delay: float) -> None:
    """Sleep for a randomized interval to avoid Yahoo's rate-limiter."""
    delay = max(0.5, random.uniform(min_delay, max_delay) + random.gauss(0, 0.3))
    time.sleep(delay)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--types",
        nargs="+",
        default=["ADRC", "CS"],
        choices=["CS", "ADRC", "ETF"],
        help="Polygon types to backfill (default: ADRC CS).",
    )
    parser.add_argument("--limit", type=int, help="Cap on tickers to process.")
    parser.add_argument("--resume", help="Start after this ticker (alphabetical).")
    parser.add_argument("--dry-run", action="store_true", help="Fetch but don't write.")
    parser.add_argument("--min-delay", type=float, default=DEFAULT_MIN_DELAY)
    parser.add_argument("--max-delay", type=float, default=DEFAULT_MAX_DELAY)
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logger = setup_logging(args.verbose)

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        logger.error("DATABASE_URL environment variable is required")
        return 1

    types = tuple(args.types)
    next_long_pause = random.randint(*LONG_PAUSE_EVERY)

    with psycopg.connect(database_url) as conn:
        targets = _load_targets(conn, types, args.resume, args.limit)
        logger.info(
            "Targets: %d active %s with NULL sic_code and no gics_override%s",
            len(targets),
            "+".join(types),
            f" (resuming after {args.resume})" if args.resume else "",
        )
        if not targets:
            logger.info("Nothing to do.")
            return 0
        if args.dry_run:
            logger.info("DRY RUN — no INSERT will be issued")

        insert_sql = """
            INSERT INTO gics_overrides (
                ticker, gics_sector, gics_industry, confidence, source, notes, updated_at
            ) VALUES (%s, %s, %s, %s, 'yfinance', %s, NOW())
            ON CONFLICT (ticker) DO UPDATE SET
                gics_sector = EXCLUDED.gics_sector,
                gics_industry = EXCLUDED.gics_industry,
                confidence = EXCLUDED.confidence,
                source = EXCLUDED.source,
                notes = EXCLUDED.notes,
                updated_at = NOW()
        """

        filled = skipped = errors = unmapped = 0
        last_commit_at = 0

        with conn.cursor() as cur:
            for i, (ticker, ttype) in enumerate(targets, start=1):
                if shutdown_requested:
                    logger.info("Shutdown — stopping after %d tickers", i - 1)
                    break

                yahoo_sector, yahoo_industry = _fetch_yfinance_sector(ticker)
                gics_sector = _map_to_gics(yahoo_sector)

                if yahoo_sector is None:
                    errors += 1
                    logger.debug("  %s (%s): yfinance returned no info", ticker, ttype)
                elif gics_sector is None:
                    unmapped += 1
                    logger.warning(
                        "  %s (%s): unknown yahoo sector %r — extend YFINANCE_TO_GICS_SECTOR",
                        ticker, ttype, yahoo_sector,
                    )
                else:
                    confidence = "high" if yahoo_industry else "medium"
                    notes = f"yfinance: sector={yahoo_sector!r}, industry={yahoo_industry!r}"
                    if not args.dry_run:
                        cur.execute(
                            insert_sql,
                            (ticker, gics_sector, yahoo_industry, confidence, notes),
                        )
                    filled += 1
                    logger.debug(
                        "  %s (%s) → %s / %s", ticker, ttype, gics_sector, yahoo_industry
                    )

                if not args.dry_run and (filled + skipped + errors + unmapped) - last_commit_at >= BATCH_COMMIT_SIZE:
                    conn.commit()
                    last_commit_at = filled + skipped + errors + unmapped
                    _save_state(ticker, filled, skipped, errors)

                if i % PROGRESS_LOG_INTERVAL == 0:
                    logger.info(
                        "  [%d/%d] filled=%d unmapped=%d errors=%d",
                        i, len(targets), filled, unmapped, errors,
                    )

                # Long stealth pause every N tickers, otherwise the
                # standard 2-6s gap.
                if i >= next_long_pause:
                    pause = random.uniform(*LONG_PAUSE_DURATION)
                    logger.info("  Long pause %.1fs after %d tickers...", pause, i)
                    time.sleep(pause)
                    next_long_pause = i + random.randint(*LONG_PAUSE_EVERY)
                else:
                    _random_delay(args.min_delay, args.max_delay)

            if not args.dry_run:
                conn.commit()

        logger.info(
            "Done: filled=%d (yfinance had a GICS-mappable sector), "
            "unmapped=%d (yfinance sector not in YFINANCE_TO_GICS_SECTOR), "
            "errors=%d (yfinance returned nothing).",
            filled, unmapped, errors,
        )
        if not args.dry_run and STATE_FILE.exists():
            STATE_FILE.unlink()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
