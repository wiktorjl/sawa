#!/usr/bin/env python3
"""
Backfill missing SIC codes on the companies table from Polygon.

The 2026-05-16 audit found that ~859 active common stocks and ~348
active ADRs have ``sic_code IS NULL`` — they were added before Polygon
had populated the field, or (for ADRs) Polygon doesn't carry SIC for
foreign issuers at all. Sawa's GICS sector lookup falls back to a
literal SIC description in that case, so unsectorized tickers can't be
filtered or aggregated cleanly.

This script re-fetches Polygon's ``/v3/reference/tickers/{t}`` endpoint
for every active CS/ADRC where ``sic_code IS NULL`` and ``UPDATE``s
``companies.sic_code`` and ``companies.sic_description`` when Polygon
returns non-empty values. The pre-existing ``sawa add-symbol`` flow
intentionally does *not* update SIC on ON CONFLICT (its SET list
excludes those fields), so a dedicated script is the right shape.

Tickers that still have NULL SIC after this script runs are the input
set for ``scripts/backfill_gics_overrides.py`` (yfinance fallback).

Usage:
    python scripts/backfill_sic_codes.py
    python scripts/backfill_sic_codes.py --limit 100
    python scripts/backfill_sic_codes.py --types CS         # CS only, skip ADRC
    python scripts/backfill_sic_codes.py --resume AAPL      # resume from ticker
    python scripts/backfill_sic_codes.py --dry-run          # fetch, no DB writes

Requires:
    POLYGON_API_KEY env var
    DATABASE_URL env var
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import time
from pathlib import Path

import psycopg
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sawa.api.client import PolygonClient  # noqa: E402
from sawa.utils import alert_missing_api_key, setup_logging  # noqa: E402

load_dotenv()

# Polygon's free tier allows 5 requests/sec; the paid plan is higher.
# 100ms keeps us under the free-tier limit with margin and is fast
# enough that ~1,200 tickers complete in ~2 minutes.
DEFAULT_DELAY_SECONDS = 0.1
BATCH_COMMIT_SIZE = 25
PROGRESS_LOG_INTERVAL = 50

STATE_FILE = Path(__file__).parent / ".sic_backfill_state"

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
    """Return ``[(ticker, type), ...]`` for active CS/ADRC missing SIC."""
    type_placeholders = ", ".join(["%s"] * len(types))
    args: list = list(types)
    where_clauses = [
        "active = true",
        "sic_code IS NULL",
        f"type IN ({type_placeholders})",
    ]
    if resume_after:
        where_clauses.append("ticker > %s")
        args.append(resume_after)
    where_sql = " AND ".join(where_clauses)
    sql = f"SELECT ticker, type FROM companies WHERE {where_sql} ORDER BY ticker"
    if limit:
        sql += f" LIMIT {int(limit)}"
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return [(row[0], row[1]) for row in cur.fetchall()]


def _save_state(ticker: str, filled: int, skipped: int, errors: int) -> None:
    STATE_FILE.write_text(
        f"last_ticker={ticker}\nfilled={filled}\nskipped={skipped}\nerrors={errors}\n"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--types",
        nargs="+",
        default=["CS", "ADRC"],
        choices=["CS", "ADRC", "ETF"],
        help="Polygon types to backfill (default: CS ADRC; ETFs rarely carry SIC).",
    )
    parser.add_argument("--limit", type=int, help="Cap on tickers to process.")
    parser.add_argument("--resume", help="Start after this ticker (alphabetical).")
    parser.add_argument("--dry-run", action="store_true", help="Fetch but don't write.")
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY_SECONDS,
        help=f"Seconds between Polygon requests (default: {DEFAULT_DELAY_SECONDS}).",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logger = setup_logging(args.verbose)

    api_key = os.environ.get("POLYGON_API_KEY")
    database_url = os.environ.get("DATABASE_URL")
    if not api_key:
        alert_missing_api_key("POLYGON_API_KEY", "Polygon ticker details", logger)
        return 1
    if not database_url:
        logger.error("DATABASE_URL environment variable is required")
        return 1

    client = PolygonClient(api_key, logger=logger)
    types = tuple(args.types)

    with psycopg.connect(database_url) as conn:
        targets = _load_targets(conn, types, args.resume, args.limit)
        logger.info(
            "Targets: %d active %s with NULL sic_code%s",
            len(targets),
            "+".join(types),
            f" (resuming after {args.resume})" if args.resume else "",
        )
        if not targets:
            logger.info("Nothing to do.")
            return 0
        if args.dry_run:
            logger.info("DRY RUN — no UPDATE will be issued")

        filled = skipped = errors = 0
        last_commit_at = 0

        with conn.cursor() as cur:
            for i, (ticker, ttype) in enumerate(targets, start=1):
                if shutdown_requested:
                    logger.info("Shutdown — stopping after %d tickers", i - 1)
                    break

                try:
                    details = client.get_ticker_details(ticker)
                except Exception as exc:  # noqa: BLE001 — log and continue
                    logger.warning("  %s: Polygon fetch failed: %s", ticker, exc)
                    errors += 1
                    continue

                results = (details or {}).get("results") or details or {}
                sic_code = results.get("sic_code")
                sic_desc = results.get("sic_description")

                if not sic_code:
                    skipped += 1
                    logger.debug("  %s (%s): Polygon returned no SIC", ticker, ttype)
                else:
                    if not args.dry_run:
                        cur.execute(
                            "UPDATE companies "
                            "SET sic_code = %s, sic_description = %s, updated_at = NOW() "
                            "WHERE ticker = %s",
                            (sic_code, sic_desc, ticker),
                        )
                    filled += 1
                    logger.debug(
                        "  %s (%s) → sic=%s (%s)", ticker, ttype, sic_code, sic_desc
                    )

                if not args.dry_run and (filled + skipped) - last_commit_at >= BATCH_COMMIT_SIZE:
                    conn.commit()
                    last_commit_at = filled + skipped
                    _save_state(ticker, filled, skipped, errors)

                if i % PROGRESS_LOG_INTERVAL == 0:
                    logger.info(
                        "  [%d/%d] filled=%d skipped=%d errors=%d",
                        i, len(targets), filled, skipped, errors,
                    )

                time.sleep(args.delay)

            if not args.dry_run:
                conn.commit()

        logger.info(
            "Done: filled=%d (Polygon had SIC), skipped=%d (Polygon still NULL — "
            "yfinance fallback territory), errors=%d",
            filled, skipped, errors,
        )
        if not args.dry_run and STATE_FILE.exists():
            STATE_FILE.unlink()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
