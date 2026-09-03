"""Quarantine price rows the provider can no longer re-base.

Polygon serves a rolling five-year history. Rows older than that horizon can
never be re-fetched, so when a later split changes the adjustment basis, the
back-adjustment silently stops at the horizon and everything older keeps the
previous basis. The series then carries a permanent step: the same company
priced two different ways on consecutive days.

Only rows that are *provably* on a stale basis are moved, identified by a
day-over-day jump whose ratio matches a split recorded for that ticker (within
5%). A jump with no matching split is left alone — a microcap really can move
3x in a day, and a reused ticker legitimately changes price.

--include-unmatched-jumps widens selection to every pre-horizon discontinuity,
including those with no matching split. Those are more likely to be real price
moves or a reused ticker than a stale basis, so it is off by default.

--prune-orphan-ta additionally deletes technical_indicators rows that have no
matching stock_prices row, which is what removing prices leaves behind.

Rows are copied to ``stock_prices_unadjustable_archive`` before removal, so the
operation is reversible. Defaults to a dry run, which performs NO writes at all
— it only counts. (An earlier version staged the writes and relied on rollback
to undo them; a dry run must not depend on a rollback it might not get.)

Extreme absolute prices are NOT a selection criterion. A ticker with compounded
reverse splits genuinely back-adjusts into the billions per share, and the
provider reports those same values on both sides of the horizon; they are
correct, not corrupt.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

import psycopg
from dotenv import load_dotenv

load_dotenv()

HORIZON_QUERY = """
SELECT MIN(date) FROM stock_prices
WHERE date >= (CURRENT_DATE - INTERVAL '5 years')
"""

# Selection differs only in the final WHERE: matched-split-ratio only, or every
# discontinuity. Both take the last jump per ticker as the cutoff.
STALE_TICKERS_QUERY = """
WITH horizon AS (SELECT %s::date AS d),
consecutive AS (
    SELECT ticker, date, close,
           lag(close) OVER (PARTITION BY ticker ORDER BY date) AS prev
    FROM stock_prices, horizon WHERE date < horizon.d
),
jumps AS (
    SELECT ticker, date, close / NULLIF(prev, 0) AS ratio
    FROM consecutive
    WHERE prev IS NOT NULL
      AND (close / NULLIF(prev, 0) > 3 OR close / NULLIF(prev, 0) < 0.34)
),
splits AS (SELECT ticker, split_to::numeric / split_from AS factor FROM stock_splits)
SELECT jumps.ticker, MAX(jumps.date) AS cutoff
FROM jumps
WHERE EXISTS (
    SELECT 1 FROM splits
    WHERE splits.ticker = jumps.ticker
      AND (abs(jumps.ratio - splits.factor) / splits.factor < 0.05
           OR abs(jumps.ratio - 1 / splits.factor) * splits.factor < 0.05)
)
GROUP BY jumps.ticker
"""

ALL_JUMP_TICKERS_QUERY = """
WITH horizon AS (SELECT %s::date AS d),
consecutive AS (
    SELECT ticker, date, close,
           lag(close) OVER (PARTITION BY ticker ORDER BY date) AS prev
    FROM stock_prices, horizon WHERE date < horizon.d
)
SELECT ticker, MAX(date) AS cutoff
FROM consecutive
WHERE prev IS NOT NULL
  AND (close / NULLIF(prev, 0) > 3 OR close / NULLIF(prev, 0) < 0.34)
GROUP BY ticker
"""

ORPHAN_TA_COUNT = """
SELECT count(*) FROM technical_indicators t
LEFT JOIN stock_prices p ON p.ticker = t.ticker AND p.date = t.date
WHERE p.ticker IS NULL
"""

ORPHAN_TA_DELETE = """
DELETE FROM technical_indicators t
WHERE NOT EXISTS (
    SELECT 1 FROM stock_prices p WHERE p.ticker = t.ticker AND p.date = t.date
)
"""

CREATE_ARCHIVE = """
CREATE TABLE IF NOT EXISTS stock_prices_unadjustable_archive (
    ticker VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open NUMERIC(24,8),
    high NUMERIC(24,8),
    low NUMERIC(24,8),
    close NUMERIC(24,8),
    volume BIGINT,
    stale_basis_cutoff DATE NOT NULL,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, date)
)
"""

COMMENT_ARCHIVE = """
COMMENT ON TABLE stock_prices_unadjustable_archive IS
    'Price rows removed from stock_prices because they sat on a pre-horizon '
    'split-adjustment basis the provider can no longer correct. Restorable.'
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="commit (default: dry run)")
    parser.add_argument(
        "--include-unmatched-jumps",
        action="store_true",
        help="also quarantine discontinuities with no matching split ratio",
    )
    parser.add_argument(
        "--prune-orphan-ta",
        action="store_true",
        help="delete technical_indicators rows that have no stock_prices row",
    )
    parser.add_argument(
        "--recompute-ta",
        action="store_true",
        help="recompute indicators for the affected tickers once rows are moved",
    )
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    args = parser.parse_args()

    if not args.database_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        return 2

    with psycopg.connect(args.database_url) as conn:
        horizon = conn.execute(HORIZON_QUERY).fetchone()[0]
        print(f"Provider history horizon (earliest re-fetchable date): {horizon}")

        query = (
            ALL_JUMP_TICKERS_QUERY
            if args.include_unmatched_jumps
            else STALE_TICKERS_QUERY
        )
        selection = (
            "every pre-horizon discontinuity"
            if args.include_unmatched_jumps
            else "discontinuities matching a recorded split ratio"
        )
        print(f"Selecting: {selection}")

        stale = conn.execute(query, (horizon,)).fetchall()
        if not stale and not args.prune_orphan_ta:
            print("Nothing to do.")
            return 0

        if not args.apply:
            # Count only. A dry run performs no writes, so it cannot leave
            # anything behind if the rollback does not happen.
            rows = 0
            for ticker, cutoff in stale:
                rows += conn.execute(
                    "SELECT count(*) FROM stock_prices "
                    "WHERE ticker = %s AND date < %s",
                    (ticker, cutoff),
                ).fetchone()[0]
            print(f"Tickers affected:      {len(stale)}")
            print(f"Rows to quarantine:    {rows}")
            if args.prune_orphan_ta:
                orphans = conn.execute(ORPHAN_TA_COUNT).fetchone()[0]
                print(f"Orphan TA rows now:    {orphans} (more appear once prices go)")
            print("DRY RUN - no writes were performed. Re-run with --apply.")
            return 0

        conn.execute(CREATE_ARCHIVE)
        conn.execute(COMMENT_ARCHIVE)

        moved = 0
        for ticker, cutoff in stale:
            copied = conn.execute(
                """
                INSERT INTO stock_prices_unadjustable_archive
                    (ticker, date, open, high, low, close, volume, stale_basis_cutoff)
                SELECT ticker, date, open, high, low, close, volume, %s
                FROM stock_prices WHERE ticker = %s AND date < %s
                ON CONFLICT (ticker, date) DO NOTHING
                """,
                (cutoff, ticker, cutoff),
            ).rowcount
            deleted = conn.execute(
                "DELETE FROM stock_prices WHERE ticker = %s AND date < %s",
                (ticker, cutoff),
            ).rowcount
            if copied != deleted:
                conn.rollback()
                print(
                    f"{ticker}: archived {copied} but would delete {deleted}; "
                    "refusing to lose rows",
                    file=sys.stderr,
                )
                return 1
            moved += deleted

        pruned = 0
        if args.prune_orphan_ta:
            pruned = conn.execute(ORPHAN_TA_DELETE).rowcount

        conn.commit()
        print(f"Tickers affected:      {len(stale)}")
        print(f"Rows quarantined:      {moved}")
        if args.prune_orphan_ta:
            print(f"Orphan TA rows pruned: {pruned}")
        print("APPLIED.")

    if not args.recompute_ta:
        # ta-backfill takes a single --ticker, so there is no one-line CLI way
        # to do this for a list. Say exactly what to run rather than leave it
        # as an exercise.
        print(
            "\nIndicators for these tickers were computed over the removed rows "
            "and are now stale.\nRe-run with --recompute-ta, or the next weekly "
            "will correct them."
        )
        return 0

    from sawa.ta_backfill import recompute_ta_for_tickers

    affected = [ticker for ticker, _ in stale]
    print(f"\nRecomputing indicators for {len(affected)} ticker(s)...")
    ta_stats = recompute_ta_for_tickers(
        database_url=args.database_url,
        tickers=affected,
        log=logging.getLogger("quarantine.ta"),
    )
    print(
        f"  recomputed {ta_stats.get('indicators_calculated', 0)} indicator rows "
        f"for {ta_stats.get('tickers_succeeded', 0)}/{len(affected)} tickers"
    )
    if not ta_stats.get("success"):
        print("  TA recompute reported failure", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
