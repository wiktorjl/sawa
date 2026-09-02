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

Rows are copied to ``stock_prices_unadjustable_archive`` before removal, so the
operation is reversible. Defaults to a dry run; pass --apply to commit.

Extreme absolute prices are NOT a selection criterion. A ticker with compounded
reverse splits genuinely back-adjusts into the billions per share, and the
provider reports those same values on both sides of the horizon; they are
correct, not corrupt.
"""

from __future__ import annotations

import argparse
import os
import sys

import psycopg
from dotenv import load_dotenv

load_dotenv()

HORIZON_QUERY = """
SELECT MIN(date) FROM stock_prices
WHERE date >= (CURRENT_DATE - INTERVAL '5 years')
"""

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
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    args = parser.parse_args()

    if not args.database_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        return 2

    with psycopg.connect(args.database_url) as conn:
        horizon = conn.execute(HORIZON_QUERY).fetchone()[0]
        print(f"Provider history horizon (earliest re-fetchable date): {horizon}")

        stale = conn.execute(STALE_TICKERS_QUERY, (horizon,)).fetchall()
        if not stale:
            print("No provably stale pre-horizon rows found.")
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

        print(f"Tickers affected:      {len(stale)}")
        print(f"Rows quarantined:      {moved}")

        if args.apply:
            conn.commit()
            print("APPLIED. Recompute technical indicators for these tickers.")
        else:
            conn.rollback()
            print("DRY RUN - nothing committed. Re-run with --apply.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
