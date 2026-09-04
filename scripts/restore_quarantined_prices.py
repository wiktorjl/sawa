"""Restore quarantined price rows on the split-adjusted basis.

``scripts/quarantine_unadjustable_prices.py`` moves rows the provider can no
longer re-fetch out of ``stock_prices`` when they sit on a stale split basis.
Those rows are not lost information: once ``stock_splits`` covers the whole
price history, the basis they need is computable from the splits that executed
after them.

For every archived ticker this script builds candidate re-basings and keeps the
one — and only one — that makes the series continuous:

* **as-traded**: every recorded split after each row's own date applies (rows a
  coldstart loaded from the as-traded flat files and nothing ever re-based);
* **last k splits**: only the k most recent splits apply uniformly (rows an
  earlier refresh had already re-based for the older splits, then a later split
  moved the provider horizon past them).

A candidate is continuous when the restored tail's last close sits within an
ordinary day's move of the first stored row after the quarantine cutoff, and no
two adjacent restored closes differ by more than the quarantine script's own
discontinuity threshold. An archive that itself steps by a split ratio holds
several segments on different bases (each earlier refresh stopped at its own
horizon); segments are re-based one at a time from the newest, each against the
segment restored before it, and restoration stops at the first segment with no
unique continuous candidate. Whatever is left, and tickers with no continuous
candidate at all, stay in the archive and are reported with the reason.

``--as-is TICKER`` restores rows unchanged. It is accepted only when the
registry records no split after those rows — then no stale basis is possible
and the quarantined jump was a genuine move — and the implied step is printed.

Defaults to a dry run that performs no writes.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from fractions import Fraction

import psycopg
from dotenv import load_dotenv

from sawa.domain.corporate_actions import SplitAdjuster, StockSplit

load_dotenv()

# Restored tail's last close vs the first stored row after the cutoff.
BOUNDARY_BOUNDS = (Decimal("0.75"), Decimal("1.3334"))
# Adjacent restored closes; matches the quarantine script's jump definition.
INTERNAL_BOUNDS = (Decimal("0.34"), Decimal("3"))

ARCHIVED_TICKERS = """
SELECT ticker, count(*), min(date), max(date), max(stale_basis_cutoff)
FROM stock_prices_unadjustable_archive
GROUP BY ticker ORDER BY ticker
"""
ARCHIVED_ROWS = """
SELECT date, open, high, low, close, volume
FROM stock_prices_unadjustable_archive WHERE ticker = %s ORDER BY date
"""
BOUNDARY_ROW = """
SELECT date, close FROM stock_prices
WHERE ticker = %s AND date >= %s ORDER BY date LIMIT 1
"""
TICKER_SPLITS = """
SELECT ticker, execution_date, split_from, split_to
FROM stock_splits WHERE ticker = %s ORDER BY execution_date
"""
INSERT_ROW = """
INSERT INTO stock_prices (ticker, date, open, high, low, close, volume)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (ticker, date) DO NOTHING
"""
DELETE_ARCHIVED = """
DELETE FROM stock_prices_unadjustable_archive WHERE ticker = %s AND date = ANY(%s)
"""


@dataclass(frozen=True)
class Candidate:
    label: str
    rows: list[dict]
    boundary_ratio: Decimal


@dataclass(frozen=True)
class Decision:
    ticker: str
    archived_rows: int
    first_date: date
    last_date: date
    restore: Candidate | None
    reason: str


def _continuity(rows: list[dict], boundary_close: Decimal) -> tuple[Decimal | None, str]:
    low, high = INTERNAL_BOUNDS
    for previous, current in zip(rows, rows[1:]):
        step = Decimal(current["close"]) / Decimal(previous["close"])
        if not low <= step <= high:
            return None, f"internal jump {step:.2f}x at {current['date']}"
    ratio = Decimal(rows[-1]["close"]) / boundary_close
    b_low, b_high = BOUNDARY_BOUNDS
    if not b_low <= ratio <= b_high:
        return None, f"boundary ratio {ratio:.3f}"
    return ratio, f"boundary ratio {ratio:.3f}"


def _segments(archived: list[dict]) -> list[list[dict]]:
    """Split the archived rows wherever adjacent closes step outside INTERNAL_BOUNDS."""
    low, high = INTERNAL_BOUNDS
    segments: list[list[dict]] = [[archived[0]]]
    for previous, current in zip(archived, archived[1:]):
        step = Decimal(current["close"]) / Decimal(previous["close"])
        if low <= step <= high:
            segments[-1].append(current)
        else:
            segments.append([current])
    return segments


def _candidates(
    splits: list[StockSplit],
    first_date: date,
    last_date: date,
) -> list[tuple[str, SplitAdjuster]]:
    within = [s for s in splits if first_date < s.execution_date <= last_date]
    after = [s for s in splits if s.execution_date > last_date]
    labelled: list[tuple[str, SplitAdjuster]] = []
    if within or after:
        labelled.append(
            (
                f"as-traded ({len(within) + len(after)} recorded split(s))",
                SplitAdjuster(within + after),
            )
        )
    for k in range(1, len(after) + 1):
        suffix = after[-k:]
        if k == len(after) and not within:
            break  # identical to the as-traded candidate
        ratio = Fraction(1)
        for split in suffix:
            ratio *= Fraction(split.split_to, split.split_from)
        labelled.append((f"last {k} split(s), ratio {ratio}", SplitAdjuster(suffix)))
    return labelled


def _rebase_segment(
    segment: list[dict],
    splits: list[StockSplit],
    reference_close: Decimal,
) -> tuple[Candidate | None, str]:
    """Pick the one re-basing of ``segment`` continuous with ``reference_close``."""
    first_date, last_date = segment[0]["date"], segment[-1]["date"]
    continuous: list[Candidate] = []
    failures: list[str] = []
    seen: set[tuple[Decimal, Decimal]] = set()
    for label, adjuster in _candidates(splits, first_date, last_date):
        try:
            rows = [adjuster.adjust_row(row) for row in segment]
        except ValueError as exc:
            failures.append(f"{label}: {exc}")
            continue
        signature = (Decimal(rows[0]["close"]), Decimal(rows[-1]["close"]))
        if signature in seen:
            continue
        seen.add(signature)
        ratio, note = _continuity(rows, reference_close)
        if ratio is None:
            failures.append(f"{label}: {note}")
        else:
            continuous.append(Candidate(label, rows, ratio))
    if len(continuous) == 1:
        return continuous[0], continuous[0].label
    if not continuous:
        return None, "no continuous re-basing: " + "; ".join(failures)
    return None, "ambiguous: " + " | ".join(c.label for c in continuous)


def decide(
    conn,
    ticker: str,
    archived_rows: int,
    first_date: date,
    last_date: date,
    cutoff: date,
    *,
    as_is: bool = False,
) -> Decision:
    archived = [
        {"ticker": ticker, "date": d, "open": o, "high": h, "low": lo, "close": c, "volume": v}
        for d, o, h, lo, c, v in conn.execute(ARCHIVED_ROWS, (ticker,)).fetchall()
    ]
    boundary = conn.execute(BOUNDARY_ROW, (ticker, cutoff)).fetchone()
    if boundary is None:
        return Decision(
            ticker, archived_rows, first_date, last_date, None, "no stored row after the cutoff"
        )
    boundary_close = Decimal(boundary[1])
    splits = [
        StockSplit(ticker=t, execution_date=e, split_from=f, split_to=to)
        for t, e, f, to in conn.execute(TICKER_SPLITS, (ticker,)).fetchall()
    ]
    later_splits = [s for s in splits if s.execution_date > first_date]

    if as_is:
        if later_splits:
            return Decision(
                ticker,
                archived_rows,
                first_date,
                last_date,
                None,
                f"refused --as-is: {len(later_splits)} recorded split(s) after the archived rows",
            )
        step = Decimal(archived[-1]["close"]) / boundary_close
        candidate = Candidate(
            f"as-is (no recorded split after these rows; step at cutoff {step:.3f})",
            archived,
            step,
        )
        return Decision(ticker, archived_rows, first_date, last_date, candidate, candidate.label)

    if not later_splits:
        return Decision(
            ticker,
            archived_rows,
            first_date,
            last_date,
            None,
            "no recorded split after the archived rows",
        )

    # Newest segment first, each validated against what was restored after it.
    restored: list[dict] = []
    labels: list[str] = []
    reference_close = boundary_close
    boundary_ratio: Decimal | None = None
    leftover = ""
    failure = ""
    for segment in reversed(_segments(archived)):
        candidate, note = _rebase_segment(segment, splits, reference_close)
        if candidate is None:
            left = sum(1 for row in archived if row["date"] <= segment[-1]["date"])
            leftover = f"; {left} older row(s) left archived ({note})"
            failure = note
            break
        restored = candidate.rows + restored
        labels.append(candidate.label)
        if boundary_ratio is None:
            boundary_ratio = candidate.boundary_ratio
        reference_close = Decimal(candidate.rows[0]["close"])

    if not restored or boundary_ratio is None:
        return Decision(ticker, archived_rows, first_date, last_date, None, failure)
    label = " then ".join(labels) if len(labels) > 1 else labels[0]
    return Decision(
        ticker,
        archived_rows,
        first_date,
        last_date,
        Candidate(label, restored, boundary_ratio),
        label + leftover,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--apply", action="store_true", help="commit (default: dry run)")
    parser.add_argument("--ticker", action="append", help="restrict to this ticker (repeatable)")
    parser.add_argument(
        "--as-is",
        action="append",
        default=[],
        metavar="TICKER",
        help="restore this ticker unchanged; accepted only with no recorded split after its rows",
    )
    parser.add_argument(
        "--recompute-ta",
        action="store_true",
        help="recompute indicators for the restored tickers once rows are back",
    )
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    args = parser.parse_args()

    if not args.database_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        return 2
    wanted = {t.upper() for t in args.ticker} if args.ticker else None
    as_is = {t.upper() for t in args.as_is}

    decisions: list[Decision] = []
    with psycopg.connect(args.database_url) as conn:
        for ticker, rows, first_date, last_date, cutoff in conn.execute(
            ARCHIVED_TICKERS
        ).fetchall():
            if wanted is not None and ticker not in wanted:
                continue
            decisions.append(
                decide(conn, ticker, rows, first_date, last_date, cutoff, as_is=ticker in as_is)
            )

        restorable = [d for d in decisions if d.restore is not None]
        skipped = [d for d in decisions if d.restore is None]

        print(f"{'Ticker':<7} {'Rows':>5} {'Span':<23} {'Action':<8} Basis / reason")
        for d in decisions:
            action = "RESTORE" if d.restore else "SKIP"
            span = f"{d.first_date}..{d.last_date}"
            print(f"{d.ticker:<7} {d.archived_rows:>5} {span} {action:<8} {d.reason}")
        print()
        restorable_rows = sum(len(d.restore.rows) for d in restorable if d.restore)
        left_rows = sum(d.archived_rows for d in decisions) - restorable_rows
        print(f"Tickers restorable:    {len(restorable)} ({restorable_rows} rows)")
        print(
            f"Rows left archived:    {left_rows} across {len(skipped)} skipped ticker(s) + partials"
        )

        if not args.apply:
            print("DRY RUN - no writes were performed. Re-run with --apply.")
            return 0
        if not restorable:
            print("Nothing to restore.")
            return 0

        for d in decisions:
            if d.restore is None:
                continue
            inserted = 0
            with conn.cursor() as cur:
                for row in d.restore.rows:
                    cur.execute(
                        INSERT_ROW,
                        (
                            row["ticker"],
                            row["date"],
                            row["open"],
                            row["high"],
                            row["low"],
                            row["close"],
                            row["volume"],
                        ),
                    )
                    inserted += cur.rowcount
                cur.execute(DELETE_ARCHIVED, (d.ticker, [row["date"] for row in d.restore.rows]))
                deleted = cur.rowcount
            if inserted != len(d.restore.rows) or deleted != len(d.restore.rows):
                conn.rollback()
                print(
                    f"{d.ticker}: inserted {inserted}, removed {deleted} of {len(d.restore.rows)} "
                    "rows; refusing to leave the two tables inconsistent",
                    file=sys.stderr,
                )
                return 1
        conn.commit()
        print(f"APPLIED: restored {restorable_rows} rows for {len(restorable)} ticker(s).")

    if not args.recompute_ta:
        print(
            "\nIndicators for the restored tickers do not yet cover the restored rows; "
            "re-run with --recompute-ta."
        )
        return 0

    from sawa.ta_backfill import recompute_ta_for_tickers

    tickers = [d.ticker for d in restorable]
    print(f"\nRecomputing indicators for {len(tickers)} ticker(s)...")
    ta_stats = recompute_ta_for_tickers(
        database_url=args.database_url,
        tickers=tickers,
        log=logging.getLogger("restore.ta"),
    )
    print(
        f"  recomputed {ta_stats.get('indicators_calculated', 0)} indicator rows "
        f"for {ta_stats.get('tickers_succeeded', 0)}/{len(tickers)} tickers"
    )
    if not ta_stats.get("success"):
        print("  TA recompute reported failure", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
