"""
Split-adjusted price refresh: Re-fetch adjusted prices for tickers with recent splits.

Purpose: After stock splits are detected, re-fetch full adjusted price history
from the Polygon REST API and upsert over the stale unadjusted data. Rows older
than the provider's history window are re-based locally by the ratio the
provider applied at the window boundary.
Re-entrant: Safe to run multiple times (upsert by ticker/date; a tail already
on the provider basis measures a ratio of 1 and is left alone).
"""

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from typing import Any

import psycopg

from sawa.api import PolygonClient
from sawa.daily import fetch_prices_via_api, insert_prices
from sawa.repositories.rate_limiter import SyncRateLimiter
from sawa.utils import setup_logging
from sawa.utils.constants import DEFAULT_API_RATE_LIMIT
from sawa.utils.dates import DATE_FORMAT
from sawa.utils.security import redact_sensitive_text

# Pre-horizon re-basing ------------------------------------------------------
# The provider serves a rolling history window. When a split re-bases the whole
# series, rows older than that window can never be re-fetched — but the
# provider's own re-basing of the first dates it *does* serve reveals the exact
# ratio it applied. Stored rows on the same basis as those boundary rows are
# re-based locally by that ratio. Rows that already disagree with the boundary
# are left alone and reported; scripts/quarantine_unadjustable_prices.py owns
# those.
BASIS_PROBE_DATES = 5
# Open and close on every probe date must agree on the ratio this closely.
BASIS_RATIO_TOLERANCE = Decimal("0.005")
# The last pre-horizon close must be within an ordinary day's move of the
# boundary close, or the tail is not on the boundary's basis at all.
BASIS_CONTINUITY_BOUNDS = (Decimal("0.34"), Decimal("3"))
_SNAP_MAX_DENOMINATOR = 1000


@dataclass(frozen=True)
class PreHorizonRebase:
    """Local re-base of the rows the provider could not serve for one ticker."""

    ticker: str
    before: date  # the provider horizon: rows dated before it are re-based
    factor: Fraction  # stored basis / provider basis on the boundary dates
    expected_rows: int


def get_stored_price_rows(
    conn,
    ticker: str,
    dates: Sequence[date],
) -> dict[date, dict[str, Decimal]]:
    """Return the stored OHLCV for ``ticker`` on the given dates."""
    if not dates:
        return {}
    rows: dict[date, dict[str, Decimal]] = {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT date, open, high, low, close, volume FROM stock_prices "
            "WHERE ticker = %s AND date = ANY(%s)",
            (ticker, list(dates)),
        )
        for price_date, open_, high, low, close, volume in cur.fetchall():
            rows[price_date] = {
                "open": Decimal(str(open_)),
                "high": Decimal(str(high)),
                "low": Decimal(str(low)),
                "close": Decimal(str(close)),
                "volume": Decimal(str(volume)),
            }
    return rows


def get_unapplied_splits_in_range(
    conn,
    ticker: str,
    after: date,
    through: date,
) -> list[tuple[date, Fraction, int]]:
    """Recorded splits inside ``(after, through]`` whose raw jump is still stored.

    A tail that was loaded as-traded and never re-based still shows each
    split as a step in the stored closes on its execution date. Such a split
    must be applied to the rows before it on top of the boundary ratio, which
    only captures splits after the horizon. A split whose neighbouring closes
    are already continuous was applied by an earlier refresh and is skipped.
    Returns ``(execution_date, ratio, stored_rows_before)`` triples.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT execution_date, split_from, split_to FROM stock_splits "
            "WHERE ticker = %s AND execution_date > %s AND execution_date <= %s "
            "ORDER BY execution_date",
            (ticker, after, through),
        )
        splits = [
            (execution_date, Fraction(int(split_to), int(split_from)))
            for execution_date, split_from, split_to in cur.fetchall()
        ]
        unapplied: list[tuple[date, Fraction, int]] = []
        for execution_date, ratio in splits:
            cur.execute(
                "SELECT "
                "(SELECT close FROM stock_prices WHERE ticker = %(t)s AND date < %(d)s "
                " ORDER BY date DESC LIMIT 1), "
                "(SELECT close FROM stock_prices WHERE ticker = %(t)s AND date >= %(d)s "
                " ORDER BY date LIMIT 1), "
                "(SELECT count(*) FROM stock_prices WHERE ticker = %(t)s AND date < %(d)s)",
                {"t": ticker, "d": execution_date},
            )
            row = cur.fetchone()
            if row is None:
                continue
            previous_close, next_close, rows_before = row
            if previous_close is None or next_close is None or not rows_before:
                continue
            if raw_split_jump_present(
                Decimal(str(previous_close)), Decimal(str(next_close)), ratio
            ):
                unapplied.append((execution_date, ratio, int(rows_before)))
    return unapplied


def raw_split_jump_present(previous_close: Decimal, next_close: Decimal, ratio: Fraction) -> bool:
    """Whether the step across a split date is closer to the raw jump than to none."""
    if previous_close <= 0 or next_close <= 0:
        return False
    step = (next_close / previous_close).ln()
    raw_jump = -(Decimal(ratio.numerator) / Decimal(ratio.denominator)).ln()
    return abs(step - raw_jump) < abs(step)


def infer_basis_factor(
    stored: Mapping[date, Mapping[str, Any]],
    fetched: Mapping[date, Mapping[str, Any]],
    probe_dates: Sequence[date],
) -> Fraction | None:
    """Ratio (stored / provider) that the provider applied at the horizon.

    Compares open and close on every probe date. Returns None unless each
    comparison agrees within ``BASIS_RATIO_TOLERANCE``: a consistent ratio
    across consecutive bars is a basis change, while a correction to one bar
    is not. The ratio is snapped to the simplest matching fraction (a split
    ratio, or a product of them) when one exists; otherwise the measured
    ratio is used as-is.
    """
    ratios: list[Decimal] = []
    for probe in probe_dates:
        stored_row = stored.get(probe)
        fetched_row = fetched.get(probe)
        if stored_row is None or fetched_row is None:
            return None
        for field in ("open", "close"):
            try:
                stored_value = Decimal(str(stored_row[field]))
                fetched_value = Decimal(str(fetched_row[field]))
            except (KeyError, InvalidOperation, ValueError):
                return None
            if (
                not stored_value.is_finite()
                or not fetched_value.is_finite()
                or stored_value <= 0
                or fetched_value <= 0
            ):
                return None
            ratios.append(stored_value / fetched_value)
    if not ratios:
        return None
    ratios.sort()
    median = ratios[len(ratios) // 2]
    if any(abs(ratio / median - 1) > BASIS_RATIO_TOLERANCE for ratio in ratios):
        return None
    exact = Fraction(median)
    snapped = exact.limit_denominator(_SNAP_MAX_DENOMINATOR)
    if snapped > 0:
        snapped_value = Decimal(snapped.numerator) / Decimal(snapped.denominator)
        if abs(snapped_value / median - 1) <= BASIS_RATIO_TOLERANCE:
            return snapped
    return exact


def plan_pre_horizon_rebases(
    conn,
    *,
    fetched_by_ticker: Mapping[str, Mapping[date, Mapping[str, Any]]],
    existing_dates: Mapping[str, set[date]],
    unreachable_by_ticker: Mapping[str, Sequence[date]],
) -> tuple[list[PreHorizonRebase], list[str], dict[str, str]]:
    """Decide, per ticker, what to do with rows the provider cannot serve.

    Returns ``(plans, already_adjusted, skipped)``: tickers whose pre-horizon
    rows will be re-based by the boundary ratio, tickers whose rows already sit
    on the provider basis, and tickers left untouched with the reason.
    """
    plans: list[PreHorizonRebase] = []
    already_adjusted: list[str] = []
    skipped: dict[str, str] = {}
    lower, upper = BASIS_CONTINUITY_BOUNDS
    for ticker in sorted(unreachable_by_ticker):
        tail_dates = unreachable_by_ticker[ticker]
        fetched = fetched_by_ticker.get(ticker, {})
        stored_dates = existing_dates.get(ticker, set())
        probe_dates = sorted(d for d in fetched if d in stored_dates)[:BASIS_PROBE_DATES]
        if not probe_dates or not tail_dates:
            skipped[ticker] = "no stored date overlaps the provider window"
            continue
        tail_date = max(tail_dates)
        stored = get_stored_price_rows(conn, ticker, [*probe_dates, tail_date])
        boundary = stored.get(probe_dates[0])
        tail = stored.get(tail_date)
        if boundary is None or tail is None:
            skipped[ticker] = "boundary or tail row is no longer stored"
            continue
        factor = infer_basis_factor(stored, fetched, probe_dates)
        if factor is None:
            skipped[ticker] = "provider re-basing at the horizon is not one consistent ratio"
            continue
        continuity = tail["close"] / boundary["close"]
        if not lower <= continuity <= upper:
            skipped[ticker] = "pre-horizon rows already sit on a different basis than the boundary"
            continue
        horizon = min(fetched)
        ticker_plans: list[PreHorizonRebase] = []
        factor_value = Decimal(factor.numerator) / Decimal(factor.denominator)
        if abs(factor_value - 1) > BASIS_RATIO_TOLERANCE:
            ticker_plans.append(
                PreHorizonRebase(
                    ticker=ticker,
                    before=horizon,
                    factor=factor,
                    expected_rows=len(tail_dates),
                )
            )
        # The boundary ratio covers splits after the horizon. A tail loaded
        # as-traded also still carries every split inside it as a raw step;
        # apply those to the rows before each one.
        for execution_date, ratio, rows_before in get_unapplied_splits_in_range(
            conn, ticker, min(tail_dates), horizon
        ):
            ticker_plans.append(
                PreHorizonRebase(
                    ticker=ticker,
                    before=execution_date,
                    factor=ratio,
                    expected_rows=rows_before,
                )
            )
        if not ticker_plans:
            already_adjusted.append(ticker)
            continue
        plans.extend(ticker_plans)
    return plans, already_adjusted, skipped


def rebase_rows_before_date(conn, ticker: str, before: date, factor: Fraction) -> int:
    """Divide every stored bar dated before ``before`` by ``factor``.

    ``factor`` is stored-basis / provider-basis, so prices are divided by it
    and volume multiplied, exactly as the provider re-bases its own history.
    Returns the number of rows updated; the caller owns the transaction.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE stock_prices
            SET open = round(open * %(den)s / %(num)s, 8),
                high = round(high * %(den)s / %(num)s, 8),
                low = round(low * %(den)s / %(num)s, 8),
                close = round(close * %(den)s / %(num)s, 8),
                volume = round(volume * %(num)s / %(den)s)::bigint
            WHERE ticker = %(ticker)s AND date < %(before)s
            """,
            {
                "ticker": ticker,
                "before": before,
                "num": Decimal(factor.numerator),
                "den": Decimal(factor.denominator),
            },
        )
        return int(cur.rowcount)


def _format_ratio(factor: Fraction) -> str:
    if factor.denominator == 1:
        return f"{factor.numerator}:1"
    if factor.numerator == 1:
        return f"1:{factor.denominator}"
    return f"{Decimal(factor.numerator) / Decimal(factor.denominator):.6g}x"


def get_tickers_with_recent_splits(
    conn,
    since_date: date,
) -> list[str]:
    """Query stock_splits for tickers with splits since the given date."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT ticker FROM stock_splits WHERE execution_date >= %s ORDER BY ticker",
            (since_date,),
        )
        return [row[0] for row in cur.fetchall()]


def get_earliest_price_date(
    conn,
    tickers: list[str],
) -> date | None:
    """Get the earliest price date across the given tickers."""
    if not tickers:
        return None
    with conn.cursor() as cur:
        cur.execute(
            "SELECT MIN(date) FROM stock_prices WHERE ticker = ANY(%s)",
            (tickers,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def get_existing_price_dates(
    conn,
    tickers: list[str],
) -> dict[str, set[date]]:
    """Return every stored date that a full adjusted refresh must replace."""
    dates: dict[str, set[date]] = {ticker: set() for ticker in tickers}
    if not tickers:
        return dates
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ticker, date FROM stock_prices WHERE ticker = ANY(%s)",
            (tickers,),
        )
        for ticker, price_date in cur.fetchall():
            if ticker in dates and isinstance(price_date, date):
                dates[ticker].add(price_date)
    return dates


def refresh_split_adjusted_prices(
    api_key: str,
    database_url: str,
    tickers: list[str] | None = None,
    since: date | None = None,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Re-fetch adjusted price history for tickers with recent stock splits.

    Args:
        api_key: Polygon API key
        database_url: PostgreSQL connection URL
        tickers: Specific tickers to adjust (default: auto-detect from stock_splits)
        since: Only consider splits since this date (default: 1 year ago)
        dry_run: If True, show what would be done without executing
        logger: Logger instance

    Returns:
        Statistics dictionary
    """
    logger = logger or setup_logging()
    stats: dict[str, Any] = {
        "success": False,
        "tickers_requested": 0,
        "tickers_adjusted": 0,
        "prices_fetched": 0,
        "prices_updated": 0,
    }

    if since is None:
        since = date.today() - timedelta(days=365)

    client = PolygonClient(api_key, logger)
    rate_limiter = SyncRateLimiter(DEFAULT_API_RATE_LIMIT)

    with psycopg.connect(database_url) as conn:
        # Determine which tickers need adjustment. Dedupe so a ticker that split
        # multiple times in the window isn't re-fetched (full earliest-to-present
        # history) once per split.
        if tickers is None:
            tickers = get_tickers_with_recent_splits(conn, since)
        else:
            # Preserve order, drop duplicates (callers may pass one entry per
            # split row, e.g. weekly's stats['split_tickers']).
            tickers = list(dict.fromkeys(tickers))

        if not tickers:
            logger.info("No tickers with recent splits found - nothing to adjust")
            stats["success"] = True
            return stats

        stats["tickers_requested"] = len(tickers)

        logger.info(f"Found {len(tickers)} ticker(s) with splits to adjust: {', '.join(tickers)}")
        # Expose the resolved ticker list so callers (e.g. the adjust-splits CLI)
        # can recompute technical indicators for exactly the adjusted tickers.
        stats["tickers"] = tickers

        # Get earliest price date to know how far back to fetch
        earliest = get_earliest_price_date(conn, tickers)
        if not earliest:
            logger.warning("No existing price data found for split tickers")
            stats["error"] = "no existing price data found for requested split tickers"
            return stats
        existing_dates = get_existing_price_dates(conn, tickers)
        tickers_without_prices = [ticker for ticker in tickers if not existing_dates[ticker]]
        if tickers_without_prices:
            stats["missing_tickers"] = tickers_without_prices
            stats["error"] = (
                "no existing price history found for "
                f"{len(tickers_without_prices)} requested split ticker(s)"
            )
            logger.warning(stats["error"])
            return stats

        end_date = date.today()
        start_str = earliest.strftime(DATE_FORMAT)
        end_str = end_date.strftime(DATE_FORMAT)

        logger.info(f"Re-fetching adjusted prices from {start_str} to {end_str}")

        if dry_run:
            logger.info("[DRY RUN] Would re-fetch adjusted prices for:")
            for ticker in tickers:
                logger.info(f"  - {ticker}")
            stats["success"] = True
            stats["dry_run"] = True
            stats["tickers"] = tickers
            return stats

        # Fetch adjusted prices for each ticker
        provider_stats: dict[str, Any] = {}
        prices = fetch_prices_via_api(
            client,
            tickers,
            start_str,
            end_str,
            logger,
            rate_limiter,
            stats=provider_stats,
        )
        stats["provider"] = provider_stats
        stats["prices_fetched"] = len(prices)
        logger.info(f"Fetched {len(prices)} adjusted price records")

        fetched_tickers = {str(price.get("ticker", "")).upper() for price in prices}
        missing_tickers = [ticker for ticker in tickers if ticker not in fetched_tickers]
        fetched_dates: set[tuple[str, date]] = set()
        fetched_by_ticker: dict[str, dict[date, dict[str, Any]]] = {}
        for price in prices:
            ticker = str(price.get("ticker", "")).upper()
            raw_date = price.get("date")
            try:
                price_date = (
                    raw_date if isinstance(raw_date, date) else date.fromisoformat(str(raw_date))
                )
            except ValueError:
                continue
            fetched_dates.add((ticker, price_date))
            fetched_by_ticker.setdefault(ticker, {})[price_date] = price
        # The provider serves a rolling history window (currently five years),
        # so stored rows older than the oldest row it will return can never be
        # re-fetched. Requiring the provider to cover every stored date
        # therefore failed the whole adjustment for any ticker with a longer
        # history — and failing meant NOTHING was re-based, leaving the series
        # discontinuous at the split instead of at the unreachable horizon.
        # Split the shortfall: a gap inside the window the provider did serve
        # is real incompleteness and still fails; anything older than that
        # window is re-based locally by the ratio the provider applied at the
        # boundary when that ratio is unambiguous (see plan_pre_horizon_rebases),
        # and otherwise reported and left untouched. No stored row is deleted.
        earliest_fetched: dict[str, date] = {}
        for ticker, price_date in fetched_dates:
            current = earliest_fetched.get(ticker)
            if current is None or price_date < current:
                earliest_fetched[ticker] = price_date

        missing_existing_dates: list[tuple[str, date]] = []
        unreachable_dates: list[tuple[str, date]] = []
        for ticker, stored_dates in existing_dates.items():
            horizon = earliest_fetched.get(ticker)
            for price_date in stored_dates:
                if (ticker, price_date) in fetched_dates:
                    continue
                if horizon is not None and price_date < horizon:
                    unreachable_dates.append((ticker, price_date))
                else:
                    missing_existing_dates.append((ticker, price_date))
        missing_existing_dates.sort()

        rebase_plans: list[PreHorizonRebase] = []
        if unreachable_dates:
            horizon_start = min(earliest_fetched.values())
            stats["provider_history_horizon"] = horizon_start.isoformat()
            unreachable_by_ticker: dict[str, list[date]] = {}
            for ticker, price_date in unreachable_dates:
                unreachable_by_ticker.setdefault(ticker, []).append(price_date)
            rebase_plans, already_adjusted, rebase_skipped = plan_pre_horizon_rebases(
                conn,
                fetched_by_ticker=fetched_by_ticker,
                existing_dates=existing_dates,
                unreachable_by_ticker=unreachable_by_ticker,
            )
            planned_tickers = {plan.ticker for plan in rebase_plans}
            stats["pre_horizon_dates_rebased"] = sum(
                len(unreachable_by_ticker[ticker]) for ticker in planned_tickers
            )
            stats["pre_horizon_dates_already_adjusted"] = sum(
                len(unreachable_by_ticker[ticker]) for ticker in already_adjusted
            )
            stats["pre_horizon_dates_not_adjusted"] = sum(
                len(unreachable_by_ticker[ticker]) for ticker in rebase_skipped
            )
            logger.info(
                f"  {len(unreachable_dates)} stored date(s) predate the provider's "
                f"available history (earliest {horizon_start.isoformat()})"
            )
            for plan in rebase_plans:
                logger.info(
                    f"  {plan.ticker}: re-basing {plan.expected_rows} row(s) before "
                    f"{plan.before.isoformat()} by {_format_ratio(plan.factor)}"
                )
            if rebase_skipped:
                stats["pre_horizon_rebase_skipped"] = dict(
                    sorted(rebase_skipped.items())[:10]
                )
                logger.warning(
                    f"  {stats['pre_horizon_dates_not_adjusted']} pre-horizon row(s) "
                    f"across {len(rebase_skipped)} ticker(s) keep their existing basis: "
                    + "; ".join(
                        f"{ticker} ({reason})"
                        for ticker, reason in sorted(rebase_skipped.items())[:5]
                    )
                )

        if missing_tickers or provider_stats.get("failed_symbols") or provider_stats.get(
            "invalid_price_rows"
        ) or missing_existing_dates:
            stats["missing_tickers"] = missing_tickers
            stats["missing_existing_price_dates"] = len(missing_existing_dates)
            stats["missing_existing_price_date_samples"] = [
                f"{ticker}/{price_date.isoformat()}"
                for ticker, price_date in missing_existing_dates[:10]
            ]
            stats["error"] = (
                "adjusted price source was incomplete "
                f"({len(fetched_tickers)}/{len(tickers)} tickers with valid rows; "
                f"{provider_stats.get('failed_symbols', 0)} request failures; "
                f"{provider_stats.get('invalid_price_rows', 0)} invalid rows; "
                f"{len(missing_existing_dates)} stored dates missing)"
            )
            logger.warning(stats["error"])
            return stats

        try:
            inserted = insert_prices(conn, prices, logger, commit=False)
        except Exception as e:
            conn.rollback()
            safe_error = f"{type(e).__name__}: {redact_sensitive_text(e)}"
            stats["error"] = f"adjusted price persistence failed: {safe_error}"
            logger.error(stats["error"])
            return stats
        stats["prices_updated"] = inserted
        logger.info(f"Upserted {inserted} adjusted price records")
        if inserted != len(prices):
            conn.rollback()
            stats["error"] = (
                f"adjusted price persistence wrote only {inserted}/{len(prices)} rows"
            )
            logger.error(stats["error"])
            return stats
        # Same transaction as the upsert: the provider-served range and the
        # locally re-based tail must land together or not at all.
        for plan in rebase_plans:
            try:
                updated = rebase_rows_before_date(conn, plan.ticker, plan.before, plan.factor)
            except Exception as e:
                conn.rollback()
                safe_error = f"{type(e).__name__}: {redact_sensitive_text(e)}"
                stats["error"] = f"pre-horizon re-base failed for {plan.ticker}: {safe_error}"
                logger.error(stats["error"])
                return stats
            if updated != plan.expected_rows:
                conn.rollback()
                stats["error"] = (
                    f"pre-horizon re-base touched {updated}/{plan.expected_rows} "
                    f"rows for {plan.ticker}"
                )
                logger.error(stats["error"])
                return stats
        conn.commit()

        stats["tickers_adjusted"] = len(fetched_tickers)
        stats["success"] = True

    return stats
