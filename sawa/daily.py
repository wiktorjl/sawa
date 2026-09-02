"""
Daily update: Pull new stock prices and news since last update.

Purpose: Update stock prices and news (fast, daily operation).
Re-entrant: Safe to run multiple times (upsert by ticker/date).
Uses REST API for near real-time data availability.
"""

import logging
import os
from datetime import date, timedelta
from math import ceil
from pathlib import Path
from typing import Any

import httpx
import psycopg
from psycopg import sql

from sawa.api import (
    CboeClient,
    CboeMarketInternalsResult,
    FredClient,
    FredMarketInternalsResult,
    PolygonClient,
)
from sawa.database import get_last_date, get_symbols_from_db
from sawa.database.news import NewsLoadResult, fetch_and_load_news
from sawa.domain.exceptions import ProviderError
from sawa.domain.price_validation import (
    is_valid_daily_ohlcv,
    normalize_provider_volume,
)
from sawa.repositories.rate_limiter import SyncRateLimiter
from sawa.utils import alert_missing_api_key, get_notifier, setup_logging
from sawa.utils.constants import (
    DEFAULT_API_RATE_LIMIT,
    DEFAULT_NEWS_DAYS,
    MARKET_INTERNALS_OVERLAP_DAYS,
)
from sawa.utils.dates import DATE_FORMAT, timestamp_to_date
from sawa.utils.market_hours import get_market_date, is_after_market_close
from sawa.utils.notify import NotificationLevel
from sawa.utils.security import redact_sensitive_text

# Must match doctor's stock_prices.latest_coverage threshold so the daily backfill
# doesn't leave a date that the post-run doctor check will then flag.
MIN_LATEST_COVERAGE = 0.85
# Re-fetch a bounded multi-session window on ordinary current tickers. A ticker
# whose own watermark is older gets its full stale window without widening
# every other ticker's request.
PRICE_REPAIR_OVERLAP_DAYS = 14


def _new_ta_rows(indicators: list[Any], last_ta: date | None) -> list[Any]:
    """Keep only rows the incremental daily path has not already persisted."""
    if last_ta is None:
        return indicators
    return [indicator for indicator in indicators if indicator.date > last_ta]


def _ta_rows_to_persist(
    indicators: list[Any],
    last_ta: date | None,
    recompute_from: date | None,
) -> list[Any]:
    """Select incremental rows or overwrite a forced historical replay range."""
    if recompute_from is not None:
        return [indicator for indicator in indicators if indicator.date >= recompute_from]
    return _new_ta_rows(indicators, last_ta)


def _effective_ta_recompute_from(
    last_ta: date | None,
    repaired_price_from: date | None,
) -> date | None:
    """Return the earliest TA date that must be overwritten for a ticker."""
    if last_ta is None or repaired_price_from is None:
        # No TA means bootstrap all history. No repaired price means preserve
        # the ordinary strictly incremental path.
        return None
    return min(repaired_price_from, last_ta + timedelta(days=1))


def _last_date_coverage(conn: Any, last_date: date) -> tuple[int, int]:
    """Return (tickers_on_last_date, baseline) for the daily backfill gate.

    Baseline is the max distinct active-ticker count across the 10 trading days
    immediately preceding ``last_date`` — same construction doctor uses.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH prior_dates AS (
                SELECT date
                FROM stock_prices
                WHERE date < %s
                GROUP BY date
                ORDER BY date DESC
                LIMIT 10
            ),
            daily_counts AS (
                SELECT COUNT(DISTINCT sp.ticker) AS n
                FROM stock_prices sp
                JOIN companies c ON c.ticker = sp.ticker
                WHERE c.active = true
                  AND sp.date IN (SELECT date FROM prior_dates)
                GROUP BY sp.date
            ),
            latest AS (
                SELECT COUNT(DISTINCT sp.ticker) AS n
                FROM stock_prices sp
                JOIN companies c ON c.ticker = sp.ticker
                WHERE c.active = true
                  AND sp.date = %s
            )
            SELECT
                (SELECT COALESCE(n, 0) FROM latest),
                COALESCE((SELECT MAX(n) FROM daily_counts), 0)
            """,
            (last_date, last_date),
        )
        row = cur.fetchone()
    return int(row[0] or 0), int(row[1] or 0)


def _symbol_price_watermarks(conn: Any, symbols: list[str]) -> dict[str, date]:
    """Return each requested ticker's latest persisted daily price date."""
    if not symbols:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ticker, MAX(date)
            FROM stock_prices
            WHERE ticker = ANY(%s)
            GROUP BY ticker
            """,
            (symbols,),
        )
        rows = cur.fetchall()
    return {
        str(ticker): watermark
        for ticker, watermark in rows
        if isinstance(watermark, date)
    }


def _symbol_repair_start_dates(
    symbols: list[str],
    watermarks: dict[str, date],
    latest_date: date,
) -> dict[str, str]:
    """Build retry-safe per-ticker windows without widening every request."""
    routine_overlap = latest_date - timedelta(days=PRICE_REPAIR_OVERLAP_DAYS)
    return {
        symbol: min(routine_overlap, watermarks.get(symbol, routine_overlap)).strftime(
            DATE_FORMAT
        )
        for symbol in symbols
    }


def fetch_prices_via_api(
    client: PolygonClient,
    symbols: list[str],
    start_date: str,
    end_date: str,
    logger: logging.Logger,
    rate_limiter: SyncRateLimiter | None = None,
    stats: dict[str, Any] | None = None,
    start_dates: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch prices for all symbols via REST API.

    Per-symbol failures (timeouts, 429s, 5xx) are counted and surfaced as a
    WARNING summary (and ``stats['fetch_errors']`` when ``stats`` is given) so a
    partial upstream outage is visible rather than silently dropping symbols.

    Returns list of price records ready for database insert.
    """
    all_prices: list[dict[str, Any]] = []
    failures = 0
    succeeded_symbols = 0
    failed_tickers: list[str] = []
    empty_tickers: list[str] = []
    invalid_rows = 0
    provider_rows = 0
    requested_end = date.fromisoformat(end_date)

    for i, symbol in enumerate(symbols, 1):
        if i % 50 == 0:
            logger.info(f"  Progress: {i}/{len(symbols)} symbols")

        before_count = len(all_prices)
        try:
            symbol_start = (start_dates or {}).get(symbol, start_date)
            requested_start = date.fromisoformat(symbol_start)
            if rate_limiter:
                rate_limiter.acquire()

            data = client.get(
                "aggregates",
                path_params={"ticker": symbol, "start": symbol_start, "end": end_date},
                params={"adjusted": "true", "limit": 50000},
            )

            if not isinstance(data, dict):
                raise ProviderError(
                    "Invalid aggregate response object", provider="polygon"
                )
            results = data.get("results", [])
            if not isinstance(results, list):
                raise ProviderError(
                    "Invalid aggregate results array", provider="polygon"
                )
            for r in results:
                provider_rows += 1
                if not isinstance(r, dict):
                    invalid_rows += 1
                    continue
                timestamp = r.get("t")
                # Polygon aggregate timestamps are integral Unix milliseconds.
                # In particular, bool is not a timestamp even though it is an
                # int subclass in Python.
                if isinstance(timestamp, bool) or not isinstance(timestamp, int):
                    invalid_rows += 1
                    continue
                try:
                    price_date = timestamp_to_date(timestamp)
                except (OverflowError, OSError, TypeError, ValueError):
                    invalid_rows += 1
                    continue
                if not requested_start <= price_date <= requested_end:
                    invalid_rows += 1
                    continue
                price = {
                    "ticker": symbol,
                    "date": price_date.strftime(DATE_FORMAT),
                    "open": r.get("o"),
                    "high": r.get("h"),
                    "low": r.get("l"),
                    "close": r.get("c"),
                    "volume": normalize_provider_volume(r.get("v")),
                }
                if _is_valid_price_row(price):
                    all_prices.append(price)
                else:
                    invalid_rows += 1

            succeeded_symbols += 1
            if len(all_prices) == before_count:
                empty_tickers.append(symbol)

        except Exception as e:
            failures += 1
            failed_tickers.append(symbol)
            logger.debug(f"  {symbol}: {redact_sensitive_text(e)}")

    if failures:
        logger.warning(
            f"  {failures}/{len(symbols)} symbols failed to fetch "
            f"({start_date}..{end_date}); they will be retried on the next run"
        )
        if stats is not None:
            stats["fetch_errors"] = failures

    if stats is not None:
        stats["requested_symbols"] = len(symbols)
        stats["succeeded_symbols"] = succeeded_symbols
        stats["failed_symbols"] = failures
        stats["failed_tickers"] = failed_tickers
        stats["empty_tickers"] = empty_tickers
        stats["provider_price_rows"] = provider_rows
        if invalid_rows:
            stats["invalid_price_rows"] = invalid_rows
    if invalid_rows:
        logger.warning(
            f"  Rejected {invalid_rows}/{provider_rows} malformed provider price rows"
        )

    return all_prices


def _is_valid_price_row(p: dict[str, Any]) -> bool:
    """Reject price rows that would corrupt downstream data.

    Guards the upsert against NULL/non-positive OHLC (after rounding to the
    stored NUMERIC(20,8) precision, so prices that collapse to 0 are excluded
    rather than overwriting a good row with 0), prices too large to fit the
    column, inverted high < low bars, and negative volume. The 8-decimal scale
    preserves sub-penny reverse-split-adjusted prices (down to 1e-8) that
    scale-4 rounding would have dropped.
    """
    return is_valid_daily_ohlcv(p)


def insert_prices(
    conn,
    prices: list[dict[str, Any]],
    logger: logging.Logger,
    *,
    commit: bool = True,
) -> int:
    """Insert prices into database with upsert.

    Invalid rows (NULL/non-positive OHLC, inverted high/low, negative volume)
    are dropped before the upsert so malformed API data cannot overwrite a
    previously-good row. Applies to every caller (daily, split-adjust, forced
    refetch).
    """
    if not prices:
        return 0

    valid = [p for p in prices if _is_valid_price_row(p)]
    skipped = len(prices) - len(valid)
    if skipped:
        logger.warning(
            f"  Skipped {skipped} invalid price rows "
            "(NULL/non-positive OHLC, high<low, or negative volume)"
        )
    prices = valid
    if not prices:
        return 0

    query = sql.SQL("""
        INSERT INTO stock_prices (ticker, date, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, date) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume
    """)

    inserted = 0
    batch_size = 1000

    try:
        with conn.cursor() as cur:
            for i in range(0, len(prices), batch_size):
                batch = prices[i : i + batch_size]
                for p in batch:
                    cur.execute(
                        query,
                        (
                            p["ticker"],
                            p["date"],
                            p["open"],
                            p["high"],
                            p["low"],
                            p["close"],
                            p["volume"],
                        ),
                    )
                    inserted += 1

                if (i + batch_size) % 5000 == 0:
                    logger.info(
                        f"  Inserted {min(i + batch_size, len(prices))}/"
                        f"{len(prices)} records"
                    )
        if commit:
            conn.commit()
    except Exception:
        if commit:
            conn.rollback()
        raise

    return inserted


def refresh_52week_extremes_if_needed(conn, logger: logging.Logger) -> bool:
    """Refresh the 52-week extremes materialized view when it lags prices.

    Args:
        conn: PostgreSQL connection
        logger: Logger instance

    Returns:
        True if the materialized view was refreshed.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.mv_52week_extremes')")
        row = cur.fetchone()
        if not row or row[0] is None:
            logger.info("52-week extremes materialized view not found - skipping refresh")
            return False

        cur.execute("""
            SELECT
                (SELECT MAX(date) FROM stock_prices) AS latest_price_date,
                (SELECT MAX(date) FROM mv_52week_extremes) AS latest_extremes_date
        """)
        latest_price_date, latest_extremes_date = cur.fetchone()

        if latest_price_date is None:
            logger.info("No stock price data found - skipping 52-week extremes refresh")
            return False

        if latest_extremes_date is not None and latest_extremes_date >= latest_price_date:
            logger.info("52-week extremes materialized view is up to date")
            return False

        logger.info(
            "Refreshing 52-week extremes materialized view "
            f"({latest_extremes_date} -> {latest_price_date})..."
        )
        cur.execute("REFRESH MATERIALIZED VIEW mv_52week_extremes")

    conn.commit()
    logger.info("  Refreshed 52-week extremes materialized view")
    return True


def fetch_market_internals(
    fred_client: FredClient,
    start_date: str,
    end_date: str,
    logger: logging.Logger,
) -> FredMarketInternalsResult:
    """Fetch market internals from FRED."""
    logger.info(f"Fetching market internals from FRED ({start_date} to {end_date})...")
    return fred_client.get_market_internals(start_date, end_date)


def merge_cboe_internals(
    fred_rows: list[dict[str, Any]],
    cboe_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Merge same-day CBOE VIX/VIX3M values into FRED market-internals rows.

    FRED stays authoritative: CBOE values only add dates FRED doesn't have
    yet (today's settlement) or fill vix/vix3m holes in existing rows.
    Appended CBOE rows carry no hy_spread; the next FRED run upserts it.
    """
    by_date = {row["date"]: row for row in fred_rows}
    for cboe_row in cboe_rows:
        existing = by_date.get(cboe_row["date"])
        if existing is None:
            row = {"date": cboe_row["date"], "vix": None, "vix3m": None, "hy_spread": None}
            row.update(cboe_row)
            fred_rows.append(row)
            by_date[cboe_row["date"]] = row
        else:
            for field in ("vix", "vix3m"):
                if existing.get(field) in (None, "") and cboe_row.get(field) is not None:
                    existing[field] = cboe_row[field]
    return fred_rows


def _heal_splits_in_window(
    api_key: str,
    database_url: str,
    start_date: date,
    logger: logging.Logger,
    stats: dict[str, Any],
) -> None:
    """Re-base prices + recompute TA for splits that executed since ``start_date``.

    Polygon's adjusted=true endpoint re-bases the FULL price history on a split,
    but the daily fetch only writes new dates — so a split executing Mon-Fri
    leaves the historical series (and all stored technical indicators) split-
    discontinuous until the next weekly run. This detects splits whose
    execution_date falls in the just-fetched window and self-heals them the same
    day: refresh the back-adjusted history, then fully recompute the affected
    tickers' technical_indicators from the adjusted series. Idempotent.
    """
    from sawa.corporate_actions import run_corporate_actions_update

    logger.info("\nChecking for splits in the fetched window (same-day self-heal)...")
    # One global splits call scoped to the window; dividends/earnings skipped.
    ca_stats = run_corporate_actions_update(
        api_key=api_key,
        database_url=database_url,
        start_date=start_date,
        include_splits=True,
        include_dividends=False,
        include_earnings=False,
        logger=logger,
    )
    stats["split_heal"] = {"splits_loaded": ca_stats.get("splits_loaded", 0)}
    if not ca_stats.get("success"):
        raise RuntimeError("corporate-actions split lookup reported an incomplete result")
    split_tickers = ca_stats.get("split_tickers", [])
    if not split_tickers:
        logger.info("  No splits in window - nothing to self-heal")
        return

    from sawa.split_adjust import refresh_split_adjusted_prices
    from sawa.ta_backfill import recompute_ta_for_tickers

    logger.info(f"  Re-adjusting prices for {len(split_tickers)} split ticker(s)...")
    adjust_stats = refresh_split_adjusted_prices(
        api_key=api_key,
        database_url=database_url,
        tickers=split_tickers,
        logger=logger,
    )
    stats["split_heal"]["split_adjust"] = adjust_stats
    if not adjust_stats.get("success"):
        raise RuntimeError(
            "split-adjusted price refresh reported an incomplete result "
            f"({adjust_stats.get('tickers_adjusted', 0)}/"
            f"{adjust_stats.get('tickers_requested', len(split_tickers))} tickers)"
        )

    logger.info(f"  Recomputing TA for {len(split_tickers)} split ticker(s)...")
    ta_stats = recompute_ta_for_tickers(
        database_url=database_url,
        tickers=split_tickers,
        log=logger,
    )
    stats["split_heal"]["ta_recompute"] = ta_stats
    if not ta_stats.get("success"):
        failed = ta_stats.get("tickers_failed", "unknown")
        raise RuntimeError(f"split TA recompute failed for {failed} ticker(s)")


def run_daily(
    api_key: str,
    database_url: str,
    output_dir: Path | None = None,
    force_from_date: date | None = None,
    skip_news: bool = False,
    skip_ta: bool = False,
    skip_prices: bool = False,
    skip_market_internals: bool = False,
    news_only: bool = False,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Run daily price, news, and technical indicator update using REST API.

    Args:
        api_key: Polygon/Massive API key
        database_url: PostgreSQL connection URL
        output_dir: Not used (kept for CLI compatibility)
        force_from_date: Optional date to force update from
        skip_news: Skip news update
        skip_ta: Skip technical indicator calculation
        skip_prices: Skip price update (for --news-only mode)
        news_only: Treat news as the requested primary output and fail on a
            total news provider or persistence failure.
        dry_run: If True, show what would be done without executing
        logger: Logger instance

    Returns:
        Statistics dictionary
    """
    logger = logger or setup_logging()
    if news_only and skip_news:
        raise ValueError("news-only mode cannot also skip news")
    stats: dict[str, Any] = {"success": False}
    symbol_price_start_dates: dict[str, str] | None = None
    ta_recompute_from_by_ticker: dict[str, date] = {}

    logger.info("=" * 60)
    logger.info("DAILY UPDATE - Stock Prices & News (API)")
    logger.info("=" * 60)

    # Initialize client and rate limiter
    client = PolygonClient(api_key, logger)
    rate_limiter = SyncRateLimiter(DEFAULT_API_RATE_LIMIT)

    try:
        # News-only runs must not depend on price watermarks or a populated
        # ticker universe: the news loader performs its own global request.
        symbols: list[str] = []
        last_price_date: date | None = None
        end_date = date.today()
        start_date = force_from_date or end_date
        split_heal_start_date = start_date
        with psycopg.connect(database_url) as conn:
            if not skip_prices:
                # Get last price date only when prices were requested.
                logger.info("Checking last price date...")
                last_price_date = get_last_date(conn, "stock_prices")

                if force_from_date:
                    start_date = force_from_date
                    logger.info(f"  Forcing update from: {start_date}")
                elif last_price_date:
                    logger.info(f"  Last price date: {last_price_date}")
                    latest_count, baseline = _last_date_coverage(conn, last_price_date)
                    required = ceil(baseline * MIN_LATEST_COVERAGE) if baseline else 0
                    # Current tickers overlap recent sessions; stale tickers are
                    # widened independently from their own persisted watermark
                    # after the universe is loaded below.
                    start_date = last_price_date - timedelta(
                        days=PRICE_REPAIR_OVERLAP_DAYS
                    )
                    if baseline and latest_count < required:
                        logger.info(
                            f"  Last date coverage {latest_count}/{baseline} "
                            f"< {MIN_LATEST_COVERAGE:.0%} ({required}); "
                            f"refetching from {start_date}"
                        )
                    else:
                        logger.info(
                            f"  Rechecking {PRICE_REPAIR_OVERLAP_DAYS}-day repair "
                            f"window from: {start_date}"
                        )
                else:
                    logger.error("No existing price data found. Run coldstart first.")
                    return stats

                split_heal_start_date = start_date

                end_date = get_market_date()

                # Skip today if market hasn't closed yet (before 5 PM ET)
                # This prevents incomplete data from overriding intraday stream.
                if not is_after_market_close():
                    end_date = end_date - timedelta(days=1)
                    logger.info("Market not yet closed - fetching through yesterday only")
                    logger.info(f"  End date: {end_date}")
                else:
                    logger.info("Market closed - including today's EOD data")

            # TA still needs the database universe when prices are skipped.
            if not skip_prices or not skip_ta:
                symbols = get_symbols_from_db(conn)
                if not symbols:
                    logger.error("No symbols in database. Run coldstart first.")
                    return stats
                logger.info(f"Found {len(symbols)} symbols in database")
                stats["symbols"] = len(symbols)

            if (
                not skip_prices
                and force_from_date is None
                and last_price_date is not None
            ):
                watermarks = _symbol_price_watermarks(conn, symbols)
                symbol_price_start_dates = _symbol_repair_start_dates(
                    symbols,
                    watermarks,
                    last_price_date,
                )
                oldest_start = min(symbol_price_start_dates.values())
                start_date = date.fromisoformat(oldest_start)
                # Cover every actual per-symbol repair window when looking for
                # splits that require a full-history price/TA rebase.
                split_heal_start_date = start_date
                routine_start = last_price_date - timedelta(
                    days=PRICE_REPAIR_OVERLAP_DAYS
                )
                stale_symbols = sum(
                    date.fromisoformat(value) < routine_start
                    for value in symbol_price_start_dates.values()
                )
                stats["price_repair_from"] = oldest_start
                stats["price_repair_stale_symbols"] = stale_symbols
                if stale_symbols:
                    logger.warning(
                        f"  Extending repair windows for {stale_symbols} ticker(s) "
                        f"with stale per-symbol watermarks; oldest is {oldest_start}"
                    )

            start_str = start_date.strftime(DATE_FORMAT)
            end_str = end_date.strftime(DATE_FORMAT)

        # Decide whether to fetch prices purely from the date window — NOT from a
        # single proxy-ticker probe. fetch_prices_via_api already returns nothing
        # on non-trading days, so an empty/halted AAPL bar must not skip the whole
        # universe's EOD.
        should_fetch_prices = not skip_prices and start_date <= end_date

        if skip_prices:
            logger.info("Skipping prices (--news-only)")
            stats["prices_inserted"] = 0
        elif not should_fetch_prices:
            logger.info("Prices already up to date.")
            stats["prices_inserted"] = 0

        if dry_run:
            logger.info("\n[DRY RUN] Would fetch:")
            if should_fetch_prices:
                logger.info(f"  - Prices for {len(symbols)} symbols")
                logger.info(f"  - Date range: {start_str} to {end_str}")
            else:
                logger.info("  - No price updates needed")
            if not skip_prices:
                logger.info("  - Refresh 52-week extremes materialized view if stale")
            if not skip_news:
                logger.info(f"  - News articles (last {DEFAULT_NEWS_DAYS} days)")
            if not skip_ta:
                logger.info(f"  - Technical indicators for {len(symbols)} symbols")
            stats["success"] = True
            stats["dry_run"] = True
            return stats

        # Fetch and insert prices. Isolated in its own try/except so a Polygon
        # outage on the price path degrades to "prices skipped" and lets the
        # downstream steps (news/TA/market-internals — separate providers) still
        # run, mirroring the per-step isolation already applied below.
        if should_fetch_prices:
            try:
                # The trading-days probe is informational only (a single
                # proxy-ticker call); the fetch below does not depend on it.
                trading_days: list[str] = []
                try:
                    logger.info(f"\nGetting trading days from {start_str} to {end_str}...")
                    trading_days = client.get_trading_days(start_str, end_str)
                    logger.info(f"  Found {len(trading_days)} trading days")
                    stats["trading_days"] = len(trading_days)
                except Exception as e:
                    probe_error = (
                        f"{type(e).__name__}: {redact_sensitive_text(e)}"
                    )
                    stats["trading_days_error"] = probe_error
                    logger.warning(
                        f"  Trading-days probe failed ({probe_error}); "
                        "proceeding with the fetch anyway"
                    )

                logger.info("\nFetching prices via API...")
                prices = fetch_prices_via_api(
                    client,
                    symbols,
                    start_str,
                    end_str,
                    logger,
                    rate_limiter,
                    stats=stats,
                    start_dates=symbol_price_start_dates,
                )
                logger.info(f"  Fetched {len(prices)} price records")
                stats["prices_fetched"] = len(prices)
                fetch_errors = int(stats.get("fetch_errors", 0) or 0)
                if symbols and fetch_errors >= len(symbols):
                    # fetch_prices_via_api isolates per-symbol failures so other
                    # feeds can still run. A total outage is nevertheless a
                    # failed price step, not a successful empty trading day.
                    stats["prices_error"] = (
                        f"all {len(symbols)} symbol price requests failed"
                    )
                elif force_from_date is not None and fetch_errors:
                    # A forced historical repair cannot rely on the ordinary
                    # per-ticker watermarks to revisit an explicitly requested
                    # date that may predate every current watermark.
                    # Fail the command so the same repair window is rerun.
                    stats["prices_error"] = (
                        f"forced historical update failed for {fetch_errors}/"
                        f"{len(symbols)} symbol price requests"
                    )
                elif not prices and int(stats.get("trading_days", 0) or 0) > 0:
                    # On a confirmed trading-day window an all-empty response is
                    # also a failed required price step, even if the provider did
                    # not raise per-symbol exceptions.
                    stats["prices_error"] = (
                        f"no price records returned for {len(symbols)} symbols "
                        f"across {stats['trading_days']} reported trading day(s)"
                    )
                elif not prices and int(stats.get("invalid_price_rows", 0) or 0) > 0:
                    stats["prices_error"] = (
                        "every provider price row was malformed or outside the "
                        "requested window"
                    )
                elif (
                    not prices
                    and force_from_date is None
                    and len(stats.get("empty_tickers", [])) == len(symbols)
                ):
                    # Ordinary runs always request a 14-day repair overlap.
                    # An all-empty universe is therefore not a plausible
                    # one-day holiday, even when the proxy calendar is empty.
                    stats["prices_error"] = (
                        f"all {len(symbols)} symbol price requests returned no rows "
                        "for the ordinary repair window"
                    )
                elif not prices and stats.get("trading_days_error"):
                    # An all-empty universe is ambiguous when the independent
                    # trading-day probe also failed. It may be a holiday, so do
                    # not turn this into a retry-storm-producing fatal error,
                    # but make the uncertainty explicit to operators.
                    stats["prices_degraded"] = (
                        "no price records returned and the trading-day probe failed"
                    )

                logger.info("\nInserting prices into database...")
                committed_inserted = 0
                pending_ta_recompute_from: dict[str, date] = {}
                with psycopg.connect(database_url) as conn:
                    inserted = insert_prices(conn, prices, logger, commit=False)

                    if prices:
                        if inserted != len(prices):
                            stats["prices_error"] = (
                                f"persisted only {inserted}/{len(prices)} valid price rows"
                            )
                        else:
                            # Any upsert in the overlap can fill a gap or revise
                            # an old close. Recompute this ticker's TA from its
                            # earliest actually persisted date, not merely from
                            # the latest previously calculated TA row.
                            for price in prices:
                                ticker = str(price["ticker"])
                                price_date = date.fromisoformat(str(price["date"]))
                                current = pending_ta_recompute_from.get(ticker)
                                if current is None or price_date < current:
                                    pending_ta_recompute_from[ticker] = price_date

                    if trading_days:
                        coverage_date = date.fromisoformat(max(trading_days))
                        persisted_count, baseline = _last_date_coverage(
                            conn, coverage_date
                        )
                        required_coverage = (
                            ceil(baseline * MIN_LATEST_COVERAGE) if baseline else 0
                        )
                        stats["latest_price_coverage"] = {
                            "date": coverage_date.isoformat(),
                            "count": persisted_count,
                            "baseline": baseline,
                            "required": required_coverage,
                        }
                        if baseline and persisted_count < required_coverage:
                            stats["prices_error"] = (
                                f"persisted trading-day coverage {persisted_count}/{baseline} "
                                f"below required {required_coverage} on {coverage_date}"
                            )

                    # If we just inserted today's EOD, cleanup intraday data for
                    # today — but only once today's EOD actually landed with
                    # adequate coverage. A partially-failed EOD fetch must not
                    # wipe the intraday fallback for tickers that got no EOD row.
                    if not stats.get("prices_error") and end_date == get_market_date():
                        latest_count, baseline = _last_date_coverage(conn, end_date)
                        required = ceil(baseline * MIN_LATEST_COVERAGE) if baseline else 0
                        if not fetch_errors and baseline and latest_count >= required:
                            try:
                                from sawa.database.intraday_load import (
                                    cleanup_today_intraday_data,
                                )

                                cleanup_today_intraday_data(
                                    conn,
                                    end_date,
                                    logger,
                                    commit=False,
                                )
                            except ImportError:
                                pass
                        else:
                            logger.warning(
                                f"  Today's EOD coverage {latest_count}/{baseline} below "
                                f"{MIN_LATEST_COVERAGE:.0%}; keeping intraday data as fallback"
                            )

                    if stats.get("prices_error"):
                        # Coverage and exact-write validation are part of the
                        # price transaction. Do not retain an advance that the
                        # job reports as failed and promises to retry.
                        conn.rollback()
                    else:
                        # Cleanup old intraday data (>7 days) in the same
                        # transaction as the EOD rows it accompanies.
                        try:
                            from sawa.database.intraday_load import (
                                cleanup_old_intraday_data,
                            )

                            cleanup_old_intraday_data(
                                conn,
                                7,
                                logger,
                                commit=False,
                            )
                        except ImportError:
                            pass
                        conn.commit()
                        committed_inserted = inserted
                        ta_recompute_from_by_ticker.update(
                            pending_ta_recompute_from
                        )

                logger.info(f"  Inserted {committed_inserted} records")
                stats["prices_inserted"] = committed_inserted
            except Exception as e:
                safe_error = f"{type(e).__name__}: {redact_sensitive_text(e)}"
                logger.warning(f"Price fetch/insert failed: {safe_error}")
                stats["prices_error"] = safe_error
                get_notifier(logger).send(
                    title="Sawa: daily price fetch failed",
                    body=(
                        f"Price fetch/insert failed during daily run.\n"
                        f"{safe_error}\n\n"
                        "Daily continued with news + TA + market internals. Prices "
                        "will be retried on the next run (last_price_date did not "
                        "advance)."
                    ),
                    level=NotificationLevel.WARNING,
                    tags=["warning", "daily", "prices"],
                )

        # Same-day split self-heal: if a split executed in the window we just
        # fetched, re-base the full history and recompute its TA now (instead of
        # waiting for the Saturday weekly run, which would leave the price/TA
        # series split-discontinuous for up to ~4 trading days). Detects splits
        # via one global Polygon /v3/reference/splits call scoped to the window;
        # idempotent (upserts) and isolated so a failure doesn't abort the run.
        if (
            not skip_prices
            and start_date <= end_date
            and not stats.get("prices_error")
            and not stats.get("prices_degraded")
        ):
            try:
                _heal_splits_in_window(
                    api_key,
                    database_url,
                    split_heal_start_date,
                    logger,
                    stats,
                )
            except Exception as e:
                safe_error = f"{type(e).__name__}: {redact_sensitive_text(e)}"
                logger.warning(f"Daily split self-heal failed: {safe_error}")
                stats["split_heal_error"] = safe_error

        if not skip_prices:
            try:
                with psycopg.connect(database_url) as conn:
                    stats["52week_extremes_refreshed"] = refresh_52week_extremes_if_needed(
                        conn, logger
                    )
            except psycopg.Error as e:
                safe_error = f"{type(e).__name__}: {redact_sensitive_text(e)}"
                logger.warning(f"52-week extremes refresh failed: {safe_error}")
                stats["52week_extremes_refresh_error"] = safe_error
                get_notifier(logger).send(
                    title="Sawa: 52-week extremes refresh failed",
                    body=(
                        f"REFRESH MATERIALIZED VIEW mv_52week_extremes failed during daily run.\n"
                        f"{safe_error}\n\n"
                        "Screener results that depend on 52-week highs/lows will be stale "
                        "until the next successful run."
                    ),
                    level=NotificationLevel.WARNING,
                    tags=["warning", "daily", "mv_refresh"],
                )

        # Fetch and load news (always, unless skipped). Non-fatal: an outage on
        # /v2/reference/news must not block downstream steps (TA, market internals).
        if not skip_news:
            logger.info(f"\nFetching news (last {DEFAULT_NEWS_DAYS} days)...")
            try:
                with psycopg.connect(database_url) as conn:
                    news_result = fetch_and_load_news(
                        conn, client, days=DEFAULT_NEWS_DAYS, limit=1000, log=logger
                    )
                stats["news"] = int(news_result)
                if isinstance(news_result, NewsLoadResult):
                    stats["news_requests"] = news_result.summary()
                    if news_result.all_requests_failed:
                        stats["news_error"] = (
                            "every news provider request failed "
                            f"({news_result.failed} request(s))"
                        )
                        logger.warning(stats["news_error"])
                    elif news_result.total_persistence_failure:
                        stats["news_error"] = (
                            "news persistence rejected every fetched article ("
                            f"{news_result.rejected_articles} article(s)"
                            ")"
                        )
                        logger.warning(stats["news_error"])
                    elif news_result.no_articles_fetched:
                        stats["news_error"] = "news provider returned no articles"
                        logger.warning(stats["news_error"])
                    elif news_result.partial_persistence_failure:
                        stats["news_degraded"] = (
                            "news persistence rejected "
                            f"{news_result.rejected_articles} article(s)"
                        )
                        logger.warning(stats["news_degraded"])
                    if news_result.failed and not news_result.all_requests_failed:
                        provider_reason = (
                            "news provider failed for "
                            f"{news_result.failed}/{news_result.requested} request(s)"
                        )
                        prior_reason = stats.get("news_degraded")
                        stats["news_degraded"] = (
                            f"{prior_reason}; {provider_reason}"
                            if prior_reason
                            else provider_reason
                        )
                        logger.warning(provider_reason)
            except (httpx.RequestError, ProviderError, psycopg.Error) as e:
                safe_error = f"{type(e).__name__}: {redact_sensitive_text(e)}"
                logger.warning(f"News fetch failed: {safe_error}")
                stats["news_error"] = safe_error
                get_notifier(logger).send(
                    title="Sawa: news fetch failed",
                    body=(
                        f"fetch_and_load_news failed during daily run.\n"
                        f"{safe_error}\n\n"
                        "Daily continued with TA + market internals. News will "
                        "catch up on the next successful run (last 30 days are "
                        "re-pulled each time)."
                    ),
                    level=NotificationLevel.WARNING,
                    tags=["warning", "daily", "news"],
                )
        else:
            logger.info("\nSkipping news (--skip-news)")

        # Calculate technical indicators (always, unless skipped)
        if not skip_ta:
            logger.info("\nCalculating technical indicators...")
            try:
                from sawa.calculation.ta_engine import (
                    calculate_indicators_for_ticker,
                    get_required_lookback_days,
                )
                from sawa.database.ta_load import (
                    get_cumulative_indicator_seed,
                    get_last_ta_date,
                    get_prices_for_ticker,
                    load_technical_indicators,
                )

                lookback_days = get_required_lookback_days()
                ta_count = 0
                effective_recompute_dates: list[date] = []
                full_history_bootstraps = 0

                with psycopg.connect(database_url) as conn:
                    # Check if technical_indicators table exists
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables
                                WHERE table_name = 'technical_indicators'
                            )
                        """)
                        row = cur.fetchone()
                        table_exists = row[0] if row else False

                    if not table_exists:
                        logger.warning("  Table 'technical_indicators' does not exist")
                        logger.warning("  Run schema migration or coldstart to create it")
                        stats["ta_skipped"] = "table does not exist"
                    else:
                        ta_failed = 0
                        for i, ticker in enumerate(symbols, 1):
                            if i % 100 == 0:
                                logger.info(f"  Progress: {i}/{len(symbols)} tickers")

                            # Isolate each ticker: a bad row or transient error
                            # must not abort TA for the remaining tickers (and
                            # the downstream market-internals step).
                            try:
                                # Get last TA date for this ticker
                                last_ta = get_last_ta_date(conn, ticker)

                                # Start date for price fetch (need warm-up lookback)
                                repair_from = ta_recompute_from_by_ticker.get(ticker)
                                # A ticker with no TA must retain the full-history
                                # bootstrap. If TA itself is stale, include every
                                # missing date as well as any older repaired bar.
                                recompute_from = _effective_ta_recompute_from(
                                    last_ta,
                                    repair_from,
                                )
                                full_history_bootstrap = (
                                    last_ta is None and repair_from is not None
                                )
                                if recompute_from is not None:
                                    price_start = recompute_from - timedelta(days=lookback_days)
                                elif last_ta:
                                    price_start = last_ta - timedelta(days=lookback_days)
                                else:
                                    price_start = None  # Fetch all prices

                                # Fetch prices
                                prices = get_prices_for_ticker(conn, ticker, start_date=price_start)
                                if not prices:
                                    continue

                                # Calculate indicators
                                cumulative_seed = None
                                if last_ta or recompute_from is not None:
                                    cumulative_seed = get_cumulative_indicator_seed(
                                        conn,
                                        ticker,
                                        prices[0]["date"],
                                    )
                                indicators = calculate_indicators_for_ticker(
                                    ticker,
                                    prices,
                                    logger,
                                    cumulative_seed,
                                )
                                if not indicators:
                                    continue

                                # A forced historical price replay overwrites
                                # every affected TA row through the latest date;
                                # the ordinary path remains strictly incremental.
                                indicators = _ta_rows_to_persist(
                                    indicators,
                                    last_ta,
                                    recompute_from,
                                )

                                if indicators:
                                    inserted = load_technical_indicators(conn, indicators, logger)
                                    if inserted != len(indicators):
                                        raise RuntimeError(
                                            "technical indicator persistence was incomplete"
                                        )
                                    ta_count += inserted
                                    if recompute_from is not None:
                                        effective_recompute_dates.append(recompute_from)
                                    elif full_history_bootstrap:
                                        full_history_bootstraps += 1
                            except Exception as e:
                                ta_failed += 1
                                safe_error = (
                                    f"{type(e).__name__}: {redact_sensitive_text(e)}"
                                )
                                logger.warning(
                                    f"  TA failed for {ticker}: {safe_error}"
                                )
                                # Recover the connection in case the txn aborted.
                                conn.rollback()
                                continue

                        stats["ta_calculated"] = ta_count
                        if effective_recompute_dates:
                            stats["ta_recomputed_from"] = min(
                                effective_recompute_dates
                            ).isoformat()
                            stats["ta_recomputed_tickers"] = len(effective_recompute_dates)
                        if full_history_bootstraps:
                            stats["ta_full_history_bootstraps"] = full_history_bootstraps
                        if ta_failed:
                            stats["ta_failed"] = ta_failed
                            logger.warning(f"  TA failed for {ta_failed} tickers")
                        logger.info(f"  Calculated {ta_count} indicator records")

            except ImportError as e:
                logger.warning(
                    "Skipping TA calculation: %s: %s",
                    type(e).__name__,
                    redact_sensitive_text(e),
                )
                logger.warning("  Install ta-lib to enable: pip install TA-Lib")
                stats["ta_skipped"] = "ta-lib not installed"
        else:
            logger.info("\nSkipping technical indicators (--skip-ta)")

        # Fetch and load market internals from FRED
        if not skip_market_internals:
            fred_api_key = os.environ.get("FRED_API_KEY")
            if fred_api_key:
                logger.info("\nFetching market internals from FRED...")
                fred_client = FredClient(fred_api_key, logger)
                try:
                    # Re-pull a fixed overlap to catch any backfill gaps
                    mi_start = (
                        date.today() - timedelta(days=MARKET_INTERNALS_OVERLAP_DAYS)
                    ).strftime(DATE_FORMAT)
                    mi_end = date.today().strftime(DATE_FORMAT)
                    mi_result = fetch_market_internals(fred_client, mi_start, mi_end, logger)
                    mi_rows = mi_result.rows
                    if mi_result.failures:
                        stats["market_internals_failures"] = mi_result.failure_details
                        stats["market_internals_degraded"] = True
                        failed_fields = ", ".join(
                            failure.field for failure in mi_result.failures
                        )
                        if mi_result.all_series_failed:
                            stats["market_internals_error"] = "all FRED series failed"
                            logger.warning("  All FRED market-internals series failed")
                        else:
                            logger.warning(
                                f"  FRED market internals are partial; failed: {failed_fields}"
                            )

                    # FRED publishes VIX/VIX3M T+1; CBOE has today's settlement
                    # (4:15 PM ET) by the time this runs, so today's row lands
                    # same-day instead of tomorrow.
                    logger.info("Fetching same-day VIX/VIX3M from CBOE...")
                    try:
                        with CboeClient(logger) as cboe_client:
                            cboe_result = cboe_client.get_market_internals()
                        if isinstance(cboe_result, CboeMarketInternalsResult):
                            cboe_rows = cboe_result.rows
                            if cboe_result.failures:
                                stats["cboe_market_internals_failures"] = (
                                    cboe_result.failure_details
                                )
                                stats["cboe_market_internals_degraded"] = True
                                failure_summary = ", ".join(
                                    f"{failure.field} ({failure.error_type})"
                                    for failure in cboe_result.failures
                                )
                                if cboe_result.all_quotes_failed:
                                    stats["cboe_market_internals_error"] = (
                                        "all CBOE quotes failed"
                                    )
                                    logger.warning(
                                        "  All CBOE market-internals quotes failed: "
                                        f"{failure_summary}"
                                    )
                                else:
                                    logger.warning(
                                        "  CBOE market internals are partial; failed: "
                                        f"{failure_summary}"
                                    )
                        else:
                            # Compatibility for injected/older clients that
                            # still return the original bare list.
                            cboe_rows = cboe_result
                        mi_rows = merge_cboe_internals(mi_rows, cboe_rows)
                    except Exception as e:
                        safe_error = redact_sensitive_text(e)
                        stats["cboe_market_internals_error"] = (
                            f"{type(e).__name__}: {safe_error}"
                        )
                        stats["cboe_market_internals_degraded"] = True
                        logger.warning(
                            f"  CBOE same-day supplement failed: {safe_error}"
                        )

                    if mi_rows:
                        from sawa.database.load import load_market_internals

                        try:
                            with psycopg.connect(database_url) as conn:
                                loaded = load_market_internals(conn, mi_rows, logger)
                            stats["market_internals"] = loaded
                        except Exception as e:
                            safe_error = redact_sensitive_text(e)
                            stats["market_internals"] = 0
                            stats["market_internals_load_error"] = (
                                f"{type(e).__name__}: {safe_error}"
                            )
                            stats["market_internals_degraded"] = True
                            logger.warning(
                                "  Market internals persistence failed: "
                                f"{safe_error}"
                            )
                    else:
                        stats["market_internals"] = 0
                finally:
                    fred_client.close()
            else:
                alert_missing_api_key(
                    "FRED_API_KEY",
                    "FRED market internals (VIX, VIX3M, HY spread)",
                    logger,
                )
                stats["market_internals_skipped"] = "FRED_API_KEY not set"
        else:
            logger.info("\nSkipping market internals (--skip-market-internals)")

        # The run completed without a fatal exception, but individual steps may
        # have degraded (caught + recorded above). Surface that explicitly so a
        # day where news/TA/internals silently failed is not reported as a clean
        # success — and so the operator/scheduler can react.
        degraded_reasons: list[str] = []
        if stats.get("prices_error"):
            degraded_reasons.append("price fetch failed")
        elif stats.get("prices_degraded"):
            degraded_reasons.append(str(stats["prices_degraded"]))
        elif stats.get("fetch_errors"):
            degraded_reasons.append(
                f"price fetch failed for {stats['fetch_errors']}/{stats.get('symbols', 0)} symbols"
            )
        if stats.get("invalid_price_rows"):
            degraded_reasons.append(
                f"rejected {stats['invalid_price_rows']} malformed provider price rows"
            )
        if stats.get("news_error"):
            degraded_reasons.append("news fetch failed")
        elif stats.get("news_degraded"):
            degraded_reasons.append(str(stats["news_degraded"]))
        if stats.get("split_heal_error"):
            degraded_reasons.append("split adjustment/TA self-heal failed")
        if stats.get("ta_skipped"):
            degraded_reasons.append(f"TA skipped ({stats['ta_skipped']})")
        if stats.get("ta_failed"):
            degraded_reasons.append(f"TA failed for {stats['ta_failed']} tickers")
        if stats.get("52week_extremes_refresh_error"):
            degraded_reasons.append("52-week extremes refresh failed")
        if stats.get("market_internals_skipped"):
            degraded_reasons.append(
                f"market internals skipped ({stats['market_internals_skipped']})"
            )
        elif stats.get("market_internals_load_error"):
            degraded_reasons.append("market internals persistence failed")
        elif stats.get("market_internals_error"):
            degraded_reasons.append("market internals failed (all FRED series)")
        elif stats.get("market_internals_degraded"):
            degraded_reasons.append("market internals partial FRED series failure")
        if stats.get("cboe_market_internals_error"):
            degraded_reasons.append("CBOE market internals supplement failed")
        elif stats.get("cboe_market_internals_degraded"):
            degraded_reasons.append("CBOE market internals partial quote failure")
        stats["degraded"] = bool(degraded_reasons)
        if degraded_reasons:
            stats["degraded_reasons"] = degraded_reasons

        # Degradation stays orthogonal to process success: optional feeds and a
        # small number of isolated ticker failures are visible but do not cause
        # retry storms. A failed required price step is fatal and returns nonzero.
        fatal_reasons: list[str] = []
        if stats.get("prices_error"):
            fatal_reasons.append("required price update failed")
        if news_only and stats.get("news_error"):
            fatal_reasons.append("required news-only update failed")
        stats["fatal_reasons"] = fatal_reasons
        stats["success"] = not fatal_reasons
        logger.info("\n" + "=" * 60)
        logger.info("DAILY UPDATE COMPLETE" + (" (DEGRADED)" if degraded_reasons else ""))
        logger.info("=" * 60)
        logger.info(f"  Price records: {stats.get('prices_inserted', 0)}")
        if not skip_news:
            logger.info(f"  News articles: {stats.get('news', 0)}")
        if not skip_ta and "ta_calculated" in stats:
            logger.info(f"  TA indicators: {stats.get('ta_calculated', 0)}")
        if "market_internals" in stats:
            logger.info(f"  Market internals: {stats['market_internals']}")
        if degraded_reasons:
            logger.warning("  DEGRADED: " + "; ".join(degraded_reasons))
            get_notifier(logger).send(
                title="Sawa: daily completed DEGRADED",
                body=(
                    "Daily finished but these steps did not fully succeed:\n- "
                    + "\n- ".join(degraded_reasons)
                    + "\n\nMCP consumers may be served stale data for the affected "
                    "feeds until the next clean run."
                ),
                level=NotificationLevel.WARNING,
                tags=["warning", "daily", "degraded"],
            )

    except Exception as e:
        safe_error = f"{type(e).__name__}: {redact_sensitive_text(e)}"
        logger.error(f"Daily update failed: {safe_error}")
        stats["error"] = safe_error
        raise

    return stats
