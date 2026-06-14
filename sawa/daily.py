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

from sawa.api import CboeClient, FredClient, PolygonClient
from sawa.database import get_last_date, get_symbols_from_db
from sawa.database.news import fetch_and_load_news
from sawa.domain.exceptions import ProviderError
from sawa.repositories.rate_limiter import SyncRateLimiter
from sawa.utils import alert_missing_api_key, get_notifier, setup_logging
from sawa.utils.constants import DEFAULT_API_RATE_LIMIT, DEFAULT_NEWS_DAYS
from sawa.utils.dates import DATE_FORMAT, timestamp_to_date
from sawa.utils.market_hours import get_market_date, is_after_market_close
from sawa.utils.notify import NotificationLevel

# Must match doctor's stock_prices.latest_coverage threshold so the daily backfill
# doesn't leave a date that the post-run doctor check will then flag.
MIN_LATEST_COVERAGE = 0.85


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


def fetch_prices_via_api(
    client: PolygonClient,
    symbols: list[str],
    start_date: str,
    end_date: str,
    logger: logging.Logger,
    rate_limiter: SyncRateLimiter | None = None,
    stats: dict[str, Any] | None = None,
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

    for i, symbol in enumerate(symbols, 1):
        if i % 50 == 0:
            logger.info(f"  Progress: {i}/{len(symbols)} symbols")

        try:
            if rate_limiter:
                rate_limiter.acquire()

            data = client.get(
                "aggregates",
                path_params={"ticker": symbol, "start": start_date, "end": end_date},
                params={"adjusted": "true", "limit": 50000},
            )

            results = data.get("results", [])
            for r in results:
                if r.get("t"):
                    price_date = timestamp_to_date(r["t"])
                    all_prices.append(
                        {
                            "ticker": symbol,
                            "date": price_date.strftime(DATE_FORMAT),
                            "open": r.get("o"),
                            "high": r.get("h"),
                            "low": r.get("l"),
                            "close": r.get("c"),
                            "volume": r.get("v"),
                        }
                    )

        except Exception as e:
            failures += 1
            logger.debug(f"  {symbol}: {e}")

    if failures:
        logger.warning(
            f"  {failures}/{len(symbols)} symbols failed to fetch "
            f"({start_date}..{end_date}); they will be retried on the next run"
        )
        if stats is not None:
            stats["fetch_errors"] = failures

    return all_prices


# stock_prices OHLC is NUMERIC(20, 8): the absolute value must round to less
# than 10^12. A reverse-split back-adjustment on a deeply diluted microcap can
# produce absurd (>$1e12) ancient bars; those rows are skipped (with a warning)
# rather than aborting the whole batch insert — and such prices are unusable
# junk anyway (they would dominate every chart and indicator).
_MAX_STORABLE_PRICE = 1e12


def _is_valid_price_row(p: dict[str, Any]) -> bool:
    """Reject price rows that would corrupt downstream data.

    Guards the upsert against NULL/non-positive OHLC (after rounding to the
    stored NUMERIC(20,8) precision, so prices that collapse to 0 are excluded
    rather than overwriting a good row with 0), prices too large to fit the
    column, inverted high < low bars, and negative volume. The 8-decimal scale
    preserves sub-penny reverse-split-adjusted prices (down to 1e-8) that
    scale-4 rounding would have dropped.
    """
    o, h, low_v, c, v = p.get("open"), p.get("high"), p.get("low"), p.get("close"), p.get("volume")
    if None in (o, h, low_v, c, v):
        return False
    try:
        o, h, low_v, c, v = float(o), float(h), float(low_v), float(c), float(v)
    except (TypeError, ValueError):
        return False
    if any(round(x, 8) <= 0 for x in (o, h, low_v, c)):
        return False
    if any(x >= _MAX_STORABLE_PRICE for x in (o, h, low_v, c)):
        return False
    if v < 0 or h < low_v:
        return False
    return True


def insert_prices(
    conn,
    prices: list[dict[str, Any]],
    logger: logging.Logger,
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
            conn.commit()

            if (i + batch_size) % 5000 == 0:
                logger.info(f"  Inserted {min(i + batch_size, len(prices))}/{len(prices)} records")

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
) -> list[dict[str, Any]]:
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
    split_tickers = ca_stats.get("split_tickers", [])
    stats["split_heal"] = {"splits_loaded": ca_stats.get("splits_loaded", 0)}
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

    logger.info(f"  Recomputing TA for {len(split_tickers)} split ticker(s)...")
    stats["split_heal"]["ta_recompute"] = recompute_ta_for_tickers(
        database_url=database_url,
        tickers=split_tickers,
        log=logger,
    )


def run_daily(
    api_key: str,
    database_url: str,
    output_dir: Path | None = None,
    force_from_date: date | None = None,
    skip_news: bool = False,
    skip_ta: bool = False,
    skip_prices: bool = False,
    skip_market_internals: bool = False,
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
        dry_run: If True, show what would be done without executing
        logger: Logger instance

    Returns:
        Statistics dictionary
    """
    logger = logger or setup_logging()
    stats: dict[str, Any] = {"success": False}

    logger.info("=" * 60)
    logger.info("DAILY UPDATE - Stock Prices & News (API)")
    logger.info("=" * 60)

    # Initialize client and rate limiter
    client = PolygonClient(api_key, logger)
    rate_limiter = SyncRateLimiter(DEFAULT_API_RATE_LIMIT)

    try:
        with psycopg.connect(database_url) as conn:
            # Get last price date
            logger.info("Checking last price date...")
            last_price_date = get_last_date(conn, "stock_prices")

            if force_from_date:
                start_date = force_from_date
                logger.info(f"  Forcing update from: {start_date}")
            elif last_price_date:
                logger.info(f"  Last price date: {last_price_date}")
                latest_count, baseline = _last_date_coverage(conn, last_price_date)
                required = ceil(baseline * MIN_LATEST_COVERAGE) if baseline else 0
                if baseline and latest_count < required:
                    # Last date was only partially populated (e.g. add-symbol wrote a
                    # subset before the daily ran). Refetch it instead of skipping past.
                    start_date = last_price_date
                    logger.info(
                        f"  Last date coverage {latest_count}/{baseline} "
                        f"< {MIN_LATEST_COVERAGE:.0%} ({required}); "
                        f"refetching from {start_date}"
                    )
                else:
                    start_date = last_price_date + timedelta(days=1)
                    logger.info(f"  Starting from: {start_date}")
            else:
                logger.error("No existing price data found. Run coldstart first.")
                return stats

            end_date = get_market_date()

            # Skip today if market hasn't closed yet (before 5 PM ET)
            # This prevents incomplete data from overriding intraday stream
            if not is_after_market_close():
                end_date = end_date - timedelta(days=1)
                logger.info("Market not yet closed - fetching through yesterday only")
                logger.info(f"  End date: {end_date}")
            else:
                logger.info("Market closed - including today's EOD data")

            start_str = start_date.strftime(DATE_FORMAT)
            end_str = end_date.strftime(DATE_FORMAT)

            # Get symbols
            symbols = get_symbols_from_db(conn)
            if not symbols:
                logger.error("No symbols in database. Run coldstart first.")
                return stats
            logger.info(f"Found {len(symbols)} symbols in database")
            stats["symbols"] = len(symbols)

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
                try:
                    logger.info(f"\nGetting trading days from {start_str} to {end_str}...")
                    trading_days = client.get_trading_days(start_str, end_str)
                    logger.info(f"  Found {len(trading_days)} trading days")
                    stats["trading_days"] = len(trading_days)
                except Exception as e:
                    logger.warning(
                        f"  Trading-days probe failed ({type(e).__name__}: {e}); "
                        "proceeding with the fetch anyway"
                    )

                logger.info("\nFetching prices via API...")
                prices = fetch_prices_via_api(
                    client, symbols, start_str, end_str, logger, rate_limiter, stats=stats
                )
                logger.info(f"  Fetched {len(prices)} price records")
                stats["prices_fetched"] = len(prices)

                logger.info("\nInserting prices into database...")
                with psycopg.connect(database_url) as conn:
                    inserted = insert_prices(conn, prices, logger)

                    # If we just inserted today's EOD, cleanup intraday data for
                    # today — but only once today's EOD actually landed with
                    # adequate coverage. A partially-failed EOD fetch must not
                    # wipe the intraday fallback for tickers that got no EOD row.
                    if end_date == get_market_date():
                        latest_count, baseline = _last_date_coverage(conn, end_date)
                        required = ceil(baseline * MIN_LATEST_COVERAGE) if baseline else 0
                        if baseline and latest_count >= required:
                            try:
                                from sawa.database.intraday_load import (
                                    cleanup_today_intraday_data,
                                )

                                cleanup_today_intraday_data(conn, logger)
                            except ImportError:
                                pass
                        else:
                            logger.warning(
                                f"  Today's EOD coverage {latest_count}/{baseline} below "
                                f"{MIN_LATEST_COVERAGE:.0%}; keeping intraday data as fallback"
                            )

                    # Cleanup old intraday data (>7 days)
                    try:
                        from sawa.database.intraday_load import cleanup_old_intraday_data

                        cleanup_old_intraday_data(conn, 7, logger)
                    except ImportError:
                        pass

                logger.info(f"  Inserted {inserted} records")
                stats["prices_inserted"] = inserted
            except (httpx.RequestError, ProviderError, psycopg.Error) as e:
                logger.warning(f"Price fetch/insert failed: {type(e).__name__}: {e}")
                stats["prices_error"] = f"{type(e).__name__}: {e}"
                get_notifier(logger).send(
                    title="Sawa: daily price fetch failed",
                    body=(
                        f"Price fetch/insert failed during daily run.\n"
                        f"{type(e).__name__}: {e}\n\n"
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
        if not skip_prices and start_date <= end_date and not stats.get("prices_error"):
            try:
                _heal_splits_in_window(api_key, database_url, start_date, logger, stats)
            except Exception as e:
                logger.warning(f"Daily split self-heal failed: {type(e).__name__}: {e}")
                stats["split_heal_error"] = f"{type(e).__name__}: {e}"

        if not skip_prices:
            try:
                with psycopg.connect(database_url) as conn:
                    stats["52week_extremes_refreshed"] = refresh_52week_extremes_if_needed(
                        conn, logger
                    )
            except psycopg.Error as e:
                logger.warning(f"52-week extremes refresh failed: {e}")
                stats["52week_extremes_refresh_error"] = str(e)
                get_notifier(logger).send(
                    title="Sawa: 52-week extremes refresh failed",
                    body=(
                        f"REFRESH MATERIALIZED VIEW mv_52week_extremes failed during daily run.\n"
                        f"{type(e).__name__}: {e}\n\n"
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
                    news_count = fetch_and_load_news(
                        conn, client, days=DEFAULT_NEWS_DAYS, limit=1000, log=logger
                    )
                stats["news"] = news_count
            except (httpx.RequestError, ProviderError, psycopg.Error) as e:
                logger.warning(f"News fetch failed: {type(e).__name__}: {e}")
                stats["news_error"] = f"{type(e).__name__}: {e}"
                get_notifier(logger).send(
                    title="Sawa: news fetch failed",
                    body=(
                        f"fetch_and_load_news failed during daily run.\n"
                        f"{type(e).__name__}: {e}\n\n"
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
                    get_last_ta_date,
                    get_prices_for_ticker,
                    load_technical_indicators,
                )

                lookback_days = get_required_lookback_days()
                ta_count = 0

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
                                if last_ta:
                                    price_start = last_ta - timedelta(days=lookback_days)
                                else:
                                    price_start = None  # Fetch all prices

                                # Fetch prices
                                prices = get_prices_for_ticker(conn, ticker, start_date=price_start)
                                if not prices:
                                    continue

                                # Calculate indicators
                                indicators = calculate_indicators_for_ticker(ticker, prices, logger)
                                if not indicators:
                                    continue

                                # Filter to only new dates (after last_ta)
                                if last_ta:
                                    indicators = [ind for ind in indicators if ind.date > last_ta]

                                if indicators:
                                    inserted = load_technical_indicators(conn, indicators, logger)
                                    ta_count += inserted
                            except Exception as e:
                                ta_failed += 1
                                logger.warning(
                                    f"  TA failed for {ticker}: {type(e).__name__}: {e}"
                                )
                                # Recover the connection in case the txn aborted.
                                conn.rollback()
                                continue

                        stats["ta_calculated"] = ta_count
                        if ta_failed:
                            stats["ta_failed"] = ta_failed
                            logger.warning(f"  TA failed for {ta_failed} tickers")
                        logger.info(f"  Calculated {ta_count} indicator records")

            except ImportError as e:
                logger.warning(f"Skipping TA calculation: {e}")
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
                    # Fetch last 30 days to catch any backfill gaps
                    mi_start = (date.today() - timedelta(days=30)).strftime(DATE_FORMAT)
                    mi_end = date.today().strftime(DATE_FORMAT)
                    mi_rows = fetch_market_internals(fred_client, mi_start, mi_end, logger)

                    # FRED publishes VIX/VIX3M T+1; CBOE has today's settlement
                    # (4:15 PM ET) by the time this runs, so today's row lands
                    # same-day instead of tomorrow.
                    logger.info("Fetching same-day VIX/VIX3M from CBOE...")
                    try:
                        with CboeClient(logger) as cboe_client:
                            cboe_rows = cboe_client.get_market_internals()
                        mi_rows = merge_cboe_internals(mi_rows, cboe_rows)
                    except Exception as e:
                        logger.warning(f"  CBOE same-day supplement failed: {e}")

                    if mi_rows:
                        from sawa.database.load import load_market_internals

                        with psycopg.connect(database_url) as conn:
                            loaded = load_market_internals(conn, mi_rows, logger)
                        stats["market_internals"] = loaded
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
        if stats.get("news_error"):
            degraded_reasons.append("news fetch failed")
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
        stats["degraded"] = bool(degraded_reasons)
        if degraded_reasons:
            stats["degraded_reasons"] = degraded_reasons

        stats["success"] = True
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
        logger.error(f"Daily update failed: {e}")
        stats["error"] = str(e)
        raise

    return stats
