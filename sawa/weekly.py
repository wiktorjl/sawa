"""
Weekly update: Pull economy data, overviews, news, and corporate actions.

Purpose: Update data that changes frequently (economy, news, corporate actions).
Re-entrant: Safe to run multiple times (upsert on primary keys).
"""

import logging
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import psycopg

from sawa.api import FredClient, PolygonClient
from sawa.corporate_actions import run_corporate_actions_update
from sawa.database import get_last_date, get_symbols_from_db
from sawa.database.load import (
    load_companies,
    load_economy,
    load_market_internals,
    load_news,
)
from sawa.repositories.rate_limiter import SyncRateLimiter
from sawa.utils import alert_missing_api_key, get_notifier, setup_logging
from sawa.utils.constants import DEFAULT_API_RATE_LIMIT, DEFAULT_NEWS_DAYS
from sawa.utils.csv_utils import write_csv_auto_fields
from sawa.utils.dates import DATE_FORMAT
from sawa.utils.notify import NotificationLevel

ECONOMY_ENDPOINT_TABLES = {
    "treasury-yields": "treasury_yields",
    "inflation": "inflation",
    "inflation-expectations": "inflation_expectations",
    "labor-market": "labor_market",
}


def download_overviews(
    client: PolygonClient,
    symbols: list[str],
    output_dir: Path,
    logger: logging.Logger,
    rate_limiter: SyncRateLimiter | None = None,
) -> int:
    """Download company overviews."""
    logger.info("Downloading company overviews...")
    output_dir.mkdir(parents=True, exist_ok=True)

    overviews: list[dict[str, Any]] = []
    for i, symbol in enumerate(symbols, 1):
        if i % 50 == 0:
            logger.info(f"  Progress: {i}/{len(symbols)}")
        try:
            if rate_limiter:
                rate_limiter.acquire()
            data = client.get_ticker_details(symbol)
            if data:
                # Flatten nested fields
                flat = {k: v for k, v in data.items() if not isinstance(v, dict)}
                if "address" in data and data["address"]:
                    for k, v in data["address"].items():
                        flat[f"address_{k}"] = v
                if "branding" in data and data["branding"]:
                    for k, v in data["branding"].items():
                        flat[f"branding_{k}"] = v
                overviews.append(flat)
        except Exception as e:
            logger.warning(f"  {symbol}: {e}")

    if overviews:
        filepath = output_dir / "overviews.csv"
        write_csv_auto_fields(filepath, overviews, logger)

    return len(overviews)


def download_economy(
    client: PolygonClient,
    start_date: str,
    end_date: str,
    output_dir: Path,
    logger: logging.Logger,
    start_dates: dict[str, str] | None = None,
) -> dict[str, int]:
    """Download economy data.

    Args:
        client: Polygon/Massive API client
        start_date: Fallback start date for all endpoints
        end_date: Shared end date for all endpoints
        output_dir: Directory to write endpoint CSV files
        logger: Logger instance
        start_dates: Optional per-endpoint start dates. Keys are endpoint names
            such as ``treasury-yields`` and ``labor-market``.

    Returns:
        Dict mapping endpoint names to downloaded row counts.
    """
    stats: dict[str, int] = {}

    output_dir.mkdir(parents=True, exist_ok=True)

    for endpoint in ECONOMY_ENDPOINT_TABLES:
        endpoint_start = start_dates.get(endpoint, start_date) if start_dates else start_date
        logger.info(f"Downloading {endpoint} ({endpoint_start} to {end_date})...")
        try:
            data = client.get_economy_data(endpoint, endpoint_start, end_date)
            if data:
                filepath = output_dir / f"{endpoint.replace('-', '_')}.csv"
                write_csv_auto_fields(filepath, data, logger)
            stats[endpoint] = len(data)
        except Exception as e:
            logger.error(f"  Failed: {e}")
            stats[endpoint] = 0

    return stats


def get_economy_start_dates(conn, end_date: date) -> dict[str, str]:
    """Get per-endpoint start dates for weekly economy updates.

    Each economy table has a different release cadence. Treasury yields are
    daily-ish, while inflation and labor data are monthly, so a shared treasury
    anchor can skip monthly backfills.

    Args:
        conn: Database connection
        end_date: End date for the update window

    Returns:
        Dict mapping Polygon/Massive endpoint names to YYYY-MM-DD start dates.
    """
    default_start = end_date - timedelta(days=365)
    start_dates: dict[str, str] = {}

    for endpoint, table_name in ECONOMY_ENDPOINT_TABLES.items():
        last_date = get_last_date(conn, table_name)
        start = last_date or default_start
        start_dates[endpoint] = start.strftime(DATE_FORMAT)

    return start_dates


def run_weekly(
    api_key: str,
    database_url: str,
    output_dir: Path,
    skip_economy: bool = False,
    skip_overviews: bool = False,
    skip_news: bool = False,
    skip_corporate_actions: bool = False,
    skip_character: bool = False,
    character_workers: int = 4,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Run weekly data update.

    Args:
        api_key: Polygon/Massive API key
        database_url: PostgreSQL connection URL
        output_dir: Directory to save downloaded data
        skip_economy: Skip economy data update
        skip_overviews: Skip company overviews update
        skip_news: Skip news update
        skip_corporate_actions: Skip corporate actions (splits/dividends) update
        skip_character: Skip stock character classification batch
        character_workers: Worker processes for character batch
        dry_run: If True, show what would be done without executing
        logger: Logger instance

    Returns:
        Statistics dictionary
    """
    logger = logger or setup_logging()
    stats: dict[str, Any] = {"success": False}

    logger.info("=" * 60)
    logger.info("WEEKLY UPDATE - Economy & Corporate Actions")
    logger.info("=" * 60)

    # Initialize client and rate limiter
    client = PolygonClient(api_key, logger)
    rate_limiter = SyncRateLimiter(DEFAULT_API_RATE_LIMIT)

    try:
        with psycopg.connect(database_url) as conn:
            # Get symbols
            symbols = get_symbols_from_db(conn)
            if not symbols:
                logger.error("No symbols in database. Run coldstart first.")
                return stats
            logger.info(f"Found {len(symbols)} symbols in database")
            stats["symbols"] = len(symbols)

            end_date = date.today()
            end_str = end_date.strftime(DATE_FORMAT)
            economy_start_dates = get_economy_start_dates(conn, end_date)

            market_internals_last_date = get_last_date(conn, "market_internals")
            market_internals_start = market_internals_last_date or (end_date - timedelta(days=365))
            market_internals_start_str = market_internals_start.strftime(DATE_FORMAT)
            fred_api_key = os.environ.get("FRED_API_KEY")

            logger.info("Economy date ranges:")
            for endpoint, start_str in economy_start_dates.items():
                logger.info(f"  {endpoint}: {start_str} to {end_str}")
            logger.info(f"  market-internals: {market_internals_start_str} to {end_str}")
            stats["economy_date_ranges"] = economy_start_dates
            stats["market_internals_start_date"] = market_internals_start_str

        if dry_run:
            logger.info("\n[DRY RUN] Would update:")
            if not skip_overviews:
                logger.info(f"  - Company overviews for {len(symbols)} symbols")
            if not skip_economy:
                logger.info("  - Economy data:")
                for endpoint, start_str in economy_start_dates.items():
                    logger.info(f"    - {endpoint} from {start_str}")
            if not skip_news:
                logger.info(f"  - News articles (last {DEFAULT_NEWS_DAYS} days)")
            if not skip_corporate_actions:
                logger.info("  - Corporate actions (splits, dividends)")
            if not skip_character:
                logger.info(f"  - Stock character batch ({character_workers} workers)")
            if fred_api_key:
                logger.info(f"  - Market internals from {market_internals_start_str}")
            stats["success"] = True
            stats["dry_run"] = True
            return stats

        step = 1
        total_steps = 5 - sum(
            [
                skip_overviews,
                skip_economy,
                skip_news,
                skip_corporate_actions,
                skip_character,
            ]
        )

        # Each independent step is wrapped so one raising does not abort the
        # rest (the steps take database_url/symbols and don't depend on each
        # other). Failures are recorded into stats and the overall run fails at
        # the end if any required step failed — mirroring the market-internals
        # step that was already guarded.
        step_errors: dict[str, str] = {}

        def _record_step_failure(name: str, exc: Exception, impact: str) -> None:
            logger.error(f"Weekly step '{name}' failed: {type(exc).__name__}: {exc}")
            step_errors[name] = f"{type(exc).__name__}: {exc}"
            stats[f"{name}_error"] = step_errors[name]
            get_notifier(logger).send(
                title=f"Sawa: weekly {name} step failed",
                body=(
                    f"The '{name}' step failed during the weekly run.\n"
                    f"{type(exc).__name__}: {exc}\n\n"
                    f"{impact} Remaining weekly steps still ran."
                ),
                level=NotificationLevel.WARNING,
                tags=["warning", "weekly", name],
            )

        # Step: Update company overviews
        if not skip_overviews:
            logger.info(f"\n[{step}/{total_steps}] Updating company overviews...")
            step += 1
            try:
                overview_count = download_overviews(
                    client, symbols, output_dir / "overviews", logger, rate_limiter
                )
                stats["overviews"] = overview_count
                # Load into database
                with psycopg.connect(database_url) as conn:
                    load_companies(conn, output_dir / "overviews" / "overviews.csv", logger)
            except Exception as e:
                _record_step_failure(
                    "overviews", e, "Company metadata will be stale until the next run."
                )

        # Step: Update economy data
        if not skip_economy:
            logger.info(f"\n[{step}/{total_steps}] Updating economy data...")
            step += 1
            try:
                econ_stats = download_economy(
                    client,
                    min(economy_start_dates.values()),
                    end_str,
                    output_dir / "economy",
                    logger,
                    start_dates=economy_start_dates,
                )
                stats["economy"] = econ_stats
                # Load into database
                with psycopg.connect(database_url) as conn:
                    load_economy(conn, output_dir / "economy", logger)
            except Exception as e:
                _record_step_failure(
                    "economy", e, "Treasury/inflation/labor data will be stale until the next run."
                )

        # Step: Update market internals from FRED
        if fred_api_key:
            logger.info(f"\n[{step}/{total_steps}] Updating market internals from FRED...")
            fred_client = FredClient(fred_api_key, logger)
            try:
                mi_rows = fred_client.get_market_internals(market_internals_start_str, end_str)
                if mi_rows:
                    with psycopg.connect(database_url) as conn:
                        loaded = load_market_internals(conn, mi_rows, logger)
                    stats["market_internals"] = loaded
                else:
                    stats["market_internals"] = 0
            except Exception as e:
                logger.warning(f"Market internals update failed: {e}")
                stats["market_internals"] = 0
                stats["market_internals_error"] = str(e)
                get_notifier(logger).send(
                    title="Sawa: market internals update failed",
                    body=(
                        f"FRED market internals fetch/load failed during weekly run.\n"
                        f"{type(e).__name__}: {e}\n\n"
                        "VIX/VIX3M/HY spread will be stale until the next successful run."
                    ),
                    level=NotificationLevel.WARNING,
                    tags=["warning", "weekly", "market_internals"],
                )
            finally:
                fred_client.close()
        else:
            alert_missing_api_key(
                "FRED_API_KEY",
                "FRED market internals (VIX, VIX3M, HY spread)",
                logger,
            )

        # Step: Update news
        if not skip_news:
            logger.info(f"\n[{step}/{total_steps}] Updating news articles...")
            step += 1
            try:
                with psycopg.connect(database_url) as conn:
                    news_count = load_news(
                        conn, client, symbols, days=DEFAULT_NEWS_DAYS, log=logger
                    )
                stats["news"] = news_count
            except Exception as e:
                _record_step_failure(
                    "news", e, "News articles will catch up on the next run (30-day re-pull)."
                )

        # Step: Update corporate actions (splits, dividends)
        if not skip_corporate_actions:
            logger.info(f"\n[{step}/{total_steps}] Updating corporate actions...")
            step += 1
            try:
                ca_stats = run_corporate_actions_update(
                    api_key=api_key,
                    database_url=database_url,
                    dry_run=False,
                    logger=logger,
                )
                stats["corporate_actions"] = ca_stats

                # If splits were loaded, re-fetch adjusted prices for affected
                # tickers, then fully recompute their technical indicators so
                # stored SMA/EMA/RSI track the back-adjusted prices instead of
                # staying off by the split ratio.
                split_tickers = ca_stats.get("split_tickers", [])
                if split_tickers:
                    from sawa.split_adjust import refresh_split_adjusted_prices

                    logger.info(
                        f"\nAdjusting prices for {len(split_tickers)} split ticker(s)..."
                    )
                    adjust_stats = refresh_split_adjusted_prices(
                        api_key=api_key,
                        database_url=database_url,
                        tickers=split_tickers,
                        logger=logger,
                    )
                    stats["split_adjust"] = adjust_stats

                    from sawa.ta_backfill import recompute_ta_for_tickers

                    logger.info(
                        f"\nRecomputing technical indicators for "
                        f"{len(split_tickers)} split ticker(s)..."
                    )
                    stats["split_ta_recompute"] = recompute_ta_for_tickers(
                        database_url=database_url,
                        tickers=split_tickers,
                        log=logger,
                    )
            except Exception as e:
                _record_step_failure(
                    "corporate_actions",
                    e,
                    "Splits/dividends and split price/TA adjustment did not update this run.",
                )

        # Step: Stock character classification
        if not skip_character:
            logger.info(f"\n[{step}/{total_steps}] Running stock character classification...")
            step += 1
            try:
                from sawa.stock_character_batch import run_stock_character_batch

                character_stats = run_stock_character_batch(
                    database_url=database_url,
                    workers=character_workers,
                    log=logger,
                )
                stats["character"] = character_stats
            except Exception as e:
                _record_step_failure(
                    "character",
                    e,
                    "Stock character classification will be stale until the next run.",
                )

        # Maintenance: refresh the MCP execute_query insights cache. Nothing else
        # regenerates it, so the "agents reaching for raw SQL where a tool exists"
        # signal would otherwise go stale. Non-fatal.
        try:
            from sawa.mcp_query_insights import analyze_query_log

            insights = analyze_query_log()
            summary = insights.get("summary", {})
            stats["mcp_query_insights"] = {
                "total": summary.get("total_queries"),
                "recent": summary.get("recent_queries"),
            }
            logger.info(
                f"\nRefreshed MCP query insights: {summary.get('total_queries', 0)} "
                f"custom queries total, {summary.get('recent_queries', 0)} in the "
                f"last {summary.get('window_days', 7)} days"
            )
            if summary.get("warning"):
                logger.warning(f"  {summary['warning']}")
        except Exception as e:
            logger.warning(f"MCP query insights refresh failed: {e}")
            stats["mcp_query_insights_error"] = str(e)

        # The run reaches here without a fatal exception, but individual steps
        # may have failed (caught + recorded above). Fail the overall run if any
        # did, so the scheduler withholds the weekly_done flag and retries — but
        # only after every independent step had its chance to run.
        if step_errors:
            stats["success"] = False
            stats["step_errors"] = step_errors
        else:
            stats["success"] = True
        logger.info("\n" + "=" * 60)
        logger.info(
            "WEEKLY UPDATE COMPLETE" + (" (DEGRADED)" if step_errors else "")
        )
        logger.info("=" * 60)

        if step_errors:
            logger.warning("  Failed steps: " + ", ".join(sorted(step_errors)))
        if "overviews" in stats:
            logger.info(f"  Overviews: {stats['overviews']}")
        if "economy" in stats:
            logger.info(f"  Economy: {sum(stats['economy'].values())} records")
        if "news" in stats:
            logger.info(f"  News: {stats['news']} articles")
        if "corporate_actions" in stats:
            ca = stats["corporate_actions"]
            logger.info(
                f"  Corporate actions: {ca.get('splits_loaded', 0)} splits, "
                f"{ca.get('dividends_loaded', 0)} dividends"
            )
        if "character" in stats:
            ch = stats["character"]
            logger.info(
                f"  Character: {ch.get('classified', 0)}/{ch.get('total', 0)} classified "
                f"({ch.get('errors', 0)} errors)"
            )

    except Exception as e:
        logger.error(f"Weekly update failed: {e}")
        stats["error"] = str(e)
        raise

    return stats
