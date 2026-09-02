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
    PersistenceResult,
    load_companies,
    load_economy,
    load_market_internals,
    load_news,
    require_complete_persistence,
)
from sawa.database.news import NewsLoadResult
from sawa.domain.exceptions import ProviderError
from sawa.provider_downloads import DownloadCount, DownloadStats, bind_provider_record
from sawa.repositories.rate_limiter import SyncRateLimiter
from sawa.utils import alert_missing_api_key, get_notifier, setup_logging
from sawa.utils.constants import (
    DEFAULT_API_RATE_LIMIT,
    DEFAULT_NEWS_DAYS,
    MARKET_INTERNALS_OVERLAP_DAYS,
)
from sawa.utils.csv_utils import write_csv_auto_fields
from sawa.utils.dates import DATE_FORMAT
from sawa.utils.notify import NotificationLevel
from sawa.utils.security import redact_sensitive_text

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
) -> DownloadCount:
    """Download company overviews."""
    logger.info("Downloading company overviews...")
    output_dir.mkdir(parents=True, exist_ok=True)

    overviews: list[dict[str, Any]] = []
    succeeded = 0
    failed = 0
    for i, symbol in enumerate(symbols, 1):
        if i % 50 == 0:
            logger.info(f"  Progress: {i}/{len(symbols)}")
        try:
            if rate_limiter:
                rate_limiter.acquire()
            data = client.get_ticker_details(symbol)
            # None is the provider's documented "no details for this ticker"
            # answer (get_ticker_details returns dict | None) and is ordinary
            # for many ETFs and delisted symbols. Only a genuinely wrong type
            # is a provider error; treating None as one made a normal weekly
            # run report ~300 failures and go DEGRADED.
            if data is not None and not isinstance(data, dict):
                raise ProviderError(
                    "Provider returned a non-object company overview",
                    provider="polygon",
                )
            if data:
                data = bind_provider_record(data, symbol, output_field="ticker")
                # Flatten nested fields
                flat = {k: v for k, v in data.items() if not isinstance(v, dict)}
                if "address" in data and data["address"]:
                    for k, v in data["address"].items():
                        flat[f"address_{k}"] = v
                if "branding" in data and data["branding"]:
                    for k, v in data["branding"].items():
                        flat[f"branding_{k}"] = v
                overviews.append(flat)
            succeeded += 1
        except Exception as e:
            failed += 1
            logger.warning(f"  {symbol}: {redact_sensitive_text(e)}")

    artifact_written = False
    if overviews:
        filepath = output_dir / "overviews.csv"
        write_csv_auto_fields(filepath, overviews, logger)
        artifact_written = True

    return DownloadCount(
        len(overviews),
        requested=len(symbols),
        succeeded=succeeded,
        failed=failed,
        artifact_written=artifact_written,
    )


def download_economy(
    client: PolygonClient,
    start_date: str,
    end_date: str,
    output_dir: Path,
    logger: logging.Logger,
    start_dates: dict[str, str] | None = None,
) -> DownloadStats:
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
    stats = DownloadStats()

    output_dir.mkdir(parents=True, exist_ok=True)

    for endpoint in ECONOMY_ENDPOINT_TABLES:
        endpoint_start = start_dates.get(endpoint, start_date) if start_dates else start_date
        logger.info(f"Downloading {endpoint} ({endpoint_start} to {end_date})...")
        try:
            data = client.get_economy_data(endpoint, endpoint_start, end_date)
            if not isinstance(data, list):
                raise ProviderError(
                    "Provider returned a non-list economy response",
                    provider="polygon",
                )
            artifact: str | None = None
            if data:
                filepath = output_dir / f"{endpoint.replace('-', '_')}.csv"
                write_csv_auto_fields(filepath, data, logger)
                artifact = ECONOMY_ENDPOINT_TABLES[endpoint]
            stats.record(
                endpoint,
                len(data),
                requested=1,
                succeeded=1,
                failed=0,
                artifact=artifact,
            )
        except Exception as e:
            logger.error(f"  Failed: {redact_sensitive_text(e)}")
            stats.record(
                endpoint,
                0,
                requested=1,
                succeeded=0,
                failed=1,
            )

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
            # Starting exactly at the last stored date collapses to a zero-width
            # window as soon as the daily run has caught up. FRED has not
            # published today's value yet, returns nothing, and the step reports
            # the pipeline as failed precisely when it is most up to date. Keep
            # the same overlap the daily run re-pulls.
            market_internals_start = min(
                market_internals_last_date or (end_date - timedelta(days=365)),
                end_date - timedelta(days=MARKET_INTERNALS_OVERLAP_DAYS),
            )
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
        # other). Required-step failures make the overall run fail; optional
        # provider degradation is reported separately without retry storms.
        step_errors: dict[str, str] = {}
        provider_degraded_reasons: list[str] = []

        def _record_step_failure(name: str, exc: Exception, impact: str) -> None:
            safe_error = f"{type(exc).__name__}: {redact_sensitive_text(exc)}"
            logger.error(f"Weekly step '{name}' failed: {safe_error}")
            step_errors[name] = safe_error
            stats[f"{name}_error"] = safe_error
            get_notifier(logger).send(
                title=f"Sawa: weekly {name} step failed",
                body=(
                    f"The '{name}' step failed during the weekly run.\n"
                    f"{safe_error}\n\n"
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
                if isinstance(overview_count, DownloadCount):
                    stats["overviews_requests"] = overview_count.summary()
                    if overview_count.all_failed:
                        raise ProviderError(
                            "All company overview provider requests failed",
                            provider="polygon",
                        )
                    if overview_count.empty_successful:
                        raise ProviderError(
                            "Company overview provider returned no rows",
                            provider="polygon",
                        )
                    if overview_count.failed:
                        provider_degraded_reasons.append(
                            "company overview provider requests partially failed"
                        )
                    artifact_written = overview_count.artifact_written
                else:
                    # Compatibility for integrations that still wrap this
                    # helper and return a plain count.
                    artifact_written = int(overview_count) > 0
                if artifact_written:
                    with psycopg.connect(database_url) as conn:
                        loaded_overviews = load_companies(
                            conn,
                            output_dir / "overviews" / "overviews.csv",
                            logger,
                        )
                    if isinstance(loaded_overviews, PersistenceResult):
                        require_complete_persistence(
                            loaded_overviews,
                            expected_rows=int(overview_count),
                        )
                        stats["overviews_loaded"] = int(loaded_overviews)
                        stats["overviews_persistence"] = loaded_overviews.summary()
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
                if isinstance(econ_stats, DownloadStats):
                    stats["economy_requests"] = econ_stats.requests
                    failed_feeds = econ_stats.failed_feeds
                    if econ_stats.has_failures and not failed_feeds:
                        provider_degraded_reasons.append(
                            "economy provider requests partially failed"
                        )
                    if econ_stats.empty_feeds:
                        provider_degraded_reasons.append(
                            "economy feeds returned no fresh rows: "
                            + ", ".join(sorted(econ_stats.empty_feeds))
                        )
                    fresh_tables = econ_stats.artifacts
                else:
                    fresh_tables = {
                        ECONOMY_ENDPOINT_TABLES[endpoint]
                        for endpoint, rows in econ_stats.items()
                        if rows > 0 and endpoint in ECONOMY_ENDPOINT_TABLES
                    }
                if fresh_tables:
                    with psycopg.connect(database_url) as conn:
                        loaded_economy = load_economy(
                            conn,
                            output_dir / "economy",
                            logger,
                            only_tables=fresh_tables,
                        )
                    if isinstance(loaded_economy, dict):
                        stats["economy_loaded"] = {
                            table: int(result)
                            for table, result in loaded_economy.items()
                        }
                        stats["economy_persistence"] = {
                            table: result.summary()
                            for table, result in loaded_economy.items()
                            if isinstance(result, PersistenceResult)
                        }
                        expected_by_table = {
                            ECONOMY_ENDPOINT_TABLES[endpoint]: int(rows)
                            for endpoint, rows in econ_stats.items()
                            if endpoint in ECONOMY_ENDPOINT_TABLES
                            and ECONOMY_ENDPOINT_TABLES[endpoint] in fresh_tables
                        }
                        for table in fresh_tables:
                            result = loaded_economy.get(table)
                            if result is None:
                                raise RuntimeError(
                                    f"Fresh {table} artifact was not loaded"
                                )
                            if isinstance(result, PersistenceResult):
                                require_complete_persistence(
                                    result,
                                    expected_rows=expected_by_table.get(table, 0),
                                )
                if isinstance(econ_stats, DownloadStats) and failed_feeds:
                    raise ProviderError(
                        "Every request failed for economy feed(s): "
                        + ", ".join(sorted(failed_feeds)),
                        provider="polygon",
                    )
            except Exception as e:
                _record_step_failure(
                    "economy", e, "Treasury/inflation/labor data will be stale until the next run."
                )

        # Step: Update market internals from FRED
        if fred_api_key:
            logger.info(f"\n[{step}/{total_steps}] Updating market internals from FRED...")
            fred_client = FredClient(fred_api_key, logger)
            try:
                mi_result = fred_client.get_market_internals(
                    market_internals_start_str, end_str
                )
                mi_rows = mi_result.rows
                if mi_result.failures:
                    stats["market_internals_failures"] = mi_result.failure_details
                    stats["market_internals_degraded"] = True
                    failure_summary = ", ".join(
                        f"{failure.field} ({failure.error_type})"
                        for failure in mi_result.failures
                    )
                    if mi_result.all_series_failed:
                        reason = "market internals failed (all FRED series)"
                        stats["market_internals_error"] = "all FRED series failed"
                    else:
                        reason = (
                            "market internals partial FRED series failure: "
                            f"{failure_summary}"
                        )
                    provider_degraded_reasons.append(reason)
                    logger.warning(f"  {reason}")
                    get_notifier(logger).send(
                        title="Sawa: weekly market internals degraded",
                        body=(
                            "The FRED market-internals fetch was incomplete.\n"
                            f"Failed series: {failure_summary}\n\n"
                            "Available series were still loaded; missing values preserve "
                            "previous database values."
                        ),
                        level=NotificationLevel.WARNING,
                        tags=["warning", "weekly", "market_internals"],
                    )
                if mi_rows:
                    with psycopg.connect(database_url) as conn:
                        loaded = load_market_internals(conn, mi_rows, logger)
                    stats["market_internals"] = loaded
                else:
                    stats["market_internals"] = 0
            except Exception as e:
                safe_error = redact_sensitive_text(e)
                logger.warning(f"Market internals update failed: {safe_error}")
                stats["market_internals"] = 0
                stats["market_internals_error"] = safe_error
                stats["market_internals_degraded"] = True
                provider_degraded_reasons.append("market internals update failed")
                get_notifier(logger).send(
                    title="Sawa: market internals update failed",
                    body=(
                        f"FRED market internals fetch/load failed during weekly run.\n"
                        f"{type(e).__name__}: {safe_error}\n\n"
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
            stats["market_internals_skipped"] = "FRED_API_KEY not set"
            provider_degraded_reasons.append(
                "market internals skipped (FRED_API_KEY not set)"
            )

        # Step: Update news
        if not skip_news:
            logger.info(f"\n[{step}/{total_steps}] Updating news articles...")
            step += 1
            try:
                with psycopg.connect(database_url) as conn:
                    news_result = load_news(
                        conn, client, symbols, days=DEFAULT_NEWS_DAYS, log=logger
                    )
                stats["news"] = int(news_result)
                if isinstance(news_result, NewsLoadResult):
                    stats["news_requests"] = news_result.summary()
                    if news_result.all_requests_failed:
                        raise ProviderError(
                            "All news provider requests failed",
                            provider="polygon",
                        )
                    if news_result.no_articles_fetched:
                        raise ProviderError(
                            "News provider returned no articles for the requested universe",
                            provider="polygon",
                        )
                    if news_result.total_persistence_failure:
                        raise RuntimeError(
                            "News persistence rejected every fetched article ("
                            f"{news_result.rejected_articles} article(s)"
                            ")"
                        )
                    if news_result.failed:
                        provider_degraded_reasons.append(
                            "news provider requests partially failed"
                        )
                    if news_result.partial_persistence_failure:
                        provider_degraded_reasons.append(
                            "news persistence partially rejected articles"
                        )
                elif int(news_result) <= 0:
                    raise RuntimeError("News loader returned no typed outcome or rows")
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
                if not ca_stats.get("success"):
                    raise RuntimeError(
                        "corporate-actions update reported an incomplete result"
                    )

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
                    if not adjust_stats.get("success"):
                        raise RuntimeError(
                            "split-adjusted price refresh reported an incomplete result "
                            f"({adjust_stats.get('tickers_adjusted', 0)}/"
                            f"{adjust_stats.get('tickers_requested', len(split_tickers))} "
                            "tickers)"
                        )

                    from sawa.ta_backfill import recompute_ta_for_tickers

                    logger.info(
                        f"\nRecomputing technical indicators for "
                        f"{len(split_tickers)} split ticker(s)..."
                    )
                    ta_stats = recompute_ta_for_tickers(
                        database_url=database_url,
                        tickers=split_tickers,
                        log=logger,
                    )
                    stats["split_ta_recompute"] = ta_stats
                    if not ta_stats.get("success"):
                        failed = ta_stats.get("tickers_failed", "unknown")
                        raise RuntimeError(
                            f"split TA recompute failed for {failed} ticker(s)"
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
                if not character_stats.get("success", False):
                    raise RuntimeError(
                        "stock character batch reported an incomplete result "
                        f"({character_stats.get('classified', 0)}/"
                        f"{character_stats.get('total', 0)} classified; "
                        f"{character_stats.get('errors', 0)} errors)"
                    )
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
            safe_error = f"{type(e).__name__}: {redact_sensitive_text(e)}"
            logger.warning(f"MCP query insights refresh failed: {safe_error}")
            stats["mcp_query_insights_error"] = safe_error

        # The run reaches here without a fatal exception, but individual steps
        # may have failed (caught + recorded above). Fail the overall run if any
        # did, so the scheduler withholds the weekly_done flag and retries — but
        # only after every independent step had its chance to run.
        degraded_reasons = [
            f"weekly step failed ({name})" for name in sorted(step_errors)
        ]
        degraded_reasons.extend(provider_degraded_reasons)
        stats["degraded"] = bool(degraded_reasons)
        if degraded_reasons:
            stats["degraded_reasons"] = degraded_reasons

        if step_errors:
            stats["success"] = False
            stats["step_errors"] = step_errors
        else:
            stats["success"] = True
        logger.info("\n" + "=" * 60)
        logger.info(
            "WEEKLY UPDATE COMPLETE" + (" (DEGRADED)" if degraded_reasons else "")
        )
        logger.info("=" * 60)

        if degraded_reasons:
            logger.warning("  DEGRADED: " + "; ".join(degraded_reasons))
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
        safe_error = f"{type(e).__name__}: {redact_sensitive_text(e)}"
        logger.error(f"Weekly update failed: {safe_error}")
        stats["error"] = safe_error
        raise

    return stats
