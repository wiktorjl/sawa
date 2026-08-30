"""
Quarterly update: Pull fundamentals data (balance sheets, income, cash flow, ratios).

Purpose: Update financial statements and ratios that are released quarterly.
Re-entrant: Safe to run multiple times (upsert on primary keys).
"""

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import psycopg

from sawa.api import PolygonClient
from sawa.database import get_last_date, get_symbols_from_db
from sawa.database.load import (
    PersistenceResult,
    load_fundamentals,
    load_ratios,
    require_complete_persistence,
)
from sawa.domain.exceptions import ProviderError
from sawa.provider_downloads import (
    DownloadCount,
    DownloadStats,
    bind_provider_record,
)
from sawa.repositories.rate_limiter import SyncRateLimiter
from sawa.utils import setup_logging
from sawa.utils.constants import DEFAULT_API_RATE_LIMIT
from sawa.utils.csv_utils import write_csv_auto_fields
from sawa.utils.dates import DATE_FORMAT
from sawa.utils.security import redact_sensitive_text

FUNDAMENTAL_ENDPOINT_TABLES = {
    "balance-sheets": "balance_sheets",
    "cash-flow": "cash_flows",
    "income-statements": "income_statements",
}


def download_fundamentals(
    client: PolygonClient,
    symbols: list[str],
    start_date: str | None,
    end_date: str,
    output_dir: Path,
    logger: logging.Logger,
    rate_limiter: SyncRateLimiter | None = None,
    filing_date_gte: str | None = None,
) -> DownloadStats:
    """Download fundamentals data (balance sheets, cash flow, income statements).

    The incremental quarterly pull filters on filing_date_gte (when reports
    became available) so late filings and restatements of older periods are
    captured; start_date (period_end.gte) is used only for a full backfill.
    """
    stats = DownloadStats()

    for endpoint, table_name in FUNDAMENTAL_ENDPOINT_TABLES.items():
        logger.info(f"Downloading {endpoint}...")
        output_dir.mkdir(parents=True, exist_ok=True)

        all_data: list[dict[str, Any]] = []
        succeeded = 0
        failed = 0
        for i, symbol in enumerate(symbols, 1):
            if i % 50 == 0:
                logger.info(f"  Progress: {i}/{len(symbols)}")
            try:
                if rate_limiter:
                    rate_limiter.acquire()
                data = client.get_fundamentals(
                    endpoint,
                    ticker=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    filing_date_gte=filing_date_gte,
                )
                if not isinstance(data, list):
                    raise ProviderError(
                        "Provider returned a non-list fundamentals response",
                        provider="polygon",
                    )
                bound_rows = [
                    bind_provider_record(
                        record,
                        symbol,
                        output_field="tickers",
                    )
                    for record in data
                ]
                all_data.extend(bound_rows)
                succeeded += 1
            except Exception as e:
                failed += 1
                logger.warning(f"  {symbol}: {redact_sensitive_text(e)}")

        artifact: str | None = None
        if all_data:
            filepath = output_dir / f"{endpoint.replace('-', '_')}.csv"
            write_csv_auto_fields(filepath, all_data, logger)
            artifact = table_name

        stats.record(
            endpoint,
            len(all_data),
            requested=len(symbols),
            succeeded=succeeded,
            failed=failed,
            artifact=artifact,
        )

    return stats


def download_ratios(
    client: PolygonClient,
    symbols: list[str],
    output_dir: Path,
    logger: logging.Logger,
    rate_limiter: SyncRateLimiter | None = None,
) -> DownloadCount:
    """Download financial ratios."""
    logger.info("Downloading financial ratios...")
    output_dir.mkdir(parents=True, exist_ok=True)

    all_ratios: list[dict[str, Any]] = []
    succeeded = 0
    failed = 0
    for i, symbol in enumerate(symbols, 1):
        if i % 50 == 0:
            logger.info(f"  Progress: {i}/{len(symbols)}")
        try:
            if rate_limiter:
                rate_limiter.acquire()
            ratios = client.get_ratios(symbol)
            if not isinstance(ratios, list):
                raise ProviderError(
                    "Provider returned a non-list ratios response",
                    provider="polygon",
                )
            bound_rows = [
                bind_provider_record(record, symbol, output_field="ticker")
                for record in ratios
            ]
            all_ratios.extend(bound_rows)
            succeeded += 1
        except Exception as e:
            failed += 1
            logger.warning(f"  {symbol}: {redact_sensitive_text(e)}")

    artifact_written = False
    if all_ratios:
        filepath = output_dir / "ratios.csv"
        write_csv_auto_fields(filepath, all_ratios, logger)
        artifact_written = True

    return DownloadCount(
        len(all_ratios),
        requested=len(symbols),
        succeeded=succeeded,
        failed=failed,
        artifact_written=artifact_written,
    )


def run_quarterly(
    api_key: str,
    database_url: str,
    output_dir: Path,
    skip_fundamentals: bool = False,
    skip_ratios: bool = False,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Run quarterly fundamentals update.

    Args:
        api_key: Polygon/Massive API key
        database_url: PostgreSQL connection URL
        output_dir: Directory to save downloaded data
        skip_fundamentals: Skip fundamentals update
        skip_ratios: Skip financial ratios update
        dry_run: If True, show what would be done without executing
        logger: Logger instance

    Returns:
        Statistics dictionary
    """
    logger = logger or setup_logging()
    stats: dict[str, Any] = {"success": False}

    logger.info("=" * 60)
    logger.info("QUARTERLY UPDATE - Fundamentals & Ratios")
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

            # Get last date for incremental updates. Anchor on MAX(filing_date)
            # (when reports became available), NOT MAX(period_end) (when the
            # fiscal period closed): filtering on period_end silently skips
            # late/amended filers, non-calendar fiscal years, and restatements
            # of older quarters whose period_end predates the global max.
            last_filing_date = get_last_date(conn, "balance_sheets", "filing_date")

            # Calculate date range
            end_date = date.today()
            end_str = end_date.strftime(DATE_FORMAT)

            # filing_date_gte drives the incremental pull; fund_start_str
            # (period_end.gte) is only used for the full-backfill cold path.
            fund_start_str: str | None
            if last_filing_date:
                # Widen overlap to 120 days to also recapture amended filings.
                filing_gte = last_filing_date - timedelta(days=120)
                filing_gte_str = filing_gte.strftime(DATE_FORMAT)
                fund_start_str = None
                logger.info(
                    f"Fundamentals incremental window: filing_date >= {filing_gte_str} "
                    f"(period_end <= {end_str})"
                )
            else:
                # No data yet: full backfill anchored on period_end.
                fund_start = end_date - timedelta(days=365)
                fund_start_str = fund_start.strftime(DATE_FORMAT)
                filing_gte_str = None
                logger.info(f"Fundamentals date range: {fund_start_str} to {end_str}")

        if dry_run:
            logger.info("\n[DRY RUN] Would update:")
            if not skip_fundamentals:
                window = (
                    f"filing_date >= {filing_gte_str}"
                    if filing_gte_str
                    else f"period_end >= {fund_start_str}"
                )
                logger.info(f"  - Fundamentals ({window})")
            if not skip_ratios:
                logger.info(f"  - Financial ratios for {len(symbols)} symbols")
            stats["success"] = True
            stats["dry_run"] = True
            return stats

        step = 1
        total_steps = 2 - sum([skip_fundamentals, skip_ratios])
        step_errors: dict[str, str] = {}
        degraded_reasons: list[str] = []

        def _record_step_failure(name: str, exc: Exception) -> None:
            safe_error = f"{type(exc).__name__}: {redact_sensitive_text(exc)}"
            logger.error(f"Quarterly step '{name}' failed: {safe_error}")
            step_errors[name] = safe_error
            stats[f"{name}_error"] = safe_error

        # Step: Update fundamentals
        if not skip_fundamentals:
            logger.info(f"\n[{step}/{total_steps}] Updating fundamentals...")
            step += 1
            try:
                fund_stats = download_fundamentals(
                    client,
                    symbols,
                    fund_start_str,
                    end_str,
                    output_dir / "fundamentals",
                    logger,
                    rate_limiter,
                    filing_date_gte=filing_gte_str,
                )
                stats["fundamentals"] = fund_stats
                stats["fundamentals_requests"] = fund_stats.requests
                failed_feeds = fund_stats.failed_feeds
                if fund_stats.has_failures and not failed_feeds:
                    degraded_reasons.append(
                        "fundamentals provider requests partially failed"
                    )
                if fund_stats.empty_feeds:
                    degraded_reasons.append(
                        "fundamentals feeds returned no fresh rows: "
                        + ", ".join(sorted(fund_stats.empty_feeds))
                    )
                if fund_stats.artifacts:
                    with psycopg.connect(database_url) as conn:
                        loaded_fundamentals = load_fundamentals(
                            conn,
                            output_dir / "fundamentals",
                            logger,
                            only_tables=fund_stats.artifacts,
                        )
                    if isinstance(loaded_fundamentals, dict):
                        stats["fundamentals_loaded"] = {
                            table: int(result)
                            for table, result in loaded_fundamentals.items()
                        }
                        stats["fundamentals_persistence"] = {
                            table: result.summary()
                            for table, result in loaded_fundamentals.items()
                            if isinstance(result, PersistenceResult)
                        }
                        for endpoint, table in FUNDAMENTAL_ENDPOINT_TABLES.items():
                            if table not in fund_stats.artifacts:
                                continue
                            result = loaded_fundamentals.get(table)
                            if result is None:
                                raise RuntimeError(
                                    f"Fresh {table} artifact was not loaded"
                                )
                            if isinstance(result, PersistenceResult):
                                require_complete_persistence(
                                    result,
                                    expected_rows=int(fund_stats.get(endpoint, 0)),
                                )
                if failed_feeds:
                    raise ProviderError(
                        "Every request failed for fundamentals feed(s): "
                        + ", ".join(sorted(failed_feeds)),
                        provider="polygon",
                    )
            except Exception as e:
                _record_step_failure("fundamentals", e)

        # Step: Update ratios
        if not skip_ratios:
            logger.info(f"\n[{step}/{total_steps}] Updating financial ratios...")
            step += 1
            try:
                ratio_count = download_ratios(
                    client, symbols, output_dir / "ratios", logger, rate_limiter
                )
                stats["ratios"] = ratio_count
                stats["ratios_requests"] = ratio_count.summary()
                if ratio_count.failed and not ratio_count.all_failed:
                    degraded_reasons.append("ratios provider requests partially failed")
                if ratio_count.empty_successful:
                    degraded_reasons.append("ratios feed returned no fresh rows")
                if ratio_count.artifact_written:
                    with psycopg.connect(database_url) as conn:
                        loaded_ratios = load_ratios(
                            conn,
                            output_dir / "ratios" / "ratios.csv",
                            logger,
                        )
                    if isinstance(loaded_ratios, PersistenceResult):
                        require_complete_persistence(
                            loaded_ratios,
                            expected_rows=int(ratio_count),
                        )
                        stats["ratios_loaded"] = int(loaded_ratios)
                        stats["ratios_persistence"] = loaded_ratios.summary()
                if ratio_count.all_failed:
                    raise ProviderError(
                        "All ratios provider requests failed",
                        provider="polygon",
                    )
            except Exception as e:
                _record_step_failure("ratios", e)

        degraded_reasons.extend(
            f"quarterly step failed ({name})" for name in sorted(step_errors)
        )
        stats["degraded"] = bool(degraded_reasons)
        if degraded_reasons:
            stats["degraded_reasons"] = degraded_reasons
        stats["success"] = not step_errors
        if step_errors:
            stats["step_errors"] = step_errors
        logger.info("\n" + "=" * 60)
        logger.info(
            "QUARTERLY UPDATE COMPLETE" + (" (DEGRADED)" if degraded_reasons else "")
        )
        logger.info("=" * 60)

        if "fundamentals" in stats:
            total = sum(int(v) for v in stats["fundamentals"].values())
            logger.info(f"  Fundamentals: {total} records")
        if "ratios" in stats:
            logger.info(f"  Ratios: {stats['ratios']}")

    except Exception as e:
        safe_error = f"{type(e).__name__}: {redact_sensitive_text(e)}"
        logger.error(f"Quarterly update failed: {safe_error}")
        stats["error"] = safe_error
        raise

    return stats
