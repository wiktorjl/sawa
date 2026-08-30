"""
Cold start: Full database setup from scratch.

Steps:
1. Drop existing tables (optional)
2. Create database schema
3. Download S&P 500 symbols from Wikipedia
4. Download all historical data:
   - Daily prices from S3
   - Fundamentals (balance sheets, income, cash flow)
   - Company overviews
   - Economy data (treasury, inflation, labor)
   - Financial ratios
5. Load all data into PostgreSQL
"""

import csv
import logging
import shutil
import tempfile
from collections.abc import Callable
from datetime import date, timedelta
from math import ceil
from pathlib import Path
from typing import Any

from sawa.api import FredClient, PolygonClient, PolygonS3Client
from sawa.api.s3 import BulkPriceRows
from sawa.database.load import (
    PersistenceResult,
    load_companies,
    load_economy,
    load_fundamentals,
    load_market_internals,
    load_news,
    load_prices,
    load_ratios,
    require_complete_persistence,
)
from sawa.database.news import NewsLoadResult
from sawa.database.schema import (
    drop_all_tables,
    execute_sql_files_atomically,
    get_sql_files,
    pin_schema_search_path,
    validate_schema_files,
    verify_materialized_views,
    verify_tables,
    verify_views,
)
from sawa.domain.exceptions import ProviderError
from sawa.domain.price_validation import is_valid_daily_ohlcv
from sawa.provider_downloads import DownloadCount, DownloadStats, bind_provider_record
from sawa.repositories.rate_limiter import SyncRateLimiter
from sawa.utils import calculate_date_range, get_notifier, setup_logging
from sawa.utils.constants import DEFAULT_API_RATE_LIMIT, DEFAULT_NEWS_DAYS
from sawa.utils.csv_utils import write_csv_auto_fields
from sawa.utils.dates import DATE_FORMAT
from sawa.utils.notify import NotificationLevel
from sawa.utils.security import redact_sensitive_text
from sawa.utils.symbols import (
    fetch_dow30_symbols,
    fetch_mag7_symbols,
    fetch_nasdaq100_symbols,
    fetch_nasdaq_listed_symbols,
    fetch_russell1000_symbols,
    fetch_sp500_symbols,
    validate_ticker,
)

# Wikipedia URL for S&P 500 constituents
WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
WIKIPEDIA_HEADERS = {"User-Agent": "SP500-Data-Downloader/1.0"}
FUNDAMENTAL_ENDPOINT_TABLES = {
    "balance-sheets": "balance_sheets",
    "cash-flow": "cash_flows",
    "income-statements": "income_statements",
}
ECONOMY_ENDPOINT_TABLES = {
    "treasury-yields": "treasury_yields",
    "inflation": "inflation",
    "inflation-expectations": "inflation_expectations",
    "labor-market": "labor_market",
}

MINIMUM_INDEX_SOURCE_COUNTS = {
    "sp500": 450,
    "nasdaq_listed": 1000,
    "us_active": 1000,
    "nasdaq100": 90,
    "dow30": 25,
    "russell1000": 850,
    "mag7": 7,
}
MAXIMUM_INDEX_SOURCE_COUNTS = {
    # Tight enough to reject a parser selecting the wrong HTML table or a
    # provider pagination loop, while leaving ample room for routine index
    # reconstitution and share-class changes.
    "sp500": 600,
    "nasdaq_listed": 20_000,
    "us_active": 30_000,
    "nasdaq100": 125,
    "dow30": 35,
    "russell1000": 1_150,
    "mag7": 10,
}


class PriceDownloadResult(int):
    """Fresh price rows plus per-trading-date source outcomes."""

    requested_dates: int
    sourced_dates: int
    missing_dates: tuple[str, ...]
    failed_dates: dict[str, str]
    empty_filtered_dates: tuple[str, ...]
    artifact_files: set[str]

    def __new__(
        cls,
        rows: int,
        *,
        requested_dates: int,
        sourced_dates: int,
        missing_dates: list[str],
        failed_dates: dict[str, str],
        empty_filtered_dates: list[str],
        artifact_files: set[str],
    ) -> "PriceDownloadResult":
        result = super().__new__(cls, rows)
        result.requested_dates = requested_dates
        result.sourced_dates = sourced_dates
        result.missing_dates = tuple(missing_dates)
        result.failed_dates = dict(failed_dates)
        result.empty_filtered_dates = tuple(empty_filtered_dates)
        result.artifact_files = set(artifact_files)
        return result

    @property
    def has_source_failures(self) -> bool:
        return bool(self.missing_dates or self.failed_dates)

    @property
    def all_sources_failed(self) -> bool:
        return self.requested_dates > 0 and self.sourced_dates == 0

    def summary(self) -> dict[str, Any]:
        return {
            "requested_dates": self.requested_dates,
            "sourced_dates": self.sourced_dates,
            "missing_dates": len(self.missing_dates),
            "failed_dates": len(self.failed_dates),
            "empty_filtered_dates": len(self.empty_filtered_dates),
            "rows": int(self),
            "artifact_files": len(self.artifact_files),
            "missing_date_samples": list(self.missing_dates[:10]),
            "failed_date_samples": list(self.failed_dates)[:10],
        }


class IndexPopulationResult(dict[str, int]):
    """Per-index counts plus failures that must remain visible to callers."""

    failures: dict[str, str]
    requested: int

    def __init__(self, *, requested: int) -> None:
        super().__init__()
        self.failures = {}
        self.requested = requested

    @property
    def all_failed(self) -> bool:
        return self.requested > 0 and len(self.failures) == self.requested

    def record_failure(self, code: str, reason: object) -> None:
        self[code] = 0
        self.failures[code] = redact_sensitive_text(reason)

    def summary(self) -> dict[str, Any]:
        return {
            "counts": dict(self),
            "requested": self.requested,
            "succeeded": self.requested - len(self.failures),
            "failures": dict(self.failures),
        }


def _check_date_already_downloaded(date_str: str, output_dir: Path) -> bool:
    """Check if a date has already been downloaded by sampling a few files."""
    if not output_dir.exists():
        return False

    # Check up to 3 existing CSV files to see if they contain this date
    csv_files = list(output_dir.glob("*.csv"))
    if not csv_files:
        return False

    for filepath in csv_files[:3]:
        try:
            with open(filepath) as f:
                # Skip header
                next(f, None)
                # Check first few lines for this date
                for _ in range(10):
                    line = f.readline()
                    if not line:
                        break
                    if line.startswith(date_str):
                        return True
        except Exception:
            continue

    return False


def download_prices(
    s3_client: PolygonS3Client,
    symbols: set[str],
    start_date: date,
    end_date: date,
    trading_days: list[str],
    output_dir: Path,
    logger: logging.Logger,
) -> PriceDownloadResult:
    """Download historical prices from S3."""
    normalized_symbols = {validate_ticker(symbol) for symbol in symbols}
    logger.info(f"Downloading prices from {start_date} to {end_date}...")
    output_dir.mkdir(parents=True, exist_ok=True)

    trading_set = set(trading_days)
    total_records = 0
    processed_dates = 0
    requested_days = {
        d for d in trading_days if start_date <= date.fromisoformat(d) <= end_date
    }
    total_trading_days = len(requested_days)
    sourced_dates = 0
    missing_dates: list[str] = []
    failed_dates: dict[str, str] = {}
    empty_filtered_dates: list[str] = []
    artifact_files: set[str] = set()

    current = start_date
    while current <= end_date:
        date_str = current.strftime(DATE_FORMAT)
        if date_str in trading_set:
            processed_dates += 1
            logger.debug(f"  {date_str}...")

            # Progress indicator every 50 dates or at milestones
            if processed_dates % 50 == 0 or processed_dates == total_trading_days:
                logger.info(
                    f"  Progress: {processed_dates}/{total_trading_days} dates, "
                    f"{total_records:,} records"
                )

            try:
                downloaded_records = s3_client.download_and_parse(
                    current, normalized_symbols
                )
            except Exception as e:
                failed_dates[date_str] = (
                    f"{type(e).__name__}: {redact_sensitive_text(e)}"
                )
                logger.error(
                    f"  {date_str}: S3 price download failed: "
                    f"{failed_dates[date_str]}"
                )
                current += timedelta(days=1)
                continue
            if not isinstance(downloaded_records, BulkPriceRows):
                failed_dates[date_str] = "untyped bulk price response"
                current += timedelta(days=1)
                continue
            if not downloaded_records.source_found:
                missing_dates.append(date_str)
                current += timedelta(days=1)
                continue
            sourced_dates += 1
            try:
                validated_records: list[dict[str, Any]] = []
                for record in downloaded_records:
                    if not isinstance(record, dict):
                        raise ValueError("bulk price row is not an object")
                    raw_symbol = record.get("symbol")
                    if not isinstance(raw_symbol, str):
                        raise ValueError("bulk price row has no string symbol")
                    symbol = validate_ticker(raw_symbol)
                    if symbol not in normalized_symbols:
                        raise ValueError(
                            "bulk price row symbol was not requested"
                        )
                    if not is_valid_daily_ohlcv(
                        record,
                        allow_numeric_strings=True,
                    ):
                        raise ValueError("bulk price row has malformed OHLCV data")
                    validated_records.append(
                        {**record, "symbol": symbol, "date": date_str}
                    )
            except ValueError as e:
                failed_dates[date_str] = f"invalid bulk price data: {e}"
                logger.error(f"  {date_str}: {failed_dates[date_str]}")
                current += timedelta(days=1)
                continue
            records = validated_records
            if records:
                # Append to per-symbol files
                for record in records:
                    sym = record["symbol"]
                    filepath = output_dir / f"{sym}.csv"
                    artifact_files.add(filepath.name)
                    file_exists = filepath.exists()
                    with open(filepath, "a", newline="") as f:
                        writer = csv.DictWriter(
                            f,
                            fieldnames=["date", "symbol", "open", "close", "high", "low", "volume"],
                        )
                        if not file_exists:
                            writer.writeheader()
                        writer.writerow(record)
                total_records += len(records)
            else:
                empty_filtered_dates.append(date_str)
        current += timedelta(days=1)

    logger.info(
        f"Complete: {processed_dates} dates, {total_records:,} records; "
        f"{len(missing_dates)} missing source(s), {len(failed_dates)} failed"
    )
    return PriceDownloadResult(
        total_records,
        requested_dates=total_trading_days,
        sourced_dates=sourced_dates,
        missing_dates=missing_dates,
        failed_dates=failed_dates,
        empty_filtered_dates=empty_filtered_dates,
        artifact_files=artifact_files,
    )


def _merge_price_artifacts(source_dir: Path, destination_dir: Path) -> None:
    """Append fresh staging rows to the persistent load-only cache."""
    destination_dir.mkdir(parents=True, exist_ok=True)
    for source in sorted(source_dir.glob("*.csv")):
        destination = destination_dir / source.name
        if not destination.exists():
            shutil.copy2(source, destination)
            continue
        with open(source, encoding="utf-8") as incoming, open(
            destination, "a", encoding="utf-8"
        ) as cached:
            next(incoming, None)
            for line in incoming:
                cached.write(line)


def download_fundamentals(
    client: PolygonClient,
    symbols: list[str],
    start_date: str,
    end_date: str,
    output_dir: Path,
    logger: logging.Logger,
    rate_limiter: SyncRateLimiter | None = None,
) -> DownloadStats:
    """Download balance sheets, cash flows, income statements."""
    logger.info("Downloading fundamentals...")
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
                    endpoint, ticker=symbol, start_date=start_date, end_date=end_date
                )
                if not isinstance(data, list):
                    raise ProviderError(
                        "Provider returned a non-list fundamentals response",
                        provider="polygon",
                    )
                bound_rows = [
                    bind_provider_record(record, symbol, output_field="tickers")
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
            if not isinstance(data, dict):
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


def get_tickers_from_csv_files(
    data_dir: Path,
    logger: logging.Logger,
    *,
    fundamental_tables: set[str] | None = None,
    include_ratios: bool = True,
) -> set[str]:
    """Extract all unique tickers from downloaded CSV files."""
    tickers: set[str] = set()

    # Check fundamentals
    fundamentals_dir = data_dir / "fundamentals"
    if fundamentals_dir.exists():
        table_files = {
            "balance_sheets": fundamentals_dir / "balance_sheets.csv",
            "cash_flows": fundamentals_dir / "cash_flow.csv",
            "income_statements": fundamentals_dir / "income_statements.csv",
        }
        csv_files = (
            fundamentals_dir.glob("*.csv")
            if fundamental_tables is None
            else (
                path
                for table, path in table_files.items()
                if table in fundamental_tables
            )
        )
        for csv_file in csv_files:
            try:
                with open(csv_file) as f:
                    import csv

                    reader = csv.DictReader(f)
                    for row in reader:
                        ticker = row.get("tickers") or row.get("ticker")
                        if ticker:
                            tickers.add(ticker.upper())
            except Exception as e:
                logger.warning(
                    "Error reading %s: %s: %s",
                    csv_file,
                    type(e).__name__,
                    redact_sensitive_text(e),
                )

    # Check ratios
    ratios_file = data_dir / "ratios" / "ratios.csv"
    if include_ratios and ratios_file.exists():
        try:
            with open(ratios_file) as f:
                import csv

                reader = csv.DictReader(f)
                for row in reader:
                    ticker = row.get("ticker")
                    if ticker:
                        tickers.add(ticker.upper())
        except Exception as e:
            logger.warning(
                "Error reading %s: %s: %s",
                ratios_file,
                type(e).__name__,
                redact_sensitive_text(e),
            )

    return tickers


def get_existing_tickers_from_db(conn) -> set[str]:
    """Get all tickers currently in the companies table."""
    with conn.cursor() as cur:
        cur.execute("SELECT ticker FROM companies")
        return {row[0].upper() for row in cur.fetchall()}


def fetch_missing_companies(
    client: PolygonClient,
    missing_tickers: set[str],
    output_dir: Path,
    logger: logging.Logger,
    rate_limiter: SyncRateLimiter | None = None,
) -> int:
    """Fetch company info for tickers not in the companies table."""
    if not missing_tickers:
        return 0

    logger.info(f"Fetching company info for {len(missing_tickers)} missing tickers...")
    output_dir.mkdir(parents=True, exist_ok=True)

    overviews: list[dict[str, Any]] = []
    for i, symbol in enumerate(sorted(missing_tickers), 1):
        if i % 20 == 0:
            logger.info(f"  Progress: {i}/{len(missing_tickers)}")
        try:
            if rate_limiter:
                rate_limiter.acquire()
            data = client.get_ticker_details(symbol)
            if not isinstance(data, dict):
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
        except Exception as e:
            # Debug level: many missing tickers are legitimately delisted.
            logger.debug(f"  {symbol}: {redact_sensitive_text(e)}")

    if overviews:
        # Append to existing overviews file or create new one
        filepath = output_dir / "overviews_missing.csv"
        write_csv_auto_fields(filepath, overviews, logger)
        logger.info(
            f"  Found company info for {len(overviews)} of {len(missing_tickers)} missing tickers"
        )

    return len(overviews)


def download_economy(
    client: PolygonClient,
    start_date: str,
    end_date: str,
    output_dir: Path,
    logger: logging.Logger,
) -> DownloadStats:
    """Download economy data."""
    stats = DownloadStats()

    output_dir.mkdir(parents=True, exist_ok=True)

    for endpoint, table_name in ECONOMY_ENDPOINT_TABLES.items():
        logger.info(f"Downloading {endpoint}...")
        try:
            data = client.get_economy_data(endpoint, start_date, end_date)
            if not isinstance(data, list):
                raise ProviderError(
                    "Provider returned a non-list economy response",
                    provider="polygon",
                )
            artifact: str | None = None
            if data:
                filepath = output_dir / f"{endpoint.replace('-', '_')}.csv"
                write_csv_auto_fields(filepath, data, logger)
                artifact = table_name
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


def _index_fetchers(
    api_key: str | None,
) -> list[tuple[str, Callable[[logging.Logger], list[str]]]]:
    """Build index sources while threading explicit provider credentials."""
    from sawa.utils.symbols import fetch_us_active_from_polygon

    return [
        ("sp500", fetch_sp500_symbols),
        (
            "nasdaq_listed",
            lambda log: fetch_nasdaq_listed_symbols(
                log,
                api_key=api_key,
                allow_fallback=False,
            ),
        ),
        (
            "us_active",
            lambda log: fetch_us_active_from_polygon(log, api_key=api_key),
        ),
        ("nasdaq100", fetch_nasdaq100_symbols),
        ("dow30", fetch_dow30_symbols),
        ("russell1000", fetch_russell1000_symbols),
        ("mag7", fetch_mag7_symbols),
    ]


def populate_index_constituents(
    conn,
    logger: logging.Logger,
    api_key: str | None = None,
    *,
    minimum_source_counts: dict[str, int] | None = None,
    maximum_source_counts: dict[str, int] | None = None,
) -> IndexPopulationResult:
    """Atomically replace validated index memberships, preserving old data on failure."""
    logger.info("Populating index constituents...")
    minimum_relative_coverage = 0.5
    minimum_eligible_source_coverage = 0.5
    source_floors = minimum_source_counts or MINIMUM_INDEX_SOURCE_COUNTS
    source_ceilings = maximum_source_counts or MAXIMUM_INDEX_SOURCE_COUNTS
    index_data = _index_fetchers(api_key)
    stats = IndexPopulationResult(requested=len(index_data))

    for code, fetcher in index_data:
        savepoint_active = False
        db_touched = False
        try:
            logger.info(f"  Fetching {code} symbols...")
            raw_symbols = fetcher(logger)
            if not isinstance(raw_symbols, list):
                raise ValueError("constituent source returned a non-list response")
            symbols = sorted({validate_ticker(symbol) for symbol in raw_symbols})
            if not symbols:
                raise ValueError("constituent source returned no symbols")
            minimum_source_count = max(1, int(source_floors.get(code, 1)))
            if len(symbols) < minimum_source_count:
                raise ValueError(
                    "constituent source fell below absolute completeness threshold "
                    f"({len(symbols)}/{minimum_source_count})"
                )
            maximum_source_count = max(
                minimum_source_count,
                int(source_ceilings.get(code, 100_000)),
            )
            if len(symbols) > maximum_source_count:
                raise ValueError(
                    "constituent source exceeded maximum plausibility threshold "
                    f"({len(symbols)}/{maximum_source_count})"
                )
            logger.info(f"    Found {len(symbols)} symbols")

            # Resolve the index and the subset that can satisfy the companies
            # foreign key before modifying existing membership.
            with conn.cursor() as cur:
                db_touched = True
                cur.execute(
                    """
                    SELECT i.id, COUNT(ic.ticker)
                    FROM indices i
                    LEFT JOIN index_constituents ic ON ic.index_id = i.id
                    WHERE i.code = %s
                    GROUP BY i.id
                    """,
                    (code,),
                )
                row = cur.fetchone()
                if not row:
                    raise ValueError(f"index not found in database: {code}")
                index_id = row[0]
                existing_count = int(row[1])

                cur.execute(
                    "SELECT ticker FROM companies WHERE ticker = ANY(%s)",
                    (symbols,),
                )
                eligible = sorted({str(item[0]).upper() for item in cur.fetchall()})
                if not eligible:
                    raise ValueError("no fetched constituents exist in companies")
                minimum_eligible_count = max(
                    1,
                    ceil(len(symbols) * minimum_eligible_source_coverage),
                )
                if len(eligible) < minimum_eligible_count:
                    raise ValueError(
                        "eligible constituent coverage fell below source threshold "
                        f"({len(eligible)}/{len(symbols)})"
                    )
                minimum_count = max(
                    1,
                    int(existing_count * minimum_relative_coverage + 0.5),
                )
                if existing_count and len(eligible) < minimum_count:
                    raise ValueError(
                        "eligible source coverage fell below preservation threshold "
                        f"({len(eligible)}/{existing_count})"
                    )

                cur.execute("SAVEPOINT index_refresh")
                savepoint_active = True
                cur.execute("DELETE FROM index_constituents WHERE index_id = %s", (index_id,))

                added = 0
                for symbol in eligible:
                    cur.execute(
                        """
                        INSERT INTO index_constituents (index_id, ticker)
                        SELECT %s, %s
                        WHERE EXISTS (SELECT 1 FROM companies WHERE ticker = %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (index_id, symbol, symbol),
                    )
                    if cur.rowcount > 0:
                        added += 1
                if added != len(eligible):
                    raise RuntimeError(
                        f"persisted only {added}/{len(eligible)} eligible constituents"
                    )

                # Update last_updated
                cur.execute(
                    "UPDATE indices SET last_updated = CURRENT_TIMESTAMP WHERE id = %s",
                    (index_id,),
                )
                cur.execute("RELEASE SAVEPOINT index_refresh")
                savepoint_active = False

                conn.commit()
                stats[code] = added
                logger.info(f"    Added {added} constituents to {code}")

        except Exception as e:
            if savepoint_active:
                try:
                    with conn.cursor() as cur:
                        cur.execute("ROLLBACK TO SAVEPOINT index_refresh")
                        cur.execute("RELEASE SAVEPOINT index_refresh")
                except Exception:
                    pass
            if db_touched:
                try:
                    conn.rollback()
                except Exception:
                    pass
            reason = redact_sensitive_text(e)
            logger.error(f"    Failed to populate {code}: {reason}")
            stats.record_failure(code, reason)

    return stats


def run_coldstart(
    api_key: str | None,
    s3_access_key: str | None,
    s3_secret_key: str | None,
    database_url: str,
    schema_dir: Path,
    output_dir: Path,
    years: int = 5,
    symbols_file: Path | None = None,
    drop_tables: bool = False,
    drop_only: bool = False,
    confirm_drop: bool = False,
    schema_only: bool = False,
    load_only: bool = False,
    skip_downloads: bool = False,
    skip_prices: bool = False,
    skip_fundamentals: bool = False,
    skip_overviews: bool = False,
    skip_economy: bool = False,
    skip_ratios: bool = False,
    skip_news: bool = False,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Run full cold start process.

    Args:
        api_key: Polygon/Massive API key (optional if skipping all downloads)
        s3_access_key: Polygon S3 access key (optional if skipping all downloads)
        s3_secret_key: Polygon S3 secret key (optional if skipping all downloads)
        database_url: PostgreSQL connection URL
        schema_dir: Directory containing SQL schema files
        output_dir: Directory to save downloaded data
        years: Years of historical data to download
        symbols_file: Optional file with symbols to use (one per line)
        drop_tables: Whether to drop existing tables
        drop_only: Only drop tables and clean data directory
        confirm_drop: Skip interactive confirmation for destructive table drops
        schema_only: Only set up schema (no download/load)
        load_only: Only load existing CSV data (no schema changes)
        skip_downloads: Skip downloads but load existing data
        skip_prices: Skip downloading price data from S3
        skip_fundamentals: Skip downloading fundamentals (balance sheets, etc.)
        skip_overviews: Skip downloading company overviews
        skip_economy: Skip downloading economy data
        skip_ratios: Skip downloading financial ratios
        skip_news: Skip downloading news articles
        logger: Logger instance

    Returns:
        Statistics dictionary
    """
    import psycopg

    logger = logger or setup_logging()
    stats: dict[str, Any] = {"success": False}
    degraded_reasons: list[str] = []
    fatal_provider_steps: set[str] = set()

    def confirm_destructive_drop(conn: Any, confirmation_text: str) -> bool:
        """Require explicit confirmation whenever any public table exists."""
        import sys

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM pg_catalog.pg_tables
                WHERE schemaname = 'public'
                """
            )
            row = cur.fetchone()
        table_count = int(row[0] if row else 0)
        if table_count == 0:
            return True

        logger.warning("")
        logger.warning("⚠️  " + "=" * 60)
        logger.warning(f"⚠️  WARNING: Found {table_count} public tables")
        logger.warning("⚠️  ALL DATA IN THOSE TABLES WILL BE PERMANENTLY DELETED!")
        logger.warning("⚠️  " + "=" * 60)
        logger.warning("")

        if confirm_drop:
            logger.info("Drop confirmed by --confirm-drop")
            return True
        if not sys.stdin.isatty():
            logger.error("❌ Non-interactive destructive mode requires --confirm-drop")
            return False
        try:
            response = input(f"❓ Type '{confirmation_text}' to confirm deletion: ")
        except (EOFError, KeyboardInterrupt):
            logger.info("❌ Aborted by user")
            return False
        if response != confirmation_text:
            logger.info("❌ Aborted by user - no data was deleted")
            return False
        return True

    selected_modes = sum(bool(value) for value in (drop_only, schema_only, load_only))
    if selected_modes > 1:
        raise ValueError("drop-only, schema-only, and load-only modes are mutually exclusive")
    if load_only and drop_tables:
        raise ValueError("load-only mode cannot be combined with dropping existing tables")

    # Resolve and validate the exact schema set before opening the database.
    # In destructive modes, require the full migration manifest so an empty or
    # truncated custom/package directory can never be discovered after DROP.
    sql_files: list[Path] | None = None
    if not load_only and not drop_only:
        sql_files = get_sql_files(schema_dir)
        validate_schema_files(
            sql_files,
            require_complete=drop_tables or schema_only,
        )

    # Determine mode
    if drop_only:
        logger.info("=" * 60)
        logger.info("DROP ONLY MODE - Dropping tables")
        logger.info("=" * 60)

        try:
            with psycopg.connect(database_url) as conn:
                if not confirm_destructive_drop(conn, "DELETE"):
                    stats["aborted"] = True
                    return stats

                logger.info("Dropping all tables...")
                if not drop_all_tables(conn, dry_run=False, logger=logger):
                    stats["error"] = "destructive schema cleanup failed"
                    return stats

            stats["success"] = True
            logger.info("Drop complete!")
            return stats
        except Exception as e:
            safe_error = f"{type(e).__name__}: {redact_sensitive_text(e)}"
            logger.error(f"Drop failed: {safe_error}")
            stats["error"] = safe_error
            raise

    # Check if all downloads are being skipped
    skip_all_downloads = skip_downloads or (
        skip_prices
        and skip_fundamentals
        and skip_overviews
        and skip_economy
        and skip_ratios
        and skip_news
    )

    logger.info("=" * 60)
    if schema_only:
        logger.info("MODE: Schema Only (drop & recreate tables)")
    elif load_only:
        logger.info("MODE: Load Only (existing CSV data)")
    elif skip_all_downloads:
        logger.info("MODE: Load (skip downloads)")
    else:
        logger.info("MODE: Full Cold Start (download + load all data)")
    logger.info("=" * 60)

    # Calculate date range
    start_date, end_date = calculate_date_range(years=years)
    start_str = start_date.strftime(DATE_FORMAT)
    end_str = end_date.strftime(DATE_FORMAT)
    logger.info(f"Date range: {start_str} to {end_str}")

    # Initialize clients only if we need to download data
    client: PolygonClient | None = None
    s3_client: PolygonS3Client | None = None
    rate_limiter: SyncRateLimiter | None = None

    needs_download = not (schema_only or load_only or skip_all_downloads)
    if needs_download:
        if not api_key or not s3_access_key or not s3_secret_key:
            raise ValueError("API credentials required when downloading data")
        client = PolygonClient(api_key, logger)
        s3_client = PolygonS3Client(s3_access_key, s3_secret_key, logger)
        rate_limiter = SyncRateLimiter(DEFAULT_API_RATE_LIMIT)

    try:
        with psycopg.connect(database_url) as conn:
            # Schema setup (skip for load_only mode)
            if not load_only:
                pin_schema_search_path(conn)
                logger.info("\n[1/9] Database schema setup")
                if drop_tables:
                    if not confirm_destructive_drop(conn, "DELETE"):
                        stats["aborted"] = True
                        return stats

                    logger.info("  Dropping existing tables...")
                    if not drop_all_tables(
                        conn,
                        dry_run=False,
                        logger=logger,
                        commit=False,
                    ):
                        stats["error"] = "destructive schema cleanup failed"
                        return stats

                assert sql_files is not None
                _, failed_files = execute_sql_files_atomically(
                    conn,
                    sql_files,
                    dry_run=False,
                    logger=logger,
                    commit=False,
                )

                # Abort on any schema failure regardless of mode. Loading data
                # against a broken/partial schema would silently produce a
                # corrupt database, so fail fast with a clear error.
                if failed_files:
                    logger.error(
                        f"\n❌ Schema setup failed! "
                        f"{len(failed_files)} file(s) had errors:"
                    )
                    for fname in failed_files:
                        logger.error(f"   - {fname}")
                    stats["success"] = False
                    stats["failed_files"] = failed_files
                    return stats

                missing_objects = {
                    "tables": verify_tables(conn),
                    "views": verify_views(conn),
                    "materialized_views": verify_materialized_views(conn),
                }
                missing_objects = {
                    kind: names for kind, names in missing_objects.items() if names
                }
                if missing_objects:
                    conn.rollback()
                    stats["error"] = "schema failed required-object verification"
                    stats["missing_schema_objects"] = missing_objects
                    return stats
                conn.commit()
                logger.info("  Committed verified schema transaction atomically")

                # Schema-only mode: exit after a successful schema setup.
                if schema_only:
                    stats["success"] = True
                    logger.info("\n✅ Schema setup complete!")
                    return stats

            # If load_only or skip_all_downloads, just load existing CSV data
            if load_only or skip_all_downloads:
                logger.info("\n[2/9] Loading existing data (skipping downloads)")
                stats["symbols"] = 0
                stats["trading_days"] = 0

                # Load existing companies
                logger.info("\n[3/9] Loading companies from CSV")
                overviews_csv = output_dir / "overviews" / "overviews.csv"
                if overviews_csv.exists():
                    loaded_overviews = load_companies(conn, overviews_csv, logger)
                    stats["overviews"] = int(loaded_overviews)
                    if isinstance(loaded_overviews, PersistenceResult):
                        stats["overviews_persistence"] = loaded_overviews.summary()
                        if loaded_overviews.source_rows == 0 or int(loaded_overviews) == 0:
                            fatal_provider_steps.add("overviews")
                            degraded_reasons.append(
                                "required company cache contained no rows"
                            )
                        else:
                            require_complete_persistence(loaded_overviews)
                else:
                    logger.warning(f"  Not found: {overviews_csv}")
                    stats["overviews"] = 0
                    fatal_provider_steps.add("overviews")
                    degraded_reasons.append("required company cache was not found")

                cached_missing_csv = (
                    output_dir / "overviews" / "overviews_missing.csv"
                )
                if cached_missing_csv.exists():
                    loaded_cached_missing = load_companies(
                        conn,
                        cached_missing_csv,
                        logger,
                    )
                    stats["missing_companies_loaded"] = int(
                        loaded_cached_missing
                    )
                    if isinstance(loaded_cached_missing, PersistenceResult):
                        require_complete_persistence(loaded_cached_missing)

                # Check for missing companies before loading fundamentals/ratios
                logger.info("\n[4/9] Validating company records")
                tickers_in_data = get_tickers_from_csv_files(output_dir, logger)
                tickers_in_db = get_existing_tickers_from_db(conn)
                missing_tickers = tickers_in_data - tickers_in_db

                if missing_tickers:
                    logger.warning(
                        f"Found {len(missing_tickers)} cached-data tickers with no "
                        "company row"
                    )
                    logger.warning(
                        "  Offline load mode never calls providers; run a download "
                        "workflow to refresh missing company metadata"
                    )

                # Load existing prices
                logger.info("\n[5/9] Loading prices from CSV")
                prices_dir = output_dir / "prices"
                if prices_dir.exists():
                    loaded_prices = load_prices(conn, prices_dir, logger)
                    stats["prices"] = int(loaded_prices)
                    stats["prices_loaded"] = int(loaded_prices)
                    if isinstance(loaded_prices, PersistenceResult):
                        stats["prices_persistence"] = loaded_prices.summary()
                        if not loaded_prices.artifact_found or int(loaded_prices) == 0:
                            fatal_provider_steps.add("prices")
                            degraded_reasons.append(
                                "required price cache contained no rows"
                            )
                        else:
                            require_complete_persistence(
                                loaded_prices,
                                require_nonempty=True,
                            )
                else:
                    logger.warning(f"  Not found: {prices_dir}")
                    stats["prices"] = 0
                    stats["prices_loaded"] = 0
                    fatal_provider_steps.add("prices")
                    degraded_reasons.append("required price cache was not found")

                # Get valid tickers from companies table for filtering
                valid_tickers = get_existing_tickers_from_db(conn)
                logger.info(f"  {len(valid_tickers)} companies in database")

                # Load existing fundamentals
                logger.info("\n[6/9] Loading fundamentals from CSV")
                fundamentals_dir = output_dir / "fundamentals"
                if fundamentals_dir.exists():
                    loaded_fundamentals = load_fundamentals(
                        conn, fundamentals_dir, logger, valid_tickers
                    )
                    stats["fundamentals"] = {
                        table: int(load_result)
                        for table, load_result in loaded_fundamentals.items()
                    }
                    stats["fundamentals_persistence"] = {
                        table: load_result.summary()
                        for table, load_result in loaded_fundamentals.items()
                        if isinstance(load_result, PersistenceResult)
                    }
                    for load_result in loaded_fundamentals.values():
                        if isinstance(load_result, PersistenceResult):
                            require_complete_persistence(
                                load_result,
                                require_nonempty=True,
                            )
                else:
                    logger.warning(f"  Not found: {fundamentals_dir}")
                    stats["fundamentals"] = {}

                # Load existing ratios
                logger.info("\n[7/9] Loading ratios from CSV")
                ratios_csv = output_dir / "ratios" / "ratios.csv"
                if ratios_csv.exists():
                    loaded_ratios = load_ratios(
                        conn, ratios_csv, logger, valid_tickers
                    )
                    stats["ratios"] = int(loaded_ratios)
                    if isinstance(loaded_ratios, PersistenceResult):
                        require_complete_persistence(
                            loaded_ratios,
                            require_nonempty=True,
                        )
                        stats["ratios_persistence"] = loaded_ratios.summary()
                else:
                    logger.warning(f"  Not found: {ratios_csv}")
                    stats["ratios"] = 0

                # Load existing economy data
                logger.info("\n[8/9] Loading economy data from CSV")
                economy_dir = output_dir / "economy"
                if economy_dir.exists():
                    loaded_economy = load_economy(conn, economy_dir, logger)
                    stats["economy"] = {
                        table: int(load_result)
                        for table, load_result in loaded_economy.items()
                    }
                    stats["economy_persistence"] = {
                        table: load_result.summary()
                        for table, load_result in loaded_economy.items()
                        if isinstance(load_result, PersistenceResult)
                    }
                    for economy_existing_result in loaded_economy.values():
                        if (
                            isinstance(economy_existing_result, PersistenceResult)
                            and economy_existing_result.artifact_found
                        ):
                            require_complete_persistence(
                                economy_existing_result,
                                require_nonempty=True,
                            )
                else:
                    logger.warning(f"  Not found: {economy_dir}")
                    stats["economy"] = {}

            else:
                # These are guaranteed to be set when not skipping all downloads
                assert client is not None
                assert s3_client is not None

                # Step 2: Fetch or load symbols
                if symbols_file and symbols_file.exists():
                    logger.info(f"\n[2/9] Loading symbols from {symbols_file}")
                    symbols = []
                    with open(symbols_file) as f:
                        for line in f:
                            sym = line.strip()
                            if sym and not sym.startswith("#"):
                                symbols.append(sym)
                    logger.info(f"  Loaded {len(symbols)} symbols from file")
                else:
                    logger.info("\n[2/9] Fetching symbols from Wikipedia")
                    logger.info("  - Fetching S&P 500...")
                    sp500_symbols = fetch_sp500_symbols(logger)
                    logger.info("  - Loading NASDAQ-listed...")
                    nasdaq_symbols = fetch_nasdaq_listed_symbols(
                        logger,
                        api_key=api_key,
                    )

                    # Merge and deduplicate
                    symbols = list(set(sp500_symbols + nasdaq_symbols))
                    symbols.sort()
                    sp_count = len(sp500_symbols)
                    nq_count = len(nasdaq_symbols)
                    logger.info(
                        f"  Total unique symbols: {len(symbols)} (S&P 500: {sp_count}, "
                        f"NASDAQ-listed: {nq_count})"
                    )

                if not symbols:
                    logger.error("No symbols were resolved; aborting before data download")
                    stats["error"] = "no symbols resolved"
                    return stats

                normalized_symbols: list[str] = []
                invalid_symbols: list[str] = []
                for raw_symbol in symbols:
                    try:
                        normalized_symbols.append(validate_ticker(raw_symbol))
                    except (AttributeError, ValueError):
                        invalid_symbols.append(str(raw_symbol))
                if invalid_symbols:
                    logger.error(
                        f"Rejected {len(invalid_symbols)} invalid symbol(s); "
                        "aborting before provider or file access"
                    )
                    stats["error"] = "invalid symbols in resolved universe"
                    stats["invalid_symbols"] = invalid_symbols[:20]
                    stats["invalid_symbol_count"] = len(invalid_symbols)
                    return stats
                symbols = sorted(set(normalized_symbols))

                # Save symbols list
                output_symbols_file = output_dir / "symbols.txt"
                output_symbols_file.parent.mkdir(parents=True, exist_ok=True)
                with open(output_symbols_file, "w") as f:
                    for s in symbols:
                        f.write(f"{s}\n")
                stats["symbols"] = len(symbols)

                # Step 3: Get trading days
                logger.info("\n[3/9] Fetching trading days calendar")
                trading_days = client.get_trading_days(start_str, end_str)
                logger.info(f"  Found {len(trading_days)} trading days")
                stats["trading_days"] = len(trading_days)
                if not trading_days and not skip_prices:
                    fatal_provider_steps.add("trading_calendar")
                    degraded_reasons.append(
                        "trading calendar returned no dates for the backfill window"
                    )

                # Step 4: Download & load company overviews (FIRST - needed for FK constraints)
                if skip_overviews:
                    logger.info("\n[4/9] Skipping overviews (--skip-overviews)")
                    stats["overviews"] = 0
                else:
                    logger.info("\n[4/9] Downloading company data")
                    overview_count = download_overviews(
                        client, symbols, output_dir / "overviews", logger, rate_limiter
                    )
                    stats["overviews"] = overview_count
                    overview_all_failed = False
                    if isinstance(overview_count, DownloadCount):
                        stats["overviews_requests"] = overview_count.summary()
                        overview_artifact_written = overview_count.artifact_written
                        if overview_count.all_failed:
                            overview_all_failed = True
                            fatal_provider_steps.add("overviews")
                            degraded_reasons.append(
                                "all company overview provider requests failed"
                            )
                        elif overview_count.failed:
                            degraded_reasons.append(
                                "company overview provider requests partially failed"
                            )
                    else:
                        overview_artifact_written = int(overview_count) > 0
                    if symbols and not overview_artifact_written and not overview_all_failed:
                        fatal_provider_steps.add("overviews")
                        degraded_reasons.append(
                            "company overviews produced no fresh artifact"
                        )
                    if overview_artifact_written:
                        logger.info("Loading companies into database...")
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
                            stats["overviews_persistence"] = (
                                loaded_overviews.summary()
                            )

                # Step 5: Download & load prices
                if skip_prices:
                    logger.info("\n[5/9] Skipping prices (--skip-prices)")
                    stats["prices"] = 0
                else:
                    logger.info("\n[5/9] Downloading historical prices")
                    prices_dir = output_dir / "prices"
                    with tempfile.TemporaryDirectory(
                        prefix="sawa-price-stage-"
                    ) as staging_name:
                        staging_dir = Path(staging_name)
                        price_count = download_prices(
                            s3_client,
                            set(symbols),
                            start_date,
                            end_date,
                            trading_days,
                            staging_dir,
                            logger,
                        )
                        stats["prices"] = price_count
                        stats["price_requests"] = price_count.summary()
                        if price_count.has_source_failures:
                            fatal_provider_steps.add("prices")
                            degraded_reasons.append(
                                "historical price source was missing or failed for "
                                f"{len(price_count.missing_dates) + len(price_count.failed_dates)} "
                                "requested trading date(s)"
                            )
                        if price_count.requested_dates == 0 or int(price_count) == 0:
                            fatal_provider_steps.add("prices")
                            degraded_reasons.append(
                                "historical price backfill produced no fresh rows"
                            )
                        elif price_count.empty_filtered_dates:
                            degraded_reasons.append(
                                f"{len(price_count.empty_filtered_dates)} sourced trading "
                                "date(s) had no rows for the selected symbols"
                            )

                        if price_count:
                            logger.info("Loading fresh prices into database...")
                            loaded_prices = load_prices(conn, staging_dir, logger)
                            stats["prices_loaded"] = int(loaded_prices)
                            if isinstance(loaded_prices, PersistenceResult):
                                stats["prices_persistence"] = loaded_prices.summary()
                                require_complete_persistence(
                                    loaded_prices,
                                    expected_rows=int(price_count),
                                    require_nonempty=True,
                                )
                            elif loaded_prices < int(price_count):
                                fatal_provider_steps.add("prices")
                                degraded_reasons.append(
                                    f"persisted only {loaded_prices}/{int(price_count)} "
                                    "fresh historical price rows"
                                )
                            try:
                                _merge_price_artifacts(staging_dir, prices_dir)
                            except OSError as e:
                                stats["price_cache_error"] = redact_sensitive_text(e)
                                degraded_reasons.append(
                                    "fresh price cache update failed after database load"
                                )

                fresh_fundamental_tables: set[str] = set()
                ratio_artifact_written = False

                # Step 6: Download & load fundamentals
                if skip_fundamentals:
                    logger.info("\n[6/9] Skipping fundamentals (--skip-fundamentals)")
                    stats["fundamentals"] = {}
                else:
                    logger.info("\n[6/9] Downloading fundamentals")
                    fund_stats = download_fundamentals(
                        client,
                        symbols,
                        start_str,
                        end_str,
                        output_dir / "fundamentals",
                        logger,
                        rate_limiter,
                    )
                    stats["fundamentals"] = fund_stats
                    if isinstance(fund_stats, DownloadStats):
                        stats["fundamentals_requests"] = fund_stats.requests
                        fresh_fundamental_tables = fund_stats.artifacts
                        if fund_stats.failed_feeds:
                            fatal_provider_steps.add("fundamentals")
                            degraded_reasons.append(
                                "every request failed for fundamentals feed(s): "
                                + ", ".join(sorted(fund_stats.failed_feeds))
                            )
                        elif fund_stats.has_failures:
                            degraded_reasons.append(
                                "fundamentals provider requests partially failed"
                            )
                        if fund_stats.empty_feeds:
                            fatal_provider_steps.add("fundamentals")
                            degraded_reasons.append(
                                "fundamentals feeds returned no rows: "
                                + ", ".join(sorted(fund_stats.empty_feeds))
                            )
                    else:
                        fresh_fundamental_tables = {
                            FUNDAMENTAL_ENDPOINT_TABLES[endpoint]
                            for endpoint, rows in fund_stats.items()
                            if rows > 0 and endpoint in FUNDAMENTAL_ENDPOINT_TABLES
                        }

                # Step 7: Download ratios (download only, load later)
                if skip_ratios:
                    logger.info("\n[7/9] Skipping ratios (--skip-ratios)")
                    stats["ratios"] = 0
                else:
                    logger.info("\n[7/9] Downloading financial ratios")
                    ratio_count = download_ratios(
                        client, symbols, output_dir / "ratios", logger, rate_limiter
                    )
                    stats["ratios"] = ratio_count
                    if isinstance(ratio_count, DownloadCount):
                        stats["ratios_requests"] = ratio_count.summary()
                        ratio_artifact_written = ratio_count.artifact_written
                        if ratio_count.all_failed:
                            fatal_provider_steps.add("ratios")
                            degraded_reasons.append(
                                "all ratios provider requests failed"
                            )
                        elif ratio_count.failed:
                            degraded_reasons.append(
                                "ratios provider requests partially failed"
                            )
                        if ratio_count.empty_successful:
                            fatal_provider_steps.add("ratios")
                            degraded_reasons.append("ratios provider returned no rows")
                    else:
                        ratio_artifact_written = int(ratio_count) > 0

                # Check for tickers in downloaded data that aren't in companies table
                if not skip_fundamentals or not skip_ratios:
                    logger.info("\nChecking for missing company records...")
                    tickers_in_data = get_tickers_from_csv_files(
                        output_dir,
                        logger,
                        fundamental_tables=fresh_fundamental_tables,
                        include_ratios=ratio_artifact_written,
                    )
                    tickers_in_db = get_existing_tickers_from_db(conn)
                    missing_tickers = tickers_in_data - tickers_in_db

                    if missing_tickers:
                        logger.info(
                            f"Found {len(missing_tickers)} tickers in data not in companies table"
                        )
                        fetched = fetch_missing_companies(
                            client, missing_tickers, output_dir / "overviews", logger, rate_limiter
                        )
                        if fetched > 0:
                            # Load the missing companies
                            missing_csv = output_dir / "overviews" / "overviews_missing.csv"
                            if missing_csv.exists():
                                logger.info("Loading missing companies into database...")
                                loaded_missing = load_companies(
                                    conn, missing_csv, logger
                                )
                                stats["missing_companies_loaded"] = int(
                                    loaded_missing
                                )
                                if isinstance(loaded_missing, PersistenceResult):
                                    require_complete_persistence(
                                        loaded_missing,
                                        expected_rows=int(fetched),
                                    )

                # Now load fundamentals and ratios (FK constraints should be satisfied)
                # Get valid tickers from companies table for filtering
                valid_tickers = get_existing_tickers_from_db(conn)
                logger.info(f"  {len(valid_tickers)} companies in database for FK filtering")

                if not skip_fundamentals and fresh_fundamental_tables:
                    logger.info("Loading fundamentals into database...")
                    loaded_fundamentals = load_fundamentals(
                        conn,
                        output_dir / "fundamentals",
                        logger,
                        valid_tickers,
                        only_tables=fresh_fundamental_tables,
                    )
                    if isinstance(loaded_fundamentals, dict):
                        stats["fundamentals_loaded"] = {
                            table: int(load_result)
                            for table, load_result in loaded_fundamentals.items()
                        }
                        stats["fundamentals_persistence"] = {
                            table: load_result.summary()
                            for table, load_result in loaded_fundamentals.items()
                            if isinstance(load_result, PersistenceResult)
                        }
                        for endpoint, table in FUNDAMENTAL_ENDPOINT_TABLES.items():
                            if table not in fresh_fundamental_tables:
                                continue
                            fund_load_result = loaded_fundamentals.get(table)
                            if fund_load_result is None:
                                raise RuntimeError(
                                    f"Fresh {table} artifact was not loaded"
                                )
                            if isinstance(fund_load_result, PersistenceResult):
                                require_complete_persistence(
                                    fund_load_result,
                                    expected_rows=int(fund_stats.get(endpoint, 0)),
                                )

                if not skip_ratios and ratio_artifact_written:
                    logger.info("Loading ratios into database...")
                    loaded_ratios = load_ratios(
                        conn,
                        output_dir / "ratios" / "ratios.csv",
                        logger,
                        valid_tickers,
                    )
                    if isinstance(loaded_ratios, PersistenceResult):
                        require_complete_persistence(
                            loaded_ratios,
                            expected_rows=int(ratio_count),
                        )
                        stats["ratios_loaded"] = int(loaded_ratios)
                        stats["ratios_persistence"] = loaded_ratios.summary()

                # Step 8: Download & load economy data
                if skip_economy:
                    logger.info("\n[8/10] Skipping economy data (--skip-economy)")
                    stats["economy"] = {}
                else:
                    logger.info("\n[8/10] Downloading economy data")
                    econ_stats = download_economy(
                        client, start_str, end_str, output_dir / "economy", logger
                    )
                    stats["economy"] = econ_stats
                    if isinstance(econ_stats, DownloadStats):
                        stats["economy_requests"] = econ_stats.requests
                        fresh_economy_tables = econ_stats.artifacts
                        if econ_stats.failed_feeds:
                            fatal_provider_steps.add("economy")
                            degraded_reasons.append(
                                "every request failed for economy feed(s): "
                                + ", ".join(sorted(econ_stats.failed_feeds))
                            )
                        elif econ_stats.has_failures:
                            degraded_reasons.append(
                                "economy provider requests partially failed"
                            )
                        if econ_stats.empty_feeds:
                            fatal_provider_steps.add("economy")
                            degraded_reasons.append(
                                "economy feeds returned no rows: "
                                + ", ".join(sorted(econ_stats.empty_feeds))
                            )
                    else:
                        fresh_economy_tables = {
                            ECONOMY_ENDPOINT_TABLES[endpoint]
                            for endpoint, rows in econ_stats.items()
                            if rows > 0 and endpoint in ECONOMY_ENDPOINT_TABLES
                        }
                    if fresh_economy_tables:
                        logger.info("Loading economy data into database...")
                        loaded_economy = load_economy(
                            conn,
                            output_dir / "economy",
                            logger,
                            only_tables=fresh_economy_tables,
                        )
                        if isinstance(loaded_economy, dict):
                            stats["economy_loaded"] = {
                                table: int(load_result)
                                for table, load_result in loaded_economy.items()
                            }
                            stats["economy_persistence"] = {
                                table: load_result.summary()
                                for table, load_result in loaded_economy.items()
                                if isinstance(load_result, PersistenceResult)
                            }
                            expected_by_table = {
                                ECONOMY_ENDPOINT_TABLES[endpoint]: int(rows)
                                for endpoint, rows in econ_stats.items()
                                if endpoint in ECONOMY_ENDPOINT_TABLES
                                and ECONOMY_ENDPOINT_TABLES[endpoint]
                                in fresh_economy_tables
                            }
                            for table in fresh_economy_tables:
                                economy_load_result = loaded_economy.get(table)
                                if economy_load_result is None:
                                    raise RuntimeError(
                                        f"Fresh {table} artifact was not loaded"
                                    )
                                if isinstance(
                                    economy_load_result, PersistenceResult
                                ):
                                    require_complete_persistence(
                                        economy_load_result,
                                        expected_rows=expected_by_table.get(
                                            table, 0
                                        ),
                                    )

                # Step 8b: Download & load market internals from FRED
                import os as _os

                fred_api_key = _os.environ.get("FRED_API_KEY")
                if fred_api_key:
                    logger.info("\n[8b/10] Downloading market internals from FRED")
                    fred_client = FredClient(fred_api_key, logger)
                    try:
                        mi_result = fred_client.get_market_internals(start_str, end_str)
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
                            degraded_reasons.append(reason)
                            logger.warning(f"  {reason}")
                            get_notifier(logger).send(
                                title="Sawa: coldstart market internals degraded",
                                body=(
                                    "The FRED market-internals fetch was incomplete.\n"
                                    f"Failed series: {failure_summary}\n\n"
                                    "Available series were still loaded; missing values "
                                    "preserve previous database values."
                                ),
                                level=NotificationLevel.WARNING,
                                tags=["warning", "coldstart", "market_internals"],
                            )
                        if mi_rows:
                            loaded = load_market_internals(conn, mi_rows, logger)
                            stats["market_internals"] = loaded

                            # Also save CSV for future load-only runs
                            mi_dir = output_dir / "economy"
                            mi_dir.mkdir(parents=True, exist_ok=True)
                            from sawa.utils.csv_utils import write_csv_auto_fields

                            write_csv_auto_fields(mi_dir / "market_internals.csv", mi_rows, logger)
                        else:
                            stats["market_internals"] = 0
                    finally:
                        fred_client.close()
                else:
                    from sawa.utils import alert_missing_api_key

                    logger.info("\n[8b/10] Market internals: skipping")
                    alert_missing_api_key(
                        "FRED_API_KEY",
                        "FRED market internals (VIX, VIX3M, HY spread)",
                        logger,
                    )
                    stats["market_internals_skipped"] = "FRED_API_KEY not set"
                    degraded_reasons.append(
                        "market internals skipped (FRED_API_KEY not set)"
                    )

                # Step 9: Download & load news
                if skip_news:
                    logger.info("\n[9/10] Skipping news (--skip-news)")
                    stats["news"] = 0
                else:
                    logger.info("\n[9/10] Downloading news articles")
                    news_result = load_news(
                        conn,
                        client,
                        symbols,
                        days=DEFAULT_NEWS_DAYS,
                    )
                    stats["news"] = int(news_result)
                    if isinstance(news_result, NewsLoadResult):
                        stats["news_requests"] = news_result.summary()
                        if news_result.all_requests_failed:
                            fatal_provider_steps.add("news")
                            degraded_reasons.append(
                                "all news provider requests failed"
                            )
                        elif news_result.failed:
                            degraded_reasons.append(
                                "news provider requests partially failed"
                            )
                        if news_result.no_articles_fetched:
                            fatal_provider_steps.add("news")
                            degraded_reasons.append(
                                "news provider returned no articles for the "
                                "requested universe"
                            )
                        if news_result.total_persistence_failure:
                            fatal_provider_steps.add("news")
                            degraded_reasons.append(
                                "news persistence rejected every fetched article ("
                                f"{news_result.rejected_articles} article(s)"
                                ")"
                            )
                        elif news_result.partial_persistence_failure:
                            degraded_reasons.append(
                                "news persistence partially rejected articles"
                            )
                    elif int(news_result) <= 0:
                        fatal_provider_steps.add("news")
                        degraded_reasons.append(
                            "news loader returned no typed outcome or rows"
                        )

            # Loading cached artifacts must not quietly make fresh external
            # requests. Index membership stays unchanged in offline modes.
            if load_only or skip_all_downloads:
                stats["indices"] = {"skipped": "offline load mode"}
            elif symbols_file is not None:
                # A caller-selected subset cannot truthfully represent complete
                # market indices. Leave membership unchanged/empty explicitly.
                stats["indices"] = {"skipped": "custom symbol universe"}
            else:
                logger.info("\nPopulating index constituents...")
                index_stats = populate_index_constituents(
                    conn,
                    logger,
                    api_key=api_key,
                )
                if isinstance(index_stats, IndexPopulationResult):
                    stats["indices"] = index_stats.summary()
                    index_failures = index_stats.failures
                else:
                    # Preserve compatibility with wrappers that return the
                    # historical plain count mapping.
                    stats["indices"] = dict(index_stats)
                    index_failures = {}
                if index_failures:
                    fatal_provider_steps.add("indices")
                    degraded_reasons.append(
                        "index constituent refresh failed for: "
                        + ", ".join(sorted(index_failures))
                    )

        stats["degraded"] = bool(degraded_reasons)
        if degraded_reasons:
            stats["degraded_reasons"] = degraded_reasons
        stats["success"] = not fatal_provider_steps
        if fatal_provider_steps:
            stats["fatal_reasons"] = [
                f"provider step failed ({name})"
                for name in sorted(fatal_provider_steps)
            ]
        logger.info("\n" + "=" * 60)
        logger.info(
            "COLD START COMPLETE" + (" (DEGRADED)" if degraded_reasons else "")
        )
        logger.info("=" * 60)
        if degraded_reasons:
            logger.warning("  DEGRADED: " + "; ".join(degraded_reasons))

    except Exception as e:
        safe_error = f"{type(e).__name__}: {redact_sensitive_text(e)}"
        logger.error(f"Cold start failed: {safe_error}")
        stats["error"] = safe_error
        raise

    return stats
