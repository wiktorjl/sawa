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
import html
import logging
import re
import urllib.error
import urllib.request
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from sawa.api import PolygonClient, PolygonS3Client
from sawa.database.load import (
    load_companies,
    load_economy,
    load_fundamentals,
    load_news,
    load_prices,
    load_ratios,
)
from sawa.database.schema import drop_all_tables, execute_sql_file, get_sql_files
from sawa.repositories.rate_limiter import SyncRateLimiter
from sawa.utils import calculate_date_range, setup_logging
from sawa.utils.constants import DEFAULT_API_RATE_LIMIT, DEFAULT_NEWS_DAYS
from sawa.utils.csv_utils import write_csv_auto_fields
from sawa.utils.dates import DATE_FORMAT

# Wikipedia URL for S&P 500 constituents
WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
WIKIPEDIA_HEADERS = {"User-Agent": "SP500-Data-Downloader/1.0"}


def fetch_sp500_symbols(logger: logging.Logger) -> list[str]:
    """Fetch S&P 500 symbols from Wikipedia."""
    logger.info("Fetching S&P 500 symbols from Wikipedia...")

    request = urllib.request.Request(WIKIPEDIA_URL, headers=WIKIPEDIA_HEADERS)
    with urllib.request.urlopen(request, timeout=30) as response:
        content = response.read().decode("utf-8")

    soup = BeautifulSoup(content, "html.parser")
    table = soup.find("table", {"id": "constituents"})
    if not table:
        raise ValueError("Could not find constituents table")

    symbols: list[str] = []
    seen: set[str] = set()

    for row in table.find_all("tr")[1:]:
        cells = row.find_all("td")
        if cells:
            text = html.unescape(cells[0].get_text())
            text = text.replace("\xa0", " ")
            text = re.sub(r"\[[^\]]*\]", "", text).strip()
            if text and text not in seen:
                symbols.append(text)
                seen.add(text)

    logger.info(f"Found {len(symbols)} symbols")
    return symbols


def download_prices(
    s3_client: PolygonS3Client,
    symbols: set[str],
    start_date: date,
    end_date: date,
    trading_days: list[str],
    output_dir: Path,
    logger: logging.Logger,
) -> int:
    """Download historical prices from S3."""
    logger.info(f"Downloading prices from {start_date} to {end_date}...")
    output_dir.mkdir(parents=True, exist_ok=True)

    trading_set = set(trading_days)
    total_records = 0

    current = start_date
    while current <= end_date:
        date_str = current.strftime(DATE_FORMAT)
        if date_str in trading_set:
            logger.info(f"  {date_str}...")
            records = s3_client.download_and_parse(current, symbols)
            if records:
                # Append to per-symbol files
                for record in records:
                    sym = record["symbol"]
                    filepath = output_dir / f"{sym}.csv"
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
        current += timedelta(days=1)

    logger.info(f"Downloaded {total_records} price records")
    return total_records


def download_fundamentals(
    client: PolygonClient,
    symbols: list[str],
    start_date: str,
    end_date: str,
    output_dir: Path,
    logger: logging.Logger,
    rate_limiter: SyncRateLimiter | None = None,
) -> dict[str, int]:
    """Download fundamentals data."""
    endpoints = ["balance-sheets", "cash-flow", "income-statements"]
    stats: dict[str, int] = {}

    for endpoint in endpoints:
        logger.info(f"Downloading {endpoint}...")
        output_dir.mkdir(parents=True, exist_ok=True)

        all_data: list[dict[str, Any]] = []
        for i, symbol in enumerate(symbols, 1):
            if i % 50 == 0:
                logger.info(f"  Progress: {i}/{len(symbols)}")
            try:
                if rate_limiter:
                    rate_limiter.acquire()
                data = client.get_fundamentals(
                    endpoint, ticker=symbol, start_date=start_date, end_date=end_date
                )
                # Clean up tickers field - API returns list like ['AAPL'], we want 'AAPL'
                for record in data:
                    if "tickers" in record and isinstance(record["tickers"], list):
                        record["tickers"] = record["tickers"][0] if record["tickers"] else ""
                all_data.extend(data)
            except Exception as e:
                logger.warning(f"  {symbol}: {e}")

        if all_data:
            filepath = output_dir / f"{endpoint.replace('-', '_')}.csv"
            write_csv_auto_fields(filepath, all_data, logger)

        stats[endpoint] = len(all_data)

    return stats


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
) -> dict[str, int]:
    """Download economy data."""
    endpoints = ["treasury-yields", "inflation", "inflation-expectations", "labor-market"]
    stats: dict[str, int] = {}

    output_dir.mkdir(parents=True, exist_ok=True)

    for endpoint in endpoints:
        logger.info(f"Downloading {endpoint}...")
        try:
            data = client.get_economy_data(endpoint, start_date, end_date)
            if data:
                filepath = output_dir / f"{endpoint.replace('-', '_')}.csv"
                write_csv_auto_fields(filepath, data, logger)
            stats[endpoint] = len(data)
        except Exception as e:
            logger.error(f"  Failed: {e}")
            stats[endpoint] = 0

    return stats


def download_ratios(
    client: PolygonClient,
    symbols: list[str],
    output_dir: Path,
    logger: logging.Logger,
    rate_limiter: SyncRateLimiter | None = None,
) -> int:
    """Download financial ratios."""
    logger.info("Downloading financial ratios...")
    output_dir.mkdir(parents=True, exist_ok=True)

    all_ratios: list[dict[str, Any]] = []
    for i, symbol in enumerate(symbols, 1):
        if i % 50 == 0:
            logger.info(f"  Progress: {i}/{len(symbols)}")
        try:
            if rate_limiter:
                rate_limiter.acquire()
            ratios = client.get_ratios(symbol)
            for r in ratios:
                r["ticker"] = symbol
            all_ratios.extend(ratios)
        except Exception as e:
            logger.warning(f"  {symbol}: {e}")

    if all_ratios:
        filepath = output_dir / "ratios.csv"
        write_csv_auto_fields(filepath, all_ratios, logger)

    return len(all_ratios)


def run_coldstart(
    api_key: str | None,
    s3_access_key: str | None,
    s3_secret_key: str | None,
    database_url: str,
    schema_dir: Path,
    output_dir: Path,
    years: int = 5,
    symbols_file: Path | None = None,
    drop_tables: bool = True,
    drop_only: bool = False,
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

    # Determine mode
    if drop_only:
        logger.info("=" * 60)
        logger.info("DROP ONLY MODE - Dropping tables")
        logger.info("=" * 60)
        try:
            with psycopg.connect(database_url) as conn:
                logger.info("Dropping all tables...")
                drop_all_tables(conn, dry_run=False, logger=logger)

            stats["success"] = True
            logger.info("Drop complete!")
            return stats
        except Exception as e:
            logger.error(f"Drop failed: {e}")
            stats["error"] = str(e)
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
        logger.info("SCHEMA ONLY MODE - Setting up database")
    elif load_only:
        logger.info("LOAD ONLY MODE - Loading existing data")
    elif skip_all_downloads:
        logger.info("COLD START - Loading existing data")
    else:
        logger.info("COLD START - Full Database Setup")
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
                logger.info("\n[1/9] Setting up database...")
                if drop_tables:
                    logger.info("  Dropping existing tables...")
                    drop_all_tables(conn, dry_run=False, logger=logger)

                sql_files = get_sql_files(schema_dir)
                for sql_file in sql_files:
                    execute_sql_file(conn, sql_file, dry_run=False, logger=logger)

                # Schema-only mode: exit after schema setup
                if schema_only:
                    stats["success"] = True
                    logger.info("\nSchema setup complete!")
                    return stats

            # If load_only or skip_all_downloads, just load existing CSV data
            if load_only or skip_all_downloads:
                logger.info("\n[2/9] Skipping downloads, loading existing data...")
                stats["symbols"] = 0
                stats["trading_days"] = 0

                # Load existing companies
                logger.info("\n[3/9] Loading existing companies...")
                overviews_csv = output_dir / "overviews" / "overviews.csv"
                if overviews_csv.exists():
                    load_companies(conn, overviews_csv, logger)
                    stats["overviews"] = 1
                else:
                    logger.warning(f"  Not found: {overviews_csv}")
                    stats["overviews"] = 0

                # Load existing prices
                logger.info("\n[4/9] Loading existing prices...")
                prices_dir = output_dir / "prices"
                if prices_dir.exists():
                    load_prices(conn, prices_dir, logger)
                    stats["prices"] = 1
                else:
                    logger.warning(f"  Not found: {prices_dir}")
                    stats["prices"] = 0

                # Load existing fundamentals
                logger.info("\n[5/9] Loading existing fundamentals...")
                fundamentals_dir = output_dir / "fundamentals"
                if fundamentals_dir.exists():
                    load_fundamentals(conn, fundamentals_dir, logger)
                    stats["fundamentals"] = {"loaded": True}
                else:
                    logger.warning(f"  Not found: {fundamentals_dir}")
                    stats["fundamentals"] = {}

                # Load existing ratios
                logger.info("\n[6/9] Loading existing ratios...")
                ratios_csv = output_dir / "ratios" / "ratios.csv"
                if ratios_csv.exists():
                    load_ratios(conn, ratios_csv, logger)
                    stats["ratios"] = 1
                else:
                    logger.warning(f"  Not found: {ratios_csv}")
                    stats["ratios"] = 0

                # Load existing economy data
                logger.info("\n[7/9] Loading existing economy data...")
                economy_dir = output_dir / "economy"
                if economy_dir.exists():
                    load_economy(conn, economy_dir, logger)
                    stats["economy"] = {"loaded": True}
                else:
                    logger.warning(f"  Not found: {economy_dir}")
                    stats["economy"] = {}

            else:
                # These are guaranteed to be set when not skipping all downloads
                assert client is not None
                assert s3_client is not None

                # Step 2: Fetch or load symbols
                if symbols_file and symbols_file.exists():
                    logger.info(f"\n[2/9] Loading symbols from {symbols_file}...")
                    symbols = []
                    with open(symbols_file) as f:
                        for line in f:
                            sym = line.strip()
                            if sym and not sym.startswith("#"):
                                symbols.append(sym)
                    logger.info(f"  Loaded {len(symbols)} symbols from file")
                else:
                    logger.info("\n[2/9] Fetching S&P 500 symbols from Wikipedia...")
                    symbols = fetch_sp500_symbols(logger)

                # Save symbols list
                output_symbols_file = output_dir / "sp500_symbols.txt"
                output_symbols_file.parent.mkdir(parents=True, exist_ok=True)
                with open(output_symbols_file, "w") as f:
                    for s in symbols:
                        f.write(f"{s}\n")
                stats["symbols"] = len(symbols)

                # Step 3: Get trading days
                logger.info("\n[3/9] Getting trading days...")
                trading_days = client.get_trading_days(start_str, end_str)
                logger.info(f"  Found {len(trading_days)} trading days")
                stats["trading_days"] = len(trading_days)

                # Step 4: Download & load company overviews (FIRST - needed for FK constraints)
                if skip_overviews:
                    logger.info("\n[4/9] Skipping overviews (--skip-overviews)")
                    stats["overviews"] = 0
                else:
                    logger.info("\n[4/9] Downloading company overviews...")
                    overview_count = download_overviews(
                        client, symbols, output_dir / "overviews", logger, rate_limiter
                    )
                    stats["overviews"] = overview_count
                    # Load companies into DB
                    logger.info("Loading companies into database...")
                    load_companies(conn, output_dir / "overviews" / "overviews.csv", logger)

                # Step 5: Download & load prices
                if skip_prices:
                    logger.info("\n[5/9] Skipping prices (--skip-prices)")
                    stats["prices"] = 0
                else:
                    logger.info("\n[5/9] Downloading historical prices...")
                    prices_dir = output_dir / "prices"
                    price_count = download_prices(
                        s3_client,
                        set(symbols),
                        start_date,
                        end_date,
                        trading_days,
                        prices_dir,
                        logger,
                    )
                    stats["prices"] = price_count
                    # Load prices into DB
                    logger.info("Loading prices into database...")
                    load_prices(conn, prices_dir, logger)

                # Step 6: Download & load fundamentals
                if skip_fundamentals:
                    logger.info("\n[6/9] Skipping fundamentals (--skip-fundamentals)")
                    stats["fundamentals"] = {}
                else:
                    logger.info("\n[6/9] Downloading fundamentals...")
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
                    # Load fundamentals into DB
                    logger.info("Loading fundamentals into database...")
                    load_fundamentals(conn, output_dir / "fundamentals", logger)

                # Step 7: Download & load ratios
                if skip_ratios:
                    logger.info("\n[7/9] Skipping ratios (--skip-ratios)")
                    stats["ratios"] = 0
                else:
                    logger.info("\n[7/9] Downloading financial ratios...")
                    ratio_count = download_ratios(
                        client, symbols, output_dir / "ratios", logger, rate_limiter
                    )
                    stats["ratios"] = ratio_count
                    # Load ratios into DB
                    logger.info("Loading ratios into database...")
                    load_ratios(conn, output_dir / "ratios" / "ratios.csv", logger)

                # Step 8: Download & load economy data
                if skip_economy:
                    logger.info("\n[8/9] Skipping economy data (--skip-economy)")
                    stats["economy"] = {}
                else:
                    logger.info("\n[8/9] Downloading economy data...")
                    econ_stats = download_economy(
                        client, start_str, end_str, output_dir / "economy", logger
                    )
                    stats["economy"] = econ_stats
                    # Load economy into DB
                    logger.info("Loading economy data into database...")
                    load_economy(conn, output_dir / "economy", logger)

                # Step 9: Download & load news
                if skip_news:
                    logger.info("\n[9/9] Skipping news (--skip-news)")
                    stats["news"] = 0
                else:
                    logger.info("\n[9/9] Downloading news articles...")
                    news_count = load_news(
                        conn, client, symbols, days=DEFAULT_NEWS_DAYS, logger=logger
                    )
                    stats["news"] = news_count

        stats["success"] = True
        logger.info("\n" + "=" * 60)
        logger.info("COLD START COMPLETE")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Cold start failed: {e}")
        stats["error"] = str(e)
        raise

    return stats
