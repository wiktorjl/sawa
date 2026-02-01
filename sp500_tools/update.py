"""
Incremental update: Pull new data since last update.

Checks the last date in the database and pulls only new data.
"""

import csv
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from sp500_tools.api import PolygonClient, PolygonS3Client
from sp500_tools.utils import setup_logging
from sp500_tools.utils.dates import DATE_FORMAT


def get_last_date(conn, table: str, date_column: str = "date") -> date | None:
    """Get the most recent date from a table."""
    query = sql.SQL("SELECT MAX({}) FROM {}").format(
        sql.Identifier(date_column),
        sql.Identifier(table),
    )
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchone()
        if result and result[0]:
            return result[0]
    return None


def get_symbols_from_db(conn) -> list[str]:
    """Get list of symbols from companies table."""
    with conn.cursor() as cur:
        cur.execute("SELECT ticker FROM companies ORDER BY ticker")
        return [row[0] for row in cur.fetchall()]


def update_prices(
    s3_client: PolygonS3Client,
    symbols: set[str],
    start_date: date,
    end_date: date,
    trading_days: list[str],
    output_dir: Path,
    logger: logging.Logger,
) -> int:
    """Download new prices and append to existing files."""
    logger.info(f"Updating prices from {start_date} to {end_date}...")
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

    logger.info(f"Downloaded {total_records} new price records")
    return total_records


def update_fundamentals(
    client: PolygonClient,
    symbols: list[str],
    start_date: str,
    end_date: str,
    output_dir: Path,
    logger: logging.Logger,
) -> dict[str, int]:
    """Download new fundamentals data."""
    endpoints = ["balance-sheets", "cash-flow", "income-statements"]
    stats: dict[str, int] = {}

    output_dir.mkdir(parents=True, exist_ok=True)

    for endpoint in endpoints:
        logger.info(f"Updating {endpoint}...")
        all_data: list[dict[str, Any]] = []

        for i, symbol in enumerate(symbols, 1):
            if i % 100 == 0:
                logger.info(f"  Progress: {i}/{len(symbols)}")
            try:
                data = client.get_fundamentals(
                    endpoint, ticker=symbol, start_date=start_date, end_date=end_date
                )
                # Clean up tickers field - API returns list like ['AAPL'], we want 'AAPL'
                for record in data:
                    if "tickers" in record and isinstance(record["tickers"], list):
                        record["tickers"] = record["tickers"][0] if record["tickers"] else ""
                all_data.extend(data)
            except Exception as e:
                logger.debug(f"  {symbol}: {e}")

        if all_data:
            filepath = output_dir / f"{endpoint.replace('-', '_')}_update.csv"
            # Collect all fieldnames
            all_fields: set[str] = set()
            for record in all_data:
                all_fields.update(record.keys())
            fieldnames = sorted(all_fields)
            with open(filepath, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(all_data)
            logger.info(f"  Found {len(all_data)} new records")

        stats[endpoint] = len(all_data)

    return stats


def update_economy(
    client: PolygonClient,
    start_date: str,
    end_date: str,
    output_dir: Path,
    logger: logging.Logger,
) -> dict[str, int]:
    """Download new economy data."""
    endpoints = ["treasury-yields", "inflation", "inflation-expectations", "labor-market"]
    stats: dict[str, int] = {}

    output_dir.mkdir(parents=True, exist_ok=True)

    for endpoint in endpoints:
        logger.info(f"Updating {endpoint}...")
        try:
            data = client.get_economy_data(endpoint, start_date, end_date)
            if data:
                filepath = output_dir / f"{endpoint.replace('-', '_')}_update.csv"
                # Collect all fieldnames
                all_fields: set[str] = set()
                for record in data:
                    all_fields.update(record.keys())
                fieldnames = sorted(all_fields)
                with open(filepath, "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                    writer.writeheader()
                    writer.writerows(data)
                logger.info(f"  Found {len(data)} new records")
            stats[endpoint] = len(data)
        except Exception as e:
            logger.error(f"  Failed: {e}")
            stats[endpoint] = 0

    return stats


def run_update(
    api_key: str,
    s3_access_key: str,
    s3_secret_key: str,
    database_url: str,
    output_dir: Path,
    force_from_date: date | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Run incremental update.

    Args:
        api_key: Polygon/Massive API key
        s3_access_key: Polygon S3 access key
        s3_secret_key: Polygon S3 secret key
        database_url: PostgreSQL connection URL
        output_dir: Directory to save downloaded data
        force_from_date: Optional date to force update from
        logger: Logger instance

    Returns:
        Statistics dictionary
    """
    logger = logger or setup_logging()
    stats: dict[str, Any] = {"success": False}

    logger.info("=" * 60)
    logger.info("INCREMENTAL UPDATE")
    logger.info("=" * 60)

    # Initialize clients
    client = PolygonClient(api_key, logger)
    s3_client = PolygonS3Client(s3_access_key, s3_secret_key, logger)

    try:
        with psycopg.connect(database_url) as conn:
            # Get last dates from various tables
            logger.info("Checking last update dates...")

            last_price_date = get_last_date(conn, "stock_prices")
            last_treasury_date = get_last_date(conn, "treasury_yields")
            last_fundamental_date = get_last_date(conn, "balance_sheets", "period_end")

            if force_from_date:
                start_date = force_from_date
                logger.info(f"  Forcing update from: {start_date}")
            elif last_price_date:
                start_date = last_price_date + timedelta(days=1)
                logger.info(f"  Last price date: {last_price_date}")
                logger.info(f"  Starting from: {start_date}")
            else:
                logger.error("No existing data found. Run cold start first.")
                return stats

            end_date = date.today()
            start_str = start_date.strftime(DATE_FORMAT)
            end_str = end_date.strftime(DATE_FORMAT)

            if start_date > end_date:
                logger.info("Already up to date!")
                stats["success"] = True
                stats["message"] = "Already up to date"
                return stats

            # Get symbols
            symbols = get_symbols_from_db(conn)
            if not symbols:
                logger.error("No symbols in database. Run cold start first.")
                return stats
            logger.info(f"Found {len(symbols)} symbols in database")
            stats["symbols"] = len(symbols)

        # Get trading days for the update period
        logger.info(f"\nGetting trading days from {start_str} to {end_str}...")
        trading_days = client.get_trading_days(start_str, end_str)
        logger.info(f"  Found {len(trading_days)} trading days")
        stats["trading_days"] = len(trading_days)

        if not trading_days:
            logger.info("No new trading days. Nothing to update.")
            stats["success"] = True
            stats["message"] = "No new trading days"
            return stats

        # Update prices
        logger.info("\n[1/3] Updating prices...")
        prices_dir = output_dir / "prices"
        price_count = update_prices(
            s3_client, set(symbols), start_date, end_date, trading_days, prices_dir, logger
        )
        stats["prices"] = price_count

        # Update fundamentals (check from last fundamental date)
        logger.info("\n[2/3] Updating fundamentals...")
        fund_start = (
            last_fundamental_date.strftime(DATE_FORMAT) if last_fundamental_date else start_str
        )
        fund_stats = update_fundamentals(
            client, symbols, fund_start, end_str, output_dir / "fundamentals", logger
        )
        stats["fundamentals"] = fund_stats

        # Update economy data (check from last treasury date)
        logger.info("\n[3/3] Updating economy data...")
        econ_start = last_treasury_date.strftime(DATE_FORMAT) if last_treasury_date else start_str
        econ_stats = update_economy(client, econ_start, end_str, output_dir / "economy", logger)
        stats["economy"] = econ_stats

        stats["success"] = True
        logger.info("\n" + "=" * 60)
        logger.info("UPDATE COMPLETE")
        logger.info("=" * 60)
        logger.info(f"  Prices: {price_count} records")
        logger.info(f"  Fundamentals: {sum(fund_stats.values())} records")
        logger.info(f"  Economy: {sum(econ_stats.values())} records")

    except Exception as e:
        logger.error(f"Update failed: {e}")
        stats["error"] = str(e)
        raise

    return stats
