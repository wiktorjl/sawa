#!/usr/bin/env python3
"""
Download stock fundamentals from Massive API.

This script downloads various financial fundamentals for stock symbols:
- Balance Sheets: assets, liabilities, equity
- Cash Flow Statements: operating, investing, financing cash flows
- Income Statements: revenue, expenses, net income, EPS
- Short Interest: bi-monthly short position data
- Short Volume: daily short sale volume
- Float: free float shares

Usage:
    python download_fundamentals.py --endpoint balance-sheets --symbols-file sp500_symbols.txt
    python download_fundamentals.py --endpoint short-interest --ticker AAPL --years 2
    python download_fundamentals.py --endpoint income-statements --timeframe annual --continue

Output:
    One CSV per ticker: fundamentals_data/<TICKER>_<endpoint>.csv

Environment Variables:
    MASSIVE_API_KEY - Your Massive API key (required)
"""

import argparse
import csv
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any
from urllib.parse import urljoin

import requests


# Configuration
MASSIVE_API_KEY_ENV = "MASSIVE_API_KEY"
MASSIVE_BASE_URL = "https://api.massive.com"

DEFAULT_YEARS = 5
DATE_FORMAT = "%Y-%m-%d"
DEFAULT_OUTPUT_DIR = "data/fundamentals"
DEFAULT_LIMIT = 10000

# Endpoint configurations
ENDPOINTS = {
    "balance-sheets": {
        "path": "/stocks/financials/v1/balance-sheets",
        "ticker_param": "tickers",
        "timeframes": ["quarterly", "annual"],
        "ticker_specific": True,
        "date_field": "period_end",
    },
    "cash-flow": {
        "path": "/stocks/financials/v1/cash-flow-statements",
        "ticker_param": "tickers",
        "timeframes": ["quarterly", "annual", "trailing_twelve_months"],
        "ticker_specific": True,
        "date_field": "period_end",
    },
    "income-statements": {
        "path": "/stocks/financials/v1/income-statements",
        "ticker_param": "tickers",
        "timeframes": ["quarterly", "annual", "trailing_twelve_months"],
        "ticker_specific": True,
        "date_field": "period_end",
    },
    "short-interest": {
        "path": "/stocks/v1/short-interest",
        "ticker_param": "ticker",
        "timeframes": [],
        "ticker_specific": True,
        "date_field": "settlement_date",
    },
    "short-volume": {
        "path": "/stocks/v1/short-volume",
        "ticker_param": "ticker",
        "timeframes": [],
        "ticker_specific": True,
        "date_field": "date",
    },
    "float": {
        "path": "/stocks/vX/float",
        "ticker_param": "ticker",
        "timeframes": [],
        "ticker_specific": True,
        "date_field": "effective_date",
    },
}


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure logging with timestamps and appropriate level."""
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    return logging.getLogger(__name__)


def get_api_key() -> Optional[str]:
    """Get Massive API key from environment variable."""
    return os.environ.get(MASSIVE_API_KEY_ENV)


def parse_date(date_str: str) -> date:
    """Parse date string in YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_str, DATE_FORMAT).date()
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Use YYYY-MM-DD."
        )


def load_symbols_from_file(filepath: str, logger: logging.Logger) -> List[str]:
    """
    Load stock symbols from a text file.

    Args:
        filepath: Path to file containing symbols (one per line)
        logger: Logger instance

    Returns:
        List of ticker symbols

    Raises:
        FileNotFoundError: If file does not exist
        IOError: If file cannot be read
    """
    logger.info(f"Loading symbols from {filepath}...")

    symbols = []
    with open(filepath, "r") as f:
        for line in f:
            symbol = line.strip()
            if symbol and not symbol.startswith("#"):
                symbols.append(symbol.upper())

    logger.info(f"Loaded {len(symbols)} symbols from file")
    return symbols


def calculate_date_range(
    start_date: Optional[date], end_date: Optional[date], years: Optional[int]
) -> Tuple[date, date]:
    """
    Calculate start and end dates from various input options.

    Priority:
    1. If start_date provided, use it (with end_date or default to today)
    2. If years provided, calculate start_date as years ago from today
    3. Default: 5 years ago to today
    """
    # Determine end date
    if end_date:
        calc_end = end_date
    else:
        calc_end = date.today()

    # Determine start date
    if start_date:
        calc_start = start_date
    elif years:
        calc_start = calc_end - timedelta(days=years * 365)
    else:
        calc_start = calc_end - timedelta(days=DEFAULT_YEARS * 365)

    # Validate range
    if calc_start >= calc_end:
        raise ValueError(f"Start date {calc_start} must be before end date {calc_end}")

    return calc_start, calc_end


def build_query_params(
    endpoint_config: Dict[str, Any],
    ticker: Optional[str],
    start_date: date,
    end_date: date,
    timeframe: Optional[str],
    fiscal_year: Optional[int],
    fiscal_quarter: Optional[int],
    limit: int,
) -> Dict[str, Any]:
    """
    Build query parameters for API request based on endpoint type.

    Args:
        endpoint_config: Configuration for the endpoint
        ticker: Optional ticker symbol
        start_date: Start date
        end_date: End date
        timeframe: Optional timeframe (quarterly, annual, ttm)
        fiscal_year: Optional fiscal year filter
        fiscal_quarter: Optional fiscal quarter filter
        limit: Maximum records per request

    Returns:
        Dictionary of query parameters
    """
    params: Dict[str, Any] = {"limit": limit, "sort": "period_end.desc"}

    # Add ticker filter if provided
    if ticker and endpoint_config["ticker_param"]:
        params[endpoint_config["ticker_param"]] = ticker

    # Add timeframe if supported and provided
    if timeframe and endpoint_config["timeframes"]:
        params["timeframe"] = timeframe

    # Add date range filters
    date_field = endpoint_config["date_field"]
    params[f"{date_field}.gte"] = start_date.strftime(DATE_FORMAT)
    params[f"{date_field}.lte"] = end_date.strftime(DATE_FORMAT)

    # Add fiscal filters for financial statement endpoints
    if fiscal_year:
        params["fiscal_year"] = fiscal_year
    if fiscal_quarter:
        params["fiscal_quarter"] = fiscal_quarter

    return params


def fetch_endpoint_data(
    api_key: str,
    endpoint_name: str,
    ticker: Optional[str],
    start_date: date,
    end_date: date,
    timeframe: Optional[str],
    fiscal_year: Optional[int],
    fiscal_quarter: Optional[int],
    limit: int,
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """
    Fetch all data for a given endpoint with pagination support.

    Args:
        api_key: Massive API key
        endpoint_name: Name of the endpoint (key in ENDPOINTS dict)
        ticker: Optional ticker symbol filter
        start_date: Start of date range (inclusive)
        end_date: End of date range (inclusive)
        timeframe: Optional timeframe filter
        fiscal_year: Optional fiscal year filter
        fiscal_quarter: Optional fiscal quarter filter
        limit: Maximum records per request
        logger: Logger instance

    Returns:
        List of data records as dictionaries

    Raises:
        requests.exceptions.RequestException: If API request fails
        ValueError: If API returns error
    """
    endpoint_config = ENDPOINTS[endpoint_name]
    url = urljoin(MASSIVE_BASE_URL, endpoint_config["path"])

    logger.info(f"Fetching {endpoint_name} data from {start_date} to {end_date}...")
    if ticker:
        logger.info(f"  Ticker filter: {ticker}")
    if timeframe:
        logger.info(f"  Timeframe: {timeframe}")

    all_results = []
    current_url = url
    page_count = 0

    # Build initial query parameters
    params = build_query_params(
        endpoint_config,
        ticker,
        start_date,
        end_date,
        timeframe,
        fiscal_year,
        fiscal_quarter,
        limit,
    )

    while current_url:
        page_count += 1
        logger.debug(f"Fetching page {page_count} from {current_url}")

        # For subsequent pages, use the next_url directly
        if current_url != url:
            response = requests.get(
                current_url, headers={"Authorization": f"Bearer {api_key}"}, timeout=30
            )
        else:
            response = requests.get(
                current_url,
                headers={"Authorization": f"Bearer {api_key}"},
                params=params,
                timeout=30,
            )

        response.raise_for_status()
        data = response.json()

        if data.get("status") != "OK":
            error_message = data.get("error", data.get("message", "Unknown API error"))
            raise ValueError(f"Massive API error: {error_message}")

        results = data.get("results", [])
        all_results.extend(results)

        logger.debug(f"Page {page_count}: Retrieved {len(results)} records")

        # Check for next page
        current_url = data.get("next_url")

        if current_url and not current_url.startswith("http"):
            # Handle relative URLs
            current_url = urljoin(MASSIVE_BASE_URL, current_url)

    logger.info(f"Retrieved {len(all_results)} total records for {endpoint_name}")
    return all_results


def get_existing_dates(
    filepath: Path, date_field: str, logger: logging.Logger
) -> Set[str]:
    """
    Get set of dates already present in a CSV file.

    Args:
        filepath: Path to CSV file
        date_field: Name of the date column
        logger: Logger instance

    Returns:
        Set of date strings already in the file
    """
    if not filepath.exists():
        return set()

    existing_dates = set()
    try:
        with open(filepath, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if date_field in row and row[date_field]:
                    existing_dates.add(row[date_field])
        logger.debug(f"Found {len(existing_dates)} existing dates in {filepath}")
    except Exception as e:
        logger.warning(f"Could not read existing file {filepath}: {e}")

    return existing_dates


def save_to_csv(
    data: List[Dict[str, Any]],
    ticker: str,
    endpoint_name: str,
    date_field: str,
    output_dir: Path,
    continue_mode: bool,
    logger: logging.Logger,
) -> Path:
    """
    Save data to CSV file for a specific ticker.

    Args:
        data: List of data records
        ticker: Ticker symbol
        endpoint_name: Name of the endpoint
        date_field: Name of the date field in the data
        output_dir: Output directory path
        continue_mode: If True, append to existing file skipping duplicates
        logger: Logger instance

    Returns:
        Path to the output file
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create filename: TICKER_endpoint.csv (e.g., AAPL_balance_sheets.csv)
    filename = f"{ticker}_{endpoint_name.replace('-', '_')}.csv"
    filepath = output_dir / filename

    # Get existing dates if continuing
    existing_dates = set()
    if continue_mode:
        existing_dates = get_existing_dates(filepath, date_field, logger)
        if existing_dates:
            logger.info(f"Resuming: {len(existing_dates)} dates already in {filename}")

    # Filter out existing records if continuing
    new_data = [
        record for record in data if record.get(date_field) not in existing_dates
    ]

    if not new_data:
        logger.info(f"No new records to add for {ticker}")
        return filepath

    # Collect all unique field names across all records
    all_fields = set()
    for record in new_data:
        all_fields.update(record.keys())

    # Sort fields with ticker first, then date field, then alphabetically
    sorted_fields = []
    if "tickers" in all_fields:
        sorted_fields.append("tickers")
    if date_field in all_fields:
        sorted_fields.append(date_field)
    remaining = sorted([f for f in all_fields if f not in sorted_fields])
    sorted_fields.extend(remaining)

    # Sort by date (descending)
    new_data.sort(key=lambda x: x.get(date_field, ""), reverse=True)

    # Write to file
    mode = "a" if continue_mode and existing_dates else "w"
    write_header = not (continue_mode and existing_dates)

    with open(filepath, mode, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=sorted_fields)
        if write_header:
            writer.writeheader()
        writer.writerows(new_data)

    action = "Appended" if mode == "a" else "Saved"
    logger.info(f"{action} {len(new_data)} records to {filepath}")

    return filepath


def group_data_by_ticker(
    data: List[Dict[str, Any]], endpoint_name: str, logger: logging.Logger
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Group API results by ticker symbol.

    Args:
        data: List of records from API
        endpoint_name: Name of the endpoint
        logger: Logger instance

    Returns:
        Dictionary mapping ticker to list of records
    """
    grouped: Dict[str, List[Dict[str, Any]]] = {}

    endpoint_config = ENDPOINTS[endpoint_name]
    ticker_param = endpoint_config["ticker_param"]

    for record in data:
        # Extract ticker(s) from record
        tickers = record.get("tickers", [])

        # Handle single ticker or list of tickers
        if isinstance(tickers, list):
            for ticker in tickers:
                ticker = ticker.upper()
                if ticker not in grouped:
                    grouped[ticker] = []
                grouped[ticker].append(record)
        elif isinstance(tickers, str):
            ticker = tickers.upper()
            if ticker not in grouped:
                grouped[ticker] = []
            grouped[ticker].append(record)

        # Handle ticker field (for short-interest, short-volume, float)
        if "ticker" in record:
            ticker = record["ticker"].upper()
            if ticker not in grouped:
                grouped[ticker] = []
            grouped[ticker].append(record)

    logger.info(f"Grouped data into {len(grouped)} tickers")
    return grouped


def download_endpoint(
    api_key: str,
    endpoint_name: str,
    symbols: Optional[List[str]],
    start_date: date,
    end_date: date,
    timeframe: Optional[str],
    fiscal_year: Optional[int],
    fiscal_quarter: Optional[int],
    output_dir: Path,
    continue_mode: bool,
    limit: int,
    logger: logging.Logger,
) -> List[Path]:
    """
    Download and save data for a single endpoint.

    Args:
        api_key: Massive API key
        endpoint_name: Name of the endpoint
        symbols: Optional list of symbols to filter by
        start_date: Start date
        end_date: End date
        timeframe: Optional timeframe filter
        fiscal_year: Optional fiscal year filter
        fiscal_quarter: Optional fiscal quarter filter
        output_dir: Output directory
        continue_mode: Resume mode flag
        limit: Maximum records per API call
        logger: Logger instance

    Returns:
        List of paths to output files
    """
    endpoint_config = ENDPOINTS[endpoint_name]
    date_field = endpoint_config["date_field"]

    # If specific symbols provided, fetch data for each symbol
    # Otherwise fetch all data (may require iterating through pages)
    if symbols:
        downloaded_files = []
        for i, ticker in enumerate(symbols, 1):
            logger.info(f"[{i}/{len(symbols)}] Processing {ticker}...")

            try:
                data = fetch_endpoint_data(
                    api_key,
                    endpoint_name,
                    ticker,
                    start_date,
                    end_date,
                    timeframe,
                    fiscal_year,
                    fiscal_quarter,
                    limit,
                    logger,
                )

                if data:
                    filepath = save_to_csv(
                        data,
                        ticker,
                        endpoint_name,
                        date_field,
                        output_dir,
                        continue_mode,
                        logger,
                    )
                    if filepath:
                        downloaded_files.append(filepath)
                else:
                    logger.warning(f"No data available for {ticker}")

            except Exception as e:
                logger.error(f"Error processing {ticker}: {e}")
                continue

        return downloaded_files
    else:
        # Fetch all data without ticker filter
        data = fetch_endpoint_data(
            api_key,
            endpoint_name,
            None,
            start_date,
            end_date,
            timeframe,
            fiscal_year,
            fiscal_quarter,
            limit,
            logger,
        )

        if not data:
            logger.warning(f"No data returned for {endpoint_name}")
            return []

        # Group by ticker and save each to separate file
        grouped_data = group_data_by_ticker(data, endpoint_name, logger)
        downloaded_files = []

        for ticker, records in grouped_data.items():
            filepath = save_to_csv(
                records,
                ticker,
                endpoint_name,
                date_field,
                output_dir,
                continue_mode,
                logger,
            )
            if filepath:
                downloaded_files.append(filepath)

        return downloaded_files


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Download stock fundamentals from Massive API.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""\
Examples:
  %(prog)s --endpoint balance-sheets --symbols-file sp500_symbols.txt
    Download balance sheets for all symbols in file

  %(prog)s --endpoint short-interest --ticker AAPL --years 2
    Download short interest for AAPL for last 2 years

  %(prog)s --endpoint income-statements --timeframe annual
    Download annual income statements for all available tickers

  %(prog)s --endpoint cash-flow --ticker MSFT --timeframe quarterly --continue
    Resume downloading quarterly cash flows for MSFT

  %(prog)s --endpoint float --ticker GOOGL
    Download float data for GOOGL

Available Endpoints:
  balance-sheets       Balance sheet data (assets, liabilities, equity)
  cash-flow            Cash flow statements (quarterly, annual, TTM)
  income-statements    Income statements (quarterly, annual, TTM)
  short-interest       Bi-monthly short interest data
  short-volume         Daily short sale volume
  float                Free float shares (experimental)

Environment Variables:
  {MASSIVE_API_KEY_ENV}
    Your Massive API key (required)
""",
    )

    # Required endpoint selection
    parser.add_argument(
        "--endpoint",
        required=True,
        choices=list(ENDPOINTS.keys()),
        metavar="ENDPOINT",
        help="Fundamentals endpoint to query",
    )

    # Symbol selection (mutually exclusive options)
    symbol_group = parser.add_mutually_exclusive_group()
    symbol_group.add_argument(
        "--symbols-file",
        metavar="FILE",
        help="Path to file containing ticker symbols (one per line)",
    )
    symbol_group.add_argument(
        "--ticker",
        metavar="TICKER",
        help="Single ticker symbol to query",
    )

    # Date range options
    parser.add_argument(
        "--start-date",
        type=parse_date,
        metavar="YYYY-MM-DD",
        help="Start date for data download",
    )

    parser.add_argument(
        "--end-date",
        type=parse_date,
        metavar="YYYY-MM-DD",
        help="End date for data download (default: today)",
    )

    parser.add_argument(
        "--years",
        type=int,
        metavar="N",
        help=f"Number of years back from today (default: {DEFAULT_YEARS})",
    )

    # Financial statement filters
    parser.add_argument(
        "--timeframe",
        choices=["quarterly", "annual", "trailing_twelve_months"],
        metavar="TF",
        help="Reporting period type (for balance-sheets, cash-flow, income-statements)",
    )

    parser.add_argument(
        "--fiscal-year",
        type=int,
        metavar="YEAR",
        help="Fiscal year filter (for financial statements)",
    )

    parser.add_argument(
        "--fiscal-quarter",
        type=int,
        choices=[1, 2, 3, 4],
        metavar="Q",
        help="Fiscal quarter filter (for financial statements)",
    )

    # Output options
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        metavar="DIR",
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )

    parser.add_argument(
        "--continue",
        dest="continue_mode",
        action="store_true",
        help="Resume download, skipping dates already in output files",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        metavar="N",
        help=f"Maximum records per API request (default: {DEFAULT_LIMIT})",
    )

    # API key
    parser.add_argument(
        "--api-key",
        help="Massive API key (overrides environment variable)",
    )

    # Logging
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose (debug) logging"
    )

    args = parser.parse_args()

    # Setup logging
    logger = setup_logging(args.verbose)

    logger.info("=" * 60)
    logger.info("Massive Fundamentals Downloader")
    logger.info("=" * 60)

    # Get API key
    api_key = args.api_key or get_api_key()

    if not api_key:
        logger.error(
            f"Massive API key not found. Please set the {MASSIVE_API_KEY_ENV} "
            "environment variable or use --api-key."
        )
        sys.exit(1)

    # Load symbols
    symbols = None
    if args.symbols_file:
        symbols = load_symbols_from_file(args.symbols_file, logger)
    elif args.ticker:
        symbols = [args.ticker.upper()]
        logger.info(f"Using single ticker: {symbols[0]}")

    try:
        # Calculate date range
        start_date, end_date = calculate_date_range(
            args.start_date, args.end_date, args.years
        )

        logger.info(f"Date range: {start_date} to {end_date}")
        logger.info(f"Endpoint: {args.endpoint}")

        if symbols:
            logger.info(f"Symbols to process: {len(symbols)}")

        # Validate timeframe for endpoint
        endpoint_config = ENDPOINTS[args.endpoint]
        if args.timeframe and args.timeframe not in endpoint_config["timeframes"]:
            logger.warning(
                f"Timeframe '{args.timeframe}' not supported for {args.endpoint}. "
                f"Supported: {endpoint_config['timeframes']}"
            )

        # Create output directory
        output_dir = Path(args.output_dir)

        # Download endpoint
        logger.info("")
        logger.info(f"Processing endpoint: {args.endpoint}")
        logger.info("-" * 40)

        downloaded_files = download_endpoint(
            api_key,
            args.endpoint,
            symbols,
            start_date,
            end_date,
            args.timeframe,
            args.fiscal_year,
            args.fiscal_quarter,
            output_dir,
            args.continue_mode,
            args.limit,
            logger,
        )

        # Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("Download Summary")
        logger.info("=" * 60)

        if downloaded_files:
            logger.info(f"Successfully downloaded {len(downloaded_files)} files")
            if args.verbose:
                for filepath in downloaded_files:
                    logger.info(f"  - {filepath}")
        else:
            logger.warning("No files were downloaded")

        logger.info("")
        logger.info("Download completed!")

    except requests.exceptions.RequestException as e:
        logger.error(f"Network error: {e}")
        sys.exit(1)
    except (ValueError, IOError) as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        if args.verbose:
            raise
        sys.exit(1)


if __name__ == "__main__":
    main()
