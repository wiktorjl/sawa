#!/usr/bin/env python3
"""
Determine trading days for a given date range.

This script identifies all trading days (days when the US stock market was open)
within a specified date range. It uses AAPL as a proxy - if AAPL traded on a day,
that day is considered a trading day.

Data is fetched from Polygon.io API.

Usage:
    python check_trading_days.py --years 5
    python check_trading_days.py --start-date 2020-01-01
    python check_trading_days.py --start-date 2020-01-01 --end-date 2023-12-31

Output:
    A text file with one trading date per line (YYYY-MM-DD format).
    Filename: trading_days_YYYY-MM-DD.txt (where date is the start date)
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

import requests


# Configuration
POLYGON_API_KEY_ENV = "POLYGON_API_KEY"
POLYGON_BASE_URL = "https://api.polygon.io"
PROXY_TICKER = "AAPL"  # Use AAPL as proxy for market open/close

DEFAULT_YEARS = 5
DATE_FORMAT = "%Y-%m-%d"


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
    """Get Polygon.io API key from environment variable."""
    return os.environ.get(POLYGON_API_KEY_ENV)


def parse_date(date_str: str) -> date:
    """Parse date string in YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_str, DATE_FORMAT).date()
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Use YYYY-MM-DD."
        )


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
        # Default: 5 years
        calc_start = calc_end - timedelta(days=DEFAULT_YEARS * 365)

    # Validate range
    if calc_start >= calc_end:
        raise ValueError(f"Start date {calc_start} must be before end date {calc_end}")

    return calc_start, calc_end


def fetch_trading_days(
    api_key: str, start_date: date, end_date: date, logger: logging.Logger
) -> List[date]:
    """
    Fetch all trading days in the date range from Polygon.io.

    Uses the Aggregates API to get daily bars for AAPL. Days with volume > 0
    are considered trading days.

    Args:
        api_key: Polygon.io API key
        start_date: Start of date range (inclusive)
        end_date: End of date range (inclusive)
        logger: Logger instance

    Returns:
        List of trading days as date objects

    Raises:
        requests.exceptions.RequestException: If API request fails
        ValueError: If API returns error or no data
    """
    logger.info(f"Fetching trading days from {start_date} to {end_date}...")
    logger.info(f"Using {PROXY_TICKER} as proxy for market open/close")

    url = f"{POLYGON_BASE_URL}/v2/aggs/ticker/{PROXY_TICKER}/range/1/day/{start_date}/{end_date}"

    params = {
        "apiKey": api_key,
        "adjusted": "true",
    }

    logger.debug(f"API URL: {url}")

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    logger.debug(f"API response: {data}")

    if data.get("status") not in ("OK", "DELAYED"):
        error_message = data.get("error", data.get("message", "Unknown API error"))
        raise ValueError(f"Polygon API error: {error_message}")

    results = data.get("results", [])

    if not results:
        raise ValueError(f"No trading data returned for {PROXY_TICKER} in date range")

    # Extract dates from bars
    trading_days = []
    for bar in results:
        # Polygon timestamps are in milliseconds since epoch
        timestamp_ms = bar.get("t")
        if timestamp_ms:
            timestamp = datetime.fromtimestamp(timestamp_ms / 1000)
            trading_days.append(timestamp.date())

    # Sort chronologically
    trading_days.sort()

    logger.info(f"Found {len(trading_days)} trading days")
    return trading_days


def save_trading_days(
    trading_days: List[date],
    start_date: date,
    output_file: Optional[str],
    logger: logging.Logger,
) -> str:
    """
    Save trading days to a text file.

    Args:
        trading_days: List of trading day dates
        start_date: Start date (used in default filename)
        output_file: Optional custom output file path
        logger: Logger instance

    Returns:
        Path to the output file

    Raises:
        IOError: If file write fails
    """
    # Determine output filename
    if output_file:
        filepath = output_file
    else:
        filename = f"trading_days_{start_date.strftime(DATE_FORMAT)}.txt"
        filepath = f"data/{filename}"

    logger.info(f"Saving {len(trading_days)} trading days to {filepath}...")

    try:
        with open(filepath, "w") as f:
            for trading_day in trading_days:
                f.write(f"{trading_day.strftime(DATE_FORMAT)}\n")
        logger.info(f"Successfully saved to {filepath}")
        return filepath
    except IOError as e:
        raise IOError(f"Failed to write to {filepath}: {e}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Determine trading days for a given date range using Polygon.io data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""\
Examples:
  %(prog)s --years 5
    Get trading days for the past 5 years

  %(prog)s --start-date 2020-01-01
    Get trading days from 2020-01-01 to today

  %(prog)s --start-date 2020-01-01 --end-date 2023-12-31
    Get trading days for specific date range

  %(prog)s --years 3 -o my_trading_days.txt
    Save to custom file

Output Format:
  One date per line in YYYY-MM-DD format (e.g., 2024-01-02)

Default Filename:
  trading_days_YYYY-MM-DD.txt (based on start date)

Environment Variables:
  {POLYGON_API_KEY_ENV}
    Your Polygon.io API key (required)
""",
    )

    # Date range options (mutually exclusive priority)
    parser.add_argument(
        "--start-date",
        type=parse_date,
        metavar="YYYY-MM-DD",
        help="Start date for trading day detection",
    )

    parser.add_argument(
        "--end-date",
        type=parse_date,
        metavar="YYYY-MM-DD",
        help="End date for trading day detection (default: today)",
    )

    parser.add_argument(
        "--years",
        type=int,
        metavar="N",
        help=f"Number of years back from today (default: {DEFAULT_YEARS})",
    )

    parser.add_argument(
        "-o",
        "--output",
        metavar="FILE",
        help="Output file path (default: trading_days_START-DATE.txt)",
    )

    parser.add_argument(
        "--api-key", help="Polygon.io API key (overrides environment variable)"
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose (debug) logging"
    )

    args = parser.parse_args()

    # Setup logging
    logger = setup_logging(args.verbose)

    logger.info("=" * 60)
    logger.info("Trading Days Detector")
    logger.info("=" * 60)

    # Get API key
    api_key = args.api_key or get_api_key()

    if not api_key:
        logger.error(
            f"Polygon.io API key not found. Please set the {POLYGON_API_KEY_ENV} "
            "environment variable or use --api-key."
        )
        sys.exit(1)

    try:
        # Calculate date range
        start_date, end_date = calculate_date_range(
            args.start_date, args.end_date, args.years
        )

        logger.info(f"Date range: {start_date} to {end_date}")

        # Fetch trading days
        trading_days = fetch_trading_days(api_key, start_date, end_date, logger)

        # Save to file
        output_path = save_trading_days(trading_days, start_date, args.output, logger)

        logger.info("=" * 60)
        logger.info(f"SUCCESS: {len(trading_days)} trading days saved to {output_path}")
        logger.info("=" * 60)

        # Print summary
        logger.info(f"First trading day: {trading_days[0]}")
        logger.info(f"Last trading day:  {trading_days[-1]}")

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
