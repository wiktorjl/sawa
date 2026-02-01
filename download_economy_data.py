#!/usr/bin/env python3
"""
Download economy data from Massive API.

This script downloads U.S. economy data from the Massive API including:
- Treasury yields (daily, back to 1962)
- Inflation metrics (monthly)
- Inflation expectations (monthly)
- Labor market indicators (monthly)

Data is saved as CSV files, one per data type.

Usage:
    python download_economy_data.py --years 5
    python download_economy_data.py --start-date 2020-01-01 --end-date 2023-12-31
    python download_economy_data.py --endpoints treasury-yields inflation

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
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests


# Configuration
MASSIVE_API_KEY_ENV = "MASSIVE_API_KEY"
MASSIVE_BASE_URL = "https://api.massive.com"

DEFAULT_YEARS = 5
DATE_FORMAT = "%Y-%m-%d"
DEFAULT_OUTPUT_DIR = "data/economy"

# Endpoint configurations
ENDPOINTS = {
    "treasury-yields": {
        "path": "/fed/v1/treasury-yields",
        "frequency": "daily",
        "fields": [
            "date",
            "yield_1_month",
            "yield_3_month",
            "yield_6_month",
            "yield_1_year",
            "yield_2_year",
            "yield_3_year",
            "yield_5_year",
            "yield_7_year",
            "yield_10_year",
            "yield_20_year",
            "yield_30_year",
        ],
    },
    "inflation": {
        "path": "/fed/v1/inflation",
        "frequency": "monthly",
        "fields": [
            "date",
            "cpi",
            "cpi_core",
            "cpi_year_over_year",
            "pce",
            "pce_core",
            "pce_spending",
        ],
    },
    "inflation-expectations": {
        "path": "/fed/v1/inflation-expectations",
        "frequency": "monthly",
        "fields": [
            "date",
            "market_5_year",
            "market_10_year",
            "forward_years_5_to_10",
            "model_1_year",
            "model_5_year",
            "model_10_year",
            "model_30_year",
        ],
    },
    "labor-market": {
        "path": "/fed/v1/labor-market",
        "frequency": "monthly",
        "fields": [
            "date",
            "unemployment_rate",
            "labor_force_participation_rate",
            "avg_hourly_earnings",
            "job_openings",
        ],
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


def fetch_endpoint_data(
    api_key: str,
    endpoint_name: str,
    start_date: date,
    end_date: date,
    logger: logging.Logger,
) -> List[Dict]:
    """
    Fetch all data for a given endpoint with pagination support.

    Args:
        api_key: Massive API key
        endpoint_name: Name of the endpoint (key in ENDPOINTS dict)
        start_date: Start of date range (inclusive)
        end_date: End of date range (inclusive)
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

    all_results = []
    current_url = url
    page_count = 0

    # Build initial query parameters
    params = {
        "date.gte": start_date.strftime(DATE_FORMAT),
        "date.lte": end_date.strftime(DATE_FORMAT),
        "limit": 50000,
        "sort": "date.asc",
    }

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


def get_existing_dates(filepath: Path, logger: logging.Logger) -> Set[str]:
    """
    Get set of dates already present in a CSV file.

    Args:
        filepath: Path to CSV file
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
                if "date" in row:
                    existing_dates.add(row["date"])
        logger.debug(f"Found {len(existing_dates)} existing dates in {filepath}")
    except Exception as e:
        logger.warning(f"Could not read existing file {filepath}: {e}")

    return existing_dates


def save_to_csv(
    data: List[Dict],
    endpoint_name: str,
    output_dir: Path,
    continue_mode: bool,
    logger: logging.Logger,
) -> Path:
    """
    Save data to CSV file.

    Args:
        data: List of data records
        endpoint_name: Name of the endpoint
        output_dir: Output directory path
        continue_mode: If True, append to existing file skipping duplicates
        logger: Logger instance

    Returns:
        Path to the output file
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{endpoint_name}.csv"
    filepath = output_dir / filename

    endpoint_config = ENDPOINTS[endpoint_name]
    fieldnames = endpoint_config["fields"]

    # Get existing dates if continuing
    existing_dates = set()
    if continue_mode:
        existing_dates = get_existing_dates(filepath, logger)
        if existing_dates:
            logger.info(f"Resuming: {len(existing_dates)} dates already in {filename}")

    # Filter out existing records if continuing
    new_data = [record for record in data if record.get("date") not in existing_dates]

    if not new_data:
        logger.info(f"No new records to add for {endpoint_name}")
        return filepath

    # Prepare records with all fields (missing fields become empty)
    rows = []
    for record in new_data:
        row = {field: record.get(field, "") for field in fieldnames}
        rows.append(row)

    # Sort by date
    rows.sort(key=lambda x: x["date"])

    # Write to file
    mode = "a" if continue_mode and existing_dates else "w"
    write_header = not (continue_mode and existing_dates)

    with open(filepath, mode, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)

    action = "Appended" if mode == "a" else "Saved"
    logger.info(f"{action} {len(rows)} records to {filepath}")

    return filepath


def download_endpoint(
    api_key: str,
    endpoint_name: str,
    start_date: date,
    end_date: date,
    output_dir: Path,
    continue_mode: bool,
    logger: logging.Logger,
) -> Path:
    """
    Download and save data for a single endpoint.

    Args:
        api_key: Massive API key
        endpoint_name: Name of the endpoint
        start_date: Start date
        end_date: End date
        output_dir: Output directory
        continue_mode: Resume mode flag
        logger: Logger instance

    Returns:
        Path to output file
    """
    data = fetch_endpoint_data(api_key, endpoint_name, start_date, end_date, logger)

    if not data:
        logger.warning(f"No data returned for {endpoint_name}")
        return None

    filepath = save_to_csv(data, endpoint_name, output_dir, continue_mode, logger)
    return filepath


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Download economy data from Massive API.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""\
Examples:
  %(prog)s --years 5
    Download all economy data for the past 5 years

  %(prog)s --start-date 2020-01-01 --end-date 2023-12-31
    Download data for a specific date range

  %(prog)s --endpoints treasury-yields inflation
    Download only treasury yields and inflation data

  %(prog)s --years 10 --output-dir ./my_data/
    Save to custom directory

  %(prog)s --years 5 --continue
    Resume an interrupted download

Available Endpoints:
  treasury-yields       Daily Treasury yields (1mo-30yr)
  inflation             Monthly inflation metrics (CPI, PCE)
  inflation-expectations Monthly inflation expectations
  labor-market          Monthly labor market indicators

Output Files:
  One CSV file per endpoint in the output directory:
  - treasury-yields.csv
  - inflation.csv
  - inflation-expectations.csv
  - labor-market.csv

Environment Variables:
  {MASSIVE_API_KEY_ENV}
    Your Massive API key (required)
""",
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

    # Endpoint selection
    parser.add_argument(
        "--endpoints",
        nargs="+",
        choices=list(ENDPOINTS.keys()),
        metavar="ENDPOINT",
        help="Specific endpoints to download (default: all)",
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
    logger.info("Massive Economy Data Downloader")
    logger.info("=" * 60)

    # Get API key
    api_key = args.api_key or get_api_key()

    if not api_key:
        logger.error(
            f"Massive API key not found. Please set the {MASSIVE_API_KEY_ENV} "
            "environment variable or use --api-key."
        )
        sys.exit(1)

    try:
        # Calculate date range
        start_date, end_date = calculate_date_range(
            args.start_date, args.end_date, args.years
        )

        logger.info(f"Date range: {start_date} to {end_date}")

        # Determine which endpoints to download
        endpoints_to_download = (
            args.endpoints if args.endpoints else list(ENDPOINTS.keys())
        )
        logger.info(f"Endpoints to download: {', '.join(endpoints_to_download)}")

        # Create output directory
        output_dir = Path(args.output_dir)

        # Download each endpoint
        downloaded_files = []
        failed_endpoints = []

        for endpoint_name in endpoints_to_download:
            logger.info("")
            logger.info(f"Processing endpoint: {endpoint_name}")
            logger.info("-" * 40)

            try:
                filepath = download_endpoint(
                    api_key,
                    endpoint_name,
                    start_date,
                    end_date,
                    output_dir,
                    args.continue_mode,
                    logger,
                )
                if filepath:
                    downloaded_files.append(filepath)
            except Exception as e:
                logger.error(f"Failed to download {endpoint_name}: {e}")
                failed_endpoints.append(endpoint_name)
                if args.verbose:
                    raise

        # Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("Download Summary")
        logger.info("=" * 60)

        if downloaded_files:
            logger.info(f"Successfully downloaded {len(downloaded_files)} files:")
            for filepath in downloaded_files:
                logger.info(f"  - {filepath}")

        if failed_endpoints:
            logger.error(f"Failed to download {len(failed_endpoints)} endpoints:")
            for endpoint in failed_endpoints:
                logger.error(f"  - {endpoint}")
            sys.exit(1)

        logger.info("")
        logger.info("All downloads completed successfully!")

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
