#!/usr/bin/env python3
"""
Download ticker overview data from Massive API.

This script fetches comprehensive company details for one or more tickers
from the Massive API, including identifiers, market data, contact info,
and branding assets.

Data is saved as a CSV file with flattened nested structures.

Usage:
    python download_overviews.py --symbols-file sp500_symbols.txt
    python download_overviews.py --tickers AAPL MSFT GOOGL
    python download_overviews.py --symbols-file symbols.txt --continue

Environment Variables:
    MASSIVE_API_KEY - Your Massive API key (required)
"""

import argparse
import csv
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin

import requests


# Configuration
MASSIVE_API_KEY_ENV = "MASSIVE_API_KEY"
MASSIVE_BASE_URL = "https://api.massive.com"

DEFAULT_OUTPUT_FILE = "data/overviews/OVERVIEWS.csv"
DEFAULT_DELAY = 0.1  # Seconds between requests
DEFAULT_MAX_RETRIES = 3

# CSV field order (flattened structure)
CSV_FIELDS = [
    "ticker",
    "name",
    "description",
    "market",
    "type",
    "locale",
    "currency_name",
    "active",
    "list_date",
    "delisted_utc",
    "primary_exchange",
    "cik",
    "composite_figi",
    "share_class_figi",
    "sic_code",
    "sic_description",
    "market_cap",
    "weighted_shares_outstanding",
    "share_class_shares_outstanding",
    "total_employees",
    "round_lot",
    "ticker_root",
    "ticker_suffix",
    "homepage_url",
    "phone_number",
    "address_address1",
    "address_city",
    "address_state",
    "address_postal_code",
    "branding_logo_url",
    "branding_icon_url",
]


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


def load_tickers_from_file(filepath: Path, logger: logging.Logger) -> List[str]:
    """
    Load ticker symbols from a text file.

    Args:
        filepath: Path to file with one ticker per line
        logger: Logger instance

    Returns:
        List of ticker symbols

    Raises:
        FileNotFoundError: If file doesn't exist
        IOError: If file read fails
    """
    logger.info(f"Loading tickers from {filepath}...")

    tickers = []
    with open(filepath, "r") as f:
        for line in f:
            ticker = line.strip()
            if ticker and not ticker.startswith("#"):
                tickers.append(ticker)

    logger.info(f"Loaded {len(tickers)} tickers from file")
    return tickers


def get_existing_tickers(filepath: Path, logger: logging.Logger) -> Set[str]:
    """
    Get set of tickers already present in output CSV.

    Args:
        filepath: Path to CSV file
        logger: Logger instance

    Returns:
        Set of ticker symbols already in the file
    """
    if not filepath.exists():
        return set()

    existing_tickers = set()
    try:
        with open(filepath, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if "ticker" in row and row["ticker"]:
                    existing_tickers.add(row["ticker"])
        logger.debug(f"Found {len(existing_tickers)} existing tickers in {filepath}")
    except Exception as e:
        logger.warning(f"Could not read existing file {filepath}: {e}")

    return existing_tickers


def flatten_overview(data: Dict) -> Dict:
    """
    Flatten nested overview data for CSV output.

    Args:
        data: Raw API response data

    Returns:
        Flattened dictionary with dot-notation for nested fields
    """
    flat = {}

    # Simple fields
    for field in [
        "ticker",
        "name",
        "description",
        "market",
        "type",
        "locale",
        "currency_name",
        "active",
        "list_date",
        "delisted_utc",
        "primary_exchange",
        "cik",
        "composite_figi",
        "share_class_figi",
        "sic_code",
        "sic_description",
        "market_cap",
        "weighted_shares_outstanding",
        "share_class_shares_outstanding",
        "total_employees",
        "round_lot",
        "ticker_root",
        "ticker_suffix",
        "homepage_url",
        "phone_number",
    ]:
        flat[field] = data.get(field, "")

    # Nested address
    address = data.get("address", {}) or {}
    flat["address_address1"] = address.get("address1", "")
    flat["address_city"] = address.get("city", "")
    flat["address_state"] = address.get("state", "")
    flat["address_postal_code"] = address.get("postal_code", "")

    # Nested branding
    branding = data.get("branding", {}) or {}
    flat["branding_logo_url"] = branding.get("logo_url", "")
    flat["branding_icon_url"] = branding.get("icon_url", "")

    return flat


def fetch_ticker_overview(
    api_key: str,
    ticker: str,
    logger: logging.Logger,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> Optional[Dict]:
    """
    Fetch overview data for a single ticker.

    Args:
        api_key: Massive API key
        ticker: Ticker symbol
        logger: Logger instance
        max_retries: Maximum retry attempts for rate limits

    Returns:
        Overview data dictionary or None if ticker not found

    Raises:
        requests.exceptions.RequestException: If API request fails (non-429)
    """
    url = urljoin(MASSIVE_BASE_URL, f"/v3/reference/tickers/{ticker}")

    logger.debug(f"Fetching overview for {ticker}...")

    for attempt in range(max_retries):
        try:
            response = requests.get(
                url,
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=30,
            )

            if response.status_code == 404:
                logger.warning(f"Ticker {ticker} not found (404)")
                return None

            if response.status_code == 429:
                # Rate limited - wait and retry
                wait_time = (attempt + 1) * 2
                logger.warning(
                    f"Rate limited (429). Waiting {wait_time}s before retry..."
                )
                time.sleep(wait_time)
                continue

            response.raise_for_status()
            data = response.json()

            if data.get("status") != "OK":
                error_message = data.get(
                    "error", data.get("message", "Unknown API error")
                )
                logger.warning(f"API error for {ticker}: {error_message}")
                return None

            results = data.get("results")
            if not results:
                logger.warning(f"No data returned for {ticker}")
                return None

            logger.debug(f"Successfully fetched overview for {ticker}")
            return results

        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                logger.warning(f"Request failed for {ticker}: {e}. Retrying...")
                time.sleep(1)
            else:
                raise

    logger.error(f"Max retries exceeded for {ticker}")
    return None


def save_overviews(
    overviews: List[Dict],
    output_file: Path,
    continue_mode: bool,
    logger: logging.Logger,
) -> None:
    """
    Save overview data to CSV file.

    Args:
        overviews: List of flattened overview dictionaries
        output_file: Output file path
        continue_mode: If True, append to existing file
        logger: Logger instance
    """
    output_file.parent.mkdir(parents=True, exist_ok=True)

    mode = "a" if continue_mode and output_file.exists() else "w"
    write_header = not (continue_mode and output_file.exists())

    with open(output_file, mode, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if write_header:
            writer.writeheader()
        writer.writerows(overviews)

    action = "Appended" if mode == "a" else "Saved"
    logger.debug(f"{action} {len(overviews)} records to {output_file}")


def download_overviews(
    api_key: str,
    tickers: List[str],
    output_file: Path,
    continue_mode: bool,
    delay: float,
    logger: logging.Logger,
) -> tuple[List[str], List[str]]:
    """
    Download overviews for all tickers.

    Args:
        api_key: Massive API key
        tickers: List of ticker symbols
        output_file: Output file path
        continue_mode: If True, skip already downloaded tickers
        delay: Seconds to wait between requests
        logger: Logger instance

    Returns:
        Tuple of (successful_tickers, failed_tickers)
    """
    # Filter out already processed tickers if continuing
    if continue_mode:
        existing = get_existing_tickers(output_file, logger)
        tickers_to_process = [t for t in tickers if t not in existing]
        skipped = len(tickers) - len(tickers_to_process)
        if skipped > 0:
            logger.info(f"Resuming: {skipped} tickers already in output file")
    else:
        tickers_to_process = tickers

    if not tickers_to_process:
        logger.info("No tickers to process")
        return [], []

    total = len(tickers_to_process)
    logger.info(f"Processing {total} tickers...")

    successful = []
    failed = []
    batch = []
    batch_size = 10  # Save every N tickers

    for i, ticker in enumerate(tickers_to_process, 1):
        logger.info(f"[{i}/{total}] Fetching {ticker}...")

        try:
            data = fetch_ticker_overview(api_key, ticker, logger)
            if data:
                flat_data = flatten_overview(data)
                batch.append(flat_data)
                successful.append(ticker)
            else:
                failed.append(ticker)

        except Exception as e:
            logger.error(f"Failed to fetch {ticker}: {e}")
            failed.append(ticker)

        # Save batch periodically
        if len(batch) >= batch_size:
            save_overviews(
                batch, output_file, continue_mode or (i > batch_size), logger
            )
            batch = []

        # Progress update
        if i % 50 == 0 or i == total:
            logger.info(
                f"Progress: {i}/{total} ({100 * i // total}%) - "
                f"Success: {len(successful)}, Failed: {len(failed)}"
            )

        # Rate limiting delay
        if i < total and delay > 0:
            time.sleep(delay)

    # Save remaining batch
    if batch:
        save_overviews(
            batch, output_file, continue_mode or (len(successful) > batch_size), logger
        )

    return successful, failed


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Download ticker overview data from Massive API.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""\
Examples:
  %(prog)s --symbols-file sp500_symbols.txt
    Download overviews for all tickers in file

  %(prog)s --tickers AAPL MSFT GOOGL
    Download overviews for specific tickers

  %(prog)s --symbols-file symbols.txt --continue
    Resume an interrupted download

  %(prog)s --symbols-file symbols.txt --delay 0.5
    Add 0.5s delay between requests (rate limiting)

  %(prog)s --symbols-file symbols.txt -o my_overviews.csv
    Save to custom output file

Output Format:
  CSV file with flattened overview data including:
  - Basic info: ticker, name, description, market, type
  - Identifiers: cik, composite_figi, share_class_figi, sic_code
  - Financials: market_cap, shares_outstanding, total_employees
  - Contact: address, phone, homepage_url
  - Branding: logo_url, icon_url

Environment Variables:
  {MASSIVE_API_KEY_ENV}
    Your Massive API key (required)
""",
    )

    # Input options (mutually exclusive)
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--symbols-file",
        type=Path,
        metavar="FILE",
        help="File containing ticker symbols (one per line)",
    )
    input_group.add_argument(
        "--tickers",
        nargs="+",
        metavar="TICKER",
        help="List of ticker symbols to download",
    )

    # Output options
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_FILE,
        metavar="FILE",
        help=f"Output CSV file (default: {DEFAULT_OUTPUT_FILE})",
    )

    parser.add_argument(
        "--continue",
        dest="continue_mode",
        action="store_true",
        help="Resume download, skipping tickers already in output file",
    )

    # Rate limiting
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        metavar="SECONDS",
        help=f"Delay between requests in seconds (default: {DEFAULT_DELAY})",
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
    logger.info("Massive Ticker Overview Downloader")
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
        # Load tickers
        if args.symbols_file:
            tickers = load_tickers_from_file(args.symbols_file, logger)
        else:
            tickers = args.tickers
            logger.info(f"Processing {len(tickers)} tickers from command line")

        if not tickers:
            logger.error("No tickers to process")
            sys.exit(1)

        logger.info(f"Output file: {args.output}")
        logger.info(f"Delay: {args.delay}s between requests")
        if args.continue_mode:
            logger.info("Resume mode: enabled")

        # Download overviews
        successful, failed = download_overviews(
            api_key,
            tickers,
            args.output,
            args.continue_mode,
            args.delay,
            logger,
        )

        # Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("Download Summary")
        logger.info("=" * 60)
        logger.info(f"Total tickers: {len(tickers)}")
        logger.info(f"Successful: {len(successful)}")
        logger.info(f"Failed: {len(failed)}")
        logger.info(f"Output file: {args.output}")

        if failed:
            failed_file = args.output.parent / f"{args.output.stem}_failed.txt"
            with open(failed_file, "w") as f:
                for ticker in failed:
                    f.write(f"{ticker}\n")
            logger.info(f"Failed tickers saved to: {failed_file}")
            sys.exit(1)

        logger.info("")
        logger.info("All downloads completed successfully!")

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        sys.exit(1)
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
