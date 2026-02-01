#!/usr/bin/env python3
"""
Download financial ratios for stock symbols from Polygon.io API.

This script fetches financial ratio data for one or more stock symbols.
Symbols can be provided either as a file (one per line) or directly as
command line arguments.

Usage:
    python ratio_downloader.py symbols.txt
    python ratio_downloader.py AAPL MSFT GOOGL
    python ratio_downloader.py symbols.txt -o ratios.csv

Output:
    CSV file with financial ratios for each symbol.
"""

import argparse
import csv
import logging
import os
import sys
from typing import List, Optional, Dict, Any

import requests


# Configuration
POLYGON_API_KEY_ENV = "POLYGON_API_KEY"
POLYGON_BASE_URL = "https://api.polygon.io"
RATIOS_ENDPOINT = "/stocks/financials/v1/ratios"

DEFAULT_OUTPUT_FILE = "data/ratios/RATIOS.csv"
DEFAULT_LIMIT = 100


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


def fetch_ratios(
    api_key: str, ticker: str, limit: int, logger: logging.Logger
) -> List[Dict[str, Any]]:
    """
    Fetch financial ratios for a ticker from Polygon.io API.

    Args:
        api_key: Polygon.io API key
        ticker: Stock ticker symbol
        limit: Maximum number of records to return
        logger: Logger instance

    Returns:
        List of ratio records (each record is a dict)

    Raises:
        requests.exceptions.RequestException: If API request fails
        ValueError: If API returns error
    """
    logger.debug(f"Fetching ratios for {ticker}...")

    url = f"{POLYGON_BASE_URL}{RATIOS_ENDPOINT}"
    params = {
        "ticker": ticker,
        "limit": limit,
        "apiKey": api_key,
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    logger.debug(f"API response for {ticker}: {data}")

    if data.get("status") not in ("OK", "DELAYED"):
        error_message = data.get("error", data.get("message", "Unknown API error"))
        raise ValueError(f"Polygon API error for {ticker}: {error_message}")

    results = data.get("results", [])
    return results


def save_ratios_to_csv(
    all_ratios: Dict[str, List[Dict[str, Any]]],
    output_file: str,
    logger: logging.Logger,
) -> None:
    """
    Save financial ratios to a CSV file.

    Args:
        all_ratios: Dict mapping ticker to list of ratio records
        output_file: Path to output CSV file
        logger: Logger instance

    Raises:
        IOError: If file write fails
    """
    logger.info(f"Saving ratios to {output_file}...")

    # Collect all unique field names across all records
    all_fields = set()
    for ticker, records in all_ratios.items():
        for record in records:
            all_fields.update(record.keys())

    # Ensure 'ticker' is first column
    fieldnames = ["ticker"] + sorted([f for f in all_fields if f != "ticker"])

    output_dir = os.path.dirname(os.path.abspath(output_file))
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for ticker, records in all_ratios.items():
            for record in records:
                row = {"ticker": ticker, **record}
                writer.writerow(row)

    total_records = sum(len(records) for records in all_ratios.values())
    logger.info(f"Saved {total_records} ratio records for {len(all_ratios)} symbols")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Download financial ratios from Polygon.io API.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  %(prog)s symbols.txt
    Load symbols from file and download ratios

  %(prog)s AAPL MSFT GOOGL
    Download ratios for specific symbols

  %(prog)s symbols.txt -o my_ratios.csv
    Save to custom output file

  %(prog)s symbols.txt --limit 50
    Limit records per symbol to 50 (default: 100)

Output Format:
  CSV file with financial ratios. Each row contains ratios for a symbol
  at a specific point in time (e.g., quarterly or annual report).

Environment Variables:
  POLYGON_API_KEY
    Your Polygon.io API key (required)
""",
    )

    parser.add_argument(
        "symbols",
        nargs="+",
        help="Stock symbols or path to file containing symbols (one per line)",
    )

    parser.add_argument(
        "-o",
        "--output",
        default=DEFAULT_OUTPUT_FILE,
        metavar="FILE",
        help=f"Output CSV file path (default: {DEFAULT_OUTPUT_FILE})",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        metavar="N",
        help=f"Maximum records per symbol (default: {DEFAULT_LIMIT})",
    )

    parser.add_argument(
        "--api-key",
        help="Polygon.io API key (overrides environment variable)",
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose (debug) logging"
    )

    args = parser.parse_args()

    # Setup logging
    logger = setup_logging(args.verbose)

    logger.info("=" * 60)
    logger.info("Financial Ratios Downloader")
    logger.info("=" * 60)

    # Get API key
    api_key = args.api_key or get_api_key()

    if not api_key:
        logger.error(
            f"Polygon.io API key not found. Please set the {POLYGON_API_KEY_ENV} "
            "environment variable or use --api-key."
        )
        sys.exit(1)

    # Determine if first argument is a file or list of symbols
    symbols_arg = args.symbols
    if len(symbols_arg) == 1 and os.path.isfile(symbols_arg[0]):
        # Load symbols from file
        symbols = load_symbols_from_file(symbols_arg[0], logger)
    else:
        # Use symbols directly from command line
        symbols = [s.upper() for s in symbols_arg]
        logger.info(f"Using {len(symbols)} symbols from command line")

    if not symbols:
        logger.error("No symbols provided")
        sys.exit(1)

    # Fetch ratios for each symbol
    all_ratios: Dict[str, List[Dict[str, Any]]] = {}
    errors = []

    for i, ticker in enumerate(symbols, 1):
        logger.info(f"[{i}/{len(symbols)}] Fetching ratios for {ticker}...")
        try:
            ratios = fetch_ratios(api_key, ticker, args.limit, logger)
            if ratios:
                all_ratios[ticker] = ratios
                logger.info(f"  Retrieved {len(ratios)} ratio records for {ticker}")
            else:
                logger.warning(f"  No ratio data available for {ticker}")
        except requests.exceptions.RequestException as e:
            logger.error(f"  Network error for {ticker}: {e}")
            errors.append((ticker, str(e)))
        except ValueError as e:
            logger.error(f"  API error for {ticker}: {e}")
            errors.append((ticker, str(e)))
        except Exception as e:
            logger.error(f"  Unexpected error for {ticker}: {e}")
            errors.append((ticker, str(e)))
            if args.verbose:
                raise

    # Save results
    if all_ratios:
        save_ratios_to_csv(all_ratios, args.output, logger)

        logger.info("=" * 60)
        logger.info(f"SUCCESS: Ratios saved to {args.output}")
        logger.info(f"  Symbols processed: {len(symbols)}")
        logger.info(f"  Symbols with data: {len(all_ratios)}")
        if errors:
            logger.info(f"  Errors: {len(errors)}")
        logger.info("=" * 60)
    else:
        logger.error("No ratio data retrieved for any symbol")
        sys.exit(1)

    # Exit with error if there were failures
    if errors:
        logger.warning(f"Completed with {len(errors)} errors")
        sys.exit(0 if len(errors) < len(symbols) else 1)


if __name__ == "__main__":
    main()
