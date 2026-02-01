#!/usr/bin/env python3
"""
Download S&P 500 constituent symbols from Wikipedia.

This script fetches the current S&P 500 constituents from Wikipedia's
"List of S&P 500 companies" page and saves them to a text file.

The S&P 500 contains 500 companies but typically returns ~503 symbols
due to multiple share classes (e.g., GOOGL/GOOG, BRK.B).

Usage:
    python download_sp500_symbols.py
    python download_sp500_symbols.py -o symbols.txt -v

Output:
    A text file with one ticker symbol per line, in source order.
"""

import argparse
import html
import logging
import os
import re
import sys
import urllib.request
from tempfile import NamedTemporaryFile
from typing import List

from bs4 import BeautifulSoup


# Configuration
WIKIPEDIA_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
DEFAULT_OUTPUT_FILE = "data/sp500_symbols.txt"

# HTTP headers for Wikipedia requests (required by their bot policy)
WIKIPEDIA_HEADERS = {"User-Agent": "SP500-Data-Downloader/1.0 (research project)"}


def create_logger(name: str, verbose: bool = False) -> logging.Logger:
    """Create and return a dedicated logger with explicit handlers."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    # Clear any existing handlers to avoid duplicates
    logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def clean_cell(text: str) -> str:
    """
    Normalize cell content by:
    - Unescaping HTML entities
    - Converting non-breaking spaces to regular spaces
    - Removing footnote markers like [1], [2], etc.
    - Normalizing whitespace
    """
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\[[^\]]*\]", "", text)
    return re.sub(r"\s+", " ", text).strip()


def fetch_sp500_symbols(logger: logging.Logger) -> List[str]:
    """
    Fetch S&P 500 constituent symbols from Wikipedia.

    Parses the "List of S&P 500 companies" Wikipedia page and extracts
    ticker symbols from the constituents table.

    Args:
        logger: Logger instance for progress reporting

    Returns:
        List of ticker symbols in source order

    Raises:
        urllib.error.URLError: If HTTP request fails
        ValueError: If parsing fails or no symbols found
    """
    logger.info(f"Fetching S&P 500 constituents from Wikipedia...")
    logger.debug(f"URL: {WIKIPEDIA_URL}")

    # Fetch Wikipedia page using urllib
    request = urllib.request.Request(WIKIPEDIA_URL, headers=WIKIPEDIA_HEADERS)

    with urllib.request.urlopen(request, timeout=30) as response:
        html_content = response.read().decode("utf-8")

    # Parse HTML with BeautifulSoup
    soup = BeautifulSoup(html_content, "html.parser")
    symbols = _parse_constituents_table(soup, logger)

    # Validate results - only check for zero symbols
    if not symbols:
        raise ValueError("No symbols extracted from Wikipedia page")

    logger.info(f"Successfully extracted {len(symbols)} unique symbols")
    return symbols


def _parse_constituents_table(soup: BeautifulSoup, logger: logging.Logger) -> List[str]:
    """
    Parse S&P 500 symbols from BeautifulSoup-parsed HTML.

    Targets the constituents table by its semantic id="constituents".
    Symbol is in the first column of each data row.

    Args:
        soup: BeautifulSoup-parsed HTML
        logger: Logger instance

    Returns:
        List of ticker symbols in source order
    """
    symbols = []
    seen = set()

    # Find the constituents table by semantic id
    table = soup.find("table", {"id": "constituents"})
    if not table:
        raise ValueError("Could not locate S&P 500 constituents table")

    # Find all data rows (skip header row)
    rows = table.find_all("tr")[1:]  # First row is header
    logger.debug(f"Found {len(rows)} data rows in constituents table")

    for row in rows:
        # Find all cells in this row
        cells = row.find_all("td")
        if cells:
            # First cell contains the ticker symbol
            ticker = clean_cell(cells[0].get_text())
            if ticker and ticker not in seen:
                symbols.append(ticker)
                seen.add(ticker)

    return symbols


def save_symbols(symbols: List[str], output_file: str, logger: logging.Logger) -> None:
    """
    Save symbols to a text file atomically, one per line.

    Uses a temporary file and os.replace() to ensure atomic writes.

    Args:
        symbols: List of ticker symbols
        output_file: Path to output file
        logger: Logger instance

    Raises:
        IOError: If file write fails
    """
    logger.info(f"Saving {len(symbols)} symbols to {output_file}...")

    output_dir = os.path.dirname(os.path.abspath(output_file))
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    try:
        # Atomic write using tempfile and os.replace
        with NamedTemporaryFile("w", delete=False, dir=output_dir or ".") as tmp:
            for symbol in symbols:
                tmp.write(f"{symbol}\n")
            tmp_name = tmp.name

        os.replace(tmp_name, output_file)
        logger.info(f"Successfully saved to {output_file}")
    except IOError as e:
        raise IOError(f"Failed to write to {output_file}: {e}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Download S&P 500 constituent symbols from Wikipedia.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""\
Examples:
  %(prog)s
    Download symbols to default file ({DEFAULT_OUTPUT_FILE})

  %(prog)s -o my_symbols.txt
    Save to custom output file

  %(prog)s -v
    Enable verbose (debug) logging

Output Format:
  One ticker symbol per line, in source order (preserves Wikipedia ordering).

Data Source:
  Wikipedia - List of S&P 500 companies
  {WIKIPEDIA_URL.replace("%", "%%")}
""",
    )

    parser.add_argument(
        "-o",
        "--output",
        default=DEFAULT_OUTPUT_FILE,
        metavar="FILE",
        help=f"Output file path (default: {DEFAULT_OUTPUT_FILE})",
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose (debug) logging"
    )

    args = parser.parse_args()

    # Setup logging
    logger = create_logger(__name__, args.verbose)

    logger.info("=" * 60)
    logger.info("S&P 500 Symbol Downloader")
    logger.info("=" * 60)

    try:
        # Fetch and save symbols
        symbols = fetch_sp500_symbols(logger)
        save_symbols(symbols, args.output, logger)

        logger.info("=" * 60)
        logger.info(f"SUCCESS: {len(symbols)} symbols saved to {args.output}")
        logger.info("=" * 60)

    except urllib.error.URLError as e:
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
