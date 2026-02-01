#!/usr/bin/env python3
"""
Download daily OHLC prices for S&P 500 symbols from Polygon.io S3 bulk data.

This script downloads bulk files from Polygon.io's Massive flat files S3 bucket
and splits them into per-symbol CSV files. This is much more efficient than
making 500+ individual API calls per day.

Supports downloading single days or date ranges with informative progress display.

Usage:
    python download_daily_prices.py 2024-01-02
    python download_daily_prices.py 2024-01-02 --end-date 2024-12-31
    python download_daily_prices.py 2024-01-02 --days 365
    python download_daily_prices.py 2024-01-02 --continue
    python download_daily_prices.py 2024-01-02 -o prices/

Output:
    One CSV file per symbol in the output directory.
    Schema: date,symbol,open,close,high,low,volume

Environment Variables:
    POLYGON_S3_ACCESS_KEY - S3 access key for Polygon Massive files
    POLYGON_S3_SECRET_KEY - S3 secret key for Polygon Massive files
"""

from __future__ import annotations

import argparse
import calendar
import csv
import gzip
import io
import logging
import os
import sys
import tempfile
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional, Set, Tuple

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError


# Configuration
S3_ENDPOINT = "https://files.polygon.io"
S3_BUCKET = "flatfiles"
DEFAULT_OUTPUT_DIR = "data/prices"
DEFAULT_SYMBOLS_FILE = "sp500_symbols.txt"
DATE_FORMAT = "%Y-%m-%d"

# S3 key template for daily aggregates
# Format: us_stocks_sip/day_aggs_v1/{year}/{month}/{date}.csv.gz
S3_KEY_TEMPLATE = "us_stocks_sip/day_aggs_v1/{year}/{month}/{date}.csv.gz"


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


def parse_date(date_str: str) -> date:
    """Parse date string in YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_str, DATE_FORMAT).date()
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Use YYYY-MM-DD."
        ) from e


def get_date_range(
    start_date: date, end_date: Optional[date], days: Optional[int]
) -> List[date]:
    """
    Generate a list of dates to download.

    Args:
        start_date: First date to download
        end_date: Last date to download (inclusive), or None
        days: Number of days to download from start_date, or None

    Returns:
        List of dates
    """
    if end_date:
        # Use explicit end date
        end = end_date
    elif days:
        # Use days offset
        end = start_date + timedelta(days=days - 1)
    else:
        # Single day
        return [start_date]

    # Generate all dates in range
    dates = []
    current = start_date
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)

    return dates


def format_duration(seconds: float) -> str:
    """Format duration in human readable form."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def get_s3_credentials(
    access_key: Optional[str], secret_key: Optional[str]
) -> Tuple[str, str]:
    """
    Get S3 credentials from arguments or environment variables.

    Args:
        access_key: Optional CLI-provided access key
        secret_key: Optional CLI-provided secret key

    Returns:
        Tuple of (access_key, secret_key)

    Raises:
        RuntimeError: If credentials are not found
    """
    access = access_key or os.environ.get("POLYGON_S3_ACCESS_KEY")
    secret = secret_key or os.environ.get("POLYGON_S3_SECRET_KEY")

    if not access or not secret:
        raise RuntimeError(
            "Missing S3 credentials. Provide --s3-access-key/--s3-secret-key or set "
            "POLYGON_S3_ACCESS_KEY and POLYGON_S3_SECRET_KEY environment variables."
        )

    return access, secret


def create_s3_client(access_key: str, secret_key: str, endpoint: str) -> boto3.client:
    """Create an S3 client with the given credentials."""
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    return session.client(
        "s3",
        endpoint_url=endpoint,
        config=BotoConfig(signature_version="s3v4"),
    )


def load_symbols(symbols_file: str, logger: logging.Logger) -> Set[str]:
    """
    Load symbol list from a text file.

    Args:
        symbols_file: Path to file with one symbol per line
        logger: Logger instance

    Returns:
        Set of symbol strings (for O(1) lookup)

    Raises:
        FileNotFoundError: If symbols file doesn't exist
    """
    logger.info(f"Loading symbols from {symbols_file}...")

    if not os.path.exists(symbols_file):
        raise FileNotFoundError(f"Symbols file not found: {symbols_file}")

    with open(symbols_file, "r") as f:
        symbols = {line.strip() for line in f if line.strip()}

    if not symbols:
        raise ValueError(f"No symbols found in {symbols_file}")

    logger.info(f"Loaded {len(symbols)} symbols")
    return symbols


def load_trading_days(trading_days_file: str, logger: logging.Logger) -> Set[date]:
    """
    Load valid trading days from a text file.

    Args:
        trading_days_file: Path to file with one date per line (YYYY-MM-DD format)
        logger: Logger instance

    Returns:
        Set of date objects

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file is empty or has invalid dates
    """
    logger.info(f"Loading trading days from {trading_days_file}...")

    if not os.path.exists(trading_days_file):
        raise FileNotFoundError(f"Trading days file not found: {trading_days_file}")

    trading_days = set()
    with open(trading_days_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                d = datetime.strptime(line, DATE_FORMAT).date()
                trading_days.add(d)
            except ValueError as e:
                logger.warning(f"Skipping invalid date in trading days file: {line}")
                continue

    if not trading_days:
        raise ValueError(f"No valid trading days found in {trading_days_file}")

    logger.info(f"Loaded {len(trading_days)} trading days")
    return trading_days


def get_completed_symbols(
    output_dir: str, target_date: date, logger: logging.Logger
) -> Set[str]:
    """
    Find symbols that already have data for the target date.

    Checks if {symbol}.csv exists and if the last line contains the target date.

    Args:
        output_dir: Path to output directory
        target_date: The date being downloaded
        logger: Logger instance

    Returns:
        Set of symbol names that are already complete for this date
    """
    completed = set()
    output_path = Path(output_dir)

    if not output_path.exists():
        return completed

    date_str = target_date.strftime(DATE_FORMAT)

    for csv_file in output_path.glob("*.csv"):
        symbol = csv_file.stem
        if is_date_in_file(csv_file, date_str):
            completed.add(symbol)

    return completed


def is_date_in_file(filepath: Path, target_date: str) -> bool:
    """
    Check if the target date is the last entry in the file.

    Efficiently reads only the last 4KB of the file to check the last line.

    Args:
        filepath: Path to the CSV file
        target_date: Date string to check for

    Returns:
        True if the last line's date matches target_date
    """
    try:
        with open(filepath, "rb") as f:
            # Seek to end to get file size
            f.seek(0, os.SEEK_END)
            size = f.tell()

            if size == 0:
                return False

            # Read last 4KB (should be enough for last line)
            offset = min(size, 4096)
            f.seek(-offset, os.SEEK_END)
            chunk = f.read(offset)

        # Parse lines from the chunk
        lines = chunk.splitlines()
        for line in reversed(lines):
            if not line:
                continue
            try:
                text = line.decode("utf-8")
            except UnicodeDecodeError:
                return False

            # Extract date from first column
            parts = text.split(",", 1)
            if parts and parts[0].strip() == target_date:
                return True

        return False
    except OSError:
        return False


def download_bulk_file(
    s3_client: boto3.client,
    bucket: str,
    key: str,
    logger: logging.Logger,
) -> Optional[str]:
    """
    Download the bulk file from S3 to a temporary location.

    Args:
        s3_client: Boto3 S3 client
        bucket: S3 bucket name
        key: S3 object key
        logger: Logger instance

    Returns:
        Path to the downloaded temporary file, or None if file not found (404)

    Raises:
        Exception: If download fails (non-404 errors)
    """
    logger.debug(f"Downloading s3://{bucket}/{key}")

    try:
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            s3_client.download_fileobj(bucket, key, tmp)
            return tmp.name
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        if error_code == "404":
            return None
        raise


def parse_bulk_file(
    filepath: str,
    target_symbols: Set[str],
    target_date: date,
    output_dir: str,
    skip_symbols: Set[str],
    logger: logging.Logger,
) -> Tuple[int, int, int]:
    """
    Parse the bulk file and write per-symbol CSV files.

    Args:
        filepath: Path to the downloaded bulk file
        target_symbols: Set of symbols to extract
        target_date: The date being processed
        output_dir: Directory to write output files
        skip_symbols: Set of symbols to skip (already completed)
        logger: Logger instance

    Returns:
        Tuple of (success_count, skipped_count, not_found_count)
    """
    success_count = 0
    skipped_count = len(skip_symbols)
    not_found_count = 0
    found_symbols = set()

    date_str = target_date.strftime(DATE_FORMAT)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Open gzip file and parse CSV
    with gzip.open(filepath, "rt", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        if reader.fieldnames is None:
            raise ValueError("CSV file missing header row")

        # Map lowercase column names to actual names
        header_map = {name.lower(): name for name in reader.fieldnames}

        # Find required columns
        symbol_col = _resolve_column(header_map, ["symbol", "ticker", "sym"])
        open_col = _resolve_column(header_map, ["open", "o"])
        close_col = _resolve_column(header_map, ["close", "c"])
        high_col = _resolve_column(header_map, ["high", "h"])
        low_col = _resolve_column(header_map, ["low", "l"])
        volume_col = _resolve_column(header_map, ["volume", "v"])

        for row in reader:
            symbol = (row.get(symbol_col) or "").strip()

            if not symbol:
                continue

            # Skip if not in our target list
            if symbol not in target_symbols:
                continue

            found_symbols.add(symbol)

            # Skip if already completed
            if symbol in skip_symbols:
                continue

            # Write to per-symbol file
            out_path = output_path / f"{symbol}.csv"
            file_exists = out_path.exists()

            # Check if this date already exists in the file (prevent duplicates)
            if file_exists and is_date_in_file(out_path, date_str):
                continue

            with open(out_path, "a", newline="") as out_f:
                writer = csv.writer(out_f)

                if not file_exists:
                    writer.writerow(
                        ["date", "symbol", "open", "close", "high", "low", "volume"]
                    )

                writer.writerow(
                    [
                        date_str,
                        symbol,
                        row.get(open_col, ""),
                        row.get(close_col, ""),
                        row.get(high_col, ""),
                        row.get(low_col, ""),
                        row.get(volume_col, ""),
                    ]
                )

            success_count += 1

    # Check for symbols not found in bulk file
    not_found = target_symbols - found_symbols
    not_found_count = len(not_found)

    return success_count, skipped_count, not_found_count


def _resolve_column(header_map: dict, candidates: list) -> str:
    """
    Resolve column name from list of candidates.

    Args:
        header_map: Mapping of lowercase column names to actual names
        candidates: List of possible column names to try

    Returns:
        Actual column name from the header

    Raises:
        RuntimeError: If no candidate matches
    """
    for name in candidates:
        key = name.lower()
        if key in header_map:
            return header_map[key]
    raise RuntimeError(f"None of the candidate columns found: {candidates}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Download daily OHLC prices from Polygon.io S3 bulk data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""\
Examples:
  %(prog)s 2024-01-02
    Download prices for all S&P 500 symbols on 2024-01-02

  %(prog)s 2024-01-02 --end-date 2024-12-31
    Download prices for entire year 2024

  %(prog)s 2024-01-02 --days 365
    Download 365 days of prices starting from 2024-01-02

  %(prog)s 2024-01-02 --trading-days trading_days_2024-01-01.txt
    Only download valid trading days from the specified file

  %(prog)s 2024-01-02 --continue
    Resume a previous download (skip already completed symbols)

  %(prog)s 2024-01-02 -o prices/ --symbols my_symbols.txt
    Use custom output directory and symbols file

  %(prog)s 2024-01-02 --s3-access-key KEY --s3-secret-key SECRET
    Provide S3 credentials via command line

Output Format:
  One CSV file per symbol: {{symbol}}.csv
  Schema: date,symbol,open,close,high,low,volume
  Files are appended to, so multiple dates can accumulate in one file.

Environment Variables:
  POLYGON_S3_ACCESS_KEY
    S3 access key for Polygon Massive files
  POLYGON_S3_SECRET_KEY
    S3 secret key for Polygon Massive files
""",
    )

    parser.add_argument(
        "start_date",
        type=parse_date,
        help="Start date to download prices for (YYYY-MM-DD format)",
    )

    parser.add_argument(
        "--end-date",
        type=parse_date,
        metavar="YYYY-MM-DD",
        help="End date (inclusive). If not specified, downloads only start_date",
    )

    parser.add_argument(
        "--days",
        type=int,
        metavar="N",
        help="Number of days to download from start_date",
    )

    parser.add_argument(
        "-o",
        "--output",
        default=DEFAULT_OUTPUT_DIR,
        metavar="DIR",
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )

    parser.add_argument(
        "--symbols",
        default=DEFAULT_SYMBOLS_FILE,
        metavar="FILE",
        help=f"Symbols file path (default: {DEFAULT_SYMBOLS_FILE})",
    )

    parser.add_argument(
        "--continue",
        dest="continue_mode",
        action="store_true",
        help="Resume interrupted download (skip already completed symbols/dates)",
    )

    parser.add_argument(
        "--s3-access-key",
        default=None,
        help="S3 access key (overrides environment variable)",
    )

    parser.add_argument(
        "--s3-secret-key",
        default=None,
        help="S3 secret key (overrides environment variable)",
    )

    parser.add_argument(
        "--trading-days",
        default=None,
        metavar="FILE",
        help="File with valid trading dates (one per line). If provided, only these dates will be downloaded.",
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose (debug) logging"
    )

    args = parser.parse_args()

    # Setup logging - use stderr for progress, stdout for clean output
    logger = setup_logging(args.verbose)

    # Calculate date range
    dates_to_download = get_date_range(args.start_date, args.end_date, args.days)

    # Filter to trading days if file provided
    if args.trading_days:
        trading_days = load_trading_days(args.trading_days, logger)
        original_count = len(dates_to_download)
        dates_to_download = [d for d in dates_to_download if d in trading_days]
        filtered_count = original_count - len(dates_to_download)
        if filtered_count > 0:
            logger.info(f"Filtered {filtered_count} non-trading days")

    total_days = len(dates_to_download)

    if total_days == 0:
        logger.error("No trading days to download!")
        sys.exit(1)

    # ANSI color codes
    BOLD = "\033[1m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    CYAN = "\033[96m"
    RESET = "\033[0m"
    CLEAR_LINE = "\r\033[K"

    def print_progress_bar(percent: float, width: int = 40) -> str:
        """Create a progress bar string."""
        filled = int(width * percent / 100)
        bar = "█" * filled + "░" * (width - filled)
        return f"{CYAN}[{bar}]{RESET}"

    def print_status_line(
        day_num: int, total: int, date_str: str, status: str, extra: str = ""
    ) -> None:
        """Print a compact status line that overwrites itself."""
        percent = (day_num / total) * 100
        bar = print_progress_bar(percent, 30)
        line = f"{CLEAR_LINE}{bar} {BOLD}{day_num}/{total}{RESET} ({percent:5.1f}%) | {date_str} | {status}"
        if extra:
            line += f" | {extra}"
        sys.stdout.write(line)
        sys.stdout.flush()

    try:
        # Load symbols
        target_symbols = load_symbols(args.symbols, logger)

        # Get S3 credentials
        access_key, secret_key = get_s3_credentials(
            args.s3_access_key, args.s3_secret_key
        )
        s3_client = create_s3_client(access_key, secret_key, S3_ENDPOINT)

        # Print header
        print(f"\n{BOLD}{'═' * 80}{RESET}")
        print(f"{BOLD}{'S&P 500 Price Downloader':^80}{RESET}")
        print(f"{BOLD}{'═' * 80}{RESET}")
        print(
            f"  📅 Range:  {CYAN}{dates_to_download[0]}{RESET} → {CYAN}{dates_to_download[-1]}{RESET} ({total_days} days)"
        )
        print(f"  📊 Symbols: {len(target_symbols)}")
        print(f"  📁 Output:  {args.output}/")
        print(f"  ⏭️  Resume: {'YES' if args.continue_mode else 'NO'}")
        print(f"{BOLD}{'─' * 80}{RESET}\n")

        # Track progress
        total_success = 0
        total_skipped = 0
        total_not_found = 0
        days_success = 0
        days_skipped = 0
        days_error = 0

        start_time = time.time()

        # Process each date
        for day_num, current_date in enumerate(dates_to_download, 1):
            date_str = current_date.strftime(DATE_FORMAT)

            try:
                # Determine which symbols to skip
                skip_symbols = set()
                if args.continue_mode:
                    skip_symbols = get_completed_symbols(
                        args.output, current_date, logger
                    )

                # Build S3 key
                year = f"{current_date.year:04d}"
                month = f"{current_date.month:02d}"
                s3_key = S3_KEY_TEMPLATE.format(year=year, month=month, date=date_str)

                print_status_line(
                    day_num, total_days, date_str, f"{YELLOW}DOWNLOADING{RESET}"
                )

                # Download and process
                temp_path = download_bulk_file(s3_client, S3_BUCKET, s3_key, logger)

                if temp_path is None:
                    days_skipped += 1
                    print_status_line(
                        day_num,
                        total_days,
                        date_str,
                        f"{YELLOW}NO DATA{RESET}",
                        "weekend/holiday",
                    )
                    print()  # New line for next entry
                    continue

                print_status_line(
                    day_num, total_days, date_str, f"{CYAN}PROCESSING{RESET}"
                )

                try:
                    success, skipped, not_found = parse_bulk_file(
                        temp_path,
                        target_symbols,
                        current_date,
                        args.output,
                        skip_symbols,
                        logger,
                    )

                    total_success += success
                    total_skipped += skipped
                    total_not_found += not_found

                    if success > 0:
                        days_success += 1
                        extra = f"+{success} symbols"
                        print_status_line(
                            day_num,
                            total_days,
                            date_str,
                            f"{GREEN}✓ DONE{RESET}",
                            extra,
                        )
                    elif skipped == len(target_symbols):
                        days_skipped += 1
                        print_status_line(
                            day_num,
                            total_days,
                            date_str,
                            f"{YELLOW}⏭ SKIPPED{RESET}",
                            "all complete",
                        )
                    else:
                        print_status_line(
                            day_num,
                            total_days,
                            date_str,
                            f"{YELLOW}⚠ EMPTY{RESET}",
                            f"{not_found} missing",
                        )

                    print()  # New line for next entry

                finally:
                    try:
                        os.unlink(temp_path)
                    except OSError:
                        pass

            except Exception as e:
                days_error += 1
                print_status_line(
                    day_num, total_days, date_str, f"{RED}✗ ERROR{RESET}", str(e)[:40]
                )
                print()
                if args.verbose:
                    logger.exception("Detailed error:")
                continue

        # Final summary
        elapsed = time.time() - start_time
        rate = total_success / elapsed if elapsed > 0 else 0

        print(f"\n{BOLD}{'═' * 80}{RESET}")
        print(f"{BOLD}{'DOWNLOAD COMPLETE':^80}{RESET}")
        print(f"{BOLD}{'═' * 80}{RESET}")
        print(f"  ⏱️  Duration: {format_duration(elapsed)} ({rate:.1f} symbols/sec)")
        print(
            f"  📅 Days:     {GREEN}{days_success} success{RESET} | {YELLOW}{days_skipped} skipped{RESET} | {RED}{days_error} errors{RESET}"
        )
        print(
            f"  📊 Symbols:  {GREEN}{total_success} written{RESET} | {YELLOW}{total_skipped} skipped{RESET} | {RED}{total_not_found} not found{RESET}"
        )
        print(f"  📁 Output:   {args.output}/")
        print(f"{BOLD}{'═' * 80}{RESET}\n")

        if days_success == 0 and days_skipped == 0:
            print(f"{RED}✗ No data downloaded{RESET}\n")
            sys.exit(1)
        else:
            print(f"{GREEN}✓ All done!{RESET}\n")

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        sys.exit(1)
    except (RuntimeError, ValueError, IOError) as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        if args.verbose:
            raise
        sys.exit(1)


if __name__ == "__main__":
    main()
