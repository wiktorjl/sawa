#!/usr/bin/env python3
"""
Combine fundamentals CSV files into consolidated files for loading.

This script combines multiple per-ticker fundamentals files (e.g., AAPL_balance_sheets.csv,
MSFT_balance_sheets.csv) into single consolidated files (balance_sheets.csv) for easier loading.

Usage:
    python combine_fundamentals.py
    python combine_fundamentals.py --input-dir data/fundamentals --output-dir data/fundamentals_combined

Output:
    Combined CSV files:
    - balance_sheets.csv
    - cash_flows.csv
    - income_statements.csv
    - short_interest.csv
    - short_volume.csv
    - float.csv
"""

import argparse
import csv
import logging
import os
import sys
from pathlib import Path
from typing import List, Set


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


def get_fundamentals_types(input_dir: Path) -> Set[str]:
    """
    Discover all fundamentals types from filenames.

    Files are named like: TICKER_type.csv (e.g., AAPL_balance_sheets.csv)
    Only includes known fundamental types to avoid picking up combined output files.
    """
    known_types = {
        "balance_sheets",
        "cash_flow",
        "income_statements",
        "short_interest",
        "short_volume",
        "float",
    }
    types = set()
    for file in input_dir.glob("*.csv"):
        # Extract type from filename (everything after first underscore, before .csv)
        parts = file.stem.split("_", 1)
        if len(parts) == 2 and parts[1] in known_types:
            types.add(parts[1])
    return types


def combine_fundamentals(
    input_dir: Path,
    output_dir: Path,
    fund_type: str,
    logger: logging.Logger,
) -> int:
    """
    Combine all CSV files of a specific fundamentals type into one file.

    Args:
        input_dir: Directory containing per-ticker CSV files
        output_dir: Directory to write combined CSV
        fund_type: Type of fundamentals (e.g., 'balance_sheets', 'cash_flow')
        logger: Logger instance

    Returns:
        Number of rows written
    """
    pattern = f"*_{fund_type}.csv"
    files = sorted(input_dir.glob(pattern))

    if not files:
        logger.warning(f"No files found for type: {fund_type}")
        return 0

    logger.info(f"Combining {len(files)} files for {fund_type}...")

    # Collect all unique headers from all files
    all_headers = set()
    file_headers_map = {}

    for file_path in files:
        with open(file_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            headers = next(reader)
            file_headers_map[file_path] = headers
            all_headers.update(headers)

    # Rename 'tickers' to 'ticker' to match database schema
    if "tickers" in all_headers:
        all_headers.remove("tickers")
        all_headers.add("ticker")
        logger.info("Renamed 'tickers' column to 'ticker'")

    # Sort headers to ensure consistent order (common columns first)
    common_headers = [
        "ticker",
        "period_end",
        "filing_date",
        "fiscal_quarter",
        "fiscal_year",
        "timeframe",
    ]
    sorted_headers = [h for h in common_headers if h in all_headers]
    sorted_headers += sorted([h for h in all_headers if h not in common_headers])

    logger.info(f"Total unique columns across all files: {len(sorted_headers)}")

    output_file = output_dir / f"{fund_type}.csv"
    output_dir.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    seen_keys = set()  # Track unique (ticker, period_end, timeframe) combinations
    duplicates = 0

    with open(output_file, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=sorted_headers)
        writer.writeheader()

        for i, file_path in enumerate(files, 1):
            logger.debug(f"Processing {i}/{len(files)}: {file_path.name}")

            with open(file_path, "r", newline="", encoding="utf-8") as in_f:
                reader = csv.DictReader(in_f)

                for row in reader:
                    # Rename tickers to ticker if present
                    if "tickers" in row and "ticker" in sorted_headers:
                        row["ticker"] = row.pop("tickers")

                    # Clean up ticker value - remove brackets and quotes
                    if "ticker" in row and row["ticker"]:
                        ticker_val = row["ticker"]
                        # Remove [' and '] or [" and "] wrapping
                        ticker_val = ticker_val.strip().strip("[]'\"")
                        # If it contains multiple tickers (comma-separated), take just the first one
                        if "," in ticker_val:
                            ticker_val = ticker_val.split(",")[0].strip().strip("'\"")
                        row["ticker"] = ticker_val

                    # Check for duplicates based on primary key (ticker, period_end, timeframe)
                    pk_key = (
                        row.get("ticker", ""),
                        row.get("period_end", ""),
                        row.get("timeframe", ""),
                    )
                    if pk_key in seen_keys:
                        duplicates += 1
                        continue
                    seen_keys.add(pk_key)

                    # Ensure all columns are present (missing ones will be empty)
                    full_row = {col: row.get(col, "") for col in sorted_headers}
                    writer.writerow(full_row)
                    total_rows += 1

            if i % 50 == 0:
                logger.info(f"Progress: {i}/{len(files)} files processed")

        if duplicates > 0:
            logger.info(f"Skipped {duplicates} duplicate rows")

    logger.info(f"Combined {total_rows} rows into {output_file}")
    return total_rows


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Combine fundamentals CSV files into consolidated files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  python combine_fundamentals.py
    Combine all fundamentals in data/fundamentals/

  python combine_fundamentals.py --input-dir data/fundamentals --output-dir data/fundamentals_combined
    Use custom directories

Output Files:
  Creates combined CSV files in output directory:
  - balance_sheets.csv (all tickers combined)
  - cash_flows.csv
  - income_statements.csv
  - etc.
""",
    )

    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/fundamentals"),
        help="Directory containing per-ticker fundamentals files (default: data/fundamentals)",
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/fundamentals"),
        help="Directory to write combined files (default: data/fundamentals)",
    )

    parser.add_argument(
        "--type",
        help="Combine only specific type (e.g., 'balance_sheets'). Default: all types",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose (debug) logging",
    )

    args = parser.parse_args()

    logger = setup_logging(args.verbose)

    logger.info("=" * 60)
    logger.info("Fundamentals Combiner")
    logger.info("=" * 60)

    if not args.input_dir.exists():
        logger.error(f"Input directory not found: {args.input_dir}")
        sys.exit(1)

    # Discover fundamentals types
    if args.type:
        types = {args.type}
    else:
        types = get_fundamentals_types(args.input_dir)

    if not types:
        logger.error(f"No fundamentals files found in {args.input_dir}")
        sys.exit(1)

    logger.info(f"Found {len(types)} fundamentals type(s): {', '.join(sorted(types))}")
    logger.info(f"Output directory: {args.output_dir}")
    logger.info("")

    # Combine each type
    total_stats = {}
    for fund_type in sorted(types):
        logger.info(f"Processing: {fund_type}")
        logger.info("-" * 60)

        try:
            rows = combine_fundamentals(
                args.input_dir,
                args.output_dir,
                fund_type,
                logger,
            )
            total_stats[fund_type] = rows
        except Exception as e:
            logger.error(f"Failed to combine {fund_type}: {e}")
            total_stats[fund_type] = -1

    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("Summary")
    logger.info("=" * 60)

    for fund_type, rows in sorted(total_stats.items()):
        if rows >= 0:
            logger.info(f"{fund_type}: {rows} rows")
        else:
            logger.info(f"{fund_type}: FAILED")

    logger.info("")
    logger.info("Done!")


if __name__ == "__main__":
    main()
