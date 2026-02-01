"""
Combine per-ticker fundamentals CSV files into consolidated files.

Refactored from: combine_fundamentals.py

Usage:
    python -m sp500_tools.processing.combine
    python -m sp500_tools.processing.combine --input-dir data/fundamentals
"""

import csv
import sys
from pathlib import Path

from sp500_tools.utils import setup_logging
from sp500_tools.utils.cli import add_common_args, create_parser

KNOWN_TYPES = {
    "balance_sheets",
    "cash_flow",
    "income_statements",
    "short_interest",
    "short_volume",
    "float",
}


def get_fundamentals_types(input_dir: Path) -> set[str]:
    """Discover fundamentals types from filenames."""
    types: set[str] = set()
    for file in input_dir.glob("*.csv"):
        parts = file.stem.split("_", 1)
        if len(parts) == 2 and parts[1] in KNOWN_TYPES:
            types.add(parts[1])
    return types


def combine_fundamentals(
    input_dir: Path,
    output_dir: Path,
    fund_type: str,
    logger,
) -> int:
    """Combine all CSV files of a specific type into one file."""
    pattern = f"*_{fund_type}.csv"
    files = sorted(input_dir.glob(pattern))

    if not files:
        logger.warning(f"No files found for type: {fund_type}")
        return 0

    logger.info(f"Combining {len(files)} files for {fund_type}...")

    # Collect all headers
    all_headers: set[str] = set()
    for file_path in files:
        with open(file_path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            headers = next(reader)
            all_headers.update(headers)

    # Rename tickers -> ticker
    if "tickers" in all_headers:
        all_headers.remove("tickers")
        all_headers.add("ticker")

    # Sort headers
    common = [
        "ticker",
        "period_end",
        "filing_date",
        "fiscal_quarter",
        "fiscal_year",
        "timeframe",
    ]
    sorted_headers = [h for h in common if h in all_headers]
    sorted_headers += sorted([h for h in all_headers if h not in common])

    logger.info(f"Total columns: {len(sorted_headers)}")

    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{fund_type}.csv"

    total_rows = 0
    seen_keys: set[tuple[str, str, str]] = set()
    duplicates = 0

    with open(output_file, "w", newline="", encoding="utf-8") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=sorted_headers)
        writer.writeheader()

        for i, file_path in enumerate(files, 1):
            with open(file_path, newline="", encoding="utf-8") as in_f:
                reader = csv.DictReader(in_f)

                for row in reader:
                    # Rename tickers to ticker
                    if "tickers" in row:
                        row["ticker"] = row.pop("tickers")

                    # Clean ticker value
                    if "ticker" in row and row["ticker"]:
                        ticker_val = row["ticker"].strip().strip("[]'\"")
                        if "," in ticker_val:
                            ticker_val = ticker_val.split(",")[0].strip().strip("'\"")
                        row["ticker"] = ticker_val

                    # Check duplicates
                    pk = (
                        row.get("ticker", ""),
                        row.get("period_end", ""),
                        row.get("timeframe", ""),
                    )
                    if pk in seen_keys:
                        duplicates += 1
                        continue
                    seen_keys.add(pk)

                    full_row = {col: row.get(col, "") for col in sorted_headers}
                    writer.writerow(full_row)
                    total_rows += 1

            if i % 50 == 0:
                logger.info(f"Progress: {i}/{len(files)} files")

    if duplicates:
        logger.info(f"Skipped {duplicates} duplicates")

    logger.info(f"Combined {total_rows} rows into {output_file}")
    return total_rows


def main() -> int:
    """Main entry point."""
    parser = create_parser(
        "Combine fundamentals CSV files into consolidated files.",
        epilog="""\
Examples:
  %(prog)s
  %(prog)s --input-dir data/fundamentals --output-dir data/combined
  %(prog)s --type balance_sheets
""",
    )

    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/fundamentals"),
        help="Input directory (default: data/fundamentals)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/fundamentals"),
        help="Output directory (default: data/fundamentals)",
    )
    parser.add_argument("--type", help="Combine only specific type")
    add_common_args(parser)

    args = parser.parse_args()
    logger = setup_logging(args.verbose)

    logger.info("=" * 60)
    logger.info("Fundamentals Combiner")
    logger.info("=" * 60)

    if not args.input_dir.exists():
        logger.error(f"Input directory not found: {args.input_dir}")
        return 1

    types = {args.type} if args.type else get_fundamentals_types(args.input_dir)

    if not types:
        logger.error(f"No fundamentals files found in {args.input_dir}")
        return 1

    logger.info(f"Types: {', '.join(sorted(types))}")
    logger.info(f"Output: {args.output_dir}")

    stats: dict[str, int] = {}
    for fund_type in sorted(types):
        logger.info(f"\nProcessing: {fund_type}")
        try:
            rows = combine_fundamentals(
                args.input_dir, args.output_dir, fund_type, logger
            )
            stats[fund_type] = rows
        except Exception as e:
            logger.error(f"Failed: {e}")
            stats[fund_type] = -1

    logger.info("\n" + "=" * 60)
    logger.info("Summary")
    logger.info("=" * 60)
    for fund_type, rows in sorted(stats.items()):
        status = f"{rows} rows" if rows >= 0 else "FAILED"
        logger.info(f"  {fund_type}: {status}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
