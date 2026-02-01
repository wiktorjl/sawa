"""
Main CLI entry point for sp500 command.

Usage:
    sp500 coldstart              # Full database setup
    sp500 update                 # Incremental update
    sp500 update --from-date 2024-01-01  # Force update from date
"""

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from sp500_tools.utils import setup_logging
from sp500_tools.utils.dates import parse_date

# Load .env file from current directory or parent directories
load_dotenv()


def cmd_coldstart(args) -> int:
    """Run cold start."""
    from sp500_tools.coldstart import run_coldstart

    logger = setup_logging(args.verbose)

    # Get credentials
    api_key = args.api_key or os.environ.get("POLYGON_API_KEY")
    s3_access = args.s3_access_key or os.environ.get("POLYGON_S3_ACCESS_KEY")
    s3_secret = args.s3_secret_key or os.environ.get("POLYGON_S3_SECRET_KEY")
    db_url = args.database_url or os.environ.get("DATABASE_URL")

    # Determine mode
    drop_only = args.drop_only
    schema_only = args.schema_only
    load_only = args.load_only
    skip_downloads = args.skip_downloads

    # API credentials only required if downloading data (not for drop/schema/load-only modes)
    needs_api = not (drop_only or schema_only or load_only or skip_downloads)
    if needs_api:
        if not api_key:
            logger.error("POLYGON_API_KEY required (env var or --api-key)")
            return 1
        if not s3_access or not s3_secret:
            logger.error("POLYGON_S3_ACCESS_KEY and POLYGON_S3_SECRET_KEY required")
            return 1

    if not db_url:
        logger.error("DATABASE_URL required (env var or --database-url)")
        return 1

    try:
        stats = run_coldstart(
            api_key=api_key,
            s3_access_key=s3_access,
            s3_secret_key=s3_secret,
            database_url=db_url,
            schema_dir=Path(args.schema_dir),
            output_dir=Path(args.output_dir),
            years=args.years,
            symbols_file=args.symbols_file,
            drop_tables=not args.no_drop,
            drop_only=drop_only,
            schema_only=schema_only,
            load_only=load_only,
            skip_downloads=skip_downloads,
            skip_prices=args.skip_prices,
            skip_fundamentals=args.skip_fundamentals,
            skip_overviews=args.skip_overviews,
            skip_economy=args.skip_economy,
            skip_ratios=args.skip_ratios,
            logger=logger,
        )
        return 0 if stats.get("success") else 1
    except Exception as e:
        logger.error(f"Cold start failed: {e}")
        if args.verbose:
            raise
        return 1


def cmd_update(args) -> int:
    """Run incremental update."""
    from sp500_tools.update import run_update

    logger = setup_logging(args.verbose)

    # Get credentials
    api_key = args.api_key or os.environ.get("POLYGON_API_KEY")
    s3_access = args.s3_access_key or os.environ.get("POLYGON_S3_ACCESS_KEY")
    s3_secret = args.s3_secret_key or os.environ.get("POLYGON_S3_SECRET_KEY")
    db_url = args.database_url or os.environ.get("DATABASE_URL")

    if not api_key:
        logger.error("POLYGON_API_KEY required (env var or --api-key)")
        return 1
    if not s3_access or not s3_secret:
        logger.error("POLYGON_S3_ACCESS_KEY and POLYGON_S3_SECRET_KEY required")
        return 1
    if not db_url:
        logger.error("DATABASE_URL required (env var or --database-url)")
        return 1

    try:
        stats = run_update(
            api_key=api_key,
            s3_access_key=s3_access,
            s3_secret_key=s3_secret,
            database_url=db_url,
            output_dir=Path(args.output_dir),
            force_from_date=args.from_date,
            logger=logger,
        )
        return 0 if stats.get("success") else 1
    except Exception as e:
        logger.error(f"Update failed: {e}")
        if args.verbose:
            raise
        return 1


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        prog="sp500",
        description="S&P 500 data download and database management.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Commands:
  coldstart    Full database setup from scratch
  update       Incremental update since last data

Examples:
  sp500 coldstart --years 5
  sp500 update
  sp500 update --from-date 2024-01-01

Environment Variables:
  POLYGON_API_KEY         Polygon/Massive API key
  POLYGON_S3_ACCESS_KEY   Polygon S3 access key
  POLYGON_S3_SECRET_KEY   Polygon S3 secret key
  DATABASE_URL            PostgreSQL connection URL
""",
    )

    # Common arguments
    parser.add_argument("-v", "--verbose", action="store_true", help="Debug logging")

    subparsers = parser.add_subparsers(dest="command", help="Command")

    # Cold start subcommand
    cold_parser = subparsers.add_parser(
        "coldstart",
        help="Full database setup from scratch",
        description="Drop database, create schema, download all historical data.",
    )
    cold_parser.add_argument("--years", type=int, default=5, help="Years of history (default: 5)")
    cold_parser.add_argument("--schema-dir", default="sqlschema", help="SQL schema directory")
    cold_parser.add_argument("--output-dir", default="data", help="Output data directory")
    cold_parser.add_argument(
        "--symbols-file", type=Path, help="File with symbols to use (one per line)"
    )
    cold_parser.add_argument("--no-drop", action="store_true", help="Don't drop existing tables")
    cold_parser.add_argument("--api-key", help="Polygon API key")
    cold_parser.add_argument("--s3-access-key", help="Polygon S3 access key")
    cold_parser.add_argument("--s3-secret-key", help="Polygon S3 secret key")
    cold_parser.add_argument("--database-url", help="PostgreSQL URL")
    # Mode options (mutually exclusive-ish)
    cold_parser.add_argument(
        "--drop-only", action="store_true", help="Only drop tables and clean data, then exit"
    )
    cold_parser.add_argument(
        "--schema-only", action="store_true", help="Only set up schema (no download/load)"
    )
    cold_parser.add_argument(
        "--load-only", action="store_true", help="Only load existing CSV data (no schema changes)"
    )
    # Skip options
    cold_parser.add_argument(
        "--skip-downloads", action="store_true", help="Skip downloads, load existing CSV data"
    )
    cold_parser.add_argument("--skip-prices", action="store_true", help="Skip price data download")
    cold_parser.add_argument(
        "--skip-fundamentals", action="store_true", help="Skip fundamentals download"
    )
    cold_parser.add_argument(
        "--skip-overviews", action="store_true", help="Skip company overviews download"
    )
    cold_parser.add_argument(
        "--skip-economy", action="store_true", help="Skip economy data download"
    )
    cold_parser.add_argument(
        "--skip-ratios", action="store_true", help="Skip financial ratios download"
    )
    cold_parser.add_argument("-v", "--verbose", action="store_true")
    cold_parser.set_defaults(func=cmd_coldstart)

    # Update subcommand
    update_parser = subparsers.add_parser(
        "update",
        help="Incremental update since last data",
        description="Check last date in database and pull new data.",
    )
    update_parser.add_argument(
        "--from-date",
        type=parse_date,
        metavar="YYYY-MM-DD",
        help="Force update from specific date",
    )
    update_parser.add_argument("--output-dir", default="data", help="Output data directory")
    update_parser.add_argument("--api-key", help="Polygon API key")
    update_parser.add_argument("--s3-access-key", help="Polygon S3 access key")
    update_parser.add_argument("--s3-secret-key", help="Polygon S3 secret key")
    update_parser.add_argument("--database-url", help="PostgreSQL URL")
    update_parser.add_argument("-v", "--verbose", action="store_true")
    update_parser.set_defaults(func=cmd_update)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
