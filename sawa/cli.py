"""
Main CLI entry point for sawa command.

Usage:
    sawa coldstart              # Full database setup
    sawa daily                  # Daily price update
    sawa weekly                 # Weekly fundamentals/economy update
    sawa update                 # Legacy: runs both daily + weekly
"""

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from sawa.utils import setup_logging
from sawa.utils.dates import parse_date

# Load .env file from current directory or parent directories
load_dotenv()


def get_log_dir(args) -> Path | None:
    """Get log directory from args, creating if needed."""
    if hasattr(args, "log_dir") and args.log_dir:
        log_dir = Path(args.log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        return log_dir
    return None


def cmd_coldstart(args) -> int:
    """Run cold start."""
    from sawa.coldstart import run_coldstart

    logger = setup_logging(args.verbose, log_dir=get_log_dir(args), run_name="coldstart")

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
            skip_news=args.skip_news,
            logger=logger,
        )
        return 0 if stats.get("success") else 1
    except Exception as e:
        logger.error(f"Cold start failed: {e}")
        if args.verbose:
            raise
        return 1


def cmd_daily(args) -> int:
    """Run daily price update."""
    from sawa.daily import run_daily

    logger = setup_logging(args.verbose, log_dir=get_log_dir(args), run_name="daily")

    # Get credentials (S3 not needed - using REST API)
    api_key = args.api_key or os.environ.get("POLYGON_API_KEY")
    db_url = args.database_url or os.environ.get("DATABASE_URL")

    if not api_key:
        logger.error("POLYGON_API_KEY required (env var or --api-key)")
        return 1
    if not db_url:
        logger.error("DATABASE_URL required (env var or --database-url)")
        return 1

    try:
        stats = run_daily(
            api_key=api_key,
            database_url=db_url,
            force_from_date=args.from_date,
            skip_news=args.skip_news,
            dry_run=args.dry_run,
            logger=logger,
        )
        return 0 if stats.get("success") else 1
    except Exception as e:
        logger.error(f"Daily update failed: {e}")
        if args.verbose:
            raise
        return 1


def cmd_add_symbol(args) -> int:
    """Add new symbols to database."""
    from sawa.add_symbol import run_add_symbols

    logger = setup_logging(args.verbose, log_dir=get_log_dir(args), run_name="add_symbol")

    # Get credentials
    api_key = args.api_key or os.environ.get("POLYGON_API_KEY")
    db_url = args.database_url or os.environ.get("DATABASE_URL")

    if not api_key:
        logger.error("POLYGON_API_KEY required (env var or --api-key)")
        return 1
    if not db_url:
        logger.error("DATABASE_URL required (env var or --database-url)")
        return 1

    # Collect symbols from args and/or file
    symbols: list[str] = list(args.symbols) if args.symbols else []

    if args.file:
        if not args.file.exists():
            logger.error(f"File not found: {args.file}")
            return 1
        with open(args.file) as f:
            for line in f:
                sym = line.strip()
                if sym and not sym.startswith("#"):
                    symbols.append(sym)

    if not symbols:
        logger.error("No symbols specified. Use positional args or --file")
        return 1

    # Remove duplicates, preserve order
    seen: set[str] = set()
    unique_symbols: list[str] = []
    for s in symbols:
        s_upper = s.upper()
        if s_upper not in seen:
            seen.add(s_upper)
            unique_symbols.append(s_upper)

    try:
        stats = run_add_symbols(
            api_key=api_key,
            database_url=db_url,
            symbols=unique_symbols,
            years=args.years,
            dry_run=args.dry_run,
            logger=logger,
        )
        return 0 if stats.get("success") else 1
    except Exception as e:
        logger.error(f"Add symbols failed: {e}")
        if args.verbose:
            raise
        return 1


def cmd_weekly(args) -> int:
    """Run weekly data update."""
    from sawa.weekly import run_weekly

    logger = setup_logging(args.verbose, log_dir=get_log_dir(args), run_name="weekly")

    # Get credentials
    api_key = args.api_key or os.environ.get("POLYGON_API_KEY")
    db_url = args.database_url or os.environ.get("DATABASE_URL")

    if not api_key:
        logger.error("POLYGON_API_KEY required (env var or --api-key)")
        return 1
    if not db_url:
        logger.error("DATABASE_URL required (env var or --database-url)")
        return 1

    try:
        stats = run_weekly(
            api_key=api_key,
            database_url=db_url,
            output_dir=Path(args.output_dir),
            skip_fundamentals=args.skip_fundamentals,
            skip_economy=args.skip_economy,
            skip_overviews=args.skip_overviews,
            skip_ratios=args.skip_ratios,
            skip_news=args.skip_news,
            dry_run=args.dry_run,
            logger=logger,
        )
        return 0 if stats.get("success") else 1
    except Exception as e:
        logger.error(f"Weekly update failed: {e}")
        if args.verbose:
            raise
        return 1


def cmd_update(args) -> int:
    """Run incremental update (legacy: daily + weekly)."""
    from sawa.update import run_update

    logger = setup_logging(args.verbose, log_dir=get_log_dir(args), run_name="update")

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
        prog="sawa",
        description="S&P 500 data download and database management.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Commands:
  coldstart    Full database setup from scratch
  daily        Daily stock price update
  weekly       Weekly fundamentals/economy update
  update       Legacy: combined daily + weekly update

Examples:
  sawa coldstart --years 5
  sawa daily
  sawa weekly --skip-news
  sawa daily --dry-run

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
    cold_parser.add_argument("--skip-news", action="store_true", help="Skip news download")
    cold_parser.add_argument("--log-dir", help="Directory for log files")
    cold_parser.add_argument("-v", "--verbose", action="store_true")
    cold_parser.set_defaults(func=cmd_coldstart)

    # Daily update subcommand (uses REST API, no S3 needed)
    daily_parser = subparsers.add_parser(
        "daily",
        help="Daily stock price and news update",
        description="Update stock prices and news via REST API (fast, daily operation).",
    )
    daily_parser.add_argument(
        "--from-date",
        type=parse_date,
        metavar="YYYY-MM-DD",
        help="Force update from specific date",
    )
    daily_parser.add_argument("--api-key", help="Polygon API key")
    daily_parser.add_argument("--database-url", help="PostgreSQL URL")
    daily_parser.add_argument("--skip-news", action="store_true", help="Skip news update")
    daily_parser.add_argument("--log-dir", help="Directory for log files")
    daily_parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    daily_parser.add_argument("-v", "--verbose", action="store_true")
    daily_parser.set_defaults(func=cmd_daily)

    # Add symbol subcommand
    add_parser = subparsers.add_parser(
        "add-symbol",
        help="Add new symbols to database",
        description="Add new stock symbols with company info and price history.",
    )
    add_parser.add_argument(
        "symbols",
        nargs="*",
        help="Ticker symbols to add (e.g., U PLTR COIN)",
    )
    add_parser.add_argument(
        "--file", "-f",
        type=Path,
        help="File with symbols (one per line)",
    )
    add_parser.add_argument(
        "--years",
        type=int,
        default=5,
        help="Years of price history to fetch (default: 5)",
    )
    add_parser.add_argument("--api-key", help="Polygon API key")
    add_parser.add_argument("--database-url", help="PostgreSQL URL")
    add_parser.add_argument("--log-dir", help="Directory for log files")
    add_parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    add_parser.add_argument("-v", "--verbose", action="store_true")
    add_parser.set_defaults(func=cmd_add_symbol)

    # Weekly update subcommand
    weekly_parser = subparsers.add_parser(
        "weekly",
        help="Weekly fundamentals/economy update",
        description="Update slow-changing data: fundamentals, economy, overviews, ratios, news.",
    )
    weekly_parser.add_argument("--output-dir", default="data", help="Output data directory")
    weekly_parser.add_argument("--api-key", help="Polygon API key")
    weekly_parser.add_argument("--database-url", help="PostgreSQL URL")
    weekly_parser.add_argument(
        "--skip-fundamentals", action="store_true", help="Skip fundamentals update"
    )
    weekly_parser.add_argument(
        "--skip-economy", action="store_true", help="Skip economy data update"
    )
    weekly_parser.add_argument(
        "--skip-overviews", action="store_true", help="Skip company overviews update"
    )
    weekly_parser.add_argument(
        "--skip-ratios", action="store_true", help="Skip financial ratios update"
    )
    weekly_parser.add_argument("--skip-news", action="store_true", help="Skip news update")
    weekly_parser.add_argument("--log-dir", help="Directory for log files")
    weekly_parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    weekly_parser.add_argument("-v", "--verbose", action="store_true")
    weekly_parser.set_defaults(func=cmd_weekly)

    # Legacy update subcommand
    update_parser = subparsers.add_parser(
        "update",
        help="Legacy: combined daily + weekly update",
        description="Check last date in database and pull new data (prices + fundamentals).",
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
    update_parser.add_argument("--log-dir", help="Directory for log files")
    update_parser.add_argument("-v", "--verbose", action="store_true")
    update_parser.set_defaults(func=cmd_update)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
