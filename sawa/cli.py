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
            skip_ta=args.skip_ta,
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
            skip_economy=args.skip_economy,
            skip_overviews=args.skip_overviews,
            skip_news=args.skip_news,
            skip_corporate_actions=args.skip_corporate_actions,
            dry_run=args.dry_run,
            logger=logger,
        )
        return 0 if stats.get("success") else 1
    except Exception as e:
        logger.error(f"Weekly update failed: {e}")
        if args.verbose:
            raise
        return 1


def cmd_quarterly(args) -> int:
    """Run quarterly fundamentals update."""
    from sawa.quarterly import run_quarterly

    logger = setup_logging(args.verbose, log_dir=get_log_dir(args), run_name="quarterly")

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
        stats = run_quarterly(
            api_key=api_key,
            database_url=db_url,
            output_dir=Path(args.output_dir),
            skip_fundamentals=args.skip_fundamentals,
            skip_ratios=args.skip_ratios,
            dry_run=args.dry_run,
            logger=logger,
        )
        return 0 if stats.get("success") else 1
    except Exception as e:
        logger.error(f"Quarterly update failed: {e}")
        if args.verbose:
            raise
        return 1


def cmd_ta_backfill(args) -> int:
    """Run technical indicator backfill."""
    from sawa.ta_backfill import run_ta_backfill

    logger = setup_logging(args.verbose, log_dir=get_log_dir(args), run_name="ta_backfill")

    # Get credentials
    db_url = args.database_url or os.environ.get("DATABASE_URL")

    if not db_url:
        logger.error("DATABASE_URL required (env var or --database-url)")
        return 1

    # Single ticker or all
    tickers = [args.ticker] if args.ticker else None

    try:
        stats = run_ta_backfill(
            database_url=db_url,
            tickers=tickers,
            workers=args.workers,
            dry_run=args.dry_run,
            estimate_only=args.estimate,
            log=logger,
        )
        return 0 if stats.get("success") else 1
    except Exception as e:
        logger.error(f"TA backfill failed: {e}")
        if args.verbose:
            raise
        return 1


def cmd_ta_show(args) -> int:
    """Show technical indicators for a ticker."""
    from sawa.ta_query import (
        format_indicators_table,
        get_indicators_history,
        get_latest_indicators,
    )

    logger = setup_logging(args.verbose, log_dir=get_log_dir(args), run_name="ta_show")

    ticker = args.ticker.upper()

    try:
        if args.from_date:
            # Show history
            results = get_indicators_history(
                ticker,
                start_date=args.from_date,
                end_date=args.to_date,
            )
            if not results:
                logger.error(f"No technical indicators found for {ticker}")
                return 1

            for r in results:
                print(format_indicators_table(r))
                print()
        else:
            # Show latest
            result = get_latest_indicators(ticker)
            if result is None:
                logger.error(f"No technical indicators found for {ticker}")
                return 1

            print(format_indicators_table(result))

        return 0
    except Exception as e:
        logger.error(f"Failed to get indicators: {e}")
        if args.verbose:
            raise
        return 1


def cmd_ta_screen(args) -> int:
    """Screen stocks by technical indicators."""
    from sawa.ta_query import format_screen_results, screen_indicators

    logger = setup_logging(args.verbose, log_dir=get_log_dir(args), run_name="ta_screen")

    # Build filters from args
    filters: dict[str, tuple[float | None, float | None]] = {}

    if args.rsi_max is not None or args.rsi_min is not None:
        filters["rsi_14"] = (args.rsi_min, args.rsi_max)

    if args.volume_min is not None:
        filters["volume_ratio"] = (args.volume_min, None)

    if args.macd_min is not None or args.macd_max is not None:
        filters["macd_histogram"] = (args.macd_min, args.macd_max)

    if not filters:
        logger.error("At least one filter is required (--rsi-max, --rsi-min, --volume-min, etc.)")
        return 1

    try:
        results = screen_indicators(
            filters=filters,
            target_date=args.date,
            index=getattr(args, "index", None),
            limit=args.limit,
        )

        print(format_screen_results(results, filters))
        return 0
    except Exception as e:
        logger.error(f"Screen failed: {e}")
        if args.verbose:
            raise
        return 1


def cmd_index_list(args) -> int:
    """List all market indices."""
    import asyncio

    from sawa.repositories.database import DatabaseIndexRepository

    logger = setup_logging(args.verbose, log_dir=get_log_dir(args), run_name="index_list")
    db_url = args.database_url or os.environ.get("DATABASE_URL")

    if not db_url:
        logger.error("DATABASE_URL required (env var or --database-url)")
        return 1

    try:
        repo = DatabaseIndexRepository(db_url)
        indices = asyncio.get_event_loop().run_until_complete(repo.list_indices())

        if not indices:
            print("No indices found in database.")
            return 0

        print(f"\n{'Code':<12} {'Name':<20} {'Constituents':>12}  Last Updated")
        print("-" * 70)
        for idx in indices:
            updated = idx.last_updated.strftime("%Y-%m-%d %H:%M") if idx.last_updated else "Never"
            print(f"{idx.code:<12} {idx.name:<20} {idx.constituent_count:>12}  {updated}")

        return 0
    except Exception as e:
        logger.error(f"Failed to list indices: {e}")
        if args.verbose:
            raise
        return 1


def cmd_index_show(args) -> int:
    """Show index details and constituents."""
    import asyncio

    from sawa.repositories.database import DatabaseIndexRepository

    logger = setup_logging(args.verbose, log_dir=get_log_dir(args), run_name="index_show")
    db_url = args.database_url or os.environ.get("DATABASE_URL")

    if not db_url:
        logger.error("DATABASE_URL required (env var or --database-url)")
        return 1

    try:
        repo = DatabaseIndexRepository(db_url)
        code = args.code.lower()

        index = asyncio.get_event_loop().run_until_complete(repo.get_index(code))
        if not index:
            logger.error(f"Index not found: {code}")
            return 1

        constituents = asyncio.get_event_loop().run_until_complete(repo.get_constituents(code))

        print(f"\nIndex: {index.name} ({index.code})")
        print(f"Description: {index.description or 'N/A'}")
        print(f"Source: {index.source_url or 'N/A'}")
        updated = (
            index.last_updated.strftime("%Y-%m-%d %H:%M:%S") if index.last_updated else "Never"
        )
        print(f"Last Updated: {updated}")
        print(f"Constituents: {len(constituents)}")

        if constituents and not args.no_tickers:
            print(f"\nTickers ({len(constituents)}):")
            # Print in columns
            cols = 10
            for i in range(0, len(constituents), cols):
                row = constituents[i : i + cols]
                print("  " + " ".join(f"{t:<6}" for t in row))

        return 0
    except Exception as e:
        logger.error(f"Failed to show index: {e}")
        if args.verbose:
            raise
        return 1


def cmd_index_update(args) -> int:
    """Update index constituents from Wikipedia."""
    import psycopg

    from sawa.coldstart import populate_index_constituents

    logger = setup_logging(args.verbose, log_dir=get_log_dir(args), run_name="index_update")
    db_url = args.database_url or os.environ.get("DATABASE_URL")

    if not db_url:
        logger.error("DATABASE_URL required (env var or --database-url)")
        return 1

    try:
        with psycopg.connect(db_url) as conn:
            stats = populate_index_constituents(conn, logger)

        print("\nIndex update complete:")
        for code, count in stats.items():
            print(f"  {code}: {count} constituents")

        return 0
    except Exception as e:
        logger.error(f"Failed to update indices: {e}")
        if args.verbose:
            raise
        return 1


def cmd_index_check(args) -> int:
    """Check which indices a ticker belongs to."""
    import asyncio

    from sawa.repositories.database import DatabaseIndexRepository

    logger = setup_logging(args.verbose, log_dir=get_log_dir(args), run_name="index_check")
    db_url = args.database_url or os.environ.get("DATABASE_URL")

    if not db_url:
        logger.error("DATABASE_URL required (env var or --database-url)")
        return 1

    try:
        repo = DatabaseIndexRepository(db_url)
        ticker = args.ticker.upper()

        indices = asyncio.get_event_loop().run_until_complete(repo.get_ticker_indices(ticker))

        if indices:
            print(f"{ticker} is a member of: {', '.join(indices)}")
        else:
            print(f"{ticker} is not a member of any tracked index")

        return 0
    except Exception as e:
        logger.error(f"Failed to check indices: {e}")
        if args.verbose:
            raise
        return 1


def cmd_corporate_actions(args) -> int:
    """Run corporate actions update (splits, dividends, earnings)."""
    from sawa.corporate_actions import run_corporate_actions_update

    logger = setup_logging(args.verbose, log_dir=get_log_dir(args), run_name="corporate_actions")

    # Get credentials
    api_key = args.api_key or os.environ.get("POLYGON_API_KEY")
    db_url = args.database_url or os.environ.get("DATABASE_URL")

    if not api_key:
        logger.error("POLYGON_API_KEY required (env var or --api-key)")
        return 1
    if not db_url:
        logger.error("DATABASE_URL required (env var or --database-url)")
        return 1

    # Parse start date
    start_date = parse_date(args.start_date) if args.start_date else None

    # Parse ticker(s)
    tickers = [args.ticker.upper()] if args.ticker else None

    # Determine what to include
    # --splits-only and --dividends-only exclude earnings
    # Earnings is off by default (Polygon API doesn't currently provide earnings data)
    include_splits = not args.dividends_only
    include_dividends = not args.splits_only
    include_earnings = args.include_earnings and not args.splits_only and not args.dividends_only

    try:
        stats = run_corporate_actions_update(
            api_key=api_key,
            database_url=db_url,
            start_date=start_date,
            tickers=tickers,
            include_splits=include_splits,
            include_dividends=include_dividends,
            include_earnings=include_earnings,
            dry_run=args.dry_run,
            logger=logger,
        )
        return 0 if stats.get("success") else 1
    except Exception as e:
        logger.error(f"Corporate actions update failed: {e}")
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
  coldstart           Full database setup from scratch
  daily               Daily stock price update
  weekly              Weekly fundamentals/economy update
  update              Legacy: combined daily + weekly update
  add-symbol          Add new symbols to database
  ta-backfill         Calculate technical indicators for all history
  ta-show             Show technical indicators for a ticker
  ta-screen           Screen stocks by technical indicators
  index-list          List all market indices
  index-show          Show index details and constituents
  index-update        Update index constituents from Wikipedia
  index-check         Check which indices a ticker belongs to
  corporate-actions   Download stock splits and dividends

Examples:
  sawa coldstart --years 5
  sawa daily
  sawa weekly --skip-news
  sawa daily --dry-run
  sawa daily --skip-ta
  sawa add-symbol PLTR COIN
  sawa ta-backfill --workers 8
  sawa ta-show AAPL
  sawa ta-show AAPL --from-date 2025-01-01
  sawa ta-screen --rsi-max 30
  sawa ta-screen --rsi-max 30 --volume-min 2.0
  sawa index-list
  sawa index-show sp500
  sawa index-check AAPL

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
    daily_parser.add_argument("--skip-ta", action="store_true", help="Skip technical indicators")
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
        "--file",
        "-f",
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
        help="Weekly economy/news/corporate actions update",
        description="Update frequently-changing data: economy, overviews, news, corporate actions.",
    )
    weekly_parser.add_argument("--output-dir", default="data", help="Output data directory")
    weekly_parser.add_argument("--api-key", help="Polygon API key")
    weekly_parser.add_argument("--database-url", help="PostgreSQL URL")
    weekly_parser.add_argument(
        "--skip-economy", action="store_true", help="Skip economy data update"
    )
    weekly_parser.add_argument(
        "--skip-overviews", action="store_true", help="Skip company overviews update"
    )
    weekly_parser.add_argument("--skip-news", action="store_true", help="Skip news update")
    weekly_parser.add_argument(
        "--skip-corporate-actions",
        action="store_true",
        help="Skip corporate actions (splits, dividends) update",
    )
    weekly_parser.add_argument("--log-dir", help="Directory for log files")
    weekly_parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    weekly_parser.add_argument("-v", "--verbose", action="store_true")
    weekly_parser.set_defaults(func=cmd_weekly)

    # Quarterly fundamentals subcommand
    quarterly_parser = subparsers.add_parser(
        "quarterly",
        help="Quarterly fundamentals update",
        description="Update financial statements: balance sheets, income, cash flow, ratios.",
    )
    quarterly_parser.add_argument("--output-dir", default="data", help="Output data directory")
    quarterly_parser.add_argument("--api-key", help="Polygon API key")
    quarterly_parser.add_argument("--database-url", help="PostgreSQL URL")
    quarterly_parser.add_argument(
        "--skip-fundamentals", action="store_true", help="Skip fundamentals update"
    )
    quarterly_parser.add_argument(
        "--skip-ratios", action="store_true", help="Skip financial ratios update"
    )
    quarterly_parser.add_argument("--log-dir", help="Directory for log files")
    quarterly_parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    quarterly_parser.add_argument("-v", "--verbose", action="store_true")
    quarterly_parser.set_defaults(func=cmd_quarterly)

    # Technical indicator backfill subcommand
    ta_parser = subparsers.add_parser(
        "ta-backfill",
        help="Calculate technical indicators for all history",
        description="Calculate technical indicators (SMA, RSI, MACD, etc.) for all stocks.",
    )
    ta_parser.add_argument("--ticker", help="Single ticker to process (default: all)")
    ta_parser.add_argument(
        "--workers", type=int, default=4, help="Number of parallel workers (default: 4)"
    )
    ta_parser.add_argument("--database-url", help="PostgreSQL URL")
    ta_parser.add_argument("--log-dir", help="Directory for log files")
    ta_parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    ta_parser.add_argument(
        "--estimate", action="store_true", help="Estimate time on 10 tickers and exit"
    )
    ta_parser.add_argument("-v", "--verbose", action="store_true")
    ta_parser.set_defaults(func=cmd_ta_backfill)

    # Technical indicator show subcommand
    ta_show_parser = subparsers.add_parser(
        "ta-show",
        help="Show technical indicators for a ticker",
        description="Display technical indicators (SMA, RSI, MACD, etc.) for a stock.",
    )
    ta_show_parser.add_argument("ticker", help="Stock ticker symbol (e.g., AAPL)")
    ta_show_parser.add_argument(
        "--from-date",
        type=parse_date,
        metavar="YYYY-MM-DD",
        help="Start date for history (shows latest if not specified)",
    )
    ta_show_parser.add_argument(
        "--to-date",
        type=parse_date,
        metavar="YYYY-MM-DD",
        help="End date for history (defaults to today)",
    )
    ta_show_parser.add_argument("--database-url", help="PostgreSQL URL")
    ta_show_parser.add_argument("--log-dir", help="Directory for log files")
    ta_show_parser.add_argument("-v", "--verbose", action="store_true")
    ta_show_parser.set_defaults(func=cmd_ta_show)

    # Technical indicator screen subcommand
    ta_screen_parser = subparsers.add_parser(
        "ta-screen",
        help="Screen stocks by technical indicators",
        description="Find stocks matching technical indicator criteria.",
    )
    ta_screen_parser.add_argument(
        "--rsi-max",
        type=float,
        help="Maximum RSI-14 (e.g., 30 for oversold)",
    )
    ta_screen_parser.add_argument(
        "--rsi-min",
        type=float,
        help="Minimum RSI-14 (e.g., 70 for overbought)",
    )
    ta_screen_parser.add_argument(
        "--volume-min",
        type=float,
        help="Minimum volume ratio (today vs 20-day avg)",
    )
    ta_screen_parser.add_argument(
        "--macd-min",
        type=float,
        help="Minimum MACD histogram (positive = bullish)",
    )
    ta_screen_parser.add_argument(
        "--macd-max",
        type=float,
        help="Maximum MACD histogram (negative = bearish)",
    )
    ta_screen_parser.add_argument(
        "--date",
        type=parse_date,
        metavar="YYYY-MM-DD",
        help="Date to screen (defaults to most recent)",
    )
    ta_screen_parser.add_argument(
        "--index",
        choices=["sp500", "nasdaq100"],
        help="Filter by index membership",
    )
    ta_screen_parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum results (default: 100)",
    )
    ta_screen_parser.add_argument("--database-url", help="PostgreSQL URL")
    ta_screen_parser.add_argument("--log-dir", help="Directory for log files")
    ta_screen_parser.add_argument("-v", "--verbose", action="store_true")
    ta_screen_parser.set_defaults(func=cmd_ta_screen)

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

    # Index management subcommands
    index_list_parser = subparsers.add_parser(
        "index-list",
        help="List all market indices",
        description="List all tracked market indices with constituent counts.",
    )
    index_list_parser.add_argument("--database-url", help="PostgreSQL URL")
    index_list_parser.add_argument("--log-dir", help="Directory for log files")
    index_list_parser.add_argument("-v", "--verbose", action="store_true")
    index_list_parser.set_defaults(func=cmd_index_list)

    index_show_parser = subparsers.add_parser(
        "index-show",
        help="Show index details and constituents",
        description="Display details and constituent stocks for a market index.",
    )
    index_show_parser.add_argument("code", help="Index code (e.g., sp500, nasdaq100)")
    index_show_parser.add_argument(
        "--no-tickers", action="store_true", help="Don't list constituent tickers"
    )
    index_show_parser.add_argument("--database-url", help="PostgreSQL URL")
    index_show_parser.add_argument("--log-dir", help="Directory for log files")
    index_show_parser.add_argument("-v", "--verbose", action="store_true")
    index_show_parser.set_defaults(func=cmd_index_show)

    index_update_parser = subparsers.add_parser(
        "index-update",
        help="Update index constituents from Wikipedia",
        description="Refresh index constituent lists from Wikipedia.",
    )
    index_update_parser.add_argument("--database-url", help="PostgreSQL URL")
    index_update_parser.add_argument("--log-dir", help="Directory for log files")
    index_update_parser.add_argument("-v", "--verbose", action="store_true")
    index_update_parser.set_defaults(func=cmd_index_update)

    index_check_parser = subparsers.add_parser(
        "index-check",
        help="Check which indices a ticker belongs to",
        description="Check index membership for a specific stock ticker.",
    )
    index_check_parser.add_argument("ticker", help="Stock ticker symbol (e.g., AAPL)")
    index_check_parser.add_argument("--database-url", help="PostgreSQL URL")
    index_check_parser.add_argument("--log-dir", help="Directory for log files")
    index_check_parser.add_argument("-v", "--verbose", action="store_true")
    index_check_parser.set_defaults(func=cmd_index_check)

    # Corporate actions subcommand
    corp_parser = subparsers.add_parser(
        "corporate-actions",
        help="Download splits, dividends, and earnings",
        description="Download and store corporate actions from Polygon.",
    )
    corp_parser.add_argument("--start-date", help="Fetch data from this date (default: 1 year ago)")
    corp_parser.add_argument("--ticker", help="Single ticker to fetch (default: all active)")
    corp_parser.add_argument("--splits-only", action="store_true", help="Only fetch splits")
    corp_parser.add_argument("--dividends-only", action="store_true", help="Only fetch dividends")
    corp_parser.add_argument(
        "--include-earnings",
        action="store_true",
        help="Include earnings (experimental, Polygon API may not provide data)",
    )
    corp_parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    corp_parser.add_argument("--api-key", help="Polygon API key")
    corp_parser.add_argument("--database-url", help="PostgreSQL URL")
    corp_parser.add_argument("--log-dir", help="Directory for log files")
    corp_parser.add_argument("-v", "--verbose", action="store_true")
    corp_parser.set_defaults(func=cmd_corporate_actions)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
