"""
Main CLI entry point for sawa command.

Usage:
    sawa coldstart              # Full database setup
    sawa daily                  # Daily price update
    sawa weekly                 # Weekly fundamentals/economy update
"""

import argparse
import os
import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from dotenv import load_dotenv

from sawa.utils import monitored_run, setup_logging
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


def _redact_database_url(db_url: str) -> str:
    """Return DATABASE_URL with password removed for logs."""
    try:
        parts = urlsplit(db_url)
    except ValueError:
        return "<unparseable DATABASE_URL>"

    if not parts.scheme or not parts.netloc:
        return db_url

    host = parts.hostname or ""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    port = f":{parts.port}" if parts.port else ""
    auth = f"{parts.username}:***@" if parts.username else ""

    return urlunsplit(
        (parts.scheme, f"{auth}{host}{port}", parts.path, parts.query, parts.fragment)
    )


def _log_schema_only_warning(logger, db_url: str) -> None:
    """Warn loudly that schema-only coldstart is destructive."""
    target = _redact_database_url(db_url)
    border = "!" * 78
    logger.warning("")
    logger.warning(border)
    logger.warning("!!! DESTRUCTIVE COMMAND: sawa coldstart --schema-only")
    logger.warning("!!! This will DROP AND RECREATE every table in the target public schema.")
    logger.warning("!!! Existing data in that database will be permanently removed.")
    logger.warning("!!! Target DATABASE_URL: %s", target)
    logger.warning("!!! Do not run this against production.")
    logger.warning("!!! For non-destructive schema upgrades, use: sawa coldstart --no-drop")
    logger.warning(border)
    logger.warning("")


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

    # Schema-only mode rebuilds the schema, so it needs to drop existing tables
    # If user wants to preserve data, they should not use --schema-only at all
    if schema_only and args.no_drop:
        logger.error("❌ ERROR: --schema-only and --no-drop are incompatible")
        logger.error("   --schema-only rebuilds the schema by dropping and recreating tables")
        logger.error("   To preserve data, don't use --schema-only")
        return 1

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

    if schema_only:
        _log_schema_only_warning(logger, db_url)

    try:
        with monitored_run("coldstart", logger=logger) as ctx:
            ctx["stats"] = run_coldstart(
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
                confirm_drop=args.confirm_drop,
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
        return 0 if ctx["stats"].get("success") else 1
    except Exception:
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
        with monitored_run("daily", logger=logger) as ctx:
            ctx["stats"] = run_daily(
                api_key=api_key,
                database_url=db_url,
                force_from_date=args.from_date,
                skip_news=args.skip_news,
                skip_ta=args.skip_ta or args.news_only,
                skip_prices=args.news_only,
                skip_market_internals=args.skip_market_internals or args.news_only,
                dry_run=args.dry_run,
                logger=logger,
            )
        return 0 if ctx["stats"].get("success") else 1
    except Exception:
        if args.verbose:
            raise
        return 1


def cmd_intraday(args) -> int:
    """Run intraday price streaming."""
    from sawa.intraday import run_intraday

    logger = setup_logging(args.verbose, log_dir=get_log_dir(args), run_name="intraday")

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
        with monitored_run("intraday", logger=logger) as ctx:
            ctx["stats"] = run_intraday(
                api_key=api_key,
                database_url=db_url,
                bar_size=args.bar_size,
                logger=logger,
            )
        return 0 if ctx["stats"].get("success") else 1
    except Exception:
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
        with monitored_run("add-symbol", logger=logger) as ctx:
            ctx["stats"] = run_add_symbols(
                api_key=api_key,
                database_url=db_url,
                symbols=unique_symbols,
                years=args.years,
                dry_run=args.dry_run,
                logger=logger,
            )
        return 0 if ctx["stats"].get("success") else 1
    except Exception:
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
        with monitored_run("weekly", logger=logger) as ctx:
            ctx["stats"] = run_weekly(
                api_key=api_key,
                database_url=db_url,
                output_dir=Path(args.output_dir),
                skip_economy=args.skip_economy,
                skip_overviews=args.skip_overviews,
                skip_news=args.skip_news,
                skip_corporate_actions=args.skip_corporate_actions,
                skip_character=args.skip_character,
                character_workers=args.character_workers,
                dry_run=args.dry_run,
                logger=logger,
            )
        return 0 if ctx["stats"].get("success") else 1
    except Exception:
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
        with monitored_run("quarterly", logger=logger) as ctx:
            ctx["stats"] = run_quarterly(
                api_key=api_key,
                database_url=db_url,
                output_dir=Path(args.output_dir),
                skip_fundamentals=args.skip_fundamentals,
                skip_ratios=args.skip_ratios,
                dry_run=args.dry_run,
                logger=logger,
            )
        return 0 if ctx["stats"].get("success") else 1
    except Exception:
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
        with monitored_run("ta-backfill", logger=logger) as ctx:
            ctx["stats"] = run_ta_backfill(
                database_url=db_url,
                tickers=tickers,
                workers=args.workers,
                dry_run=args.dry_run,
                estimate_only=args.estimate,
                log=logger,
            )
        return 0 if ctx["stats"].get("success") else 1
    except Exception:
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
        indices = asyncio.run(repo.list_indices())

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

        index = asyncio.run(repo.get_index(code))
        if not index:
            logger.error(f"Index not found: {code}")
            return 1

        constituents = asyncio.run(repo.get_constituents(code))

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

        indices = asyncio.run(repo.get_ticker_indices(ticker))

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
        with monitored_run("corporate-actions", logger=logger) as ctx:
            ctx["stats"] = run_corporate_actions_update(
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
        return 0 if ctx["stats"].get("success") else 1
    except Exception:
        if args.verbose:
            raise
        return 1


def cmd_adjust_splits(args) -> int:
    """Re-fetch adjusted prices for tickers with recent stock splits."""
    from sawa.split_adjust import refresh_split_adjusted_prices

    logger = setup_logging(args.verbose, log_dir=get_log_dir(args), run_name="adjust_splits")

    # Get credentials
    api_key = args.api_key or os.environ.get("POLYGON_API_KEY")
    db_url = args.database_url or os.environ.get("DATABASE_URL")

    if not api_key:
        logger.error("POLYGON_API_KEY required (env var or --api-key)")
        return 1
    if not db_url:
        logger.error("DATABASE_URL required (env var or --database-url)")
        return 1

    # Parse ticker and since date
    tickers = [args.ticker.upper()] if args.ticker else None
    since = parse_date(args.since) if args.since else None

    try:
        with monitored_run("adjust-splits", logger=logger) as ctx:
            stats = refresh_split_adjusted_prices(
                api_key=api_key,
                database_url=db_url,
                tickers=tickers,
                since=since,
                dry_run=args.dry_run,
                logger=logger,
            )
            ctx["stats"] = stats

            # Re-adjusting prices rewrites historical OHLC, which leaves the
            # stored technical_indicators stale (computed from pre-adjustment
            # prices). Recompute TA for exactly the adjusted tickers so the
            # standalone CLI matches the weekly/daily auto-heal behaviour.
            adjusted = stats.get("tickers") or []
            if not args.dry_run and stats.get("success") and adjusted:
                from sawa.ta_backfill import recompute_ta_for_tickers

                logger.info(
                    f"Recomputing technical indicators for {len(adjusted)} "
                    f"adjusted ticker(s)..."
                )
                stats["ta_recompute"] = recompute_ta_for_tickers(
                    database_url=db_url,
                    tickers=adjusted,
                    log=logger,
                )
        return 0 if ctx["stats"].get("success") else 1
    except Exception:
        if args.verbose:
            raise
        return 1


def cmd_character(args) -> int:
    """Run stock character classification batch."""
    from sawa.stock_character_batch import run_stock_character_batch

    logger = setup_logging(args.verbose, log_dir=get_log_dir(args), run_name="character")

    db_url = args.database_url or os.environ.get("DATABASE_URL")

    if not db_url:
        logger.error("DATABASE_URL required (env var or --database-url)")
        return 1

    tickers = [t.upper() for t in args.tickers] if args.tickers else None

    try:
        with monitored_run("character", logger=logger) as ctx:
            ctx["stats"] = run_stock_character_batch(
                database_url=db_url,
                tickers=tickers,
                workers=args.workers,
                run_date=args.run_date,
                log=logger,
            )
        return 0 if ctx["stats"].get("success") else 1
    except Exception:
        if args.verbose:
            raise
        return 1


def cmd_logs(args) -> int:
    """Inspect log files written by sawa runs."""
    from sawa.logs import (
        format_entry_row,
        grep_runs,
        latest_run,
        list_runs,
        tail_lines,
    )
    from sawa.utils.logging import get_default_log_dir

    action = args.logs_action

    if action == "path":
        print(get_default_log_dir())
        return 0

    if action == "list":
        entries = list_runs(run_type=args.type, days=args.days)
        if not entries:
            print("No matching log files.")
            return 0
        print(f"{'When':<19}  {'Type':<12} {'Size':>8}  Filename")
        print(f"{'‾' * 19}  {'‾' * 12} {'‾' * 8}  {'‾' * 8}")
        for entry in entries[: args.limit]:
            print(format_entry_row(entry))
        if len(entries) > args.limit:
            print(f"… {len(entries) - args.limit} more (use --limit to show all)")
        return 0

    if action == "tail":
        entry = latest_run(run_type=args.type)
        if entry is None:
            print("No matching log files.")
            return 1
        print(f"# {entry.filename} ({entry.when:%Y-%m-%d %H:%M:%S})")
        for line in tail_lines(entry.path, args.lines):
            print(line, end="" if line.endswith("\n") else "\n")
        return 0

    if action == "grep":
        results = grep_runs(
            args.pattern,
            run_type=args.type,
            days=args.days,
            max_matches=args.limit,
        )
        if not results:
            print(f"No matches for {args.pattern!r} in the last {args.days} day(s).")
            return 1
        current_file: str | None = None
        for entry, lineno, line in results:
            if entry.filename != current_file:
                print(f"\n# {entry.filename}")
                current_file = entry.filename
            print(f"  {lineno}: {line}")
        return 0

    print("Unknown logs action. Use: list | tail | grep | path")
    return 1


def cmd_notify(args) -> int:
    """Send a notification through the configured notifier."""
    from sawa.utils import get_notifier
    from sawa.utils.notify import NotificationLevel

    logger = setup_logging(args.verbose, log_dir=get_log_dir(args), run_name="notify")
    notifier = get_notifier(logger)

    level_map = {
        "info": NotificationLevel.INFO,
        "warning": NotificationLevel.WARNING,
        "error": NotificationLevel.ERROR,
    }

    sent = notifier.send(
        title=args.title,
        body=args.body or "",
        level=level_map[args.level],
        tags=args.tag or None,
    )
    if not sent:
        logger.warning("Notification not delivered (no backend configured or send failed)")
        return 1
    return 0


def cmd_data_status(args) -> int:
    """Show latest stock price data in the database."""
    import psycopg

    logger = setup_logging(args.verbose, log_dir=get_log_dir(args), run_name="data_status")
    db_url = args.database_url or os.environ.get("DATABASE_URL")

    if not db_url:
        logger.error("DATABASE_URL required (env var or --database-url)")
        return 1

    query = """
        SELECT
            (SELECT MAX(date) FROM stock_prices) AS prices_latest_date,
            (SELECT COUNT(DISTINCT ticker) FROM stock_prices) AS prices_ticker_count,
            (SELECT COUNT(*) FROM stock_prices) AS prices_row_count,
            (SELECT MAX(timestamp) FROM stock_prices_intraday) AS intraday_latest_timestamp,
            (SELECT COUNT(DISTINCT ticker) FROM stock_prices_intraday) AS intraday_ticker_count,
            (SELECT COUNT(*) FROM stock_prices_intraday) AS intraday_row_count,
            (SELECT MAX(date) FROM stock_prices_live) AS live_latest_date,
            (SELECT COUNT(DISTINCT ticker) FROM stock_prices_live) AS live_ticker_count
    """

    try:
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()

        if not row:
            print("No data found.")
            return 0

        (
            prices_date, prices_tickers, prices_rows,
            intraday_ts, intraday_tickers, intraday_rows,
            live_date, live_tickers,
        ) = row

        print("\nStock Price Data Status\n")
        print(f"{'Table':<26}{'Latest Data':<22}{'Tickers':>8}{'Rows':>12}")
        print("-" * 68)
        print(
            f"{'stock_prices':<26}"
            f"{str(prices_date or 'N/A'):<22}"
            f"{prices_tickers or 0:>8}"
            f"{prices_rows or 0:>12,}"
        )
        print(
            f"{'stock_prices_intraday':<26}"
            f"{str(intraday_ts or 'N/A'):<22}"
            f"{intraday_tickers or 0:>8}"
            f"{intraday_rows or 0:>12,}"
        )
        print(
            f"{'stock_prices_live':<26}"
            f"{str(live_date or 'N/A'):<22}"
            f"{live_tickers or 0:>8}"
            f"{'':>12}"
        )

        return 0
    except Exception as e:
        logger.error(f"Failed to get data status: {e}")
        if args.verbose:
            raise
        return 1


def cmd_doctor(args) -> int:
    """Run database doctor checks."""
    from sawa.doctor import run_doctor

    logger = setup_logging(args.verbose, log_dir=get_log_dir(args), run_name="doctor")
    db_url = args.database_url or os.environ.get("DATABASE_URL")

    if not db_url:
        logger.error("DATABASE_URL required (env var or --database-url)")
        return 1

    try:
        with monitored_run("doctor", logger=logger) as ctx:
            ctx["stats"] = run_doctor(
                database_url=db_url,
                job=args.job,
                min_coverage=args.min_coverage,
                max_staleness_days=args.max_staleness_days,
                logger=logger,
            )
        return 0 if ctx["stats"].get("success") else 1
    except Exception:
        if args.verbose:
            raise
        return 1


def cmd_mcp_query_insights(args) -> int:
    """Analyze execute_query audit logs for missing-tool signals."""
    from sawa.mcp_query_insights import analyze_query_log, format_query_insights

    try:
        cache = analyze_query_log(
            log_dir=get_log_dir(args),
            reset=args.reset,
            window_days=args.window_days,
            warning_threshold=args.warning_threshold,
            top_n=args.limit,
        )
        if args.json:
            import json

            print(json.dumps(cache, indent=2, sort_keys=True, default=str))
        else:
            print(format_query_insights(cache, top_n=args.limit))
        return 0
    except Exception as e:
        if args.verbose:
            raise
        print(f"Failed to analyze MCP query logs: {e}", file=sys.stderr)
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
  weekly              Weekly economy/news/corporate actions/character update
  add-symbol          Add new symbols to database
  ta-backfill         Calculate technical indicators for all history
  ta-show             Show technical indicators for a ticker
  ta-screen           Screen stocks by technical indicators
  index-list          List all market indices
  index-show          Show index details and constituents
  index-update        Update index constituents from Wikipedia
  index-check         Check which indices a ticker belongs to
  corporate-actions   Download stock splits and dividends
  adjust-splits       Re-fetch adjusted prices after stock splits
  character           Classify stocks by behavioral character (Hurst, regime, scorecard)
  doctor              Check whether database contents look healthy after a job
  data-status         Show latest stock price data in the database
  mcp-query-insights  Analyze execute_query usage for missing MCP tools
  logs                Inspect sawa log files (list, tail, grep, path)
  notify              Send a notification through the configured backend

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
  sawa doctor --job daily
  sawa mcp-query-insights

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
    cold_parser.add_argument(
        "--no-drop",
        action="store_true",
        help="Don't drop existing tables (RECOMMENDED for schema updates)",
    )
    cold_parser.add_argument("--api-key", help="Polygon API key")
    cold_parser.add_argument("--s3-access-key", help="Polygon S3 access key")
    cold_parser.add_argument("--s3-secret-key", help="Polygon S3 secret key")
    cold_parser.add_argument("--database-url", help="PostgreSQL URL")
    # Mode options (mutually exclusive-ish)
    cold_parser.add_argument(
        "--drop-only",
        action="store_true",
        help="⚠️  Only drop tables and clean data, then exit (requires confirmation)",
    )
    cold_parser.add_argument(
        "--confirm-drop",
        action="store_true",
        help="Confirm destructive table drops for non-interactive runs",
    )
    cold_parser.add_argument(
        "--schema-only",
        action="store_true",
        help="DANGER: drop and recreate every table in the target database; no download/load.",
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
    daily_parser.add_argument(
        "--skip-market-internals", action="store_true", help="Skip market internals (FRED)"
    )
    daily_parser.add_argument(
        "--news-only", action="store_true", help="Only update news (skip prices and TA)"
    )
    daily_parser.add_argument("--log-dir", help="Directory for log files")
    daily_parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    daily_parser.add_argument("-v", "--verbose", action="store_true")
    daily_parser.set_defaults(func=cmd_daily)

    # Intraday subcommand
    intraday_parser = subparsers.add_parser(
        "intraday",
        help="Stream live intraday prices via WebSocket",
        description="Stream real-time 5-minute bars (15-min delayed, manual start/stop).",
    )
    intraday_parser.add_argument(
        "--bar-size",
        type=int,
        default=5,
        choices=[1, 5, 15, 30, 60],
        help="Bar size in minutes (default: 5)",
    )
    intraday_parser.add_argument("--api-key", help="Polygon API key")
    intraday_parser.add_argument("--database-url", help="PostgreSQL URL")
    intraday_parser.add_argument("--log-dir", help="Directory for log files")
    intraday_parser.add_argument("-v", "--verbose", action="store_true")
    intraday_parser.set_defaults(func=cmd_intraday)

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
        help="Weekly economy/news/corporate actions/character update",
        description=(
            "Update frequently-changing data: economy, overviews, news, corporate actions, "
            "and stock character classification."
        ),
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
    weekly_parser.add_argument(
        "--skip-character",
        action="store_true",
        help="Skip stock character classification batch",
    )
    weekly_parser.add_argument(
        "--character-workers",
        type=int,
        default=4,
        help="Worker processes for character batch (default: 4)",
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
        choices=["sp500", "nasdaq_listed", "us_active", "nasdaq100", "dow30", "russell1000", "mag7"],
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
    index_show_parser.add_argument("code", help="Index code (e.g., sp500, nasdaq_listed)")
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

    # Adjust splits subcommand
    adjust_parser = subparsers.add_parser(
        "adjust-splits",
        help="Re-fetch adjusted prices after stock splits",
        description="Re-fetch split-adjusted price history for tickers with recent splits.",
    )
    adjust_parser.add_argument("--ticker", help="Single ticker to adjust (default: auto-detect)")
    adjust_parser.add_argument(
        "--since",
        help="Only consider splits since this date (default: 1 year ago)",
    )
    adjust_parser.add_argument("--dry-run", action="store_true", help="Preview without changes")
    adjust_parser.add_argument("--api-key", help="Polygon API key")
    adjust_parser.add_argument("--database-url", help="PostgreSQL URL")
    adjust_parser.add_argument("--log-dir", help="Directory for log files")
    adjust_parser.add_argument("-v", "--verbose", action="store_true")
    adjust_parser.set_defaults(func=cmd_adjust_splits)

    # Stock character batch subcommand
    character_parser = subparsers.add_parser(
        "character",
        help="Classify stocks by behavioral character (Hurst, regime, scorecard)",
        description=(
            "Run the stock character classification pipeline across tickers. "
            "Writes baseline, classification, flags, and scorecard rows."
        ),
    )
    character_parser.add_argument(
        "tickers",
        nargs="*",
        help="Tickers to process (default: all tickers with prices)",
    )
    character_parser.add_argument(
        "--workers", type=int, default=4, help="Number of parallel workers (default: 4)"
    )
    character_parser.add_argument(
        "--run-date",
        type=parse_date,
        metavar="YYYY-MM-DD",
        help="Classification date (default: today)",
    )
    character_parser.add_argument("--database-url", help="PostgreSQL URL")
    character_parser.add_argument("--log-dir", help="Directory for log files")
    character_parser.add_argument("-v", "--verbose", action="store_true")
    character_parser.set_defaults(func=cmd_character)

    # Logs subcommand
    logs_parser = subparsers.add_parser(
        "logs",
        help="Inspect log files (list, tail, grep)",
        description="Browse sawa log files in ~/.sawa/logs/.",
    )
    logs_sub = logs_parser.add_subparsers(dest="logs_action", required=True)

    logs_list = logs_sub.add_parser("list", help="List log files, newest first")
    logs_list.add_argument(
        "--type",
        help="Filter to a single run type (daily, weekly, intraday, …)",
    )
    logs_list.add_argument(
        "--days", type=int, help="Only include logs from the last N days"
    )
    logs_list.add_argument(
        "--limit", type=int, default=25, help="Maximum rows to display (default: 25)"
    )

    logs_tail = logs_sub.add_parser("tail", help="Tail the most recent log of a type")
    logs_tail.add_argument(
        "--type", help="Run type to tail (default: most recent of any type)"
    )
    logs_tail.add_argument(
        "--lines", "-n", type=int, default=50, help="Lines to show (default: 50)"
    )

    logs_grep = logs_sub.add_parser("grep", help="Regex search across recent logs")
    logs_grep.add_argument("pattern", help="Regex pattern to search for")
    logs_grep.add_argument("--type", help="Restrict to a single run type")
    logs_grep.add_argument(
        "--days", type=int, default=7, help="Search the last N days (default: 7)"
    )
    logs_grep.add_argument(
        "--limit", type=int, default=200, help="Maximum matches (default: 200)"
    )

    logs_sub.add_parser("path", help="Print the resolved log directory")

    logs_parser.add_argument("-v", "--verbose", action="store_true")
    logs_parser.set_defaults(func=cmd_logs)

    # Notify subcommand — same Notifier the run wrappers use, callable from bash.
    notify_parser = subparsers.add_parser(
        "notify",
        help="Send a notification via the configured backend (NTFY by default)",
        description=(
            "Thin wrapper around the Notifier abstraction so the bash scheduler "
            "and other shell scripts can dispatch alerts without duplicating "
            "curl logic."
        ),
    )
    notify_parser.add_argument("--title", required=True, help="Notification title")
    notify_parser.add_argument(
        "--body", default="", help="Notification body (default: empty)"
    )
    notify_parser.add_argument(
        "--level",
        choices=["info", "warning", "error"],
        default="info",
        help="Severity (info/warning/error). Maps to ntfy Priority header.",
    )
    notify_parser.add_argument(
        "--tag",
        action="append",
        default=[],
        help="Tag (repeatable). Maps to ntfy ``Tags`` header.",
    )
    notify_parser.add_argument("--log-dir", help="Override log directory")
    notify_parser.add_argument("-v", "--verbose", action="store_true")
    notify_parser.set_defaults(func=cmd_notify)

    # Data status subcommand
    status_parser = subparsers.add_parser(
        "data-status",
        help="Show latest stock price data in the database",
        description="Check data freshness across stock price tables.",
    )
    status_parser.add_argument("--database-url", help="PostgreSQL URL")
    status_parser.add_argument("--log-dir", help="Directory for log files")
    status_parser.add_argument("-v", "--verbose", action="store_true")
    status_parser.set_defaults(func=cmd_data_status)

    # Doctor subcommand
    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Check whether database contents look healthy after a job",
        description="Run database-only sanity and completeness checks after scheduled jobs.",
    )
    doctor_parser.add_argument(
        "--job",
        choices=["all", "daily", "weekly", "quarterly", "coldstart"],
        default="all",
        help="Job scope to validate (default: all)",
    )
    doctor_parser.add_argument("--database-url", help="PostgreSQL URL")
    doctor_parser.add_argument(
        "--min-coverage",
        type=float,
        default=0.85,
        help="Minimum ticker coverage ratio for completeness checks (default: 0.85)",
    )
    doctor_parser.add_argument(
        "--max-staleness-days",
        type=int,
        default=5,
        help="Maximum allowed age for latest stock_prices data (default: 5)",
    )
    doctor_parser.add_argument("--log-dir", help="Directory for log files")
    doctor_parser.add_argument("-v", "--verbose", action="store_true")
    doctor_parser.set_defaults(func=cmd_doctor)

    # MCP query insights subcommand
    insights_parser = subparsers.add_parser(
        "mcp-query-insights",
        help="Analyze execute_query usage for missing MCP tools",
        description=(
            "Analyze structured MCP execute_query audit logs and cache a summary. "
            "The MCP server reads only the cached summary on startup."
        ),
    )
    insights_parser.add_argument(
        "--window-days",
        type=int,
        default=7,
        help="Recent usage window for warning threshold (default: 7)",
    )
    insights_parser.add_argument(
        "--warning-threshold",
        type=int,
        default=25,
        help="Warn when recent custom query count reaches this value (default: 25)",
    )
    insights_parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of top tables/patterns to show (default: 10)",
    )
    insights_parser.add_argument(
        "--reset",
        action="store_true",
        help="Ignore cached offsets and rebuild insights from the structured log",
    )
    insights_parser.add_argument("--json", action="store_true", help="Print raw JSON summary")
    insights_parser.add_argument("--log-dir", help="Directory for MCP query logs")
    insights_parser.add_argument("-v", "--verbose", action="store_true")
    insights_parser.set_defaults(func=cmd_mcp_query_insights)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
