"""
Rebuild the stock database using SQL schema files.

Refactored from: rebuild_database.py

Security: Uses psycopg.sql for safe identifier handling.

Usage:
    python -m sawa.database.schema --database-url postgresql://...
"""

import os
import sys
from pathlib import Path

import psycopg
from psycopg import sql

from sawa.utils import setup_logging
from sawa.utils.cli import add_common_args, create_parser
from sawa.utils.resources import resolve_project_resource
from sawa.utils.security import redact_sensitive_text

EXPECTED_TABLES = {
    "companies",
    "stock_prices",
    "financial_ratios",
    "balance_sheets",
    "cash_flows",
    "income_statements",
    "treasury_yields",
    "inflation",
    "inflation_expectations",
    "labor_market",
    "sic_gics_mapping",
    "gics_overrides",
    "news_articles",
    "news_article_tickers",
    "news_sentiment",
    "technical_indicators",
    "technical_indicator_metadata",
    "indices",
    "index_constituents",
    "stock_splits",
    "dividends",
    "dividend_identity_conflicts",
    "earnings",
    "stock_prices_intraday",
    "market_internals",
    "trader_cards",
    "stock_character_classification",
    "stock_character_baseline",
    "stock_character_flags",
    "stock_character_scorecard",
}

EXPECTED_VIEWS = {
    "stock_prices_live",
    "v_company_summary",
    "v_company_with_indices",
    "v_economy_dashboard",
    "v_latest_fundamentals",
    "v_market_internals_enriched",
    "v_sector_summary",
}

EXPECTED_MATERIALIZED_VIEWS = {
    "mv_52week_extremes",
}

# Destructive rebuilds must have the complete ordered migration set available
# before they touch the database. Keep this manifest explicit so a packaging
# omission cannot redefine an incomplete directory as "complete" at runtime.
REQUIRED_SCHEMA_FILENAMES = frozenset(
    {
        "00_setup.sql",
        "01_companies.sql",
        "02_market_data.sql",
        "03_fundamentals.sql",
        "04_economy.sql",
        "05_indexes.sql",
        "06_views.sql",
        "07_procedures.sql",
        "08_sic_gics_mapping.sql",
        "09_sic_gics_data.sql",
        "10_news.sql",
        "11_technical_indicators.sql",
        "12_indices.sql",
        "13_gics_sector_function.sql",
        "14_52week_extremes.sql",
        "16_cleanup.sql",
        "17_extended_sma.sql",
        "18_corporate_actions.sql",
        "19_earnings_yfinance.sql",
        "20_drop_revenue_estimate.sql",
        "21_intraday_prices.sql",
        "22_views_advanced.sql",
        "23_add_cpi_yoy.sql",
        "24_widen_price_precision.sql",
        "25_trader_cards.sql",
        "26_market_internals.sql",
        "27_stock_character.sql",
        "29_consolidate_vix.sql",
        "30_add_adx_and_extended_indicators.sql",
        "31_widen_indicator_precision.sql",
        "32_us_active_index.sql",
        "33_schema_integrity_and_time_semantics.sql",
        "34_rename_nasdaq5000.sql",
        "35_additional_indices.sql",
        "36_sic_gics_data_extension.sql",
        "37_gics_overrides.sql",
        "38_gics_function_v2.sql",
        "39_russell1000_index.sql",
        "40_repair_intraday_tz_and_live_view.sql",
        "41_dashboard_spine_and_integrity.sql",
        "42_widen_price_for_reverse_splits.sql",
        "43_intraday_ohlc_sanity_check.sql",
        "44_ohlcv_completeness.sql",
        "45_intraday_bar_size_identity.sql",
        "46_dividend_identity.sql",
        "47_widen_indicator_overflow_headroom.sql",
        "48_widen_price_for_compounded_reverse_splits.sql",
    }
)


def get_sql_files(schema_dir: Path) -> list[Path]:
    """Get SQL files in execution order (all numbered files).

    Loads all files matching pattern: NN_*.sql where NN is 01-99.
    Files are executed in numeric order.

    Note: Migration files (16+) contain ALTER statements and should only
    be run on existing databases. For fresh installations, run all files
    in order.
    """
    import re

    schema_dir = resolve_project_resource(schema_dir, "sqlschema")
    pattern = re.compile(r"^\d{2}_.*\.sql$")
    sql_files = [
        f
        for f in schema_dir.glob("*.sql")
        if pattern.match(f.name)
    ]
    return sorted(sql_files)


def validate_schema_files(
    sql_files: list[Path],
    *,
    require_complete: bool,
) -> None:
    """Reject an empty or incomplete schema set before any database mutation."""
    if not sql_files:
        raise ValueError("schema directory contains no numbered SQL files")
    names = {path.name for path in sql_files}
    if require_complete:
        missing = sorted(REQUIRED_SCHEMA_FILENAMES - names)
        unexpected = sorted(names - REQUIRED_SCHEMA_FILENAMES)
        if missing or unexpected:
            sample = ", ".join(missing[:5])
            suffix = "..." if len(missing) > 5 else ""
            unexpected_detail = ""
            if unexpected:
                unexpected_detail = (
                    f"; {len(unexpected)} unexpected: {', '.join(unexpected[:5])}"
                )
            raise ValueError(
                "schema directory does not match the required migration manifest "
                f"({len(missing)} missing: {sample}{suffix}{unexpected_detail})"
            )
    for file_path in sql_files:
        try:
            content = file_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ValueError(
                f"schema file is not readable: {file_path.name} ({type(exc).__name__})"
            ) from None
        if not content.strip():
            raise ValueError(f"schema file is empty: {file_path.name}")


def execute_sql_file(conn, file_path: Path, dry_run: bool, logger) -> bool:
    """Execute a SQL file."""
    sql_content = file_path.read_text()

    if dry_run:
        logger.info(f"  [DRY-RUN] Would execute: {file_path.name}")
        return True

    try:
        with conn.cursor() as cur:
            cur.execute(sql_content)
            conn.commit()
            logger.info(f"  Executed: {file_path.name}")
            return True
    except psycopg.Error as e:
        conn.rollback()
        logger.error(
            "  Failed: %s - %s: %s",
            file_path.name,
            type(e).__name__,
            redact_sensitive_text(e),
        )
        return False


def pin_schema_search_path(conn) -> None:
    """Pin unqualified legacy migrations to the verified public schema."""
    with conn.cursor() as cur:
        # pg_catalog remains implicitly first when it is omitted from an
        # explicit search_path; unqualified CREATE statements target public.
        cur.execute("SET LOCAL search_path TO public")


def execute_sql_files_atomically(
    conn,
    sql_files: list[Path],
    dry_run: bool,
    logger,
    *,
    commit: bool = True,
) -> tuple[int, list[str]]:
    """Execute an ordered schema set in one transaction.

    The no-drop upgrade path must never leave a half-applied schema merely
    because a later migration fails. PostgreSQL transactional DDL lets us
    commit the complete ordered set or roll it all back.
    """
    if dry_run:
        for file_path in sql_files:
            logger.info(f"  [DRY-RUN] Would execute: {file_path.name}")
        return len(sql_files), []

    current_file: Path | None = None
    try:
        pin_schema_search_path(conn)
        for current_file in sql_files:
            sql_content = current_file.read_text()
            with conn.cursor() as cur:
                cur.execute(sql_content)
            logger.info(f"  Executed (pending commit): {current_file.name}")
        if commit:
            conn.commit()
            logger.info("  Committed schema files atomically")
        else:
            logger.info("  Schema files executed in pending rebuild transaction")
        return len(sql_files), []
    except psycopg.Error as e:
        conn.rollback()
        failed_name = current_file.name if current_file else "<schema transaction>"
        logger.error(
            "  Failed: %s - %s: %s; rolled back all schema changes",
            failed_name,
            type(e).__name__,
            redact_sensitive_text(e),
        )
        return 0, [failed_name]


def drop_all_tables(
    conn,
    dry_run: bool,
    logger,
    *,
    commit: bool = True,
) -> bool:
    """Drop all tables in public schema safely."""
    if dry_run:
        logger.info("  [DRY-RUN] Would drop all existing tables")
        return True

    try:
        with conn.cursor() as cur:
            # Get tables
            cur.execute("""
                SELECT tablename FROM pg_tables
                WHERE schemaname = 'public'
            """)
            tables = [row[0] for row in cur.fetchall()]

            if tables:
                logger.info(f"  Dropping {len(tables)} tables...")
                for table in tables:
                    # Use sql.Identifier for safe table name handling
                    drop_stmt = sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                        sql.Identifier("public"),
                        sql.Identifier(table),
                    )
                    cur.execute(drop_stmt)
                    logger.info(f"    Dropped: {table}")

            # Drop functions
            cur.execute("""
                SELECT p.proname, pg_catalog.pg_get_function_identity_arguments(p.oid)
                FROM pg_catalog.pg_proc p
                JOIN pg_catalog.pg_namespace n ON p.pronamespace = n.oid
                WHERE n.nspname = 'public' AND p.prokind = 'f'
            """)
            functions = cur.fetchall()

            if functions:
                logger.info(f"  Dropping {len(functions)} functions...")
                for func, identity_args in functions:
                    drop_stmt = sql.SQL(
                        "DROP FUNCTION IF EXISTS {}.{}({}) CASCADE"
                    ).format(
                        sql.Identifier("public"),
                        sql.Identifier(func),
                        sql.SQL(identity_args),
                    )
                    cur.execute(drop_stmt)

            # PostgreSQL DDL is transactional: one commit ensures a function
            # cleanup failure cannot leave tables partially deleted.
            if commit:
                conn.commit()

            return True
    except psycopg.Error as e:
        conn.rollback()
        logger.error(
            "  Failed to drop tables: %s: %s",
            type(e).__name__,
            redact_sensitive_text(e),
        )
        return False


def verify_tables(conn) -> list[str]:
    """Verify expected tables exist."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        """)
        actual = {row[0] for row in cur.fetchall()}
    return list(EXPECTED_TABLES - actual)


def verify_views(conn) -> list[str]:
    """Verify expected regular views exist."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name FROM information_schema.views
            WHERE table_schema = 'public'
        """)
        actual = {row[0] for row in cur.fetchall()}
    return list(EXPECTED_VIEWS - actual)


def verify_materialized_views(conn) -> list[str]:
    """Verify expected materialized views exist."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT matviewname FROM pg_matviews
            WHERE schemaname = 'public'
        """)
        actual = {row[0] for row in cur.fetchall()}
    return list(EXPECTED_MATERIALIZED_VIEWS - actual)


def confirm_rebuild() -> bool:
    """Ask user to confirm rebuild."""
    print("\nWARNING: This will DROP and recreate all tables!")
    print("All existing data will be lost.")
    response = input("\nType 'rebuild' to continue: ")
    return response.strip().lower() == "rebuild"


def main() -> int:
    """Main entry point."""
    parser = create_parser(
        "Rebuild the stock database from SQL schema files.",
        epilog="""\
Examples:
  %(prog)s --database-url postgresql://user:pass@localhost/stock_data
  %(prog)s --force --schema-dir ./my_schema
  DATABASE_URL=postgresql://... %(prog)s --dry-run
""",
    )

    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL"),
        help="PostgreSQL connection URL",
    )
    parser.add_argument(
        "--schema-dir",
        type=Path,
        default=Path("sqlschema"),
        help="Directory with SQL files (default: sqlschema/)",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true", help="Skip confirmation")
    parser.add_argument(
        "--drop", action="store_true", help="Drop existing tables first"
    )
    add_common_args(parser)

    args = parser.parse_args()
    logger = setup_logging(args.verbose)

    if not args.database_url:
        logger.error("--database-url required or set DATABASE_URL")
        return 1

    schema_dir = resolve_project_resource(args.schema_dir, "sqlschema")

    if not schema_dir.exists():
        logger.error(f"Schema directory not found: {schema_dir}")
        return 1

    sql_files = get_sql_files(schema_dir)
    try:
        validate_schema_files(sql_files, require_complete=args.drop)
    except ValueError as exc:
        logger.error("Invalid schema set: %s", redact_sensitive_text(exc))
        return 1

    logger.info(f"Found {len(sql_files)} SQL files:")
    for f in sql_files:
        logger.info(f"  - {f.name}")

    if args.dry_run:
        logger.info("\n[DRY-RUN MODE]")
    elif not args.force:
        if not confirm_rebuild():
            logger.info("Aborted.")
            return 0

    try:
        with psycopg.connect(args.database_url) as conn:
            logger.info("Connected to database.")
            if not args.dry_run:
                pin_schema_search_path(conn)

            if args.drop:
                logger.info("\nDropping existing tables...")
                if not drop_all_tables(
                    conn,
                    args.dry_run,
                    logger,
                    commit=False,
                ):
                    return 1

            success, failed_files = execute_sql_files_atomically(
                conn,
                sql_files,
                args.dry_run,
                logger,
                commit=False,
            )

            logger.info(f"\n{success}/{len(sql_files)} files executed")
            if failed_files:
                logger.error("Failed schema files: %s", ", ".join(failed_files))
                return 1

            if not args.dry_run:
                logger.info("\nVerifying tables...")
                missing = verify_tables(conn)
                if missing:
                    logger.error(f"  Missing tables: {', '.join(missing)}")
                    conn.rollback()
                    return 1
                else:
                    logger.info("  All expected tables present")

                missing_views = verify_views(conn)
                if missing_views:
                    logger.error(f"  Missing views: {', '.join(missing_views)}")
                    conn.rollback()
                    return 1
                logger.info("  All expected views present")

                missing_matviews = verify_materialized_views(conn)
                if missing_matviews:
                    logger.error(
                        f"  Missing materialized views: {', '.join(missing_matviews)}"
                    )
                    conn.rollback()
                    return 1
                logger.info("  All expected materialized views present")
                conn.commit()
                logger.info("  Committed verified schema transaction atomically")

    except psycopg.Error as e:
        logger.error(
            "Could not connect: %s: %s",
            type(e).__name__,
            redact_sensitive_text(e),
        )
        return 1
    except KeyboardInterrupt:
        logger.info("\nInterrupted.")
        return 1

    logger.info("\nDone!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
