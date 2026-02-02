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
}


def get_sql_files(schema_dir: Path) -> list[Path]:
    """Get SQL files in execution order (01-07)."""
    sql_files = []
    for num in range(1, 8):
        pattern = f"{num:02d}_*.sql"
        matches = list(schema_dir.glob(pattern))
        if matches:
            sql_files.append(matches[0])
    return sorted(sql_files)


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
        logger.error(f"  Failed: {file_path.name} - {e}")
        return False


def drop_all_tables(conn, dry_run: bool, logger) -> bool:
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
                    drop_stmt = sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
                        sql.Identifier(table)
                    )
                    cur.execute(drop_stmt)
                    logger.info(f"    Dropped: {table}")
                conn.commit()

            # Drop functions
            cur.execute("""
                SELECT proname FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                WHERE n.nspname = 'public'
            """)
            functions = [row[0] for row in cur.fetchall()]

            if functions:
                logger.info(f"  Dropping {len(functions)} functions...")
                for func in functions:
                    drop_stmt = sql.SQL("DROP FUNCTION IF EXISTS {} CASCADE").format(
                        sql.Identifier(func)
                    )
                    cur.execute(drop_stmt)
                conn.commit()

            return True
    except psycopg.Error as e:
        conn.rollback()
        logger.error(f"  Failed to drop tables: {e}")
        return False


def verify_tables(conn) -> list[str]:
    """Verify expected tables exist."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        actual = {row[0] for row in cur.fetchall()}
    return list(EXPECTED_TABLES - actual)


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

    if not args.schema_dir.exists():
        logger.error(f"Schema directory not found: {args.schema_dir}")
        return 1

    sql_files = get_sql_files(args.schema_dir)
    if not sql_files:
        logger.error(f"No SQL files found in {args.schema_dir}")
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

            if args.drop:
                logger.info("\nDropping existing tables...")
                if not drop_all_tables(conn, args.dry_run, logger):
                    return 1

            success = 0
            for sql_file in sql_files:
                if execute_sql_file(conn, sql_file, args.dry_run, logger):
                    success += 1

            logger.info(f"\n{success}/{len(sql_files)} files executed")

            if not args.dry_run:
                logger.info("\nVerifying tables...")
                missing = verify_tables(conn)
                if missing:
                    logger.warning(f"  Missing tables: {', '.join(missing)}")
                else:
                    logger.info("  All expected tables present")

    except psycopg.OperationalError as e:
        logger.error(f"Could not connect: {e}")
        return 1
    except KeyboardInterrupt:
        logger.info("\nInterrupted.")
        return 1

    logger.info("\nDone!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
