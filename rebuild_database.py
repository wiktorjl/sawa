#!/usr/bin/env python3
"""
Rebuild the stock database using SQL schema files.

Usage:
    python rebuild_database.py --database-url postgresql://user:pass@host:5432/stock_data
    python rebuild_database.py  # Uses DATABASE_URL environment variable

Options:
    --schema-dir PATH    Directory containing SQL files (default: sqlschema/)
    --dry-run           Show what would be executed without running
    --force             Skip confirmation prompt
"""

import argparse
import os
import sys
from pathlib import Path
from typing import List, Optional
import psycopg


def get_sql_files(schema_dir: Path) -> List[Path]:
    """Get list of SQL files in execution order (01-07)."""
    sql_files = []
    for num in range(1, 8):
        pattern = f"{num:02d}_*.sql"
        matches = list(schema_dir.glob(pattern))
        if matches:
            sql_files.append(matches[0])
    return sorted(sql_files)


def execute_sql_file(conn, file_path: Path, dry_run: bool = False) -> bool:
    """Execute a SQL file and return success status."""
    sql_content = file_path.read_text()

    if dry_run:
        print(f"  [DRY-RUN] Would execute: {file_path.name}")
        return True

    try:
        with conn.cursor() as cur:
            cur.execute(sql_content)
            conn.commit()
            print(f"  ✓ Executed: {file_path.name}")
            return True
    except psycopg.Error as e:
        conn.rollback()
        print(f"  ✗ Failed: {file_path.name}")
        print(f"    Error: {e}")
        return False


def verify_tables(conn) -> List[str]:
    """Verify expected tables exist in database."""
    expected_tables = {
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

    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        actual_tables = {row[0] for row in cur.fetchall()}

    missing = expected_tables - actual_tables
    return list(missing)


def confirm_rebuild() -> bool:
    """Ask user to confirm database rebuild."""
    print("\n⚠️  WARNING: This will DROP and recreate all tables!")
    print("   All existing data will be lost.")
    response = input("\nAre you sure? Type 'rebuild' to continue: ")
    return response.strip().lower() == "rebuild"


def drop_all_tables(conn, dry_run: bool = False) -> bool:
    """Drop all tables in the public schema."""
    if dry_run:
        print("  [DRY-RUN] Would drop all existing tables")
        return True

    try:
        with conn.cursor() as cur:
            # Get all tables in public schema
            cur.execute("""
                SELECT tablename FROM pg_tables
                WHERE schemaname = 'public'
            """)
            tables = [row[0] for row in cur.fetchall()]

            if tables:
                print(f"  Dropping {len(tables)} existing tables...")
                for table in tables:
                    cur.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
                    print(f"    ✓ Dropped: {table}")
                conn.commit()
            else:
                print("  No existing tables to drop")

            # Also drop any functions/procedures
            cur.execute("""
                SELECT proname FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                WHERE n.nspname = 'public'
            """)
            functions = [row[0] for row in cur.fetchall()]

            if functions:
                print(f"  Dropping {len(functions)} existing functions...")
                for func in functions:
                    cur.execute(f'DROP FUNCTION IF EXISTS "{func}" CASCADE')
                    print(f"    ✓ Dropped function: {func}")
                conn.commit()

            return True
    except psycopg.Error as e:
        conn.rollback()
        print(f"  ✗ Failed to drop tables: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Rebuild the stock database from SQL schema files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --database-url postgresql://user:pass@localhost/stock_data
  %(prog)s --force --schema-dir ./my_schema
  DATABASE_URL=postgresql://... %(prog)s --dry-run
""",
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL"),
        help="PostgreSQL connection URL (or set DATABASE_URL env var)",
    )
    parser.add_argument(
        "--schema-dir",
        type=Path,
        default=Path("sqlschema"),
        help="Directory containing SQL schema files (default: sqlschema/)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be executed without running",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip confirmation prompt (use with caution!)",
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop all existing tables before rebuilding",
    )

    args = parser.parse_args()

    if not args.database_url:
        print(
            "Error: --database-url required or DATABASE_URL environment variable must be set"
        )
        sys.exit(1)

    if not args.schema_dir.exists():
        print(f"Error: Schema directory not found: {args.schema_dir}")
        sys.exit(1)

    sql_files = get_sql_files(args.schema_dir)
    if not sql_files:
        print(f"Error: No SQL files found in {args.schema_dir}")
        sys.exit(1)

    print(f"Found {len(sql_files)} SQL files to execute:")
    for f in sql_files:
        print(f"  - {f.name}")

    if args.dry_run:
        print("\n[DRY-RUN MODE - No changes will be made]")
    elif not args.force:
        if not confirm_rebuild():
            print("Aborted.")
            sys.exit(0)

    print(f"\nConnecting to database...")

    try:
        with psycopg.connect(args.database_url) as conn:
            print("Connected successfully.\n")

            if args.drop:
                print("Dropping existing tables...")
                if not drop_all_tables(conn, args.dry_run):
                    print("\nFailed to drop tables. Aborting.")
                    sys.exit(1)
                print()

            success_count = 0
            for sql_file in sql_files:
                if execute_sql_file(conn, sql_file, args.dry_run):
                    success_count += 1

            print(f"\n{success_count}/{len(sql_files)} SQL files executed successfully")

            if not args.dry_run:
                print("\nVerifying tables...")
                missing = verify_tables(conn)
                if missing:
                    print(f"  ⚠️  Missing tables: {', '.join(missing)}")
                else:
                    print("  ✓ All expected tables present")

    except psycopg.OperationalError as e:
        print(f"Error: Could not connect to database - {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
        sys.exit(1)

    print("\nDone!")


if __name__ == "__main__":
    main()
