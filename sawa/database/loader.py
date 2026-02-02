"""
Load CSV files into PostgreSQL database.

Refactored from: load_csv_to_postgres.py

Security: Uses psycopg2.sql for safe identifier handling.

Usage:
    python -m sawa.database.loader --csv data.csv --table companies
"""

import csv
import json
import sys
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from sawa.utils import setup_logging
from sawa.utils.cli import add_common_args, create_parser

from .connection import get_connection, get_connection_params

DEFAULT_BATCH_SIZE = 1000


def get_table_primary_key(conn, table_name: str) -> list[str] | None:
    """Get primary key columns for table."""
    query = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_name = %s
        ORDER BY kcu.ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(query, (table_name,))
        rows = cur.fetchall()
        return [row[0] for row in rows] if rows else None


def get_column_types(conn, table_name: str) -> dict[str, str]:
    """Get PostgreSQL column types for table."""
    query = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(query, (table_name,))
        return {row[0]: row[1] for row in cur.fetchall()}


def convert_value(value: str, pg_type: str) -> Any:
    """Convert string value to appropriate Python type."""
    if value is None or value == "":
        return None

    if pg_type in ("bigint", "integer", "smallint"):
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    if pg_type in ("numeric", "real", "double precision"):
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    if pg_type == "boolean":
        val_lower = value.lower()
        if val_lower in ("true", "t", "yes", "y", "1"):
            return True
        elif val_lower in ("false", "f", "no", "n", "0"):
            return False
        return None

    return value


def load_csv_data(
    csv_path: Path,
    columns: list[str],
    column_types: dict[str, str],
    logger,
) -> list[tuple]:
    """Load and convert data from CSV file."""
    logger.info(f"Loading CSV: {csv_path}")

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    data = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        csv_columns = reader.fieldnames or []
        missing = [col for col in columns if col not in csv_columns]
        if missing:
            raise ValueError(f"Columns not found in CSV: {missing}")

        for row in reader:
            values = []
            for col in columns:
                val = row.get(col, "")
                pg_type = column_types.get(col, "text")
                values.append(convert_value(val, pg_type))
            data.append(tuple(values))

    logger.info(f"Loaded {len(data)} rows")
    return data


def insert_data_batch(
    conn,
    table_name: str,
    columns: list[str],
    data: list[tuple],
    batch_size: int,
    upsert: bool,
    logger,
) -> int:
    """
    Insert data with safe SQL construction.

    Uses psycopg2.sql to prevent SQL injection.
    """
    inserted = 0
    pk_columns = get_table_primary_key(conn, table_name) if upsert else None

    # Build safe SQL using psycopg2.sql module
    columns_sql = sql.SQL(", ").join(map(sql.Identifier, columns))

    if upsert and pk_columns:
        pk_sql = sql.SQL(", ").join(map(sql.Identifier, pk_columns))
        update_cols = [c for c in columns if c not in pk_columns]

        if update_cols:
            set_sql = sql.SQL(", ").join(
                [
                    sql.SQL("{} = EXCLUDED.{}").format(
                        sql.Identifier(c), sql.Identifier(c)
                    )
                    for c in update_cols
                ]
            )
            query = sql.SQL(
                "INSERT INTO {} ({}) VALUES %s ON CONFLICT ({}) DO UPDATE SET {}"
            ).format(
                sql.Identifier(table_name),
                columns_sql,
                pk_sql,
                set_sql,
            )
        else:
            query = sql.SQL(
                "INSERT INTO {} ({}) VALUES %s ON CONFLICT ({}) DO NOTHING"
            ).format(
                sql.Identifier(table_name),
                columns_sql,
                pk_sql,
            )
        logger.info(f"Using UPSERT on: {pk_columns}")
    else:
        query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            sql.Identifier(table_name),
            columns_sql,
        )

    logger.info(f"Inserting into {table_name} (batch size: {batch_size})")

    with conn.cursor() as cur:
        total_batches = (len(data) + batch_size - 1) // batch_size

        for batch_num in range(total_batches):
            start = batch_num * batch_size
            end = min(start + batch_size, len(data))
            batch = data[start:end]

            try:
                execute_values(cur, query, batch)
                conn.commit()
                inserted += len(batch)

                if (batch_num + 1) % 10 == 0 or batch_num == total_batches - 1:
                    logger.info(f"Progress: {end}/{len(data)} rows")

            except psycopg2.Error as e:
                conn.rollback()
                logger.error(f"Batch {batch_num + 1} failed: {e}")
                raise

    return inserted


def main() -> int:
    """Main entry point."""
    parser = create_parser(
        "Load CSV files into PostgreSQL database.",
        epilog="""\
Examples:
  %(prog)s --csv data.csv --table companies --columns ticker,name
  %(prog)s --mapping data_mappings.json
  %(prog)s --csv data.csv --table companies --upsert

Environment: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
""",
    )

    parser.add_argument("--csv", type=Path, help="Path to CSV file")
    parser.add_argument("--table", help="Target table name")
    parser.add_argument("--columns", help="Comma-separated column names")
    parser.add_argument("--mapping", type=Path, help="JSON mapping file")
    parser.add_argument(
        "--tables", help="Tables to load from mapping (comma-separated)"
    )
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--database")
    parser.add_argument("--user")
    parser.add_argument("--password")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--upsert", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    add_common_args(parser)

    args = parser.parse_args()
    logger = setup_logging(args.verbose)

    logger.info("=" * 60)
    logger.info("PostgreSQL CSV Loader")
    logger.info("=" * 60)

    # Validate args
    if args.mapping:
        if args.csv or args.table or args.columns:
            logger.error("Cannot use --csv/--table/--columns with --mapping")
            return 1
    else:
        if not args.csv or not args.table or not args.columns:
            logger.error("--csv, --table, and --columns are required")
            return 1

    try:
        conn_params = get_connection_params(
            args.host, args.port, args.database, args.user, args.password
        )
        conn = get_connection(conn_params)
        logger.info(
            f"Connected to {conn_params['host']}:{conn_params['port']}/{conn_params['database']}"
        )

        # Build load configs
        if args.mapping:
            with open(args.mapping) as f:
                mappings = json.load(f)
            filter_tables = args.tables.split(",") if args.tables else None
            configs = [
                {"table": t, "csv": Path(c["csv"]), "columns": c["columns"]}
                for t, c in mappings.items()
                if not filter_tables or t in filter_tables
            ]
        else:
            configs = [
                {
                    "table": args.table,
                    "csv": args.csv,
                    "columns": args.columns.split(","),
                }
            ]

        total = 0
        for config in configs:
            logger.info(f"\nProcessing: {config['csv']} -> {config['table']}")
            column_types = get_column_types(conn, config["table"])
            data = load_csv_data(config["csv"], config["columns"], column_types, logger)

            if args.dry_run:
                logger.info(f"[DRY RUN] Would load {len(data)} rows")
                continue

            inserted = insert_data_batch(
                conn,
                config["table"],
                config["columns"],
                data,
                args.batch_size,
                args.upsert,
                logger,
            )
            total += inserted

        conn.close()
        logger.info(f"\nTotal rows loaded: {total}")
        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        if args.verbose:
            raise
        return 1


if __name__ == "__main__":
    sys.exit(main())
