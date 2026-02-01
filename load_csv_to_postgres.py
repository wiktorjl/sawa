#!/usr/bin/env python3
"""
Load CSV files into PostgreSQL database.

This script loads CSV data into PostgreSQL tables using INSERT statements with
batch processing for performance. Supports UPSERT (INSERT ... ON CONFLICT UPDATE).

Usage:
    # Load single CSV
    python load_csv_to_postgres.py --csv data/overviews/OVERVIEWS.csv --table companies

    # Load with explicit column mapping
    python load_csv_to_postgres.py --csv data.csv --table mytable --columns col1,col2,col3

    # Load multiple CSVs using mapping file
    python load_csv_to_postgres.py --mapping data_mappings.json

    # Dry run (preview without inserting)
    python load_csv_to_postgres.py --csv data.csv --table companies --dry-run

    # Load specific tables from mapping file
    python load_csv_to_postgres.py --mapping data_mappings.json --tables companies,financial_ratios

Environment Variables:
    PGHOST - PostgreSQL host (default: localhost)
    PGPORT - PostgreSQL port (default: 5432)
    PGDATABASE - Database name (required)
    PGUSER - Database user (required)
    PGPASSWORD - Database password (required)
"""

import argparse
import csv
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import execute_values


# Configuration
DEFAULT_BATCH_SIZE = 1000
DEFAULT_PORT = 5432
DEFAULT_HOST = "localhost"

# Environment variable names
ENV_HOST = "PGHOST"
ENV_PORT = "PGPORT"
ENV_DATABASE = "PGDATABASE"
ENV_USER = "PGUSER"
ENV_PASSWORD = "PGPASSWORD"


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


def get_connection_params(args) -> Dict[str, Any]:
    """
    Get database connection parameters from environment variables or CLI args.

    Priority: CLI args > Environment variables > Defaults
    """
    host = args.host or os.environ.get(ENV_HOST, DEFAULT_HOST)
    port = args.port or int(os.environ.get(ENV_PORT, DEFAULT_PORT))
    database = args.database or os.environ.get(ENV_DATABASE)
    user = args.user or os.environ.get(ENV_USER)
    password = args.password or os.environ.get(ENV_PASSWORD)

    if not database:
        raise ValueError(
            f"Database name required. Set {ENV_DATABASE} env var or use --database"
        )
    if not user:
        raise ValueError(
            f"Database user required. Set {ENV_USER} env var or use --user"
        )
    if not password:
        raise ValueError(
            f"Database password required. Set {ENV_PASSWORD} env var or use --password"
        )

    return {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
    }


def connect_to_database(conn_params: Dict[str, Any], logger: logging.Logger):
    """Establish connection to PostgreSQL database."""
    logger.info(
        f"Connecting to PostgreSQL at {conn_params['host']}:{conn_params['port']}/{conn_params['database']}"
    )

    try:
        conn = psycopg2.connect(**conn_params)
        logger.info("Connected successfully")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


def get_table_primary_key(
    conn, table_name: str, logger: logging.Logger
) -> Optional[List[str]]:
    """
    Get the primary key columns for a table.

    Returns:
        List of primary key column names, or None if no PK exists
    """
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

        if rows:
            pk_columns = [row[0] for row in rows]
            logger.debug(f"Primary key for {table_name}: {pk_columns}")
            return pk_columns
        else:
            logger.warning(f"No primary key found for table {table_name}")
            return None


def get_column_types(conn, table_name: str, logger: logging.Logger) -> Dict[str, str]:
    """
    Get PostgreSQL column types for a table.

    Returns:
        Dict mapping column name to data type
    """
    query = """
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = %s
        ORDER BY ordinal_position
    """

    column_types = {}
    with conn.cursor() as cur:
        cur.execute(query, (table_name,))
        for row in cur.fetchall():
            column_types[row[0]] = row[1]

    logger.debug(f"Column types for {table_name}: {column_types}")
    return column_types


def convert_value(value: str, pg_type: str) -> Any:
    """
    Convert a string value to the appropriate Python type for PostgreSQL.

    Args:
        value: String value from CSV
        pg_type: PostgreSQL data type

    Returns:
        Converted value or None
    """
    if value is None or value == "":
        return None

    # Numeric types - handle floats that should be integers
    if pg_type in ("bigint", "integer", "smallint"):
        try:
            # Convert to float first (in case it has .0), then to int
            return int(float(value))
        except (ValueError, TypeError):
            return None

    # Other numeric types
    if pg_type in ("numeric", "real", "double precision"):
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    # Boolean
    if pg_type == "boolean":
        val_lower = value.lower()
        if val_lower in ("true", "t", "yes", "y", "1"):
            return True
        elif val_lower in ("false", "f", "no", "n", "0"):
            return False
        return None

    # Date types
    if pg_type in ("date", "timestamp", "timestamp without time zone"):
        return value  # Keep as string, PostgreSQL will parse

    # Default: return as string
    return value


def load_csv_data(
    csv_path: Path,
    columns: List[str],
    column_types: Dict[str, str],
    logger: logging.Logger,
) -> List[Tuple]:
    """
    Load data from CSV file with type conversion.

    Args:
        csv_path: Path to CSV file
        columns: List of column names to extract (must match CSV header)
        column_types: Dict mapping column names to PostgreSQL types
        logger: Logger instance

    Returns:
        List of tuples containing row data
    """
    logger.info(f"Loading CSV: {csv_path}")

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    data = []
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # Validate that all required columns exist in CSV
        csv_columns = reader.fieldnames
        missing_columns = [col for col in columns if col not in csv_columns]
        if missing_columns:
            raise ValueError(
                f"Columns not found in CSV: {missing_columns}. Available: {csv_columns}"
            )

        for row_num, row in enumerate(
            reader, start=2
        ):  # start=2 because row 1 is header
            try:
                # Extract values in the specified column order with type conversion
                values = []
                for col in columns:
                    val = row.get(col, "")
                    pg_type = column_types.get(col, "text")
                    converted = convert_value(val, pg_type)
                    values.append(converted)

                data.append(tuple(values))
            except Exception as e:
                logger.error(f"Error processing row {row_num}: {e}")
                raise

    logger.info(f"Loaded {len(data)} rows from CSV")
    return data


def insert_data_batch(
    conn,
    table_name: str,
    columns: List[str],
    data: List[Tuple],
    batch_size: int,
    upsert: bool,
    logger: logging.Logger,
) -> Tuple[int, int]:
    """
    Insert data into table using batch processing.

    Args:
        conn: Database connection
        table_name: Target table name
        columns: List of column names
        data: List of row tuples
        batch_size: Number of rows per batch
        upsert: Whether to perform UPSERT (update on conflict)
        logger: Logger instance

    Returns:
        Tuple of (inserted_count, updated_count)
    """
    inserted_count = 0
    updated_count = 0

    # Build column list string
    columns_str = ", ".join(columns)

    # Get primary key for upsert
    pk_columns = get_table_primary_key(conn, table_name, logger) if upsert else None

    if upsert and pk_columns:
        # Build UPSERT query
        placeholders = ", ".join(["%s"] * len(columns))

        # Build SET clause for update (exclude PK columns)
        update_columns = [col for col in columns if col not in pk_columns]
        if update_columns:
            set_clause = ", ".join(
                [f"{col} = EXCLUDED.{col}" for col in update_columns]
            )
            query = f"""
                INSERT INTO {table_name} ({columns_str}) 
                VALUES %s 
                ON CONFLICT ({", ".join(pk_columns)}) 
                DO UPDATE SET {set_clause}
            """
        else:
            # All columns are PK, just ignore conflicts
            query = f"""
                INSERT INTO {table_name} ({columns_str}) 
                VALUES %s 
                ON CONFLICT ({", ".join(pk_columns)}) 
                DO NOTHING
            """
        logger.info(f"Using UPSERT on primary key: {pk_columns}")
    else:
        # Simple INSERT
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES %s"
        if upsert and not pk_columns:
            logger.warning("UPSERT requested but no primary key found, using INSERT")

    logger.info(f"Inserting data into {table_name} (batch size: {batch_size})")

    with conn.cursor() as cur:
        total_batches = (len(data) + batch_size - 1) // batch_size

        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(data))
            batch = data[start_idx:end_idx]

            try:
                execute_values(cur, query, batch)
                conn.commit()

                if upsert and pk_columns:
                    # Rough estimate: assume half are updates (we can't easily tell from execute_values)
                    inserted_count += len(batch)
                else:
                    inserted_count += len(batch)

                if (batch_num + 1) % 10 == 0 or batch_num == total_batches - 1:
                    logger.info(
                        f"Progress: {end_idx}/{len(data)} rows processed ({100 * end_idx // len(data)}%)"
                    )

            except psycopg2.Error as e:
                conn.rollback()
                error_msg = str(e).lower()

                # Check if it's a constraint violation we can skip
                is_unique_violation = "unique constraint" in error_msg
                is_fk_violation = (
                    "foreign key" in error_msg or "violates foreign key" in error_msg
                )

                if (is_unique_violation or is_fk_violation) and upsert:
                    violation_type = (
                        "unique constraint" if is_unique_violation else "foreign key"
                    )
                    logger.warning(
                        f"{violation_type.title()} violation in batch {batch_num + 1}: {e}"
                    )
                    logger.info(
                        "Attempting to insert rows individually to skip problematic ones..."
                    )

                    # Try inserting one by one to skip problematic rows
                    individual_success = 0
                    individual_failed = 0

                    for row in batch:
                        try:
                            # Build simple INSERT query
                            simple_query = f"INSERT INTO {table_name} ({columns_str}) VALUES %s ON CONFLICT DO NOTHING"
                            execute_values(cur, simple_query, [row])
                            conn.commit()
                            individual_success += 1
                        except psycopg2.Error as row_error:
                            conn.rollback()
                            individual_failed += 1
                            if individual_failed <= 5:  # Only log first few errors
                                logger.debug(f"Skipping row due to: {row_error}")

                    logger.info(
                        f"Individual insert results: {individual_success} succeeded, {individual_failed} skipped"
                    )
                    inserted_count += individual_success
                else:
                    logger.error(f"Error inserting batch {batch_num + 1}: {e}")
                    raise

    return inserted_count, updated_count


def load_from_mapping_file(
    mapping_path: Path, tables: Optional[List[str]], logger: logging.Logger
) -> List[Dict]:
    """
    Load mapping configuration from JSON file.

    Args:
        mapping_path: Path to mapping JSON file
        tables: Optional list of specific tables to load (if None, load all)
        logger: Logger instance

    Returns:
        List of load configurations
    """
    logger.info(f"Loading mapping file: {mapping_path}")

    with open(mapping_path, "r") as f:
        mappings = json.load(f)

    configs = []
    for table_name, config in mappings.items():
        if tables and table_name not in tables:
            logger.debug(f"Skipping table: {table_name}")
            continue

        configs.append(
            {
                "table": table_name,
                "csv": Path(config["csv"]),
                "columns": config["columns"],
            }
        )

    logger.info(f"Found {len(configs)} table(s) to load from mapping file")
    return configs


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load CSV files into PostgreSQL database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  # Load single CSV with explicit columns
  python load_csv_to_postgres.py --csv data/overviews/OVERVIEWS.csv --table companies \\
      --columns ticker,name,description,market,type,locale,currency_name,active

  # Load using mapping file
  python load_csv_to_postgres.py --mapping data_mappings.json

  # Load specific tables from mapping
  python load_csv_to_postgres.py --mapping data_mappings.json --tables companies,financial_ratios

  # Dry run to preview
  python load_csv_to_postgres.py --csv data.csv --table companies --dry-run

Environment Variables:
  PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
""",
    )

    # Input options
    parser.add_argument(
        "--csv",
        type=Path,
        help="Path to CSV file to load",
    )

    parser.add_argument(
        "--table",
        help="Target table name",
    )

    parser.add_argument(
        "--columns",
        help="Comma-separated list of column names (must match CSV header)",
    )

    parser.add_argument(
        "--mapping",
        type=Path,
        help="Path to JSON mapping file for batch loading",
    )

    parser.add_argument(
        "--tables",
        help="Comma-separated list of tables to load from mapping file (default: all)",
    )

    # Database connection
    parser.add_argument(
        "--host",
        help=f"PostgreSQL host (default: {DEFAULT_HOST} or {ENV_HOST} env var)",
    )

    parser.add_argument(
        "--port",
        type=int,
        help=f"PostgreSQL port (default: {DEFAULT_PORT} or {ENV_PORT} env var)",
    )

    parser.add_argument(
        "--database",
        help=f"Database name (or {ENV_DATABASE} env var)",
    )

    parser.add_argument(
        "--user",
        help=f"Database user (or {ENV_USER} env var)",
    )

    parser.add_argument(
        "--password",
        help=f"Database password (or {ENV_PASSWORD} env var)",
    )

    # Processing options
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Number of rows per batch (default: {DEFAULT_BATCH_SIZE})",
    )

    parser.add_argument(
        "--upsert",
        action="store_true",
        help="Enable UPSERT (update existing rows on conflict)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be loaded without inserting",
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose (debug) logging"
    )

    args = parser.parse_args()

    # Setup logging
    logger = setup_logging(args.verbose)

    logger.info("=" * 60)
    logger.info("PostgreSQL CSV Loader")
    logger.info("=" * 60)

    # Validate arguments
    if args.mapping:
        # Batch mode with mapping file
        if args.csv or args.table or args.columns:
            logger.error("Cannot use --csv/--table/--columns with --mapping")
            sys.exit(1)
    else:
        # Single CSV mode
        if not args.csv or not args.table:
            logger.error("--csv and --table are required (or use --mapping)")
            sys.exit(1)
        if not args.columns:
            logger.error("--columns is required for single CSV mode")
            sys.exit(1)

    try:
        # Get connection parameters
        conn_params = get_connection_params(args)

        # Connect to database
        conn = connect_to_database(conn_params, logger)

        # Prepare load configurations
        if args.mapping:
            tables_to_load = args.tables.split(",") if args.tables else None
            load_configs = load_from_mapping_file(args.mapping, tables_to_load, logger)
        else:
            columns = args.columns.split(",")
            load_configs = [{"table": args.table, "csv": args.csv, "columns": columns}]

        # Process each configuration
        total_stats = {"loaded": 0, "errors": 0}

        for config in load_configs:
            logger.info("")
            logger.info(f"Processing: {config['csv']} -> {config['table']}")
            logger.info("-" * 60)

            try:
                # Get column types for type conversion
                column_types = get_column_types(conn, config["table"], logger)

                # Load CSV data
                data = load_csv_data(
                    config["csv"], config["columns"], column_types, logger
                )

                if args.dry_run:
                    logger.info(
                        f"[DRY RUN] Would load {len(data)} rows into {config['table']}"
                    )
                    logger.info(f"[DRY RUN] First row: {data[0] if data else 'N/A'}")
                    continue

                # Insert data
                inserted, updated = insert_data_batch(
                    conn,
                    config["table"],
                    config["columns"],
                    data,
                    args.batch_size,
                    args.upsert,
                    logger,
                )

                total_stats["loaded"] += inserted
                logger.info(
                    f"Successfully loaded {inserted} rows into {config['table']}"
                )

            except Exception as e:
                logger.error(f"Failed to load {config['csv']}: {e}")
                total_stats["errors"] += 1
                if not args.mapping:
                    raise

        # Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("Load Summary")
        logger.info("=" * 60)
        logger.info(f"Total rows loaded: {total_stats['loaded']}")
        logger.info(f"Files with errors: {total_stats['errors']}")

        conn.close()
        logger.info("Done!")

    except Exception as e:
        logger.error(f"Error: {e}")
        if args.verbose:
            raise
        sys.exit(1)


if __name__ == "__main__":
    main()
