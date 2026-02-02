"""
High-level data loading functions for coldstart workflow.

Loads downloaded CSV data into PostgreSQL tables.
"""

import csv
import logging
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from sawa.api.client import PolygonClient
from sawa.utils.constants import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_NEWS_DAYS,
    DEFAULT_NEWS_LIMIT_PER_SYMBOL,
)

logger = logging.getLogger(__name__)


def load_csv_to_table(
    conn,
    csv_path: Path,
    table_name: str,
    column_mapping: dict[str, str],
    log: logging.Logger | None = None,
    upsert: bool = True,
) -> int:
    """
    Load CSV file into PostgreSQL table.

    Args:
        conn: Database connection
        csv_path: Path to CSV file
        table_name: Target table name
        column_mapping: Dict mapping CSV columns to DB columns
        logger: Logger instance
        upsert: Use ON CONFLICT DO UPDATE

    Returns:
        Number of rows loaded
    """
    log = log or logger
    if not csv_path.exists():
        log.warning(f"CSV not found: {csv_path}")
        return 0

    # Read CSV data
    rows: list[dict[str, Any]] = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mapped_row = {}
            for csv_col, db_col in column_mapping.items():
                val = row.get(csv_col, "")
                # Handle list-formatted tickers like "['AAPL']"
                if db_col == "ticker" and val.startswith("["):
                    # Extract ticker from list format: "['AAPL']" -> "AAPL"
                    val = val.strip("[]'\"").split("'")[0].split('"')[0]
                # Clean numeric values - convert "123.0" to "123" for integer columns
                if val and val.endswith(".0") and val[:-2].replace("-", "").isdigit():
                    val = val[:-2]
                # Convert empty strings to None
                mapped_row[db_col] = val if val != "" else None
            rows.append(mapped_row)

    if not rows:
        log.warning(f"No data in {csv_path}")
        return 0

    db_columns = list(column_mapping.values())
    return _insert_rows(conn, table_name, db_columns, rows, upsert, log)


def _insert_rows(
    conn,
    table_name: str,
    columns: list[str],
    rows: list[dict[str, Any]],
    upsert: bool,
    log: logging.Logger | None = None,
) -> int:
    """Insert rows into table with optional upsert."""
    log = log or logger
    if not rows:
        return 0

    # Get primary key columns
    pk_columns = _get_primary_key(conn, table_name)

    # Build INSERT statement
    cols_sql = sql.SQL(", ").join(map(sql.Identifier, columns))
    placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(columns))

    if upsert and pk_columns:
        pk_sql = sql.SQL(", ").join(map(sql.Identifier, pk_columns))
        update_cols = [c for c in columns if c not in pk_columns]

        if update_cols:
            set_sql = sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                for c in update_cols
            )
            query = sql.SQL(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}"
            ).format(sql.Identifier(table_name), cols_sql, placeholders, pk_sql, set_sql)
        else:
            query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING").format(
                sql.Identifier(table_name), cols_sql, placeholders, pk_sql
            )
    else:
        query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table_name), cols_sql, placeholders
        )

    inserted = 0
    batch_size = DEFAULT_BATCH_SIZE

    errors = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            for row in batch:
                values = [row.get(col) for col in columns]
                try:
                    cur.execute(query, values)
                    inserted += 1
                except psycopg.Error as e:
                    conn.rollback()  # Rollback to recover from error
                    errors += 1
                    if errors <= 3:
                        log.warning(f"  Insert failed: {e}")
                    elif errors == 4:
                        log.warning("  (suppressing further errors...)")
            conn.commit()

            if (i + batch_size) % 5000 == 0:
                log.info(f"  Progress: {min(i + batch_size, len(rows))}/{len(rows)}")

    if errors > 0:
        log.warning(f"  {errors} rows failed to insert (check FK constraints)")

    log.info(f"  Loaded {inserted} rows into {table_name}")
    return inserted


def _get_primary_key(conn, table_name: str) -> list[str]:
    """Get primary key columns for table."""
    query = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_name = %s
        ORDER BY kcu.ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(query, (table_name,))
        return [row[0] for row in cur.fetchall()]


# Column mappings for each table
COMPANY_COLUMNS = {
    "ticker": "ticker",
    "name": "name",
    "description": "description",
    "market": "market",
    "type": "type",
    "locale": "locale",
    "currency_name": "currency_name",
    "active": "active",
    "list_date": "list_date",
    "primary_exchange": "primary_exchange",
    "cik": "cik",
    "sic_code": "sic_code",
    "sic_description": "sic_description",
    "market_cap": "market_cap",
    "weighted_shares_outstanding": "weighted_shares_outstanding",
    "total_employees": "total_employees",
    "homepage_url": "homepage_url",
    "phone_number": "phone_number",
    "address_address1": "address_address1",
    "address_city": "address_city",
    "address_state": "address_state",
    "address_postal_code": "address_postal_code",
}

PRICE_COLUMNS = {
    "symbol": "ticker",
    "date": "date",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
}

RATIO_COLUMNS = {
    "ticker": "ticker",
    "date": "date",
    "average_volume": "average_volume",
    "cash": "cash",
    "current": "current",
    "debt_to_equity": "debt_to_equity",
    "dividend_yield": "dividend_yield",
    "earnings_per_share": "earnings_per_share",
    "enterprise_value": "enterprise_value",
    "ev_to_ebitda": "ev_to_ebitda",
    "ev_to_sales": "ev_to_sales",
    "free_cash_flow": "free_cash_flow",
    "market_cap": "market_cap",
    "price": "price",
    "price_to_book": "price_to_book",
    "price_to_cash_flow": "price_to_cash_flow",
    "price_to_earnings": "price_to_earnings",
    "price_to_free_cash_flow": "price_to_free_cash_flow",
    "price_to_sales": "price_to_sales",
    "quick": "quick",
    "return_on_assets": "return_on_assets",
    "return_on_equity": "return_on_equity",
}


def _find_file(directory: Path, name: str) -> Path | None:
    """Find file case-insensitively, trying both hyphens and underscores."""
    if not directory.exists():
        return None

    # Try exact match first
    exact = directory / name
    if exact.exists():
        return exact

    # Try case-insensitive and hyphen/underscore variants
    name_lower = name.lower()
    name_variants = [
        name_lower,
        name_lower.replace("_", "-"),
        name_lower.replace("-", "_"),
        name.upper(),
        name.upper().replace("_", "-"),
    ]

    for f in directory.iterdir():
        if f.is_file() and (f.name.lower() in name_variants or f.name in name_variants):
            return f

    return None


def load_companies(conn, csv_path: Path, log: logging.Logger | None = None) -> int:
    """Load company overviews into companies table."""
    log = log or logger
    log.info("Loading companies...")

    # Try to find the file if exact path doesn't exist
    if not csv_path.exists():
        found = _find_file(csv_path.parent, csv_path.name)
        if found:
            csv_path = found
            log.info(f"  Found: {csv_path}")

    return load_csv_to_table(conn, csv_path, "companies", COMPANY_COLUMNS, log)


def load_prices(conn, prices_dir: Path, log: logging.Logger | None = None) -> int:
    """Load stock prices from per-symbol CSV files."""
    log = log or logger
    log.info("Loading stock prices...")

    if not prices_dir.exists():
        log.warning(f"Prices directory not found: {prices_dir}")
        return 0

    csv_files = list(prices_dir.glob("*.csv"))
    if not csv_files:
        log.warning("No price CSV files found")
        return 0

    total = 0
    for i, csv_file in enumerate(csv_files, 1):
        count = load_csv_to_table(conn, csv_file, "stock_prices", PRICE_COLUMNS, log, upsert=True)
        total += count
        if i % 50 == 0:
            log.info(f"  Processed {i}/{len(csv_files)} symbol files")

    log.info(f"  Total price records: {total}")
    return total


def load_ratios(conn, csv_path: Path, log: logging.Logger | None = None) -> int:
    """Load financial ratios."""
    log = log or logger
    log.info("Loading financial ratios...")

    # Try to find the file if exact path doesn't exist
    if not csv_path.exists():
        found = _find_file(csv_path.parent, csv_path.name)
        if found:
            csv_path = found
            log.info(f"  Found: {csv_path}")

    return load_csv_to_table(conn, csv_path, "financial_ratios", RATIO_COLUMNS, log)


def load_fundamentals(
    conn, fundamentals_dir: Path, log: logging.Logger | None = None
) -> dict[str, int]:
    """Load fundamentals (balance sheets, income statements, cash flows)."""
    log = log or logger
    log.info("Loading fundamentals...")
    stats: dict[str, int] = {}

    # Balance sheets
    bs_path = fundamentals_dir / "balance_sheets.csv"
    if bs_path.exists():
        stats["balance_sheets"] = _load_fundamentals_file(conn, bs_path, "balance_sheets", log)

    # Income statements
    is_path = fundamentals_dir / "income_statements.csv"
    if is_path.exists():
        stats["income_statements"] = _load_fundamentals_file(
            conn, is_path, "income_statements", log
        )

    # Cash flows
    cf_path = fundamentals_dir / "cash_flow.csv"
    if cf_path.exists():
        stats["cash_flows"] = _load_fundamentals_file(conn, cf_path, "cash_flows", log)

    return stats


def _load_fundamentals_file(
    conn, csv_path: Path, table_name: str, log: logging.Logger | None = None
) -> int:
    """Load a fundamentals CSV file, auto-mapping columns."""
    log = log or logger
    if not csv_path.exists():
        return 0

    # Read CSV headers and create identity mapping for matching columns
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        csv_headers = next(reader, [])

    # Get table columns
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = %s AND column_name != 'created_at'
        """,
            (table_name,),
        )
        db_columns = {row[0] for row in cur.fetchall()}

    # Map CSV columns to DB columns (identity mapping for matching names)
    column_mapping = {}
    for csv_col in csv_headers:
        # Normalize column name
        normalized = csv_col.lower().replace(" ", "_").replace("-", "_")
        if normalized in db_columns:
            column_mapping[csv_col] = normalized
        elif csv_col == "tickers":
            column_mapping[csv_col] = "ticker"

    log.info(f"  Loading {table_name} ({len(column_mapping)} columns mapped)...")
    return load_csv_to_table(conn, csv_path, table_name, column_mapping, log)


def load_economy(conn, economy_dir: Path, log: logging.Logger | None = None) -> dict[str, int]:
    """Load economy data tables."""
    log = log or logger
    log.info("Loading economy data...")
    stats: dict[str, int] = {}

    tables = {
        "treasury_yields": "treasury_yields.csv",
        "inflation": "inflation.csv",
        "inflation_expectations": "inflation_expectations.csv",
        "labor_market": "labor_market.csv",
    }

    for table_name, filename in tables.items():
        csv_path = economy_dir / filename
        if not csv_path.exists():
            # Try to find with different naming conventions
            found = _find_file(economy_dir, filename)
            if found:
                csv_path = found
                log.info(f"  Found: {csv_path.name}")

        if csv_path.exists():
            count = _load_economy_file(conn, csv_path, table_name, log)
            stats[table_name] = count
        else:
            log.debug(f"  {filename} not found")
            stats[table_name] = 0

    return stats


def _load_economy_file(
    conn, csv_path: Path, table_name: str, log: logging.Logger | None = None
) -> int:
    """Load an economy CSV file, auto-mapping columns."""
    log = log or logger
    if not csv_path.exists():
        return 0

    # Read CSV headers
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        csv_headers = next(reader, [])

    # Get table columns
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = %s AND column_name != 'created_at'
        """,
            (table_name,),
        )
        db_columns = {row[0] for row in cur.fetchall()}

    # Map CSV columns to DB columns
    column_mapping = {}
    for csv_col in csv_headers:
        normalized = csv_col.lower().replace(" ", "_").replace("-", "_")
        if normalized in db_columns:
            column_mapping[csv_col] = normalized

    log.info(f"  Loading {table_name}...")
    return load_csv_to_table(conn, csv_path, table_name, column_mapping, log)


def load_news(
    conn,
    client: PolygonClient,
    symbols: list[str],
    days: int = DEFAULT_NEWS_DAYS,
    limit_per_symbol: int = DEFAULT_NEWS_LIMIT_PER_SYMBOL,
    log: logging.Logger | None = None,
) -> int:
    """
    Load news articles for symbols directly from API into database.

    Args:
        conn: Database connection
        client: Polygon API client
        symbols: List of ticker symbols
        days: Days of news history to fetch
        limit_per_symbol: Max articles per symbol
        log: Logger instance

    Returns:
        Total number of articles loaded
    """
    from sawa.database.news import fetch_news_for_symbols

    log = log or logger
    log.info(f"Loading news for {len(symbols)} symbols (last {days} days)...")

    total = fetch_news_for_symbols(
        conn,
        client,
        symbols,
        days=days,
        limit_per_symbol=limit_per_symbol,
        log=log,
    )

    log.info(f"  Total news articles loaded: {total}")
    return total
