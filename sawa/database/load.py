"""
High-level data loading functions for coldstart workflow.

Loads downloaded CSV data into PostgreSQL tables.
"""

from __future__ import annotations

import csv
import logging
import re
from collections.abc import Callable
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from sawa.api.client import PolygonClient
from sawa.database.news import NewsLoadResult, fetch_news_for_symbols
from sawa.domain.corporate_actions import SplitAdjuster
from sawa.domain.price_validation import (
    is_plausible_daily_price_date,
    is_valid_daily_ohlcv,
)
from sawa.utils.constants import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_NEWS_DAYS,
    DEFAULT_NEWS_LIMIT_PER_SYMBOL,
)

logger = logging.getLogger(__name__)

_STRICT_ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
_MAX_PROVIDER_FUTURE_DAYS = 1
_MAX_MARKET_INTERNAL_VALUE = Decimal("9999.9999")
_MARKET_INTERNAL_VALUE_COLUMNS = (
    "vix",
    "vix3m",
    "hy_spread",
    "put_call_ratio",
)


class PersistenceResult(int):
    """Integer-compatible row count with artifact persistence provenance."""

    table: str
    artifact_found: bool
    source_rows: int
    eligible_rows: int
    skipped_rows: int

    def __new__(
        cls,
        inserted: int,
        *,
        table: str,
        artifact_found: bool,
        source_rows: int,
        eligible_rows: int,
        skipped_rows: int = 0,
    ) -> PersistenceResult:
        result = super().__new__(cls, inserted)
        result.table = table
        result.artifact_found = artifact_found
        result.source_rows = source_rows
        result.eligible_rows = eligible_rows
        result.skipped_rows = skipped_rows
        return result

    @property
    def inserted_rows(self) -> int:
        return int(self)

    @property
    def failed_rows(self) -> int:
        return max(0, self.eligible_rows - int(self))

    @property
    def fully_persisted(self) -> bool:
        return (
            self.artifact_found
            and self.failed_rows == 0
            and self.skipped_rows == 0
        )

    def summary(self) -> dict[str, str | int | bool]:
        """Return serialization-safe persistence counters for job stats."""
        return {
            "table": self.table,
            "artifact_found": self.artifact_found,
            "source_rows": self.source_rows,
            "eligible_rows": self.eligible_rows,
            "inserted_rows": int(self),
            "skipped_rows": self.skipped_rows,
            "failed_rows": self.failed_rows,
            "fully_persisted": self.fully_persisted,
        }


def require_complete_persistence(
    result: PersistenceResult,
    *,
    expected_rows: int | None = None,
    require_nonempty: bool = False,
) -> None:
    """Raise when an artifact did not persist exactly as represented."""
    if not result.artifact_found:
        raise RuntimeError(f"Required {result.table} artifact was not found")
    if expected_rows is not None and result.source_rows != expected_rows:
        raise RuntimeError(
            f"{result.table} artifact contains {result.source_rows} row(s); "
            f"expected {expected_rows} freshly downloaded row(s)"
        )
    if result.skipped_rows or result.failed_rows:
        raise RuntimeError(
            f"{result.table} persisted {int(result)}/{result.source_rows} source row(s)"
        )
    if expected_rows is not None and int(result) != expected_rows:
        raise RuntimeError(
            f"{result.table} persisted {int(result)}/{expected_rows} expected row(s)"
        )
    if require_nonempty and (result.source_rows == 0 or int(result) == 0):
        raise RuntimeError(f"{result.table} artifact persisted no rows")


def _is_plausible_market_internal_date(value: object) -> bool:
    """Return whether a storage date is real, strict, and not far-future."""
    if type(value) is date:
        parsed = value
    elif isinstance(value, str) and _STRICT_ISO_DATE.fullmatch(value) is not None:
        try:
            parsed = date.fromisoformat(value)
        except ValueError:
            return False
    else:
        # In particular, datetime is deliberately rejected: provider rows use
        # an unambiguous calendar date and must not be silently truncated.
        return False

    return parsed <= date.today() + timedelta(days=_MAX_PROVIDER_FUTURE_DAYS)


def _is_plausible_market_internal_value(value: object) -> bool:
    """Protect NUMERIC(8,4) columns from non-finite or semantic junk."""
    if value in (None, ""):
        return True
    if isinstance(value, bool):
        return False
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return False
    return (
        parsed.is_finite()
        and parsed >= 0
        and parsed <= _MAX_MARKET_INTERNAL_VALUE
    )


def _is_plausible_market_internal_row(row: dict[str, Any]) -> bool:
    """Validate the complete storage boundary for a market-internals row."""
    values = [row.get(column) for column in _MARKET_INTERNAL_VALUE_COLUMNS]
    return (
        _is_plausible_market_internal_date(row.get("date"))
        and any(value not in (None, "") for value in values)
        and all(_is_plausible_market_internal_value(value) for value in values)
    )


def load_csv_to_table(
    conn,
    csv_path: Path,
    table_name: str,
    column_mapping: dict[str, str],
    log: logging.Logger | None = None,
    upsert: bool = True,
    valid_tickers: set[str] | None = None,
    strict: bool = False,
    row_transform: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> PersistenceResult:
    """
    Load CSV file into PostgreSQL table.

    Args:
        conn: Database connection
        csv_path: Path to CSV file
        table_name: Target table name
        column_mapping: Dict mapping CSV columns to DB columns
        log: Logger instance
        upsert: Use ON CONFLICT DO UPDATE
        valid_tickers: Optional set of valid ticker symbols. If provided,
            rows with tickers not in this set will be skipped.
        strict: Atomically require every source row to be eligible and written.
        row_transform: Applied to each eligible row (keyed by DB column) after
            mapping and ticker filtering, before the write — e.g. re-basing
            as-traded bars onto the split-adjusted basis.

    Returns:
        Number of rows loaded
    """
    log = log or logger
    if not csv_path.exists():
        log.warning(f"CSV not found: {csv_path}")
        return PersistenceResult(
            0,
            table=table_name,
            artifact_found=False,
            source_rows=0,
            eligible_rows=0,
        )

    # Read CSV data
    rows: list[dict[str, Any]] = []
    skipped_tickers: set[str] = set()
    source_rows = 0
    skipped_rows = 0
    rejected_market_internal_rows = 0

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            source_rows += 1
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

            if (
                table_name == "market_internals"
                and not _is_plausible_market_internal_row(mapped_row)
            ):
                rejected_market_internal_rows += 1
                continue

            # Filter by valid_tickers if provided
            if valid_tickers is not None and "ticker" in mapped_row:
                ticker = mapped_row["ticker"]
                if ticker and ticker.upper() not in valid_tickers:
                    skipped_tickers.add(ticker)
                    skipped_rows += 1
                    continue

            if row_transform is not None:
                mapped_row = row_transform(mapped_row)
            rows.append(mapped_row)

    # Log skipped tickers summary
    if skipped_tickers:
        sample = sorted(skipped_tickers)[:10]
        suffix = f" (and {len(skipped_tickers) - 10} more)" if len(skipped_tickers) > 10 else ""
        log.info(f"  Skipped {len(skipped_tickers)} unknown tickers: {', '.join(sample)}{suffix}")

    if rejected_market_internal_rows:
        log.warning(
            f"  Rejected {rejected_market_internal_rows} market_internals row(s) with "
            "malformed dates or invalid values"
        )
        raise ValueError(
            f"Rejected {rejected_market_internal_rows} invalid market_internals row(s)"
        )

    effective_strict = strict or table_name == "market_internals"
    if effective_strict and skipped_rows:
        raise ValueError(
            f"Rejected {skipped_rows}/{source_rows} {table_name} source row(s) "
            "during eligibility filtering"
        )

    if not rows:
        log.warning(f"No data in {csv_path}")
        return PersistenceResult(
            0,
            table=table_name,
            artifact_found=True,
            source_rows=source_rows,
            eligible_rows=0,
            skipped_rows=skipped_rows,
        )

    db_columns = list(column_mapping.values())
    inserted = _insert_rows(
        conn,
        table_name,
        db_columns,
        rows,
        upsert,
        log,
        strict=effective_strict,
    )
    if effective_strict and inserted != len(rows):
        raise RuntimeError(
            f"Persisted only {inserted}/{len(rows)} {table_name} row(s)"
        )
    return PersistenceResult(
        inserted,
        table=table_name,
        artifact_found=True,
        source_rows=source_rows,
        eligible_rows=len(rows),
        skipped_rows=skipped_rows,
    )


def _insert_rows(
    conn,
    table_name: str,
    columns: list[str],
    rows: list[dict[str, Any]],
    upsert: bool,
    log: logging.Logger | None = None,
    coalesce_columns: list[str] | None = None,
    strict: bool = False,
) -> int:
    """Insert rows into table with optional upsert.

    Args:
        coalesce_columns: Columns whose upsert uses
            ``col = COALESCE(EXCLUDED.col, table.col)`` so an incoming NULL keeps
            the stored value instead of clobbering it. Use for series that are
            never legitimately retracted to NULL (e.g. market_internals), so a
            transient upstream gap cannot erase good history.
        strict: Roll back and raise on the first failed row, and commit only
            after every row succeeds. Intended for small integrity-sensitive
            batches whose callers must never mistake partial persistence for a
            clean run.
    """
    log = log or logger
    if not rows:
        return 0

    coalesce_set = set(coalesce_columns or [])

    # Get primary key columns
    pk_columns = _get_primary_key(conn, table_name)

    # Build INSERT statement
    cols_sql = sql.SQL(", ").join(map(sql.Identifier, columns))
    placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(columns))

    if upsert and pk_columns:
        pk_sql = sql.SQL(", ").join(map(sql.Identifier, pk_columns))
        update_cols = [c for c in columns if c not in pk_columns]

        def _set_clause(c: str) -> sql.Composed:
            if c in coalesce_set:
                return sql.SQL("{col} = COALESCE(EXCLUDED.{col}, {tbl}.{col})").format(
                    col=sql.Identifier(c), tbl=sql.Identifier(table_name)
                )
            return sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))

        if update_cols:
            set_sql = sql.SQL(", ").join(_set_clause(c) for c in update_cols)
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
                    cur.execute("SAVEPOINT row_insert")
                    cur.execute(query, values)
                    cur.execute("RELEASE SAVEPOINT row_insert")
                    inserted += 1
                except psycopg.Error as e:
                    cur.execute("ROLLBACK TO SAVEPOINT row_insert")
                    cur.execute("RELEASE SAVEPOINT row_insert")
                    errors += 1
                    if strict:
                        conn.rollback()
                        raise RuntimeError(
                            f"Atomic insert into {table_name} failed"
                        ) from e
                    if errors <= 3:
                        log.warning(f"  Insert failed: {e}")
                    elif errors == 4:
                        log.warning("  (suppressing further errors...)")
            if not strict:
                conn.commit()

            if (i + batch_size) % 5000 == 0:
                log.info(f"  Progress: {min(i + batch_size, len(rows))}/{len(rows)}")

    if strict:
        conn.commit()
    elif errors > 0:
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


def load_companies(
    conn, csv_path: Path, log: logging.Logger | None = None
) -> PersistenceResult:
    """Load company overviews into companies table."""
    log = log or logger
    log.info("Loading companies...")

    # Try to find the file if exact path doesn't exist
    if not csv_path.exists():
        found = _find_file(csv_path.parent, csv_path.name)
        if found:
            csv_path = found
            log.info(f"  Found: {csv_path}")

    return load_csv_to_table(
        conn,
        csv_path,
        "companies",
        COMPANY_COLUMNS,
        log,
        strict=True,
    )


def load_prices(
    conn,
    prices_dir: Path,
    log: logging.Logger | None = None,
    *,
    split_adjuster: SplitAdjuster | None = None,
) -> PersistenceResult:
    """Load stock prices from per-symbol CSV files.

    The CSV artifacts hold as-traded flat-file bars, while ``stock_prices`` is
    maintained on Polygon's split-adjusted basis by every later REST write.
    Pass ``split_adjuster`` so each bar is re-based as it is written; loading
    without one stores a ticker that split inside the window on two bases.
    """
    log = log or logger
    log.info("Loading stock prices...")
    if split_adjuster is not None:
        log.info(
            f"  Re-basing as-traded bars with {len(split_adjuster)} recorded split(s)"
        )

    if not prices_dir.exists():
        log.warning(f"Prices directory not found: {prices_dir}")
        return PersistenceResult(
            0,
            table="stock_prices",
            artifact_found=False,
            source_rows=0,
            eligible_rows=0,
        )

    csv_files = sorted(prices_dir.glob("*.csv"))
    if not csv_files:
        log.warning("No price CSV files found")
        return PersistenceResult(
            0,
            table="stock_prices",
            artifact_found=False,
            source_rows=0,
            eligible_rows=0,
        )

    # Validate the complete cache before the first database call. A malformed
    # later symbol file must not be discovered only after earlier files were
    # committed, which would turn a rejected cache into a partial ingestion.
    required_columns = set(PRICE_COLUMNS)
    for csv_file in csv_files:
        source_rows = 0
        with open(csv_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = set(reader.fieldnames or [])
            missing_columns = sorted(required_columns - fieldnames)
            if missing_columns:
                raise ValueError(
                    f"Price CSV {csv_file.name} is missing required column(s): "
                    + ", ".join(missing_columns)
                )
            for row_number, row in enumerate(reader, start=2):
                source_rows += 1
                if not is_plausible_daily_price_date(row.get("date")):
                    raise ValueError(
                        "Rejected malformed or future stock_prices date in "
                        f"{csv_file.name} at CSV row {row_number}"
                    )
                if not is_valid_daily_ohlcv(row, allow_numeric_strings=True):
                    raise ValueError(
                        "Rejected malformed stock_prices OHLCV row in "
                        f"{csv_file.name} at CSV row {row_number}"
                    )
        if source_rows == 0:
            raise RuntimeError("stock_prices artifact persisted no rows")

    total = 0
    source_rows = 0
    eligible_rows = 0
    for i, csv_file in enumerate(csv_files, 1):
        count = load_csv_to_table(
            conn,
            csv_file,
            "stock_prices",
            PRICE_COLUMNS,
            log,
            upsert=True,
            strict=True,
            row_transform=(
                split_adjuster.adjust_row if split_adjuster is not None else None
            ),
        )
        require_complete_persistence(count, require_nonempty=True)
        total += count
        source_rows += count.source_rows
        eligible_rows += count.eligible_rows
        if i % 50 == 0:
            log.info(f"  Processed {i}/{len(csv_files)} symbol files")

    log.info(f"  Total price records: {total}")
    return PersistenceResult(
        total,
        table="stock_prices",
        artifact_found=True,
        source_rows=source_rows,
        eligible_rows=eligible_rows,
    )


def load_ratios(
    conn,
    csv_path: Path,
    log: logging.Logger | None = None,
    valid_tickers: set[str] | None = None,
) -> PersistenceResult:
    """Load financial ratios.

    Args:
        conn: Database connection
        csv_path: Path to ratios CSV file
        log: Logger instance
        valid_tickers: Optional set of valid ticker symbols to filter by

    Returns:
        Number of rows loaded
    """
    log = log or logger
    log.info("Loading financial ratios...")

    # Try to find the file if exact path doesn't exist
    if not csv_path.exists():
        found = _find_file(csv_path.parent, csv_path.name)
        if found:
            csv_path = found
            log.info(f"  Found: {csv_path}")

    return load_csv_to_table(
        conn,
        csv_path,
        "financial_ratios",
        RATIO_COLUMNS,
        log,
        valid_tickers=valid_tickers,
        strict=True,
    )


def load_fundamentals(
    conn,
    fundamentals_dir: Path,
    log: logging.Logger | None = None,
    valid_tickers: set[str] | None = None,
    only_tables: set[str] | None = None,
) -> dict[str, PersistenceResult]:
    """Load fundamentals (balance sheets, income statements, cash flows).

    Args:
        conn: Database connection
        fundamentals_dir: Directory containing fundamentals CSV files
        log: Logger instance
        valid_tickers: Optional set of valid ticker symbols to filter by
        only_tables: Optional table allowlist. This is used by download-and-load
            runs to avoid consuming an older sibling CSV that was not refreshed
            during the current invocation. ``None`` keeps load-only behavior.

    Returns:
        Dict with count of rows loaded per table
    """
    log = log or logger
    log.info("Loading fundamentals...")
    stats: dict[str, PersistenceResult] = {}

    # Balance sheets
    bs_path = fundamentals_dir / "balance_sheets.csv"
    if (only_tables is None or "balance_sheets" in only_tables) and bs_path.exists():
        stats["balance_sheets"] = _load_fundamentals_file(
            conn, bs_path, "balance_sheets", log, valid_tickers
        )

    # Income statements
    is_path = fundamentals_dir / "income_statements.csv"
    if (only_tables is None or "income_statements" in only_tables) and is_path.exists():
        stats["income_statements"] = _load_fundamentals_file(
            conn, is_path, "income_statements", log, valid_tickers
        )

    # Cash flows
    cf_path = fundamentals_dir / "cash_flow.csv"
    if (only_tables is None or "cash_flows" in only_tables) and cf_path.exists():
        stats["cash_flows"] = _load_fundamentals_file(
            conn, cf_path, "cash_flows", log, valid_tickers
        )

    return stats


def _load_fundamentals_file(
    conn,
    csv_path: Path,
    table_name: str,
    log: logging.Logger | None = None,
    valid_tickers: set[str] | None = None,
) -> PersistenceResult:
    """Load a fundamentals CSV file, auto-mapping columns.

    Args:
        conn: Database connection
        csv_path: Path to CSV file
        table_name: Target table name
        log: Logger instance
        valid_tickers: Optional set of valid ticker symbols to filter by

    Returns:
        Number of rows loaded
    """
    log = log or logger
    if not csv_path.exists():
        return PersistenceResult(
            0,
            table=table_name,
            artifact_found=False,
            source_rows=0,
            eligible_rows=0,
        )

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
    return load_csv_to_table(
        conn,
        csv_path,
        table_name,
        column_mapping,
        log,
        valid_tickers=valid_tickers,
        strict=True,
    )


def load_economy(
    conn,
    economy_dir: Path,
    log: logging.Logger | None = None,
    only_tables: set[str] | None = None,
) -> dict[str, int | PersistenceResult]:
    """Load economy data tables."""
    log = log or logger
    log.info("Loading economy data...")
    stats: dict[str, int | PersistenceResult] = {}

    tables = {
        "treasury_yields": "treasury_yields.csv",
        "inflation": "inflation.csv",
        "inflation_expectations": "inflation_expectations.csv",
        "labor_market": "labor_market.csv",
        "market_internals": "market_internals.csv",
    }

    for table_name, filename in tables.items():
        if only_tables is not None and table_name not in only_tables:
            continue
        csv_path = economy_dir / filename
        if not csv_path.exists():
            # Try to find with different naming conventions
            found = _find_file(economy_dir, filename)
            if found:
                csv_path = found
                log.info(f"  Found: {csv_path.name}")

        if csv_path.exists():
            if table_name == "market_internals":
                # Partial FRED artifacts intentionally contain blanks for a
                # failed series. Replay them through the direct loader so its
                # COALESCE upsert preserves an existing non-NULL sibling value.
                count = load_market_internals_csv(conn, csv_path, log)
            else:
                count = _load_economy_file(conn, csv_path, table_name, log)
            stats[table_name] = count
        else:
            log.debug(f"  {filename} not found")
            stats[table_name] = PersistenceResult(
                0,
                table=table_name,
                artifact_found=False,
                source_rows=0,
                eligible_rows=0,
            )

    return stats


def _load_economy_file(
    conn, csv_path: Path, table_name: str, log: logging.Logger | None = None
) -> PersistenceResult:
    """Load an economy CSV file, auto-mapping columns."""
    log = log or logger
    if not csv_path.exists():
        return PersistenceResult(
            0,
            table=table_name,
            artifact_found=False,
            source_rows=0,
            eligible_rows=0,
        )

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
    return load_csv_to_table(
        conn,
        csv_path,
        table_name,
        column_mapping,
        log,
        strict=True,
    )


def load_market_internals(
    conn,
    rows: list[dict],
    log: logging.Logger | None = None,
) -> int:
    """
    Load market internals data directly from FRED API response into database.

    Args:
        conn: Database connection
        rows: List of dicts with keys: date, vix, vix3m, hy_spread, etc.
        log: Logger instance

    Returns:
        Number of rows loaded
    """
    log = log or logger
    if not rows:
        log.warning("No market internals data to load")
        return 0

    log.info(f"Loading {len(rows)} market internals rows...")

    columns = ["date", "vix", "vix3m", "hy_spread", "put_call_ratio"]
    db_rows: list[dict[str, Any]] = []
    rejected_rows = 0
    for row in rows:
        if not isinstance(row, dict) or not _is_plausible_market_internal_row(row):
            rejected_rows += 1
            continue
        db_row = {}
        for col in columns:
            val = row.get(col)
            db_row[col] = val if val not in ("", None) else None
        db_rows.append(db_row)

    if rejected_rows:
        log.warning(
            f"Rejected {rejected_rows} market internals row(s) with "
            "malformed dates or invalid values"
        )
        raise ValueError(f"Rejected {rejected_rows} invalid market internals row(s)")
    if not db_rows:
        log.warning("No valid market internals data to load")
        return 0

    # COALESCE value columns: a NULL from a transient FRED-series failure (or
    # the never-populated put_call_ratio) must not overwrite good stored data.
    inserted = _insert_rows(
        conn,
        "market_internals",
        columns,
        db_rows,
        upsert=True,
        log=log,
        coalesce_columns=["vix", "vix3m", "hy_spread", "put_call_ratio"],
        strict=True,
    )
    if inserted != len(db_rows):
        raise RuntimeError(
            f"Persisted only {inserted}/{len(db_rows)} market internals row(s)"
        )
    return inserted


def load_market_internals_csv(
    conn,
    csv_path: Path,
    log: logging.Logger | None = None,
) -> int:
    """Load a market-internals CSV with strict, NULL-preserving semantics."""
    log = log or logger
    log.info("Loading market internals from CSV...")
    if not csv_path.exists():
        log.warning(f"CSV not found: {csv_path}")
        return 0

    allowed_columns = {"date", *_MARKET_INTERNAL_VALUE_COLUMNS}
    rows: list[dict[str, Any]] = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for csv_row in reader:
            normalized_row = {
                key.lower().replace(" ", "_").replace("-", "_"): value
                for key, value in csv_row.items()
                if key is not None
            }
            rows.append(
                {
                    column: normalized_row.get(column)
                    for column in allowed_columns
                }
            )

    return load_market_internals(conn, rows, log)


def load_news(
    conn,
    client: PolygonClient,
    symbols: list[str],
    days: int = DEFAULT_NEWS_DAYS,
    limit_per_symbol: int = DEFAULT_NEWS_LIMIT_PER_SYMBOL,
    log: logging.Logger | None = None,
) -> NewsLoadResult:
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
        Total number of distinct articles loaded across all symbols
    """
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
