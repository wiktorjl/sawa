"""Database connection and query utilities for the TUI."""

import logging
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row

from sawa_tui.config import get_database_url

logger = logging.getLogger(__name__)


@contextmanager
def get_connection() -> Generator[psycopg.Connection, None, None]:
    """
    Get a database connection as a context manager.

    Yields:
        psycopg.Connection: Database connection configured for dict rows
    """
    conn = psycopg.connect(get_database_url(), row_factory=dict_row)
    try:
        yield conn
    finally:
        conn.close()


def execute_query(
    sql: str,
    params: dict[str, Any] | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """
    Execute a SELECT query and return results as list of dicts.

    Args:
        sql: SQL query string (can use %(name)s placeholders)
        params: Query parameters
        limit: Optional row limit to append

    Returns:
        List of row dictionaries
    """
    if limit is not None and "LIMIT" not in sql.upper():
        sql = f"{sql} LIMIT {limit}"

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]


def execute_write(
    sql: str,
    params: dict[str, Any] | None = None,
) -> int:
    """
    Execute an INSERT/UPDATE/DELETE query.

    Args:
        sql: SQL query string
        params: Query parameters

    Returns:
        Number of affected rows
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rowcount = cur.rowcount
            conn.commit()
            return rowcount


def execute_write_returning(
    sql: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """
    Execute an INSERT/UPDATE with RETURNING clause.

    Args:
        sql: SQL query string with RETURNING clause
        params: Query parameters

    Returns:
        Returned row as dict, or None if no row returned
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            result = cur.fetchone()
            conn.commit()
            return dict(result) if result else None


def init_schema(schema_dir: str = "sqlschema") -> None:
    """
    Initialize the TUI-specific schema (watchlists, glossary, users).

    Executes schema files: 08_watchlists.sql, 09_glossary.sql, 10_news.sql, 11_users.sql

    Args:
        schema_dir: Directory containing SQL schema files (default: sqlschema)
    """
    from pathlib import Path

    # Get project root (assuming this file is in tui/sawa_tui/)
    project_root = Path(__file__).parent.parent.parent
    schema_path = project_root / schema_dir

    # Schema files to execute in order
    schema_files = [
        "08_watchlists.sql",
        "09_glossary.sql",
        "10_news.sql",
        "11_users.sql",
    ]

    with get_connection() as conn:
        with conn.cursor() as cur:
            for schema_file in schema_files:
                file_path = schema_path / schema_file
                if file_path.exists():
                    logger.info(f"Executing schema file: {schema_file}")
                    with open(file_path) as f:
                        sql = f.read()
                        cur.execute(sql)
                else:
                    logger.warning(f"Schema file not found: {file_path}")
            conn.commit()

    logger.info("TUI schema initialized")
