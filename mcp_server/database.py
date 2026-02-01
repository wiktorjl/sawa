"""Database connection and query execution for the MCP server."""

import logging
import os
import re
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row

logger = logging.getLogger(__name__)

# Configuration
MAX_ROWS = int(os.environ.get("MCP_MAX_ROWS", "1000"))
QUERY_TIMEOUT = int(os.environ.get("MCP_QUERY_TIMEOUT", "30"))


def get_database_url() -> str:
    """Get database connection string from environment."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise ValueError(
            "DATABASE_URL environment variable is required. "
            "Example: postgresql://user:pass@host:port/database"
        )
    return database_url


@contextmanager
def get_connection():
    """Get a database connection as a context manager."""
    conn = None
    try:
        conn = psycopg.connect(get_database_url(), row_factory=dict_row)
        yield conn
    finally:
        if conn:
            conn.close()


def validate_select_query(sql: str) -> bool:
    """
    Validate that a SQL query is a safe SELECT statement.

    Checks:
    - Must start with SELECT
    - No DDL commands (CREATE, DROP, ALTER, etc.)
    - No DML commands (INSERT, UPDATE, DELETE, etc.)

    Args:
        sql: SQL query string

    Returns:
        True if valid SELECT query

    Raises:
        ValueError: If query is not a valid SELECT statement
    """
    # Normalize the query
    normalized = sql.strip().upper()

    # Must start with SELECT
    if not normalized.startswith("SELECT"):
        raise ValueError("Only SELECT queries are allowed")

    # Check for forbidden keywords
    forbidden_patterns = [
        r"\bINSERT\b",
        r"\bUPDATE\b",
        r"\bDELETE\b",
        r"\bDROP\b",
        r"\bCREATE\b",
        r"\bALTER\b",
        r"\bTRUNCATE\b",
        r"\bGRANT\b",
        r"\bREVOKE\b",
        r"\bCOPY\b",
        r";\s*\w+",  # Multiple statements
    ]

    for pattern in forbidden_patterns:
        if re.search(pattern, normalized):
            raise ValueError(f"Query contains forbidden SQL pattern: {pattern}")

    return True


def execute_query(
    sql: str,
    params: dict[str, Any] | None = None,
    validate: bool = True,
) -> list[dict[str, Any]]:
    """
    Execute a SQL query and return results.

    Args:
        sql: SQL query string
        params: Optional query parameters for safe interpolation
        validate: Whether to validate query is SELECT-only

    Returns:
        List of result rows as dictionaries

    Raises:
        ValueError: If query validation fails
        psycopg.Error: If database error occurs
    """
    if validate:
        validate_select_query(sql)

    # Add LIMIT if not present and query is a SELECT
    sql_upper = sql.strip().upper()
    if sql_upper.startswith("SELECT") and "LIMIT" not in sql_upper:
        sql = f"{sql.rstrip(';')} LIMIT {MAX_ROWS}"

    with get_connection() as conn:
        with conn.cursor() as cur:
            # Set query timeout
            cur.execute(f"SET statement_timeout = '{QUERY_TIMEOUT}s'")

            logger.debug(f"Executing query: {sql[:100]}...")
            cur.execute(sql, params or {})

            results = cur.fetchall()
            logger.debug(f"Query returned {len(results)} rows")

            return results
