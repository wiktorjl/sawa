"""Database connection and query execution for the MCP server."""

import logging
import os
import re
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from sawa.utils.config import require_database_url as get_database_url  # noqa: F401

logger = logging.getLogger(__name__)

# Configuration
MAX_ROWS = int(os.environ.get("MCP_MAX_ROWS", "1000"))
QUERY_TIMEOUT = int(os.environ.get("MCP_QUERY_TIMEOUT", "30"))

# Query audit log
QUERY_LOG_DIR = Path(os.environ.get("MCP_QUERY_LOG_DIR", "logs"))
QUERY_LOG_FILE = QUERY_LOG_DIR / "execute_query.log"


def _ensure_log_dir() -> None:
    """Ensure log directory exists."""
    QUERY_LOG_DIR.mkdir(parents=True, exist_ok=True)


def log_execute_query(query: str, params: dict[str, Any] | None = None) -> None:
    """
    Log execute_query usage to file for audit/review and console.

    Args:
        query: SQL query string
        params: Optional query parameters
    """
    _ensure_log_dir()
    timestamp = datetime.now().isoformat()

    # Log to file
    with open(QUERY_LOG_FILE, "a") as f:
        f.write(f"[{timestamp}] QUERY: {query}\n")
        if params:
            f.write(f"[{timestamp}] PARAMS: {params}\n")
        f.write("\n")

    # Log to console via logger (goes to stderr)
    logger.info(f"[QUERY] {query}")
    if params:
        logger.info(f"[QUERY PARAMS] {params}")


@contextmanager
def get_connection():
    """
    Get a database connection as a context manager.

    Security: Sets read-only mode to prevent accidental modifications.
    """
    conn = None
    try:
        conn = psycopg.connect(get_database_url(), row_factory=dict_row)  # type: ignore[arg-type]
        # Set read-only mode for security
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SET default_transaction_read_only = on"))
        yield conn
    finally:
        if conn:
            conn.close()


def validate_select_query(query: str) -> bool:
    """
    Validate that a SQL query is a safe SELECT statement.

    Checks:
    - Must start with SELECT or WITH (CTE)
    - No DDL commands (CREATE, DROP, ALTER, etc.)
    - No DML commands (INSERT, UPDATE, DELETE, etc.)

    Args:
        query: SQL query string

    Returns:
        True if valid SELECT query

    Raises:
        ValueError: If query is not a valid SELECT statement
    """
    # Normalize the query
    normalized = query.strip().upper()

    # Must start with SELECT or WITH (CTE)
    if not (normalized.startswith("SELECT") or normalized.startswith("WITH")):
        raise ValueError("Only SELECT queries are allowed (WITH/CTE supported)")

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
    query: str,
    params: dict[str, Any] | None = None,
    validate: bool = True,
) -> list[dict[str, Any]]:
    """
    Execute a SQL query and return results.

    Args:
        query: SQL query string
        params: Optional query parameters for safe interpolation
        validate: Whether to validate query is SELECT-only

    Returns:
        List of result rows as dictionaries

    Raises:
        ValueError: If query validation fails
        psycopg.Error: If database error occurs
    """
    if validate:
        validate_select_query(query)

    # Add LIMIT if not present and query is a SELECT
    query_upper = query.strip().upper()
    if query_upper.startswith("SELECT") and "LIMIT" not in query_upper:
        query = f"{query.rstrip(';')} LIMIT {MAX_ROWS}"

    with get_connection() as conn:
        with conn.cursor() as cur:
            # Set query timeout using SQL module for safety
            timeout_sql = sql.SQL("SET statement_timeout = {}").format(
                sql.Literal(f"{QUERY_TIMEOUT}s")
            )
            cur.execute(timeout_sql)

            logger.debug(f"Executing query: {query[:100]}...")

            # Execute with parameters if provided
            # Use sql.SQL for the query to satisfy type checker
            query_sql = sql.SQL(query)  # type: ignore[arg-type]
            if params:
                cur.execute(query_sql, params)
            else:
                cur.execute(query_sql)

            results = cur.fetchall()
            logger.debug(f"Query returned {len(results)} rows")

            # Convert to list of dicts explicitly
            return [dict(row) for row in results]
