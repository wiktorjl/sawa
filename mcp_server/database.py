"""Database connection and query execution for the MCP server."""

import atexit
import json
import logging
import os
import re
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from sawa.utils.config import require_database_url as get_database_url  # noqa: F401

logger = logging.getLogger(__name__)

# Configuration
MAX_ROWS = int(os.environ.get("MCP_MAX_ROWS", "1000"))
QUERY_TIMEOUT = int(os.environ.get("MCP_QUERY_TIMEOUT", "30"))

# Pool configuration
POOL_MIN_SIZE = int(os.environ.get("MCP_POOL_MIN_SIZE", "2"))
POOL_MAX_SIZE = int(os.environ.get("MCP_POOL_MAX_SIZE", "10"))

# Module-level pool singleton
_pool: ConnectionPool | None = None


def _configure_connection(conn: psycopg.Connection) -> None:
    """Configure a connection after it is created by the pool."""
    conn.row_factory = dict_row  # type: ignore[assignment]
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SET default_transaction_read_only = on"))
    conn.commit()


def _get_pool() -> ConnectionPool:
    """Get or create the module-level connection pool (lazy init)."""
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            conninfo=get_database_url(),  # type: ignore[arg-type]
            min_size=POOL_MIN_SIZE,
            max_size=POOL_MAX_SIZE,
            configure=_configure_connection,
            open=True,
            timeout=60,
            max_idle=300,
            max_lifetime=1800,
        )
        atexit.register(close_pool)
        logger.info(
            "Connection pool created (min_size=%d, max_size=%d)",
            POOL_MIN_SIZE,
            POOL_MAX_SIZE,
        )
    return _pool


def close_pool() -> None:
    """Close the connection pool. Safe to call multiple times."""
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None
        logger.info("Connection pool closed")


# Query audit log
_DEFAULT_QUERY_LOG_DIR = Path.home() / ".sawa" / "logs"
QUERY_LOG_DIR = Path(os.environ.get("MCP_QUERY_LOG_DIR") or _DEFAULT_QUERY_LOG_DIR)
QUERY_LOG_FILE = QUERY_LOG_DIR / "execute_query.log"
QUERY_LOG_JSONL_FILE = QUERY_LOG_DIR / "execute_query.jsonl"

# Append-mode audit logs have no built-in rotation, so cap each sink and roll
# it to "<name>.1" once it grows past this size. A single ".1" backup keeps
# disk usage bounded without bringing in a full logging handler.
QUERY_LOG_MAX_BYTES = int(os.environ.get("MCP_QUERY_LOG_MAX_BYTES", str(5 * 1024 * 1024)))


def _ensure_log_dir() -> None:
    """Ensure log directory exists."""
    QUERY_LOG_DIR.mkdir(parents=True, exist_ok=True)


def _rotate_if_needed(path: Path) -> None:
    """Rotate ``path`` to ``path.1`` when it exceeds ``QUERY_LOG_MAX_BYTES``.

    Keeps a single backup; the previous ``.1`` is overwritten. Best-effort:
    any OS error is swallowed so auditing never blocks query execution.
    """
    if QUERY_LOG_MAX_BYTES <= 0:
        return
    try:
        if path.exists() and path.stat().st_size >= QUERY_LOG_MAX_BYTES:
            path.replace(path.with_name(path.name + ".1"))
    except OSError as e:
        logger.warning("Failed to rotate audit log %s: %s", path, e)


def log_execute_query(query: str, params: dict[str, Any] | None = None) -> None:
    """
    Log execute_query usage to file for audit/review and console.

    Args:
        query: SQL query string
        params: Optional query parameters
    """
    _ensure_log_dir()
    _rotate_if_needed(QUERY_LOG_FILE)
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


def log_execute_query_result(
    query: str,
    params: dict[str, Any] | None,
    *,
    duration_ms: float,
    row_count: int | None,
    success: bool,
    error: str | None = None,
) -> None:
    """Log structured execute_query outcome data for tool-gap analysis."""
    _ensure_log_dir()
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event": "execute_query",
        "sql": query,
        "params": params or {},
        "duration_ms": round(duration_ms, 2),
        "row_count": row_count,
        "success": success,
        "error": error,
    }
    try:
        _rotate_if_needed(QUERY_LOG_JSONL_FILE)
        with open(QUERY_LOG_JSONL_FILE, "a") as f:
            f.write(json.dumps(record, default=str, sort_keys=True) + "\n")
    except OSError as e:
        logger.warning("Failed to write structured execute_query log: %s", e)


@contextmanager
def get_connection():
    """
    Get a database connection from the pool as a context manager.

    Security: Read-only mode is enforced via the pool's configure callback.
    The connection is automatically returned to the pool on exit.
    """
    pool = _get_pool()
    with pool.connection() as conn:
        yield conn


def _strip_leading_sql_comments(text: str) -> str:
    """Strip leading whitespace and SQL comments from a query.

    Handles both line comments (``-- ...``) and block comments
    (``/* ... */``) so a legitimately commented SELECT is not rejected by the
    startswith check. Only leading comments are removed; the body is left for
    the SELECT/WITH check and the keyword blocklist below.
    """
    prev = None
    while text != prev:
        prev = text
        text = text.lstrip()
        if text.startswith("--"):
            # Drop through end of line (or end of string).
            newline = text.find("\n")
            text = "" if newline == -1 else text[newline + 1 :]
        elif text.startswith("/*"):
            end = text.find("*/")
            # Unterminated block comment: nothing parseable remains.
            text = "" if end == -1 else text[end + 2 :]
    return text


def validate_select_query(query: str) -> bool:
    """
    Validate that a SQL query is a safe SELECT statement.

    Checks:
    - Must start with SELECT or WITH (CTE), ignoring leading SQL comments
    - No DDL commands (CREATE, DROP, ALTER, etc.)
    - No DML commands (INSERT, UPDATE, DELETE, etc.)

    The connection runs in a read-only transaction (enforced by the pool's
    ``configure`` callback), which is the real guard against writes. This
    function is defense-in-depth: the keyword blocklist is best-effort and the
    read-only transaction must not be removed in favour of it.

    Args:
        query: SQL query string

    Returns:
        True if valid SELECT query

    Raises:
        ValueError: If query is not a valid SELECT statement
    """
    # Normalize the query, ignoring any leading SQL comments so that a
    # legitimately commented SELECT (e.g. "-- report\nSELECT ...") still passes.
    normalized = _strip_leading_sql_comments(query).upper()

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
    query: str | sql.Composable,
    params: dict[str, Any] | None = None,
    validate: bool = True,
) -> list[dict[str, Any]]:
    """
    Execute a SQL query and return results.

    Args:
        query: SQL query string or psycopg sql.Composable object
        params: Optional query parameters for safe interpolation
        validate: Whether to validate query is SELECT-only

    Returns:
        List of result rows as dictionaries

    Raises:
        ValueError: If query validation fails
        psycopg.Error: If database error occurs
    """
    if isinstance(query, str):
        if validate:
            validate_select_query(query)

        query = query.strip().rstrip(";")
        query_sql: sql.Composable = sql.SQL("SELECT * FROM ({}) AS _mcp_limited LIMIT {}").format(
            sql.SQL(query),
            sql.Literal(MAX_ROWS),
        )
    else:
        # Composable queries are built safely via psycopg sql module
        query_sql = query

    with get_connection() as conn:
        with conn.cursor() as cur:
            # Set query timeout using SQL module for safety
            timeout_sql = sql.SQL("SET statement_timeout = {}").format(
                sql.Literal(f"{QUERY_TIMEOUT}s")
            )
            cur.execute(timeout_sql)

            logger.debug("Executing query...")

            if params:
                cur.execute(query_sql, params)
            else:
                cur.execute(query_sql)

            results = cur.fetchall()
            logger.debug("Query returned %d rows", len(results))

            # Convert to list of dicts explicitly
            return [dict(row) for row in results]
