"""Database connection and query execution for the MCP server."""

import atexit
import json
import logging
import os
import re
import threading
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from sawa.utils.config import require_database_url as get_database_url  # noqa: F401
from sawa.utils.security import (
    ensure_private_directory,
    open_private_text,
    redact_parameter_values,
    redact_sensitive_text,
    redact_sql_literals,
)

from .utils.json_values import compact_json_size

logger = logging.getLogger(__name__)


def _bounded_env_int(
    name: str,
    default: int,
    *,
    minimum: int,
    maximum: int,
) -> int:
    """Parse one integer setting without making module import fragile."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        logger.warning("Ignoring invalid integer setting %s", name)
        return default
    if not minimum <= value <= maximum:
        logger.warning("Ignoring out-of-range integer setting %s", name)
        return default
    return value


# Configuration
MAX_ROWS = _bounded_env_int("MCP_MAX_ROWS", 1000, minimum=0, maximum=10_000)
QUERY_TIMEOUT = _bounded_env_int("MCP_QUERY_TIMEOUT", 30, minimum=1, maximum=300)
MAX_RESULT_BYTES = _bounded_env_int(
    "MCP_MAX_RESULT_BYTES",
    5 * 1024 * 1024,
    minimum=2,
    maximum=50 * 1024 * 1024,
)

# Pool configuration
POOL_MIN_SIZE = _bounded_env_int("MCP_POOL_MIN_SIZE", 2, minimum=1, maximum=100)
POOL_MAX_SIZE = _bounded_env_int("MCP_POOL_MAX_SIZE", 10, minimum=1, maximum=100)
if POOL_MAX_SIZE < POOL_MIN_SIZE:
    logger.warning("MCP_POOL_MAX_SIZE is below MCP_POOL_MIN_SIZE; using the minimum")
    POOL_MAX_SIZE = POOL_MIN_SIZE

# Module-level pool singleton
_pool: ConnectionPool | None = None
_pool_lock = threading.Lock()
_pool_exit_handler_registered = False


def _configure_connection(conn: psycopg.Connection) -> None:
    """Configure a connection after it is created by the pool."""
    conn.row_factory = dict_row  # type: ignore[assignment]
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SET default_transaction_read_only = on"))
        cur.execute(sql.SQL("SET search_path TO pg_catalog, public"))
    conn.commit()


def _reset_connection(conn: psycopg.Connection) -> None:
    """Return a pooled session to a known state before it can be reused."""
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        conn.rollback()
    previous_autocommit = conn.autocommit
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            # DISCARD ALL clears session GUCs, roles, advisory locks, prepared
            # statements, and temporary objects. Re-apply the pool invariant
            # immediately afterwards because DISCARD resets it too.
            cur.execute(sql.SQL("DISCARD ALL"))
            cur.execute(sql.SQL("SET default_transaction_read_only = on"))
            cur.execute(sql.SQL("SET search_path TO pg_catalog, public"))
    finally:
        conn.autocommit = previous_autocommit


def _get_pool() -> ConnectionPool:
    """Get or create the module-level connection pool (lazy init)."""
    global _pool, _pool_exit_handler_registered
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = ConnectionPool(
                    conninfo=get_database_url(),  # type: ignore[arg-type]
                    min_size=POOL_MIN_SIZE,
                    max_size=POOL_MAX_SIZE,
                    configure=_configure_connection,
                    reset=_reset_connection,
                    open=True,
                    timeout=60,
                    max_idle=300,
                    max_lifetime=1800,
                )
                if not _pool_exit_handler_registered:
                    atexit.register(close_pool)
                    _pool_exit_handler_registered = True
                logger.info(
                    "Connection pool created (min_size=%d, max_size=%d)",
                    POOL_MIN_SIZE,
                    POOL_MAX_SIZE,
                )
    assert _pool is not None
    return _pool


def close_pool() -> None:
    """Close the connection pool. Safe to call multiple times."""
    global _pool
    with _pool_lock:
        pool = _pool
        _pool = None
    if pool is not None:
        pool.close()
        logger.info("Connection pool closed")


# Query audit log
_DEFAULT_QUERY_LOG_DIR = Path.home() / ".sawa" / "logs"
QUERY_LOG_DIR = Path(os.environ.get("MCP_QUERY_LOG_DIR") or _DEFAULT_QUERY_LOG_DIR)
QUERY_LOG_FILE = QUERY_LOG_DIR / "execute_query.log"
QUERY_LOG_JSONL_FILE = QUERY_LOG_DIR / "execute_query.jsonl"

# Append-mode audit logs have no built-in rotation, so cap each sink and roll
# it to "<name>.1" once it grows past this size. A single ".1" backup keeps
# disk usage bounded without bringing in a full logging handler.
QUERY_LOG_MAX_BYTES = _bounded_env_int(
    "MCP_QUERY_LOG_MAX_BYTES",
    5 * 1024 * 1024,
    minimum=0,
    maximum=100 * 1024 * 1024,
)


def _ensure_log_dir() -> None:
    """Ensure log directory exists."""
    ensure_private_directory(QUERY_LOG_DIR)


def _rotate_if_needed(path: Path) -> None:
    """Rotate ``path`` to ``path.1`` when it exceeds ``QUERY_LOG_MAX_BYTES``.

    Keeps a single backup; the previous ``.1`` is overwritten. Best-effort:
    any OS error is swallowed so auditing never blocks query execution.
    """
    if QUERY_LOG_MAX_BYTES <= 0:
        return
    try:
        if path.is_symlink():
            raise OSError(f"Refusing to rotate symlinked audit log: {path}")
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
    safe_query = redact_sql_literals(query)
    safe_params = redact_parameter_values(params)
    timestamp = datetime.now(timezone.utc).isoformat()

    try:
        _ensure_log_dir()
        _rotate_if_needed(QUERY_LOG_FILE)
        with open_private_text(QUERY_LOG_FILE) as f:
            f.write(f"[{timestamp}] QUERY: {safe_query}\n")
            if safe_params:
                f.write(f"[{timestamp}] PARAMS: {safe_params}\n")
            f.write("\n")
    except OSError as e:
        logger.warning("Failed to write execute_query audit log: %s", e)

    logger.info("[QUERY] %s", safe_query)
    if safe_params:
        logger.info("[QUERY PARAMS] %s", safe_params)


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
    safe_query = redact_sql_literals(query)
    safe_params = redact_parameter_values(params)
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event": "execute_query",
        # Explicit override hook: set MCP_QUERY_SOURCE=review (or forensic/audit)
        # on a server used for a code review or data audit so those queries are
        # tagged and excluded from the missing-tool signal. Defaults to "agent".
        "source": os.environ.get("MCP_QUERY_SOURCE", "agent"),
        "sql": safe_query,
        "params": safe_params,
        "duration_ms": round(duration_ms, 2),
        "row_count": row_count,
        "success": success,
        "error": redact_sensitive_text(error) if error else None,
    }
    try:
        _ensure_log_dir()
        _rotate_if_needed(QUERY_LOG_JSONL_FILE)
        with open_private_text(QUERY_LOG_JSONL_FILE) as f:
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
    row_limit = max(MAX_ROWS, 0)
    if isinstance(query, str):
        if validate:
            validate_select_query(query)

        query = query.strip().rstrip(";")
        inner_query: sql.Composable = sql.SQL(query)
    else:
        # Composable queries are built safely via psycopg sql module
        inner_query = query

    # Apply the row cap inside PostgreSQL for both plain and Composable calls.
    # That lets the stream run to completion on ordinary capped results instead
    # of cancelling an otherwise-unbounded statement after the Python loop.
    query_sql = sql.SQL("SELECT * FROM ({}) AS _mcp_limited LIMIT {}").format(
        inner_query,
        sql.Literal(row_limit),
    )

    # The empty JSON array is the smallest successful result representation.
    result_bytes = 2
    if result_bytes > MAX_RESULT_BYTES:
        raise ValueError(
            f"Query result exceeds maximum serialized size of {MAX_RESULT_BYTES} bytes"
        )

    with get_connection() as conn:
        with conn.cursor() as control_cursor:
            # Establish the invariants in this transaction too; do not rely
            # solely on session defaults surviving arbitrary prior activity.
            control_cursor.execute(sql.SQL("SET TRANSACTION READ ONLY"))
            timeout_sql = sql.SQL("SET LOCAL statement_timeout = {}").format(
                sql.Literal(f"{QUERY_TIMEOUT}s")
            )
            control_cursor.execute(timeout_sql)

        logger.debug("Executing query...")

        # Cursor.stream() uses libpq single-row mode: one PostgreSQL statement
        # (so statement_timeout remains a total statement bound) and one row
        # materialized at a time. Explicitly closing the generator is required
        # to recover the connection if the byte guard rejects a row early.
        # Preserve the helper's historical empty-mapping behavior. Psycopg
        # treats ``{}`` as parameterized execution and will parse literal
        # percent/modulo operators as placeholders; no parameters means None.
        stream_params = params or None
        with conn.cursor() as cur, closing(cur.stream(query_sql, stream_params)) as rows:
            output: list[dict[str, Any]] = []
            for row in rows:
                # Keep a Python-side guard as defense in depth even though the
                # SQL wrapper applies the cap before rows cross the wire. The
                # loop itself must still advance through normal EOF so libpq
                # receives PGRES completion and leaves the connection usable.
                if len(output) >= row_limit:
                    logger.warning("Database stream exceeded its SQL row cap")
                    break
                item = dict(row)
                item_bytes = compact_json_size(item)
                # Canonical compact arrays use one comma between items.
                candidate_bytes = result_bytes + item_bytes + (1 if output else 0)
                if candidate_bytes > MAX_RESULT_BYTES:
                    raise ValueError(
                        "Query result exceeds maximum serialized size of "
                        f"{MAX_RESULT_BYTES} bytes"
                    )
                output.append(item)
                result_bytes = candidate_bytes

            logger.debug("Query returned %d rows", len(output))
            return output
