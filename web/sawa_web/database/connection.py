"""Async PostgreSQL connection pool using asyncpg."""

import logging
from contextlib import asynccontextmanager
from typing import Any

import asyncpg

from sawa_web.config import get_settings

logger = logging.getLogger(__name__)

# Global connection pool
_pool: asyncpg.Pool | None = None


async def init_pool() -> asyncpg.Pool:
    """Initialize the connection pool."""
    global _pool
    if _pool is not None:
        return _pool

    settings = get_settings()
    logger.info("Initializing database connection pool...")

    _pool = await asyncpg.create_pool(
        settings.asyncpg_url,
        min_size=2,
        max_size=10,
        command_timeout=60,
    )

    logger.info("Database connection pool initialized")
    return _pool


async def close_pool() -> None:
    """Close the connection pool."""
    global _pool
    if _pool is not None:
        logger.info("Closing database connection pool...")
        await _pool.close()
        _pool = None
        logger.info("Database connection pool closed")


def get_pool() -> asyncpg.Pool:
    """Get the connection pool (must be initialized first)."""
    if _pool is None:
        raise RuntimeError("Database pool not initialized. Call init_pool() first.")
    return _pool


@asynccontextmanager
async def get_connection():
    """Get a database connection from the pool."""
    pool = get_pool()
    async with pool.acquire() as conn:
        yield conn


async def execute_query(
    sql: str,
    *args,
    fetch_one: bool = False,
) -> list[dict[str, Any]] | dict[str, Any] | None:
    """
    Execute a SELECT query and return results as list of dicts.

    Args:
        sql: SQL query string with $1, $2, etc. placeholders
        *args: Query parameters
        fetch_one: If True, return only the first row

    Returns:
        List of row dictionaries, or single dict if fetch_one=True
    """
    async with get_connection() as conn:
        if fetch_one:
            row = await conn.fetchrow(sql, *args)
            return dict(row) if row else None
        rows = await conn.fetch(sql, *args)
        return [dict(row) for row in rows]


async def execute_write(
    sql: str,
    *args,
) -> str:
    """
    Execute an INSERT/UPDATE/DELETE query.

    Args:
        sql: SQL query string
        *args: Query parameters

    Returns:
        Status string (e.g., "DELETE 1")
    """
    async with get_connection() as conn:
        return await conn.execute(sql, *args)


async def execute_write_returning(
    sql: str,
    *args,
) -> dict[str, Any] | None:
    """
    Execute an INSERT/UPDATE with RETURNING clause.

    Args:
        sql: SQL query string with RETURNING clause
        *args: Query parameters

    Returns:
        Returned row as dict, or None if no row returned
    """
    async with get_connection() as conn:
        row = await conn.fetchrow(sql, *args)
        return dict(row) if row else None
