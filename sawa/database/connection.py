"""Shared database connection handling and common queries."""

import os
from datetime import date
from typing import Any

import psycopg
from psycopg import sql

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5432

ENV_HOST = "PGHOST"
ENV_PORT = "PGPORT"
ENV_DATABASE = "PGDATABASE"
ENV_USER = "PGUSER"
ENV_PASSWORD = "PGPASSWORD"


def get_connection_params(
    host: str | None = None,
    port: int | None = None,
    database: str | None = None,
    user: str | None = None,
    password: str | None = None,
) -> dict[str, Any]:
    """
    Get database connection parameters from args or environment.

    Args:
        host: Database host
        port: Database port
        database: Database name
        user: Database user
        password: Database password

    Returns:
        Connection parameters dict

    Raises:
        ValueError: If required parameters are missing
    """
    params = {
        "host": host or os.environ.get(ENV_HOST, DEFAULT_HOST),
        "port": port or int(os.environ.get(ENV_PORT, str(DEFAULT_PORT))),
        "dbname": database or os.environ.get(ENV_DATABASE),
        "user": user or os.environ.get(ENV_USER),
        "password": password or os.environ.get(ENV_PASSWORD),
    }

    if not params["dbname"]:
        raise ValueError(f"Database required. Set {ENV_DATABASE} or use --database")
    if not params["user"]:
        raise ValueError(f"User required. Set {ENV_USER} or use --user")
    if not params["password"]:
        raise ValueError(f"Password required. Set {ENV_PASSWORD} or use --password")

    return params


def get_connection(conn_params: dict[str, Any]) -> psycopg.Connection[tuple[Any, ...]]:
    """
    Create database connection.

    Args:
        conn_params: Connection parameters from get_connection_params()

    Returns:
        Database connection
    """
    return psycopg.connect(**conn_params)


def get_last_date(conn, table: str, date_column: str = "date") -> date | None:
    """Get the most recent date from a table.

    Args:
        conn: Database connection
        table: Table name
        date_column: Date column name (default: "date")

    Returns:
        Most recent date, or None if table is empty
    """
    query = sql.SQL("SELECT MAX({}) FROM {}").format(
        sql.Identifier(date_column),
        sql.Identifier(table),
    )
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchone()
        if result and result[0]:
            return result[0]
    return None


def get_symbols_from_db(conn) -> list[str]:
    """Get list of ticker symbols from companies table.

    Args:
        conn: Database connection

    Returns:
        List of ticker symbols, sorted alphabetically
    """
    with conn.cursor() as cur:
        cur.execute("SELECT ticker FROM companies ORDER BY ticker")
        return [row[0] for row in cur.fetchall()]
