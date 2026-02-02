"""Shared database connection handling."""

import os
from typing import Any

import psycopg2

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
        "database": database or os.environ.get(ENV_DATABASE),
        "user": user or os.environ.get(ENV_USER),
        "password": password or os.environ.get(ENV_PASSWORD),
    }

    if not params["database"]:
        raise ValueError(f"Database required. Set {ENV_DATABASE} or use --database")
    if not params["user"]:
        raise ValueError(f"User required. Set {ENV_USER} or use --user")
    if not params["password"]:
        raise ValueError(f"Password required. Set {ENV_PASSWORD} or use --password")

    return params


def get_connection(conn_params: dict[str, Any]) -> psycopg2.extensions.connection:
    """
    Create database connection.

    Args:
        conn_params: Connection parameters from get_connection_params()

    Returns:
        Database connection
    """
    return psycopg2.connect(**conn_params)
