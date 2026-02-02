"""Configuration and environment variable utilities."""

import os


def get_env(key: str, default: str | None = None, required: bool = False) -> str | None:
    """
    Get environment variable with optional validation.

    Args:
        key: Environment variable name
        default: Default value if not set
        required: Raise error if not set and no default

    Returns:
        Environment variable value or default

    Raises:
        ValueError: If required and not set
    """
    value = os.environ.get(key)
    if value is None:
        if required and default is None:
            raise ValueError(f"Required environment variable {key} is not set")
        return default
    return value


def get_polygon_api_key() -> str | None:
    """Get Polygon.io API key from environment."""
    return get_env("POLYGON_API_KEY")


def get_polygon_s3_credentials() -> tuple[str | None, str | None]:
    """Get Polygon S3 credentials from environment."""
    return get_env("POLYGON_S3_ACCESS_KEY"), get_env("POLYGON_S3_SECRET_KEY")


def get_massive_api_key() -> str | None:
    """Get Massive API key from environment."""
    return get_env("MASSIVE_API_KEY")


def get_database_url(required: bool = False) -> str | None:
    """
    Get PostgreSQL database URL from environment.

    Checks DATABASE_URL first, then falls back to individual PG* variables.

    Args:
        required: If True, raise ValueError if no database config found

    Returns:
        PostgreSQL connection URL, or None if not configured

    Raises:
        ValueError: If required=True and no database configuration is found
    """
    # Check for DATABASE_URL first
    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        return database_url

    # Fall back to individual PG* variables
    host = os.environ.get("PGHOST")
    port = os.environ.get("PGPORT", "5432")
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("PGUSER")
    password = os.environ.get("PGPASSWORD")

    if all([host, database, user]):
        if password:
            return f"postgresql://{user}:{password}@{host}:{port}/{database}"
        return f"postgresql://{user}@{host}:{port}/{database}"

    if required:
        raise ValueError(
            "Database configuration not found. "
            "Set DATABASE_URL or PGHOST/PGDATABASE/PGUSER/PGPASSWORD environment variables."
        )

    return None


def require_database_url() -> str:
    """
    Get PostgreSQL database URL, raising if not configured.

    This is a convenience function for code that requires a database connection.

    Returns:
        PostgreSQL connection URL

    Raises:
        ValueError: If no database configuration is found
    """
    url = get_database_url(required=True)
    assert url is not None  # For type narrowing
    return url
