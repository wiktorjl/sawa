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


def get_database_url() -> str | None:
    """Get PostgreSQL database URL from environment."""
    return get_env("DATABASE_URL")
