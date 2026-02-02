"""Repository configuration.

This module defines the RepositoryConfig dataclass and provides functions
to load configuration from environment variables.

Note: This module is separate from factory.py to avoid circular imports.
The factory imports config, not the other way around.

Usage:
    from sawa.repositories.config import get_config

    config = get_config()
    print(config.database_url)
    print(config.default_price_provider)
"""

import os
from dataclasses import dataclass


@dataclass
class RepositoryConfig:
    """Configuration for repositories.

    Attributes:
        database_url: PostgreSQL connection URL
        polygon_api_key: Polygon.io REST API key
        polygon_s3_access_key: Polygon S3 access key
        polygon_s3_secret_key: Polygon S3 secret key
        massive_api_key: Massive API key
        default_price_provider: Default provider for prices ("database" or "polygon")
        default_fundamental_provider: Default provider for fundamentals
        default_company_provider: Default provider for company info
        default_ratios_provider: Default provider for financial ratios
        default_economy_provider: Default provider for economy data
        cache_enabled: Whether to enable caching for API providers
        cache_max_size: Maximum cache entries
        cache_ttl_seconds: Default cache TTL in seconds
        polygon_rate_limit: Polygon API rate limit (requests per second)
        massive_rate_limit: Massive API rate limit (requests per second)
    """

    # Database (for TUI/MCP - reading loaded data)
    database_url: str | None = None

    # API Keys (for CLI - fetching data)
    polygon_api_key: str | None = None
    polygon_s3_access_key: str | None = None
    polygon_s3_secret_key: str | None = None
    massive_api_key: str | None = None

    # Provider selection
    # "database" for TUI/MCP (default), "polygon" for CLI downloads
    default_price_provider: str = "database"
    default_fundamental_provider: str = "database"
    default_company_provider: str = "database"
    default_ratios_provider: str = "database"
    default_economy_provider: str = "database"

    # Cache settings (for API providers)
    cache_enabled: bool = True
    cache_max_size: int = 1000
    cache_ttl_seconds: float = 300

    # Rate limits (for API providers)
    polygon_rate_limit: float = 5.0
    massive_rate_limit: float = 2.0


def get_env(
    key: str,
    default: str | None = None,
    required: bool = False,
) -> str | None:
    """Get environment variable with optional validation.

    Args:
        key: Environment variable name
        default: Default value if not set
        required: If True, raise ValueError if not set and no default

    Returns:
        Environment variable value, or default

    Raises:
        ValueError: If required=True and variable is not set
    """
    value = os.environ.get(key)
    if value is None:
        if required and default is None:
            raise ValueError(f"Required environment variable {key} is not set")
        return default
    return value


def get_database_url() -> str | None:
    """Get database URL from environment.

    Tries DATABASE_URL first, then constructs from individual PG* variables.

    Returns:
        Database URL, or None if not configured
    """
    # Try DATABASE_URL first
    url = os.environ.get("DATABASE_URL")
    if url:
        return url

    # Try to construct from individual variables
    host = os.environ.get("PGHOST")
    port = os.environ.get("PGPORT", "5432")
    database = os.environ.get("PGDATABASE")
    user = os.environ.get("PGUSER")
    password = os.environ.get("PGPASSWORD")

    if all([host, database, user, password]):
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"

    return None


def get_config() -> RepositoryConfig:
    """Create repository configuration from environment.

    Reads configuration from environment variables and returns a
    RepositoryConfig instance.

    Environment Variables:
        DATABASE_URL: PostgreSQL connection URL
        PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD: Individual PG config
        POLYGON_API_KEY: Polygon.io API key
        POLYGON_S3_ACCESS_KEY: Polygon S3 access key
        POLYGON_S3_SECRET_KEY: Polygon S3 secret key
        MASSIVE_API_KEY: Massive API key
        DEFAULT_PRICE_PROVIDER: Price provider ("database" or "polygon")
        DEFAULT_FUNDAMENTAL_PROVIDER: Fundamental provider
        DEFAULT_COMPANY_PROVIDER: Company provider
        CACHE_ENABLED: Enable caching ("true" or "false")
        CACHE_TTL_SECONDS: Cache TTL in seconds

    Returns:
        RepositoryConfig instance
    """
    cache_enabled_str = get_env("CACHE_ENABLED", "true")
    cache_enabled = cache_enabled_str.lower() == "true" if cache_enabled_str else True

    cache_ttl_str = get_env("CACHE_TTL_SECONDS", "300")
    cache_ttl = float(cache_ttl_str) if cache_ttl_str else 300.0

    return RepositoryConfig(
        database_url=get_database_url(),
        polygon_api_key=get_env("POLYGON_API_KEY"),
        polygon_s3_access_key=get_env("POLYGON_S3_ACCESS_KEY"),
        polygon_s3_secret_key=get_env("POLYGON_S3_SECRET_KEY"),
        massive_api_key=get_env("MASSIVE_API_KEY"),
        # Default to database for reading, can override for API access
        default_price_provider=get_env("DEFAULT_PRICE_PROVIDER", "database") or "database",
        default_fundamental_provider=get_env("DEFAULT_FUNDAMENTAL_PROVIDER", "database")
        or "database",
        default_company_provider=get_env("DEFAULT_COMPANY_PROVIDER", "database") or "database",
        default_ratios_provider=get_env("DEFAULT_RATIOS_PROVIDER", "database") or "database",
        default_economy_provider=get_env("DEFAULT_ECONOMY_PROVIDER", "database") or "database",
        cache_enabled=cache_enabled,
        cache_ttl_seconds=cache_ttl,
    )
