"""Configuration management for the TUI application."""

import os


def get_database_url() -> str:
    """
    Get the PostgreSQL database URL from environment.

    Checks DATABASE_URL first, then falls back to individual PG* variables.

    Returns:
        PostgreSQL connection URL

    Raises:
        ValueError: If no database configuration is found
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

    raise ValueError(
        "Database configuration not found. "
        "Set DATABASE_URL or PGHOST/PGDATABASE/PGUSER/PGPASSWORD environment variables."
    )


def get_zai_api_key() -> str | None:
    """
    Get the Z.AI API key from environment.

    Returns:
        API key string or None if not configured
    """
    return os.environ.get("ZAI_API_KEY")


def get_zai_api_url() -> str:
    """
    Get the Z.AI API endpoint URL.

    Returns:
        API endpoint URL (defaults to coding plan endpoint)
    """
    return os.environ.get("ZAI_API_URL", "https://api.z.ai/api/coding/paas/v4/chat/completions")


# Default settings
DEFAULT_SETTINGS = {
    "chart_period_days": "60",
    "auto_refresh": "false",
    "refresh_interval_seconds": "60",
    "number_format": "compact",  # 'compact' or 'full'
    "fundamentals_timeframe": "quarterly",  # 'quarterly' or 'annual'
    "table_rows": "25",
}
