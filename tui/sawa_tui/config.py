"""Configuration management for the TUI application.

Database-backed settings are now used via models/settings.py.
This module only handles environment variable lookups.
"""

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


# Temporary compatibility shims for logo/theme functionality
# These return hardcoded defaults until those features are updated to use SettingsManager


class _LegacyConfigShim:
    """Temporary shim to maintain compatibility until all code uses SettingsManager."""

    def get(self, section: str, key: str, default=None):
        """Get config value - returns hardcoded defaults."""
        defaults = {
            "theme": {"name": "osaka-jade"},
            "logo": {"enabled": True, "width": 28, "height": 10},
        }
        return defaults.get(section, {}).get(key, default)

    @property
    def logo_enabled(self) -> bool:
        return True

    @property
    def logo_width(self) -> int:
        return 28

    @property
    def logo_height(self) -> int:
        return 10

    @property
    def theme_name(self) -> str:
        return "osaka-jade"


def get_tui_config() -> _LegacyConfigShim:
    """
    Legacy function for compatibility.

    Returns default values. Code should migrate to using SettingsManager.
    """
    return _LegacyConfigShim()


def get_tui_log_file():
    """Get log file path for compatibility."""
    from pathlib import Path

    log_dir = Path.home() / ".local" / "state" / "sp500-tui"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / "app.log"
