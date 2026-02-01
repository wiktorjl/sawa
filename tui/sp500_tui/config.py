"""Configuration management for the TUI application.

Uses XDG Base Directory Specification for config file storage:
- Config: ~/.config/sp500-tui/config.toml
- State/Logs: ~/.local/state/sp500-tui/
"""

import os
from pathlib import Path
from typing import Any

from sp500_tools.utils.xdg import (
    ensure_dirs,
    get_config_file,
    get_log_file,
    load_config,
    save_config,
)

# App name for XDG directories
APP_NAME = "sp500-tui"


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


def get_tui_log_file() -> Path:
    """Get path to TUI log file."""
    return get_log_file(APP_NAME, "app.log")


def get_tui_config_file() -> Path:
    """Get path to TUI config file."""
    return get_config_file(APP_NAME)


# Default settings (XDG config file format)
DEFAULT_SETTINGS: dict[str, Any] = {
    "theme": {
        "name": "osaka-jade",
    },
    "charts": {
        "detail": "normal",
        "colors_enabled": True,
    },
    "display": {
        "chart_period_days": 60,
        "number_format": "compact",  # 'compact' or 'full'
        "table_rows": 25,
    },
    "fundamentals": {
        "default_timeframe": "quarterly",  # 'quarterly' or 'annual'
    },
    "behavior": {
        "auto_refresh": False,
        "refresh_interval_seconds": 60,
    },
    "logo": {
        "enabled": True,
        "width": 28,
        "height": 10,
    },
    "api": {
        "polygon_api_key": "",
    },
}


class TUIConfig:
    """TUI configuration manager using XDG config file."""

    _instance: "TUIConfig | None" = None
    _config: dict[str, Any] | None = None

    def __new__(cls) -> "TUIConfig":
        """Singleton pattern to ensure single config instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def _load(self) -> dict[str, Any]:
        """Load configuration from XDG config file."""
        if self._config is None:
            ensure_dirs(APP_NAME)
            self._config = load_config(APP_NAME, DEFAULT_SETTINGS)
        return self._config

    def reload(self) -> None:
        """Reload configuration from file."""
        self._config = None
        self._load()

    def save(self) -> None:
        """Save current configuration to file."""
        if self._config is not None:
            save_config(self._config, APP_NAME)

    def get(self, section: str, key: str, default: Any = None) -> Any:
        """
        Get a configuration value.

        Args:
            section: Config section (e.g., 'display', 'theme')
            key: Key within section
            default: Default value if not found

        Returns:
            Configuration value
        """
        config = self._load()
        section_data = config.get(section, {})
        return section_data.get(key, default)

    def set(self, section: str, key: str, value: Any) -> None:
        """
        Set a configuration value.

        Args:
            section: Config section
            key: Key within section
            value: Value to set
        """
        config = self._load()
        if section not in config:
            config[section] = {}
        config[section][key] = value
        self.save()

    # Convenience accessors
    @property
    def theme_name(self) -> str:
        """Get current theme name."""
        return self.get("theme", "name", "osaka-jade")

    @theme_name.setter
    def theme_name(self, value: str) -> None:
        """Set theme name."""
        self.set("theme", "name", value)

    @property
    def chart_detail(self) -> str:
        """Get chart detail level."""
        return self.get("charts", "detail", "normal")

    @chart_detail.setter
    def chart_detail(self, value: str) -> None:
        """Set chart detail level."""
        self.set("charts", "detail", value)

    @property
    def colors_enabled(self) -> bool:
        """Get whether colors are enabled."""
        return self.get("charts", "colors_enabled", True)

    @property
    def chart_period_days(self) -> int:
        """Get chart period in days."""
        return self.get("display", "chart_period_days", 60)

    @chart_period_days.setter
    def chart_period_days(self, value: int) -> None:
        """Set chart period in days."""
        self.set("display", "chart_period_days", value)

    @property
    def number_format(self) -> str:
        """Get number format (compact or full)."""
        return self.get("display", "number_format", "compact")

    @number_format.setter
    def number_format(self, value: str) -> None:
        """Set number format."""
        self.set("display", "number_format", value)

    @property
    def table_rows(self) -> int:
        """Get number of table rows."""
        return self.get("display", "table_rows", 25)

    @table_rows.setter
    def table_rows(self, value: int) -> None:
        """Set number of table rows."""
        self.set("display", "table_rows", value)

    @property
    def fundamentals_timeframe(self) -> str:
        """Get fundamentals timeframe."""
        return self.get("fundamentals", "default_timeframe", "quarterly")

    @fundamentals_timeframe.setter
    def fundamentals_timeframe(self, value: str) -> None:
        """Set fundamentals timeframe."""
        self.set("fundamentals", "default_timeframe", value)

    @property
    def auto_refresh(self) -> bool:
        """Get auto refresh setting."""
        return self.get("behavior", "auto_refresh", False)

    @auto_refresh.setter
    def auto_refresh(self, value: bool) -> None:
        """Set auto refresh."""
        self.set("behavior", "auto_refresh", value)

    @property
    def refresh_interval_seconds(self) -> int:
        """Get refresh interval in seconds."""
        return self.get("behavior", "refresh_interval_seconds", 60)

    @refresh_interval_seconds.setter
    def refresh_interval_seconds(self, value: int) -> None:
        """Set refresh interval."""
        self.set("behavior", "refresh_interval_seconds", value)

    @property
    def logo_enabled(self) -> bool:
        """Get whether company logos are enabled."""
        return self.get("logo", "enabled", True)

    @logo_enabled.setter
    def logo_enabled(self, value: bool) -> None:
        """Set whether company logos are enabled."""
        self.set("logo", "enabled", value)

    @property
    def logo_width(self) -> int:
        """Get logo width in characters."""
        return self.get("logo", "width", 28)

    @logo_width.setter
    def logo_width(self, value: int) -> None:
        """Set logo width in characters."""
        self.set("logo", "width", value)

    @property
    def logo_height(self) -> int:
        """Get logo height in lines."""
        return self.get("logo", "height", 10)

    @logo_height.setter
    def logo_height(self, value: int) -> None:
        """Set logo height in lines."""
        self.set("logo", "height", value)


def get_tui_config() -> TUIConfig:
    """Get the TUI configuration instance."""
    return TUIConfig()
