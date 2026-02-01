"""User settings management.

Uses XDG-compliant configuration file for local settings.
The TUIConfig singleton provides access to all user preferences.
"""

from sp500_tui.config import TUIConfig, get_tui_config


class SettingsManager:
    """
    Manager for user settings.

    Provides a backwards-compatible interface while using XDG config files
    instead of database storage. Local preferences (theme, chart settings,
    display options) are now stored in ~/.config/sp500-tui/config.toml
    """

    _config: TUIConfig | None = None

    @classmethod
    def _get_config(cls) -> TUIConfig:
        """Get or create the TUI config instance."""
        if cls._config is None:
            cls._config = get_tui_config()
        return cls._config

    @classmethod
    def invalidate_cache(cls) -> None:
        """Reload configuration from file."""
        config = cls._get_config()
        config.reload()

    @classmethod
    def get(cls, key: str, default: str | None = None) -> str | None:
        """
        Get a setting value.

        Args:
            key: Setting key (legacy format)
            default: Default value if not found

        Returns:
            Setting value or default
        """
        config = cls._get_config()

        # Map legacy keys to new config structure
        key_mapping = {
            "chart_period_days": lambda: str(config.chart_period_days),
            "auto_refresh": lambda: str(config.auto_refresh).lower(),
            "refresh_interval_seconds": lambda: str(config.refresh_interval_seconds),
            "number_format": lambda: config.number_format,
            "fundamentals_timeframe": lambda: config.fundamentals_timeframe,
            "table_rows": lambda: str(config.table_rows),
            "theme": lambda: config.theme_name,
            "chart_detail": lambda: config.chart_detail,
            "logo_enabled": lambda: str(config.logo_enabled).lower(),
            "logo_width": lambda: str(config.logo_width),
            "logo_height": lambda: str(config.logo_height),
        }

        if key in key_mapping:
            return key_mapping[key]()
        return default

    @classmethod
    def get_int(cls, key: str, default: int = 0) -> int:
        """Get a setting value as integer."""
        value = cls.get(key)
        if value is None:
            return default
        try:
            return int(value)
        except ValueError:
            return default

    @classmethod
    def get_bool(cls, key: str, default: bool = False) -> bool:
        """Get a setting value as boolean."""
        value = cls.get(key)
        if value is None:
            return default
        return value.lower() in ("true", "1", "yes", "on")

    @classmethod
    def set(cls, key: str, value: str) -> bool:
        """
        Set a setting value.

        Args:
            key: Setting key (legacy format)
            value: Setting value

        Returns:
            True if successful
        """
        config = cls._get_config()

        # Map legacy keys to new config structure
        try:
            if key == "chart_period_days":
                config.chart_period_days = int(value)
            elif key == "auto_refresh":
                config.auto_refresh = value.lower() in ("true", "1", "yes", "on")
            elif key == "refresh_interval_seconds":
                config.refresh_interval_seconds = int(value)
            elif key == "number_format":
                config.number_format = value
            elif key == "fundamentals_timeframe":
                config.fundamentals_timeframe = value
            elif key == "table_rows":
                config.table_rows = int(value)
            elif key == "theme":
                config.theme_name = value
            elif key == "chart_detail":
                config.chart_detail = value
            elif key == "logo_enabled":
                config.logo_enabled = value.lower() in ("true", "1", "yes", "on")
            elif key == "logo_width":
                config.logo_width = int(value)
            elif key == "logo_height":
                config.logo_height = int(value)
            else:
                return False
            return True
        except (ValueError, TypeError):
            return False

    @classmethod
    def get_all(cls) -> dict[str, str]:
        """Get all settings as a dictionary."""
        config = cls._get_config()
        return {
            "chart_period_days": str(config.chart_period_days),
            "auto_refresh": str(config.auto_refresh).lower(),
            "refresh_interval_seconds": str(config.refresh_interval_seconds),
            "number_format": config.number_format,
            "fundamentals_timeframe": config.fundamentals_timeframe,
            "table_rows": str(config.table_rows),
            "theme": config.theme_name,
            "chart_detail": config.chart_detail,
            "logo_enabled": str(config.logo_enabled).lower(),
            "logo_width": str(config.logo_width),
            "logo_height": str(config.logo_height),
        }

    @classmethod
    def reset_to_defaults(cls) -> None:
        """Reset all settings to defaults."""
        config = cls._get_config()
        config.chart_period_days = 60
        config.auto_refresh = False
        config.refresh_interval_seconds = 60
        config.number_format = "compact"
        config.fundamentals_timeframe = "quarterly"
        config.table_rows = 25
        config.theme_name = "osaka-jade"
        config.chart_detail = "normal"
        config.logo_enabled = True
        config.logo_width = 28
        config.logo_height = 10

    # Convenience properties for common settings
    @classmethod
    def chart_period_days(cls) -> int:
        """Get the chart period in days."""
        return cls._get_config().chart_period_days

    @classmethod
    def auto_refresh(cls) -> bool:
        """Get auto-refresh setting."""
        return cls._get_config().auto_refresh

    @classmethod
    def refresh_interval(cls) -> int:
        """Get refresh interval in seconds."""
        return cls._get_config().refresh_interval_seconds

    @classmethod
    def number_format(cls) -> str:
        """Get number format setting (compact or full)."""
        return cls._get_config().number_format

    @classmethod
    def fundamentals_timeframe(cls) -> str:
        """Get fundamentals timeframe (quarterly or annual)."""
        return cls._get_config().fundamentals_timeframe

    @classmethod
    def table_rows(cls) -> int:
        """Get number of rows to show in tables."""
        return cls._get_config().table_rows

    @classmethod
    def theme_name(cls) -> str:
        """Get current theme name."""
        return cls._get_config().theme_name

    @classmethod
    def chart_detail(cls) -> str:
        """Get chart detail level."""
        return cls._get_config().chart_detail
