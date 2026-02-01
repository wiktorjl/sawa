"""User settings management."""

from sp500_tui.config import DEFAULT_SETTINGS
from sp500_tui.database import execute_query, execute_write


class SettingsManager:
    """Manager for user settings stored in the database."""

    _cache: dict[str, str] | None = None

    @classmethod
    def _load_all(cls) -> dict[str, str]:
        """Load all settings from database into cache."""
        if cls._cache is not None:
            return cls._cache

        sql = "SELECT key, value FROM user_settings"
        rows = execute_query(sql)
        cls._cache = {row["key"]: row["value"] for row in rows}

        # Fill in defaults for missing keys
        for key, default in DEFAULT_SETTINGS.items():
            if key not in cls._cache:
                cls._cache[key] = default

        return cls._cache

    @classmethod
    def invalidate_cache(cls) -> None:
        """Invalidate the settings cache."""
        cls._cache = None

    @classmethod
    def get(cls, key: str, default: str | None = None) -> str | None:
        """
        Get a setting value.

        Args:
            key: Setting key
            default: Default value if not found

        Returns:
            Setting value or default
        """
        settings = cls._load_all()
        return settings.get(key, default or DEFAULT_SETTINGS.get(key))

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
            key: Setting key
            value: Setting value

        Returns:
            True if successful
        """
        sql = """
            INSERT INTO user_settings (key, value)
            VALUES (%(key)s, %(value)s)
            ON CONFLICT (key) DO UPDATE SET value = %(value)s
        """
        result = execute_write(sql, {"key": key, "value": value})
        cls.invalidate_cache()
        return result > 0

    @classmethod
    def get_all(cls) -> dict[str, str]:
        """Get all settings as a dictionary."""
        return cls._load_all().copy()

    @classmethod
    def reset_to_defaults(cls) -> None:
        """Reset all settings to defaults."""
        for key, value in DEFAULT_SETTINGS.items():
            cls.set(key, value)
        cls.invalidate_cache()

    # Convenience properties for common settings
    @classmethod
    def chart_period_days(cls) -> int:
        """Get the chart period in days."""
        return cls.get_int("chart_period_days", 60)

    @classmethod
    def auto_refresh(cls) -> bool:
        """Get auto-refresh setting."""
        return cls.get_bool("auto_refresh", False)

    @classmethod
    def refresh_interval(cls) -> int:
        """Get refresh interval in seconds."""
        return cls.get_int("refresh_interval_seconds", 60)

    @classmethod
    def number_format(cls) -> str:
        """Get number format setting (compact or full)."""
        return cls.get("number_format", "compact") or "compact"

    @classmethod
    def fundamentals_timeframe(cls) -> str:
        """Get fundamentals timeframe (quarterly or annual)."""
        return cls.get("fundamentals_timeframe", "quarterly") or "quarterly"

    @classmethod
    def table_rows(cls) -> int:
        """Get number of rows to show in tables."""
        return cls.get_int("table_rows", 25)
