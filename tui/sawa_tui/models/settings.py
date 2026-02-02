"""User settings management.

Database-backed user settings with admin-managed default template.
"""

from sawa_tui.database import execute_query, execute_write


class SettingsManager:
    """
    Manager for user-specific settings stored in the database.

    Settings are stored per-user in the user_settings table.
    New users inherit settings from the default_settings template.
    """

    # Setting definitions with types and validation
    SETTINGS_SCHEMA = {
        "zai_api_key": {"type": "string", "default": ""},
        "chart_period_days": {"type": "int", "default": "60", "min": 1, "max": 730},
        "number_format": {"type": "enum", "default": "compact", "values": ["compact", "full"]},
        "fundamentals_timeframe": {
            "type": "enum",
            "default": "quarterly",
            "values": ["quarterly", "annual"],
        },
        "theme": {"type": "string", "default": "osaka-jade"},
        "chart_detail": {"type": "string", "default": "normal"},
        "logo_enabled": {"type": "bool", "default": "true"},
        "logo_width": {"type": "int", "default": "28", "min": 10, "max": 100},
        "logo_height": {"type": "int", "default": "10", "min": 5, "max": 50},
    }

    @staticmethod
    def get(user_id: int, key: str, default: str | None = None) -> str | None:
        """
        Get a setting value for a user.

        Args:
            user_id: User ID
            key: Setting key
            default: Default value if not found

        Returns:
            Setting value or default
        """
        sql = "SELECT value FROM user_settings WHERE user_id = %(user_id)s AND key = %(key)s"
        rows = execute_query(sql, {"user_id": user_id, "key": key})
        if rows:
            return rows[0]["value"]

        # Fall back to schema default
        if key in SettingsManager.SETTINGS_SCHEMA:
            return SettingsManager.SETTINGS_SCHEMA[key]["default"]

        return default

    @staticmethod
    def get_int(user_id: int, key: str, default: int = 0) -> int:
        """Get a setting value as integer."""
        value = SettingsManager.get(user_id, key)
        if value is None:
            return default
        try:
            return int(value)
        except ValueError:
            return default

    @staticmethod
    def get_bool(user_id: int, key: str, default: bool = False) -> bool:
        """Get a setting value as boolean."""
        value = SettingsManager.get(user_id, key)
        if value is None:
            return default
        return value.lower() in ("true", "1", "yes", "on")

    @staticmethod
    def set(user_id: int, key: str, value: str) -> tuple[bool, str]:
        """
        Set a setting value for a user.

        Args:
            user_id: User ID
            key: Setting key
            value: Setting value

        Returns:
            Tuple of (success, error_message)
        """
        # Validate key exists in schema
        if key not in SettingsManager.SETTINGS_SCHEMA:
            return False, f"Invalid setting key: {key}"

        # Validate value based on schema
        schema = SettingsManager.SETTINGS_SCHEMA[key]
        validation_result = SettingsManager._validate_value(key, value, schema)
        if not validation_result[0]:
            return validation_result

        sql = """
            INSERT INTO user_settings (user_id, key, value)
            VALUES (%(user_id)s, %(key)s, %(value)s)
            ON CONFLICT (user_id, key) DO UPDATE SET value = %(value)s, updated_at = NOW()
        """
        success = execute_write(sql, {"user_id": user_id, "key": key, "value": value}) > 0
        return (True, "") if success else (False, "Failed to save setting")

    @staticmethod
    def _validate_value(key: str, value: str, schema: dict) -> tuple[bool, str]:
        """Validate a setting value based on schema."""
        setting_type = schema["type"]

        if setting_type == "int":
            try:
                int_value = int(value)
                if "min" in schema and int_value < schema["min"]:
                    return False, f"{key} must be at least {schema['min']}"
                if "max" in schema and int_value > schema["max"]:
                    return False, f"{key} must be at most {schema['max']}"
            except ValueError:
                return False, f"{key} must be an integer"

        elif setting_type == "bool":
            if value.lower() not in ("true", "false", "1", "0", "yes", "no", "on", "off"):
                return False, f"{key} must be a boolean (true/false)"

        elif setting_type == "enum":
            if value not in schema["values"]:
                return False, f"{key} must be one of: {', '.join(schema['values'])}"

        return True, ""

    @staticmethod
    def get_all(user_id: int) -> dict[str, str]:
        """
        Get all settings for a user.

        Returns a dictionary with all settings, including defaults for unset values.
        """
        sql = "SELECT key, value FROM user_settings WHERE user_id = %(user_id)s"
        rows = execute_query(sql, {"user_id": user_id})

        # Start with all defaults
        settings = {
            key: schema["default"] for key, schema in SettingsManager.SETTINGS_SCHEMA.items()
        }

        # Override with user's actual settings
        for row in rows:
            settings[row["key"]] = row["value"]

        return settings

    @staticmethod
    def initialize_user_settings(user_id: int) -> bool:
        """
        Initialize settings for a new user by copying from default_settings.

        This is automatically called when creating a new user.

        Returns:
            True if successful
        """
        sql = """
            INSERT INTO user_settings (user_id, key, value)
            SELECT %(user_id)s, key, value
            FROM default_settings
            ON CONFLICT DO NOTHING
        """
        return execute_write(sql, {"user_id": user_id}) >= 0

    @staticmethod
    def reset_to_defaults(user_id: int) -> bool:
        """
        Reset a user's settings to the default template.

        Deletes all user settings and re-copies from default_settings.

        Returns:
            True if successful
        """
        # Delete all user settings
        delete_sql = "DELETE FROM user_settings WHERE user_id = %(user_id)s"
        execute_write(delete_sql, {"user_id": user_id})

        # Re-initialize from defaults
        return SettingsManager.initialize_user_settings(user_id)


class DefaultSettingsManager:
    """
    Manager for the default settings template (admin-managed).

    These settings are used as the starting point for new users.
    Admins can edit this template to control default preferences.
    """

    @staticmethod
    def get(key: str, default: str | None = None) -> str | None:
        """Get a default setting value."""
        sql = "SELECT value FROM default_settings WHERE key = %(key)s"
        rows = execute_query(sql, {"key": key})
        if rows:
            return rows[0]["value"]

        # Fall back to schema default
        if key in SettingsManager.SETTINGS_SCHEMA:
            return SettingsManager.SETTINGS_SCHEMA[key]["default"]

        return default

    @staticmethod
    def get_int(key: str, default: int = 0) -> int:
        """Get a default setting value as integer."""
        value = DefaultSettingsManager.get(key)
        if value is None:
            return default
        try:
            return int(value)
        except ValueError:
            return default

    @staticmethod
    def get_bool(key: str, default: bool = False) -> bool:
        """Get a default setting value as boolean."""
        value = DefaultSettingsManager.get(key)
        if value is None:
            return default
        return value.lower() in ("true", "1", "yes", "on")

    @staticmethod
    def set(key: str, value: str) -> tuple[bool, str]:
        """
        Set a default setting value (admin only).

        Args:
            key: Setting key
            value: Setting value

        Returns:
            Tuple of (success, error_message)
        """
        # Validate key exists in schema
        if key not in SettingsManager.SETTINGS_SCHEMA:
            return False, f"Invalid setting key: {key}"

        # Validate value based on schema
        schema = SettingsManager.SETTINGS_SCHEMA[key]
        validation_result = SettingsManager._validate_value(key, value, schema)
        if not validation_result[0]:
            return validation_result

        sql = """
            INSERT INTO default_settings (key, value)
            VALUES (%(key)s, %(value)s)
            ON CONFLICT (key) DO UPDATE SET value = %(value)s, updated_at = NOW()
        """
        success = execute_write(sql, {"key": key, "value": value}) > 0
        return (True, "") if success else (False, "Failed to save default setting")

    @staticmethod
    def get_all() -> dict[str, str]:
        """
        Get all default settings.

        Returns a dictionary with all settings, including schema defaults for unset values.
        """
        sql = "SELECT key, value FROM default_settings"
        rows = execute_query(sql)

        # Start with all schema defaults
        settings = {
            key: schema["default"] for key, schema in SettingsManager.SETTINGS_SCHEMA.items()
        }

        # Override with actual default_settings values
        for row in rows:
            settings[row["key"]] = row["value"]

        return settings

    @staticmethod
    def reset_to_schema_defaults() -> bool:
        """
        Reset all default settings to schema defaults.

        Returns:
            True if successful
        """
        # Delete all default settings
        delete_sql = "DELETE FROM default_settings"
        execute_write(delete_sql)

        # Re-insert schema defaults
        for key, schema in SettingsManager.SETTINGS_SCHEMA.items():
            DefaultSettingsManager.set(key, schema["default"])

        return True
