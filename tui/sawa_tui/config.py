"""Configuration management for the TUI application.

Database-backed settings are now used via models/settings.py.
This module only handles environment variable lookups.
"""

import os

from sawa.utils.config import require_database_url as get_database_url  # noqa: F401


def get_zai_api_key(user_id: int | None = None) -> str | None:
    """
    Get the Z.AI API key.

    Checks in order:
    1. Environment variable ZAI_API_KEY
    2. Database user_settings (if user_id provided or can be determined)

    Returns:
        API key string or None if not configured
    """
    # Check environment first
    env_key = os.environ.get("ZAI_API_KEY")
    if env_key:
        return env_key

    # Try to get from database settings
    try:
        from sawa_tui.models.settings import SettingsManager
        from sawa_tui.models.users import UserManager

        # Get user_id if not provided
        if user_id is None:
            active_user = UserManager.get_active()
            if active_user:
                user_id = active_user.id

        if user_id is not None:
            db_key = SettingsManager.get(user_id, "zai_api_key")
            if db_key:
                return db_key
    except Exception:
        # Database not available or other error
        pass

    return None


def get_zai_api_url() -> str:
    """
    Get the Z.AI API endpoint URL.

    Returns:
        API endpoint URL (defaults to coding plan endpoint)
    """
    return os.environ.get("ZAI_API_URL", "https://api.z.ai/api/coding/paas/v4/chat/completions")


def get_tui_log_file():
    """Get log file path for compatibility."""
    from pathlib import Path

    log_dir = Path.home() / ".local" / "state" / "sawa-tui"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / "app.log"
