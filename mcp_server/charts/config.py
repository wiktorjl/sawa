"""Chart configuration management."""

import os
import shutil
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from sawa.utils.xdg import load_config

# App name for XDG paths
APP_NAME = "sp500-tools"


class ChartDetail(Enum):
    """Chart detail levels."""

    COMPACT = "compact"  # Minimal, 80+ chars
    NORMAL = "normal"  # Standard, 100+ chars
    DETAILED = "detailed"  # Full, 120+ chars


# Minimum width requirements for each detail level
DETAIL_MIN_WIDTH = {
    ChartDetail.COMPACT: 80,
    ChartDetail.NORMAL: 100,
    ChartDetail.DETAILED: 120,
}


@dataclass
class ChartConfig:
    """Configuration for chart rendering."""

    # Theme name
    theme: str = "osaka-jade"

    # Detail level
    detail: ChartDetail = ChartDetail.NORMAL

    # Terminal width (0 = auto-detect)
    width: int = 0

    # Whether colors are enabled
    colors_enabled: bool = True

    # Cached terminal width
    _terminal_width: int = field(default=0, repr=False)

    def get_width(self) -> int:
        """
        Get effective terminal width.

        Returns configured width, or auto-detected width.
        """
        if self.width > 0:
            return self.width

        if self._terminal_width == 0:
            size = shutil.get_terminal_size(fallback=(100, 24))
            self._terminal_width = size.columns

        return self._terminal_width

    def get_min_width(self) -> int:
        """Get minimum width required for current detail level."""
        return DETAIL_MIN_WIDTH.get(self.detail, 80)

    def is_width_sufficient(self) -> bool:
        """Check if terminal width is sufficient for current detail level."""
        return self.get_width() >= self.get_min_width()

    def get_best_detail(self) -> ChartDetail:
        """Get the best detail level for current terminal width."""
        width = self.get_width()

        if width >= DETAIL_MIN_WIDTH[ChartDetail.DETAILED]:
            return ChartDetail.DETAILED
        elif width >= DETAIL_MIN_WIDTH[ChartDetail.NORMAL]:
            return ChartDetail.NORMAL
        else:
            return ChartDetail.COMPACT

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ChartConfig":
        """Create ChartConfig from dictionary."""
        config = cls()

        if "theme" in data:
            config.theme = str(data["theme"])

        if "detail" in data:
            detail_str = str(data["detail"]).lower()
            try:
                config.detail = ChartDetail(detail_str)
            except ValueError:
                pass  # Keep default

        if "width" in data:
            try:
                config.width = int(data["width"])
            except (ValueError, TypeError):
                pass

        if "colors_enabled" in data:
            config.colors_enabled = bool(data["colors_enabled"])

        return config


# Default configuration
DEFAULT_CONFIG: dict[str, Any] = {
    "theme": {
        "name": "osaka-jade",
    },
    "charts": {
        "detail": "normal",
        "min_width": 80,
        "colors_enabled": True,
    },
    "display": {
        "number_format": "compact",
        "table_rows": 25,
    },
    "fundamentals": {
        "default_timeframe": "quarterly",
    },
    "watchlist": {
        "chart_period_days": 60,
        "auto_refresh": False,
        "refresh_interval_seconds": 60,
    },
}


def get_chart_config() -> ChartConfig:
    """
    Get chart configuration from environment and config file.

    Priority (highest to lowest):
    1. Environment variables (MCP_CHART_THEME, MCP_CHART_DETAIL, etc.)
    2. XDG config file (~/.config/sp500-tools/config.toml)
    3. Default values

    Returns:
        ChartConfig instance
    """
    # Load from XDG config file
    file_config = load_config(APP_NAME, DEFAULT_CONFIG)

    config = ChartConfig()

    # Apply config file settings
    if "theme" in file_config and "name" in file_config["theme"]:
        config.theme = file_config["theme"]["name"]

    if "charts" in file_config:
        charts = file_config["charts"]
        if "detail" in charts:
            try:
                config.detail = ChartDetail(str(charts["detail"]).lower())
            except ValueError:
                pass
        if "colors_enabled" in charts:
            config.colors_enabled = bool(charts["colors_enabled"])

    # Override with environment variables
    if env_theme := os.environ.get("MCP_CHART_THEME"):
        config.theme = env_theme

    if env_detail := os.environ.get("MCP_CHART_DETAIL"):
        try:
            config.detail = ChartDetail(env_detail.lower())
        except ValueError:
            pass

    if env_width := os.environ.get("MCP_CHART_WIDTH"):
        try:
            config.width = int(env_width)
        except ValueError:
            pass

    # Check NO_COLOR standard environment variable
    if os.environ.get("NO_COLOR"):
        config.colors_enabled = False

    return config
