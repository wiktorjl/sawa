"""Layout and terminal width management."""

import shutil
from dataclasses import dataclass

from ..config import ChartConfig, ChartDetail, get_chart_config


@dataclass
class Layout:
    """Layout configuration for chart rendering."""

    # Total available width
    width: int

    # Detail level
    detail: ChartDetail

    # Whether colors are enabled
    colors_enabled: bool

    # Content width (excluding borders)
    @property
    def content_width(self) -> int:
        """Get width available for content (excluding 2-char border)."""
        return max(self.width - 4, 40)

    # Sparkline width based on detail level
    @property
    def sparkline_width(self) -> int:
        """Get appropriate sparkline width for detail level."""
        if self.detail == ChartDetail.DETAILED:
            return min(60, self.content_width - 40)
        elif self.detail == ChartDetail.NORMAL:
            return min(40, self.content_width - 30)
        else:
            return min(25, self.content_width - 20)

    # Whether to show extended data
    @property
    def show_extended(self) -> bool:
        """Whether to show extended information."""
        return self.detail in (ChartDetail.NORMAL, ChartDetail.DETAILED)

    # Whether to show full charts
    @property
    def show_full_charts(self) -> bool:
        """Whether to show full-size charts."""
        return self.detail == ChartDetail.DETAILED

    # Number of columns for data tables
    @property
    def table_columns(self) -> int:
        """Get number of columns to show in tables."""
        if self.detail == ChartDetail.DETAILED:
            return 10
        elif self.detail == ChartDetail.NORMAL:
            return 6
        else:
            return 4


def get_terminal_width(fallback: int = 100) -> int:
    """
    Get current terminal width.

    Args:
        fallback: Width to use if detection fails

    Returns:
        Terminal width in columns
    """
    try:
        size = shutil.get_terminal_size(fallback=(fallback, 24))
        return size.columns
    except (ValueError, OSError):
        return fallback


def get_layout(config: ChartConfig | None = None) -> Layout:
    """
    Get layout configuration.

    Args:
        config: Chart configuration. If None, loads from environment/config file.

    Returns:
        Layout instance
    """
    if config is None:
        config = get_chart_config()

    width = config.get_width()

    # Auto-adjust detail level if width is insufficient
    if not config.is_width_sufficient():
        detail = config.get_best_detail()
    else:
        detail = config.detail

    return Layout(
        width=width,
        detail=detail,
        colors_enabled=config.colors_enabled,
    )


def calculate_column_widths(
    total_width: int,
    columns: list[tuple[str, int | None]],
    min_width: int = 5,
    padding: int = 1,
) -> list[int]:
    """
    Calculate column widths to fit available space.

    Args:
        total_width: Total available width
        columns: List of (name, preferred_width) tuples. None = flexible.
        min_width: Minimum column width
        padding: Padding between columns

    Returns:
        List of calculated column widths
    """
    num_columns = len(columns)
    if num_columns == 0:
        return []

    # Account for padding
    available = total_width - (padding * (num_columns - 1))

    # Calculate fixed and flexible widths
    fixed_total = 0
    flexible_count = 0

    for _, width in columns:
        if width is not None:
            fixed_total += width
        else:
            flexible_count += 1

    # Distribute remaining space to flexible columns
    remaining = available - fixed_total
    flexible_width = remaining // flexible_count if flexible_count > 0 else 0

    # Build result
    widths = []
    for _, width in columns:
        if width is not None:
            widths.append(max(width, min_width))
        else:
            widths.append(max(flexible_width, min_width))

    return widths
