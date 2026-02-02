"""Number formatting and chart rendering utilities."""

from rich.text import Text

from sawa_tui.themes import get_theme

# Sparkline characters (8 levels)
SPARK_CHARS = "▁▂▃▄▅▆▇█"


def render_sparkline(values: list[float | None], width: int = 20) -> Text:
    """
    Render a sparkline from a list of values.

    Args:
        values: List of numeric values (None values are skipped)
        width: Target width of sparkline

    Returns:
        Rich Text object with colored sparkline
    """
    theme = get_theme()
    # Filter out None values
    clean_values = [v for v in values if v is not None]
    if not clean_values:
        return Text("-" * width, style=theme.text_muted)

    # Resample if needed
    if len(clean_values) > width:
        step = len(clean_values) / width
        sampled = []
        for i in range(width):
            idx = int(i * step)
            sampled.append(clean_values[idx])
        clean_values = sampled
    elif len(clean_values) < width:
        # Pad with last value or leave shorter
        pass

    min_val = min(clean_values)
    max_val = max(clean_values)
    val_range = max_val - min_val

    result = Text()
    for i, val in enumerate(clean_values):
        if val_range > 0:
            normalized = (val - min_val) / val_range
            char_idx = min(int(normalized * 7), 7)
        else:
            char_idx = 4  # Middle if all same

        # Color based on trend (compare to previous)
        if i > 0 and clean_values[i] > clean_values[i - 1]:
            style = theme.positive
        elif i > 0 and clean_values[i] < clean_values[i - 1]:
            style = theme.negative
        else:
            style = theme.warning

        result.append(SPARK_CHARS[char_idx], style=style)

    return result


def format_rate_as_pct(value: float | None) -> tuple[str, float | None]:
    """
    Format a rate value as percentage string.

    Handles both decimal (0.044 = 4.4%) and already-percentage (4.4 = 4.4%) formats.
    Returns (formatted_string, normalized_value_for_comparison).
    """
    if value is None:
        return "-", None
    # If value > 1, it's already a percentage (e.g., 4.4 means 4.4%)
    # If value <= 1, it's a decimal (e.g., 0.044 means 4.4%)
    if abs(value) > 1:
        return f"{value:.1f}%", value
    else:
        return f"{value * 100:.1f}%", value * 100


def render_trend_indicator(current: float | None, previous: float | None, width: int = 8) -> Text:
    """Render a trend arrow based on current vs previous value."""
    theme = get_theme()
    if current is None or previous is None:
        return Text(" " * width, style=theme.text_muted)

    if current > previous:
        pct = ((current - previous) / abs(previous)) * 100 if previous != 0 else 0
        text = f"▲{pct:+.1f}%"
        return Text(f"{text:<{width}}", style=theme.positive)
    elif current < previous:
        pct = ((current - previous) / abs(previous)) * 100 if previous != 0 else 0
        text = f"▼{pct:+.1f}%"
        return Text(f"{text:<{width}}", style=theme.negative)
    else:
        text = "► 0.0%"
        return Text(f"{text:<{width}}", style=theme.warning)


def format_number(value: float | int | None, decimals: int = 2, prefix: str = "") -> str:
    """Format a number for display with K/M/B suffixes."""
    if value is None:
        return "-"
    if abs(value) >= 1_000_000_000:
        return f"{prefix}{value / 1_000_000_000:.1f}B"
    if abs(value) >= 1_000_000:
        return f"{prefix}{value / 1_000_000:.1f}M"
    if abs(value) >= 1_000:
        return f"{prefix}{value / 1_000:.1f}K"
    return f"{prefix}{value:.{decimals}f}"


def format_change(value: float | None) -> Text:
    """Format a price change with color."""
    theme = get_theme()
    if value is None:
        return Text("-")
    if value >= 0:
        return Text(f"+{value:.2f}", style=theme.positive)
    return Text(f"{value:.2f}", style=theme.negative)


def format_pct_change(value: float | None) -> Text:
    """Format a percentage change with color."""
    theme = get_theme()
    if value is None:
        return Text("-")
    if value >= 0:
        return Text(f"+{value:.2f}%", style=theme.positive)
    return Text(f"{value:.2f}%", style=theme.negative)
