"""Reusable UI components for consistent styling."""

from rich.text import Text

# Standardized dimensions
SIDEBAR_WIDTH = 28
HEADER_HEIGHT = 3
FOOTER_HEIGHT = 3
TAB_HEADER_HEIGHT = 4

# Colors - consistent palette across the TUI
COLORS = {
    "selected": "bold green on dark_green",
    "focused_border": "green",
    "unfocused_border": "dim",
    "tab_active": "bold black on green",
    "tab_inactive": "dim",
    "positive": "green",
    "negative": "red",
    "neutral": "yellow",
    "label": "yellow",
    "hint": "dim",
    "link": "cyan",
    "error": "red",
}


def render_tabs(tabs: list[tuple[str, str, bool]], extra: str = "") -> Text:
    """
    Render horizontal tab bar.

    Args:
        tabs: List of (key, label, is_active) tuples
        extra: Additional text to show after tabs (e.g., "[t] Quarterly")

    Returns:
        Rich Text object with formatted tabs

    Example:
        >>> tabs = [("1", "Income", True), ("2", "Balance", False)]
        >>> render_tabs(tabs, "[t] Quarterly")
    """
    text = Text()

    for key, label, is_active in tabs:
        if is_active:
            text.append(f" [{key}]{label} ", style=COLORS["tab_active"])
        else:
            text.append(f" [{key}]{label} ", style=COLORS["tab_inactive"])

    if extra:
        text.append("   ")
        text.append(extra, style="yellow")

    return text


def render_empty_state(message: str, hint: str = "") -> Text:
    """
    Render a consistent empty state message.

    Args:
        message: Main message (e.g., "No data available")
        hint: Optional hint (e.g., "Press n to create one")

    Returns:
        Rich Text object with centered empty state

    Example:
        >>> render_empty_state("No watchlists", "Press n to create one")
    """
    text = Text()
    text.append("\n\n")
    text.append(f"  {message}\n", style="dim")
    if hint:
        text.append(f"\n  {hint}", style=COLORS["hint"])
    text.append("\n\n")
    return text


def render_scroll_indicator(current: int, total: int, visible: int) -> str:
    """
    Generate scroll indicator string.

    Args:
        current: Current scroll offset (0-based)
        total: Total number of items
        visible: Number of visible items

    Returns:
        String like "(1-20 of 45) ^v" with appropriate indicators

    Example:
        >>> render_scroll_indicator(0, 100, 20)
        '(1-20 of 100) v'
        >>> render_scroll_indicator(80, 100, 20)
        '(81-100 of 100) ^'
    """
    if total <= visible:
        return f"({total})" if total > 0 else ""

    start = current + 1
    end = min(current + visible, total)

    indicators = ""
    if current > 0:
        indicators += "^"
    if current + visible < total:
        indicators += "v"

    return f"({start}-{end} of {total}){' ' + indicators if indicators else ''}"


def panel_title(title: str, is_focused: bool, scroll_info: str = "") -> str:
    """
    Format panel title with focus indicator.

    Args:
        title: Panel title text
        is_focused: Whether panel is currently focused
        scroll_info: Optional scroll indicator (from render_scroll_indicator)

    Returns:
        Formatted title string with Rich markup

    Example:
        >>> panel_title("WATCHLISTS", True, "(1-10 of 25) v")
        '[yellow]> WATCHLISTS[/] (1-10 of 25) v'
    """
    prefix = "> " if is_focused else "  "
    suffix = f" {scroll_info}" if scroll_info else ""
    return f"[yellow]{prefix}{title}[/]{suffix}"
