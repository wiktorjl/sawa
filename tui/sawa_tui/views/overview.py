"""Company AI overview panel for stock detail view."""

from rich.panel import Panel
from rich.text import Text

from sawa_tui.components import render_scroll_indicator
from sawa_tui.state import AppState
from sawa_tui.themes import get_theme

# Panel height minus borders and padding
OVERVIEW_VISIBLE_LINES = 15


def render_overview_panel(state: AppState) -> Panel:
    """Render the AI company overview panel."""
    theme = get_theme()

    # Show regeneration menu if active
    if state.detail_overview_show_regen_menu:
        return _render_overview_regen_menu(state)

    # Loading state with streaming content
    if state.detail_overview_loading:
        content = Text()
        content.append(" Generating AI overview...", style=f"bold {theme.warning}")
        content.append("\n\n", style=theme.text_muted)

        # Progress indicator
        content.append(" ", style=theme.text_muted)
        content.append("[", style=theme.text_muted)
        content.append("=" * 20, style=theme.positive)
        content.append(">", style=f"bold {theme.positive}")
        content.append(" " * 10, style=theme.text_muted)
        content.append("]", style=theme.text_muted)
        content.append("\n\n", style=theme.text_muted)

        # Show streaming content as it arrives
        if state.detail_overview_stream_content:
            # Truncate for display, show last portion
            stream = state.detail_overview_stream_content
            if len(stream) > 800:
                stream = "..." + stream[-800:]
            content.append(stream, style=theme.text_muted)

        return Panel(
            content,
            title=f"[{theme.warning}]Generating AI Overview...[/]",
            border_style=theme.warning,
        )

    # Error state
    if state.detail_overview_error:
        content = Text()
        content.append(" Error\n\n", style=f"bold {theme.negative}")
        content.append(f" {state.detail_overview_error}\n\n", style=theme.negative)
        content.append(" Press 'o' to retry", style=theme.text_muted)

        return Panel(
            content,
            title=f"[{theme.negative}]Error[/]",
            border_style=theme.negative,
        )

    # No overview yet
    overview = state.detail_overview
    if not overview:
        content = Text()
        content.append("\n No AI overview available.\n", style=theme.text_muted)
        content.append(" Press 'o' to generate.\n", style=theme.text_muted)

        return Panel(
            content,
            title=f"[{theme.header}]AI Overview[/]",
            border_style=theme.text_muted,
        )

    # Build content lines for scrolling
    lines = _build_overview_lines(overview, theme)
    total_lines = len(lines)

    # Apply scroll offset
    scroll = state.detail_overview_scroll
    max_scroll = max(0, total_lines - OVERVIEW_VISIBLE_LINES)
    if scroll > max_scroll:
        scroll = max_scroll
        state.detail_overview_scroll = scroll

    visible_lines = lines[scroll : scroll + OVERVIEW_VISIBLE_LINES]

    # Build content from visible lines
    content = Text()
    for line_text, line_style in visible_lines:
        content.append(line_text + "\n", style=line_style)

    # Scroll indicator
    scroll_info = ""
    if total_lines > OVERVIEW_VISIBLE_LINES:
        scroll_info = render_scroll_indicator(scroll, total_lines, OVERVIEW_VISIBLE_LINES)
        scroll_info += " [Up/Down to scroll]"

    return Panel(
        content,
        title=f"[{theme.header}]AI Overview - {overview.ticker}[/] {scroll_info}",
        border_style=theme.border,
    )


def _build_overview_lines(overview, theme) -> list[tuple[str, str]]:
    """Build list of (text, style) tuples for each line."""
    lines = []

    # Main Product
    lines.append((" MAIN PRODUCT", f"bold {theme.info}"))
    for line in _wrap_text(overview.main_product, 95).split("\n"):
        lines.append((f" {line}", theme.text))
    lines.append(("", theme.text_muted))

    # Revenue Model
    lines.append((" REVENUE MODEL", f"bold {theme.info}"))
    for line in _wrap_text(overview.revenue_model, 95).split("\n"):
        lines.append((f" {line}", theme.text))
    lines.append(("", theme.text_muted))

    # Headwinds
    lines.append((" HEADWINDS", f"bold {theme.negative}"))
    for i, hw in enumerate(overview.headwinds[:3]):
        wrapped = _wrap_text(hw, 90)
        for j, line in enumerate(wrapped.split("\n")):
            prefix = " - " if j == 0 else "   "
            lines.append((f"{prefix}{line}", theme.text))
    lines.append(("", theme.text_muted))

    # Tailwinds
    lines.append((" TAILWINDS", f"bold {theme.positive}"))
    for i, tw in enumerate(overview.tailwinds[:3]):
        wrapped = _wrap_text(tw, 90)
        for j, line in enumerate(wrapped.split("\n")):
            prefix = " + " if j == 0 else "   "
            lines.append((f"{prefix}{line}", theme.text))
    lines.append(("", theme.text_muted))

    # Sector Outlook
    lines.append((" SECTOR OUTLOOK", f"bold {theme.info}"))
    for line in _wrap_text(overview.sector_outlook, 95).split("\n"):
        lines.append((f" {line}", theme.text))
    lines.append(("", theme.text_muted))

    # Competitive Position
    lines.append((" COMPETITIVE POSITION", f"bold {theme.info}"))
    for line in _wrap_text(overview.competitive_position, 95).split("\n"):
        lines.append((f" {line}", theme.text))

    # Metadata
    if overview.custom_prompt:
        lines.append(("", theme.text_muted))
        lines.append((f" [Custom: {overview.custom_prompt[:40]}...]", theme.text_muted))

    if overview.is_user_override:
        lines.append((" [User Override]", theme.text_muted))

    return lines


def _render_overview_regen_menu(state: AppState) -> Panel:
    """Render the regeneration options menu."""
    theme = get_theme()
    content = Text()
    content.append("\n Regenerate overview with:\n\n", style=f"bold {theme.warning}")

    options = [
        ("1", "More bullish", "Focus on growth potential and positive catalysts"),
        ("2", "More bearish", "Focus on risks and challenges"),
        ("3", "Technical detail", "Add more product/technical details"),
        ("4", "Valuation focus", "Add valuation and price context"),
        ("c", "Custom", "Enter your own instructions"),
    ]

    for key, label, desc in options:
        content.append(f"  [{key}] ", style=theme.warning)
        content.append(f"{label}\n", style=f"bold {theme.text}")
        content.append(f"      {desc}\n\n", style=theme.text_muted)

    content.append("\n  Press Esc to cancel", style=theme.text_muted)

    ticker = state.detail_ticker or "Overview"
    return Panel(
        content,
        title=f"[{theme.warning}]Regenerate: {ticker}[/]",
        border_style=theme.warning,
    )


def _wrap_text(text: str, max_width: int) -> str:
    """Simple text wrapping for long content."""
    if not text:
        return ""
    if len(text) <= max_width:
        return text

    words = text.split()
    lines = []
    current_line = ""

    for word in words:
        if len(current_line) + len(word) + 1 <= max_width:
            if current_line:
                current_line += " " + word
            else:
                current_line = word
        else:
            if current_line:
                lines.append(current_line)
            current_line = word

    if current_line:
        lines.append(current_line)

    return "\n".join(lines)
