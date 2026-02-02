"""Base view rendering: header, footer, and main render function."""

from rich.align import Align
from rich.console import Console
from rich.layout import Layout
from rich.panel import Panel
from rich.text import Text

from sawa_tui.components import COLORS
from sawa_tui.state import AppState, View
from sawa_tui.themes import get_theme


def render_header(state: AppState) -> Panel:
    """Render the top navigation header."""
    theme = get_theme()

    views = [
        ("F1", "Stocks", View.STOCKS),
        ("F2", "Fundamentals", View.FUNDAMENTALS),
        ("F3", "Economy", View.ECONOMY),
        ("F4", "Settings", View.SETTINGS),
        ("F5", "Glossary", View.GLOSSARY),
        ("F6", "Screener", View.SCREENER),
    ]

    parts = [Text(" S&P 500 ", style=f"bold {theme.primary}"), Text(" | ")]

    for key, name, view in views:
        # Stock detail and news fullscreen are sub-views of Stocks
        is_current = state.current_view == view or (
            view == View.STOCKS and state.current_view in (View.STOCK_DETAIL, View.NEWS_FULLSCREEN)
        )
        if is_current:
            parts.append(Text(f" {key}:{name}", style=COLORS["tab_active"]))

            # Add breadcrumb for sub-views
            if state.current_view == View.STOCK_DETAIL:
                parts.append(Text(" > ", style=COLORS["tab_active"]))
                parts.append(Text(f"{state.detail_ticker}", style=COLORS["tab_active"]))
            elif state.current_view == View.NEWS_FULLSCREEN:
                parts.append(Text(" > ", style=COLORS["tab_active"]))
                parts.append(Text(f"{state.detail_ticker} > News", style=COLORS["tab_active"]))

            parts.append(Text(" ", style=""))
        else:
            parts.append(Text(f" {key}:{name} ", style=theme.text_muted))

    parts.append(Text(" | ", style=theme.text_muted))

    # Context-sensitive global keys
    if state.current_view in (View.STOCK_DETAIL, View.NEWS_FULLSCREEN):
        parts.append(Text("Esc:Back  ", style=theme.text_muted))
    parts.append(Text("q:Quit  r:Refresh  ?:Help", style=theme.text_muted))

    header_text = Text()
    for part in parts:
        header_text.append_text(part)

    return Panel(header_text, border_style=theme.border, height=3, padding=(0, 1))


def render_footer(state: AppState) -> Panel:
    """Render the bottom status bar."""
    theme = get_theme()

    if state.input_mode:
        # Input mode
        text = Text()
        text.append(state.input_prompt, style=theme.warning)
        text.append(": ")
        text.append(state.input_value, style=theme.text_bright)
        text.append("_", style="blink")
        text.append("  [Esc] Cancel", style=theme.text_muted)
    elif state.message:
        # Status message
        style = theme.negative if state.message_error else theme.positive
        text = Text(state.message, style=style)
    else:
        # Default help based on current view
        text = _get_view_help(state)

    return Panel(text, border_style=theme.border, height=3, padding=(0, 1))


def _get_view_help(state: AppState) -> Text:
    """Get help text for current view."""
    theme = get_theme()
    text = Text()

    # User info (always show)
    if state.current_user:
        text.append(f"{state.current_user.name}", style=theme.info)
        if state.current_user.is_admin:
            text.append(" [Admin]", style=theme.warning)
        text.append("  ", style="")
        text.append("Ctrl+U", style=theme.warning)
        text.append(":Switch  ", style=theme.text_muted)
        if state.current_user.is_admin:
            text.append("Ctrl+P", style=theme.warning)
            text.append(":Manage  ", style=theme.text_muted)

    if state.current_view == View.STOCKS:
        if state.focus_sidebar:
            text.append("Tab", style=theme.warning)
            text.append(":Focus Stocks  ", style=theme.text_muted)
            text.append("Enter", style=theme.warning)
            text.append(":Select  ", style=theme.text_muted)
            text.append("n", style=theme.warning)
            text.append(":New  ", style=theme.text_muted)
            text.append("d", style=theme.warning)
            text.append(":Delete  ", style=theme.text_muted)
        else:
            text.append("Tab", style=theme.warning)
            text.append(":Focus Lists  ", style=theme.text_muted)
            text.append("Enter", style=theme.warning)
            text.append(":View Detail  ", style=theme.text_muted)
            text.append("/", style=theme.warning)
            text.append(":Filter  ", style=theme.text_muted)
            text.append("a", style=theme.warning)
            text.append(":Add  ", style=theme.text_muted)
            text.append("x", style=theme.warning)
            text.append(":Remove  ", style=theme.text_muted)
        text.append("?", style=theme.warning)
        text.append(":Help", style=theme.text_muted)

    elif state.current_view == View.STOCK_DETAIL:
        text.append("Esc", style=theme.warning)
        text.append(":Back  ", style=theme.text_muted)
        text.append("a", style=theme.warning)
        text.append(":Add to Watchlist  ", style=theme.text_muted)
        text.append("v", style=theme.warning)
        text.append(":Toggle News  ", style=theme.text_muted)
        text.append("V", style=theme.warning)
        text.append(":Fullscreen News  ", style=theme.text_muted)
        text.append("?", style=theme.warning)
        text.append(":Help", style=theme.text_muted)

    elif state.current_view == View.NEWS_FULLSCREEN:
        text.append("Esc", style=theme.warning)
        text.append(":Back  ", style=theme.text_muted)
        text.append("Up/Down", style=theme.warning)
        text.append(":Navigate  ", style=theme.text_muted)
        text.append("Enter", style=theme.warning)
        text.append(":Open URL", style=theme.text_muted)

    elif state.current_view == View.FUNDAMENTALS:
        text.append("1", style=theme.warning)
        text.append(":Income  ", style=theme.text_muted)
        text.append("2", style=theme.warning)
        text.append(":Balance  ", style=theme.text_muted)
        text.append("3", style=theme.warning)
        text.append(":Cash Flow  ", style=theme.text_muted)
        text.append("t", style=theme.warning)
        text.append(":Toggle Q/A  ", style=theme.text_muted)
        text.append("/", style=theme.warning)
        text.append(":Search Ticker", style=theme.text_muted)

    elif state.current_view == View.ECONOMY:
        text.append("1", style=theme.warning)
        text.append(":Yields  ", style=theme.text_muted)
        text.append("2", style=theme.warning)
        text.append(":Inflation  ", style=theme.text_muted)
        text.append("3", style=theme.warning)
        text.append(":Labor", style=theme.text_muted)

    elif state.current_view == View.SETTINGS:
        text.append("1-5", style=theme.warning)
        text.append(":Switch Tab  ", style=theme.text_muted)
        text.append("Up/Down", style=theme.warning)
        text.append(":Navigate  ", style=theme.text_muted)
        text.append("Enter", style=theme.warning)
        text.append(":Select  ", style=theme.text_muted)
        text.append("?", style=theme.warning)
        text.append(":Help", style=theme.text_muted)

    elif state.current_view == View.GLOSSARY:
        if state.glossary_show_regen_menu:
            text.append("1-4", style=theme.warning)
            text.append(":Select  ", style=theme.text_muted)
            text.append("c", style=theme.warning)
            text.append(":Custom  ", style=theme.text_muted)
            text.append("Esc", style=theme.warning)
            text.append(":Cancel", style=theme.text_muted)
        elif state.glossary_loading:
            text.append("Generating definition...", style=theme.warning)
        else:
            text.append("/", style=theme.warning)
            text.append(":Search  ", style=theme.text_muted)
            text.append("Enter", style=theme.warning)
            text.append(":Generate  ", style=theme.text_muted)
            text.append("g", style=theme.warning)
            text.append(":Regen  ", style=theme.text_muted)
            text.append("n", style=theme.warning)
            text.append(":Add  ", style=theme.text_muted)
            text.append("d", style=theme.warning)
            text.append(":Delete  ", style=theme.text_muted)
            text.append("1-5", style=theme.warning)
            text.append(":Related", style=theme.text_muted)

    elif state.current_view == View.SCREENER:
        text.append("/", style=theme.warning)
        text.append(":Filter  ", style=theme.text_muted)
        text.append("Enter", style=theme.warning)
        text.append(":Detail  ", style=theme.text_muted)
        text.append("Up/Down", style=theme.warning)
        text.append(":Nav", style=theme.text_muted)
        text.append("  Vars: pe, pb, cap, yield, sector, roe, eps", style=theme.info)

    elif state.current_view == View.USER_SWITCHER:
        text.append("Up/Down", style=theme.warning)
        text.append(":Navigate  ", style=theme.text_muted)
        text.append("Enter", style=theme.warning)
        text.append(":Switch  ", style=theme.text_muted)
        text.append("Esc", style=theme.warning)
        text.append(":Cancel", style=theme.text_muted)

    elif state.current_view == View.USER_MANAGEMENT:
        text.append("Enter", style=theme.warning)
        text.append(":Switch  ", style=theme.text_muted)
        text.append("n", style=theme.warning)
        text.append(":New  ", style=theme.text_muted)
        text.append("d", style=theme.warning)
        text.append(":Delete  ", style=theme.text_muted)
        text.append("r", style=theme.warning)
        text.append(":Rename  ", style=theme.text_muted)
        text.append("t", style=theme.warning)
        text.append(":Toggle Admin  ", style=theme.text_muted)
        text.append("Esc", style=theme.warning)
        text.append(":Close", style=theme.text_muted)

    return text


def render_app(console: Console, state: AppState) -> None:
    """Render the complete application."""
    # Import view functions here to avoid circular imports
    from sawa_tui.views.economy import render_economy_view
    from sawa_tui.views.fundamentals import render_fundamentals_view
    from sawa_tui.views.glossary import render_glossary_view
    from sawa_tui.views.settings import render_settings_view
    from sawa_tui.views.stocks import (
        render_news_fullscreen_view,
        render_stock_detail_view,
        render_stocks_view,
    )
    from sawa_tui.views.users import render_user_management_view, render_user_switcher_view
    from sawa_tui.views_screener import render_screener_view

    layout = Layout()

    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body"),
        Layout(name="footer", size=3),
    )

    # Header
    layout["header"].update(render_header(state))

    # Body based on current view
    if state.current_view == View.STOCKS:
        layout["body"].update(render_stocks_view(state))
    elif state.current_view == View.STOCK_DETAIL:
        layout["body"].update(render_stock_detail_view(state))
    elif state.current_view == View.NEWS_FULLSCREEN:
        layout["body"].update(render_news_fullscreen_view(state))
    elif state.current_view == View.FUNDAMENTALS:
        layout["body"].update(render_fundamentals_view(state))
    elif state.current_view == View.ECONOMY:
        layout["body"].update(render_economy_view(state))
    elif state.current_view == View.SETTINGS:
        layout["body"].update(render_settings_view(state))
    elif state.current_view == View.GLOSSARY:
        layout["body"].update(render_glossary_view(state))
    elif state.current_view == View.SCREENER:
        layout["body"].update(render_screener_view(state))
    elif state.current_view == View.USER_MANAGEMENT:
        layout["body"].update(render_user_management_view(state))
    elif state.current_view == View.USER_SWITCHER:
        layout["body"].update(render_user_switcher_view(state))

    # Footer
    layout["footer"].update(render_footer(state))

    # Move cursor to home and overwrite (no flicker)
    # Using ANSI escape: \033[H moves cursor to row 1, col 1
    print("\033[H", end="")

    # If help overlay is active, render it instead of the main layout
    if state.show_help_overlay:
        from sawa_tui.help import render_help_overlay

        # First render the main layout (background)
        console.print(layout)

        # Then overlay the centered help on top
        help_panel = render_help_overlay(state.current_view, state.term_width, state.term_height)

        # Create a layout for centered help
        overlay_layout = Layout()
        overlay_layout.update(Align.center(help_panel, vertical="middle"))

        # Position cursor and render overlay
        print("\033[H", end="")
        console.print(overlay_layout)
    else:
        console.print(layout)
