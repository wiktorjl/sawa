"""View rendering using Rich."""

from rich.console import Console
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from sp500_tui.components import (
    COLORS,
    SIDEBAR_WIDTH,
    TAB_HEADER_HEIGHT,
    panel_title,
    render_empty_state,
    render_scroll_indicator,
    render_tabs,
)
from sp500_tui.config import get_tui_config
from sp500_tui.state import AppState, EconomyTab, FundamentalsTab, SettingsCategory, View
from sp500_tui.themes import get_theme
from sp500_tui.views_screener import render_screener_view

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
    """Format a number for display."""
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


# =============================================================================
# HEADER / FOOTER
# =============================================================================


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


# =============================================================================
# STOCKS VIEW
# =============================================================================


def render_stocks_view(state: AppState) -> Layout:
    """Render the stocks/watchlist view."""
    layout = Layout()

    layout.split_row(
        Layout(name="sidebar", size=SIDEBAR_WIDTH),
        Layout(name="main"),
    )

    # Sidebar: watchlist list
    layout["sidebar"].update(_render_watchlist_sidebar(state))

    # Main: stock table
    layout["main"].update(_render_stock_table(state))

    return layout


def _render_watchlist_sidebar(state: AppState) -> Panel:
    """Render the watchlist sidebar."""
    theme = get_theme()
    lines = []
    visible_rows = state.get_visible_watchlist_rows()
    start = state.watchlist_scroll_offset
    end = start + visible_rows

    for i, wl in enumerate(state.watchlists[start:end], start=start):
        prefix = "* " if wl.is_default else "  "
        name = f"{prefix}{wl.name} ({wl.symbol_count})"

        if i == state.selected_watchlist_idx and state.focus_sidebar:
            lines.append(Text(name, style=f"{theme.selected_text} {theme.selected}"))
        elif i == state.selected_watchlist_idx:
            lines.append(Text(name, style=theme.primary))
        else:
            lines.append(Text(name, style=theme.text_muted))

    content = (
        Text("\n").join(lines)
        if lines
        else render_empty_state("No watchlists", "Press n to create one")
    )

    # Calculate scroll info
    total = len(state.watchlists)
    scroll_info = render_scroll_indicator(start, total, visible_rows) if total > 0 else ""

    # Use focused panel title
    title = panel_title("WATCHLISTS", state.focus_sidebar, scroll_info)

    return Panel(
        content,
        title=title,
        title_align="left",
        border_style=theme.border_focus if state.focus_sidebar else theme.text_muted,
        height=None,
    )


def _render_stock_table(state: AppState) -> Panel:
    """Render the stock table."""
    theme = get_theme()
    wl = state.current_watchlist()

    # Get filtered stocks
    stocks = state.get_filtered_stocks()

    # Build title with filter indicator
    title_parts = [wl.name if wl else "No Watchlist"]
    if state.stock_filter:
        title_parts.append(f"(filtered: {state.stock_filter})")
    title = " ".join(title_parts)

    table = Table(
        show_header=True,
        header_style=f"bold {theme.header}",
        expand=True,
        row_styles=[theme.text, theme.text_muted],
        border_style=theme.border,
    )

    table.add_column("Ticker", width=10)
    table.add_column("Name", width=25)
    table.add_column("Price", justify="right", width=12)
    table.add_column("Change", justify="right", width=10)
    table.add_column("Change %", justify="right", width=10)
    table.add_column("Volume", justify="right", width=12)

    # Scrolling
    visible_rows = state.get_visible_stock_rows()
    start = state.stock_scroll_offset
    end = start + visible_rows

    for i, stock in enumerate(stocks[start:end], start=start):
        ticker = stock.ticker
        name = (stock.name or "")[:24]
        price = stock.price
        change = stock.change
        change_pct = stock.change_pct
        volume = stock.volume

        # Highlight selected row
        if i == state.selected_stock_idx and not state.focus_sidebar:
            style = f"{theme.selected_text} {theme.selected}"
        else:
            style = ""

        table.add_row(
            Text(ticker, style=style or theme.primary),
            Text(name, style=style),
            Text(f"${price:.2f}" if price else "-", style=style),
            format_change(change)
            if not style
            else Text(f"{change:+.2f}" if change else "-", style=style),
            format_pct_change(change_pct)
            if not style
            else Text(f"{change_pct:+.2f}%" if change_pct else "-", style=style),
            Text(format_number(volume, decimals=0) if volume else "-", style=style),
        )

    # Scroll indicators using shared component
    total = len(stocks)
    scroll_info = render_scroll_indicator(start, total, visible_rows) if total > 0 else ""

    return Panel(
        table,
        title=f"[{theme.header}]{title}[/] ({len(stocks)} stocks) {scroll_info}",
        title_align="left",
        border_style=theme.border_focus if not state.focus_sidebar else theme.text_muted,
    )


# =============================================================================
# STOCK DETAIL VIEW
# =============================================================================


def render_stock_detail_view(state: AppState) -> Layout:
    """Render the stock detail view."""
    layout = Layout()

    if state.detail_show_news:
        # Layout with news pane at bottom
        layout.split_column(
            Layout(name="header", size=6),
            Layout(name="body"),
            Layout(name="news", size=12),
        )
    else:
        # Layout without news pane
        layout.split_column(
            Layout(name="header", size=6),
            Layout(name="body"),
        )

    # Header with ticker/price/key stats
    layout["header"].update(_render_stock_header(state))

    # Body: chart on left, info panels on right
    layout["body"].split_row(
        Layout(name="chart", ratio=3),
        Layout(name="sidebar", ratio=2),
    )

    layout["body"]["chart"].update(_render_price_chart(state))

    # Sidebar: company info + ratios
    # Give more space to info panel when logo is enabled to show description
    from sp500_tui.config import get_tui_config

    config = get_tui_config()
    info_ratio = 3 if config.logo_enabled else 1

    layout["body"]["sidebar"].split_column(
        Layout(name="info", ratio=info_ratio),
        Layout(name="ratios", ratio=2),
    )
    layout["body"]["sidebar"]["info"].update(_render_company_info(state))
    layout["body"]["sidebar"]["ratios"].update(_render_ratios(state))

    # News pane (if visible)
    if state.detail_show_news:
        layout["news"].update(_render_news_pane(state))

    return layout


def _render_stock_header(state: AppState) -> Panel:
    """Render stock detail header with price and key stats."""
    theme = get_theme()
    company = state.detail_company
    if not company:
        return Panel(Text(f"Loading {state.detail_ticker}...", style=theme.warning))

    # Line 1: Ticker, Name, Sector
    text = Text()
    text.append(f" {company.ticker} ", style=f"bold {theme.text_bright} on {theme.primary}")
    text.append(f" {company.name} ", style=f"bold {theme.text}")
    text.append(f" {company.sector or 'Unknown'} ", style=theme.text_muted)
    if company.exchange:
        text.append(f" [{company.exchange}]", style=theme.info)

    # Line 2: Price and change
    text.append("\n ")
    if state.detail_prices:
        latest = state.detail_prices[0]
        text.append(f"${latest.close:.2f}", style=f"bold {theme.header}")

        if len(state.detail_prices) > 1:
            prev = state.detail_prices[1]
            change = latest.close - prev.close
            pct = (change / prev.close * 100) if prev.close else 0
            if change >= 0:
                text.append(f"  +{change:.2f} (+{pct:.2f}%)", style=f"bold {theme.positive}")
            else:
                text.append(f"  {change:.2f} ({pct:.2f}%)", style=f"bold {theme.negative}")

        # Add today's OHLV
        text.append(f"   O:{latest.open:.2f}", style=theme.text_muted)
        text.append(f"  H:{latest.high:.2f}", style=theme.text_muted)
        text.append(f"  L:{latest.low:.2f}", style=theme.text_muted)
        text.append(f"  V:{format_number(latest.volume, decimals=0)}", style=theme.text_muted)

    # Line 3: 52-week range
    text.append("\n ")
    if state.detail_52w_low and state.detail_52w_high:
        text.append("52W: ", style=theme.text_muted)
        text.append(f"${state.detail_52w_low:.2f}", style=theme.negative)
        text.append(" - ", style=theme.text_muted)
        text.append(f"${state.detail_52w_high:.2f}", style=theme.positive)

        # Show position in range as a mini bar
        if state.detail_prices:
            current = state.detail_prices[0].close
            range_52w = state.detail_52w_high - state.detail_52w_low
            if range_52w > 0:
                pos = (current - state.detail_52w_low) / range_52w
                bar_width = 20
                filled = int(pos * bar_width)
                text.append("  [", style=theme.text_muted)
                text.append("=" * filled, style=theme.positive)
                text.append("-" * (bar_width - filled), style=theme.text_muted)
                text.append("]", style=theme.text_muted)

    if state.detail_avg_volume:
        text.append(
            f"   Avg Vol: {format_number(state.detail_avg_volume, decimals=0)}",
            style=theme.text_muted,
        )

    return Panel(text, border_style=theme.border, padding=(0, 1))


def _render_price_chart(state: AppState) -> Panel:
    """Render ASCII price chart that fills available space."""
    theme = get_theme()
    if not state.detail_prices:
        return Panel(
            Text("No price data", style=theme.text_muted),
            title=f"[{theme.header}]Price Chart[/]",
        )

    # Use more price data and dynamic sizing
    prices = list(reversed(state.detail_prices))
    if not prices:
        return Panel(
            Text("No data", style=theme.text_muted), title=f"[{theme.header}]Price Chart[/]"
        )

    # Calculate chart dimensions based on terminal size
    # Reserve space for: header(6) + footer(3) + panel borders(2) + y-axis labels(10)
    chart_height = max(8, state.term_height - 17)
    # Width: terminal width * 0.6 (chart ratio) - panel borders - y-axis
    chart_width = max(20, int(state.term_width * 0.55) - 14)

    # Resample prices if we have more data points than width
    if len(prices) > chart_width:
        step = len(prices) / chart_width
        sampled = []
        for i in range(chart_width):
            idx = int(i * step)
            sampled.append(prices[idx])
        prices = sampled
    else:
        chart_width = len(prices)

    closes = [p.close for p in prices]
    highs = [p.high for p in prices]
    lows = [p.low for p in prices]

    min_p = min(lows)
    max_p = max(highs)
    range_p = max_p - min_p or 1

    # Build chart with candlestick-style bars
    lines = []
    for row in range(chart_height, -1, -1):
        threshold_high = min_p + (range_p * (row + 0.5) / chart_height)
        threshold_low = min_p + (range_p * (row - 0.5) / chart_height)
        line_chars = []

        for i in range(len(prices)):
            h, l, c = highs[i], lows[i], closes[i]
            prev_c = closes[i - 1] if i > 0 else c

            # Determine what to draw at this position
            if l <= threshold_high and h >= threshold_low:
                # Price range crosses this row
                if c >= prev_c:
                    # Up candle
                    if c >= threshold_low and c <= threshold_high:
                        line_chars.append(("█", theme.positive))
                    elif h >= threshold_low and l <= threshold_high:
                        line_chars.append(("│", theme.positive))
                    else:
                        line_chars.append((" ", ""))
                else:
                    # Down candle
                    if c >= threshold_low and c <= threshold_high:
                        line_chars.append(("█", theme.negative))
                    elif h >= threshold_low and l <= threshold_high:
                        line_chars.append(("│", theme.negative))
                    else:
                        line_chars.append((" ", ""))
            else:
                line_chars.append((" ", ""))

        # Y-axis label
        if row == chart_height:
            label = f"${max_p:>8.2f}"
        elif row == 0:
            label = f"${min_p:>8.2f}"
        elif row == chart_height // 2:
            mid = (max_p + min_p) / 2
            label = f"${mid:>8.2f}"
        elif row == chart_height * 3 // 4:
            q3 = min_p + range_p * 0.75
            label = f"${q3:>8.2f}"
        elif row == chart_height // 4:
            q1 = min_p + range_p * 0.25
            label = f"${q1:>8.2f}"
        else:
            label = "         "

        # Build the line with colors
        line_text = Text(f"{label} │")
        for char, color in line_chars:
            if color:
                line_text.append(char, style=color)
            else:
                line_text.append(char)
        lines.append(line_text)

    # X-axis
    x_axis = Text("         └" + "─" * chart_width)
    lines.append(x_axis)

    # Date labels
    if prices:
        start_date = prices[0].date.strftime("%m/%d")
        end_date = prices[-1].date.strftime("%m/%d")
        date_line = Text(f"          {start_date}")
        padding = chart_width - len(start_date) - len(end_date)
        date_line.append(" " * max(0, padding))
        date_line.append(end_date)
        lines.append(date_line)

    # Combine all lines
    chart_content = Text()
    for i, line in enumerate(lines):
        if i > 0:
            chart_content.append("\n", style=theme.text_muted)
        chart_content.append_text(line)

    period = len(state.detail_prices)
    return Panel(
        chart_content,
        title=f"[{theme.header}]{period}-Day Price Chart[/]",
        border_style=theme.border,
    )


def _render_company_info(state: AppState) -> Panel:
    """Render company information panel with description and logo."""
    import textwrap

    theme = get_theme()
    company = state.detail_company
    config = get_tui_config()

    content = Text()

    # Check if we have a logo to display
    has_logo = config.logo_enabled and (state.detail_logo_ascii or state.detail_logo_loading)

    if has_logo and state.detail_logo_ascii:
        # Split logo into lines
        logo_lines = str(state.detail_logo_ascii).split("\n")
        logo_height = len(logo_lines)
        logo_width = config.logo_width + 2  # Add spacing

        # Prepare text content
        text_parts = []
        if company:
            if company.description:
                text_parts.append(company.description)

        # Wrap text to fit next to logo (assume ~60 char panel width)
        text_width = 45  # Width available for text next to logo
        wrapped_lines = []
        for part in text_parts:
            wrapped_lines.extend(textwrap.wrap(part, width=text_width))

        # Combine logo and text side-by-side
        for i in range(max(logo_height, len(wrapped_lines))):
            # Logo column
            if i < logo_height:
                content.append(logo_lines[i])
                # Pad to logo width if line is shorter
                line_len = len(logo_lines[i])
                if line_len < logo_width:
                    content.append(" " * (logo_width - line_len))
            else:
                content.append(" " * logo_width)

            # Text column
            if i < len(wrapped_lines):
                content.append(wrapped_lines[i], style=theme.text)

            content.append("\n")

        # Add remaining company info below logo
        if company:
            content.append("\n")
            stats = []
            if company.employees:
                stats.append(f"Employees: {company.employees:,}")
            if company.exchange:
                stats.append(f"Exchange: {company.exchange}")
            if company.cik:
                stats.append(f"CIK: {company.cik}")

            if stats:
                content.append(" | ".join(stats), style=theme.text_muted)

            if company.homepage_url:
                content.append("\n")
                url = company.homepage_url
                if len(url) > 45:
                    url = url[:42] + "..."
                content.append(url, style=theme.info)

            if company.address:
                content.append("\n")
                content.append(company.address, style=theme.text_muted)
    elif has_logo and state.detail_logo_loading:
        content.append("Loading...", style=theme.text_muted)
        content.append("\n\n")
        if company and company.description:
            content.append(company.description, style=theme.text)
    else:
        # No logo - just text
        if company:
            if company.description:
                content.append(company.description, style=theme.text)
                content.append("\n\n")

            stats = []
            if company.employees:
                stats.append(f"Employees: {company.employees:,}")
            if company.exchange:
                stats.append(f"Exchange: {company.exchange}")
            if company.cik:
                stats.append(f"CIK: {company.cik}")

            if stats:
                content.append(" | ".join(stats), style=theme.text_muted)

            if company.homepage_url:
                content.append("\n")
                url = company.homepage_url
                if len(url) > 45:
                    url = url[:42] + "..."
                content.append(url, style=theme.info)

            if company.address:
                content.append("\n")
                content.append(company.address, style=theme.text_muted)
        else:
            content.append("No company data", style=theme.text_muted)

    return Panel(content, title=f"[{theme.header}]About[/]", border_style=theme.text_muted)


def _render_ratios(state: AppState) -> Panel:
    """Render key ratios in two columns."""
    theme = get_theme()
    ratios = state.detail_ratios

    table = Table(show_header=False, expand=True, box=None, padding=(0, 1))
    table.add_column("Metric", style=theme.text_muted, width=12)
    table.add_column("Value", style=theme.positive, width=10)
    table.add_column("Metric", style=theme.text_muted, width=12)
    table.add_column("Value", style=theme.positive, width=10)

    def fmt_ratio(value: float | None, decimals: int = 2) -> str:
        if value is None:
            return "-"
        return f"{value:.{decimals}f}"

    def fmt_pct(value: float | None) -> str:
        if value is None:
            return "-"
        return f"{value * 100:.1f}%"

    def fmt_money(value: float | None) -> str:
        if value is None:
            return "-"
        return format_number(value)

    if ratios:
        # Row 1: P/E, P/B
        table.add_row(
            "P/E Ratio", fmt_ratio(ratios.pe_ratio, 1), "P/B Ratio", fmt_ratio(ratios.pb_ratio)
        )
        # Row 2: P/S, EV/EBITDA
        table.add_row(
            "P/S Ratio", fmt_ratio(ratios.ps_ratio), "EV/EBITDA", fmt_ratio(ratios.ev_to_ebitda, 1)
        )
        # Row 3: EV/Sales, Debt/Equity
        table.add_row(
            "EV/Sales", fmt_ratio(ratios.ev_to_sales), "Debt/Eq", fmt_ratio(ratios.debt_to_equity)
        )
        # Row 4: ROE, ROA
        table.add_row("ROE", fmt_pct(ratios.roe), "ROA", fmt_pct(ratios.roa))
        # Row 5: Current Ratio, Quick Ratio
        table.add_row(
            "Current", fmt_ratio(ratios.current_ratio), "Quick", fmt_ratio(ratios.quick_ratio)
        )
        # Row 6: EPS, Div Yield
        table.add_row(
            "EPS",
            f"${ratios.eps:.2f}" if ratios.eps else "-",
            "Div Yield",
            fmt_pct(ratios.dividend_yield),
        )
        # Row 7: Market Cap, EV
        table.add_row("Mkt Cap", fmt_money(ratios.market_cap), "EV", fmt_money(ratios.ev))
        # Row 8: FCF
        table.add_row("FCF", fmt_money(ratios.fcf), "", "")
    else:
        table.add_row(Text("No ratio data", style=theme.text_muted), "", "", "")

    return Panel(table, title=f"[{theme.header}]Key Ratios[/]", border_style=theme.text_muted)


def _render_news_pane(state: AppState) -> Panel:
    """Render news and sentiment pane."""
    theme = get_theme()
    content = Text()

    # Sentiment summary header
    sentiment = state.detail_news_sentiment
    if sentiment:
        pos = sentiment.get("positive", 0)
        neg = sentiment.get("negative", 0)
        neu = sentiment.get("neutral", 0)
        total = pos + neg + neu

        content.append(" SENTIMENT (30d): ", style=f"bold {theme.info}")
        if pos > 0:
            content.append(f"+{pos}", style=f"bold {theme.positive}")
            content.append("  ", style=theme.text_muted)
        if neg > 0:
            content.append(f"-{neg}", style=f"bold {theme.negative}")
            content.append("  ", style=theme.text_muted)
        if neu > 0:
            content.append(f"~{neu}", style=theme.text_muted)
            content.append("  ", style=theme.text_muted)

        # Sentiment bar
        if total > 0:
            bar_width = 20
            pos_width = int((pos / total) * bar_width)
            neg_width = int((neg / total) * bar_width)
            neu_width = bar_width - pos_width - neg_width
            content.append("[", style=theme.text_muted)
            content.append("+" * pos_width, style=theme.positive)
            content.append("-" * neg_width, style=theme.negative)
            content.append("=" * neu_width, style=theme.text_muted)
            content.append("]", style=theme.text_muted)
        content.append("\n\n", style=theme.text_muted)

    # News headlines
    if state.detail_news:
        for article in state.detail_news[:6]:  # Show top 6 articles
            # Sentiment indicator
            if article.sentiment == "positive":
                content.append(" + ", style=theme.positive)
            elif article.sentiment == "negative":
                content.append(" - ", style=theme.negative)
            else:
                content.append(" ~ ", style=theme.text_muted)

            # Date
            date_str = article.published_utc.strftime("%m/%d")
            content.append(f"[{date_str}] ", style=theme.text_muted)

            # Title (truncated)
            title = article.title
            max_title_len = state.term_width - 25
            if len(title) > max_title_len:
                title = title[: max_title_len - 3] + "..."
            content.append(title, style=theme.text)

            # Publisher
            if article.publisher_name:
                content.append(f" ({article.publisher_name})", style=theme.text_muted)

            content.append("\n", style=theme.text_muted)
    else:
        content.append(" No recent news available", style=theme.text_muted)

    title = f"[{theme.header}]News & Sentiment[/] [{theme.text_muted}](n:toggle N:fullscreen)[/]"
    return Panel(content, title=title, border_style=theme.text_muted)


def _calculate_detail_height(state: AppState) -> int:
    """Calculate optimal height for news detail pane."""
    article = state.current_news_article()
    if not article:
        return 5  # Minimal height for "Select an article" message

    # Estimate content width (panel padding)
    content_width = max(40, state.term_width - 6)

    # Count lines needed:
    lines = 2  # Panel borders (top + bottom)

    # Title - estimate wrapping
    title_len = len(article.title) if article.title else 0
    title_lines = (title_len // content_width) + 1
    lines += title_lines

    # Author/publisher line
    if article.author or article.publisher_name:
        lines += 1

    # Date/sentiment line
    lines += 1

    # Description - truncated to 200 chars, then wrapped
    if article.description:
        desc = article.description
        if len(desc) > 200:
            desc = desc[:197] + "..."
        desc_lines = (len(desc) // content_width) + 1
        lines += desc_lines + 1  # +1 for blank line before

    # URL hint
    if article.article_url:
        lines += 2  # blank line + hint

    # Calculate max allowed height
    # We have: app_header(3) + app_footer(3) + news_header(5) + articles_panel + detail_panel
    # Articles panel needs: min 5 rows + 2 borders = 7 lines minimum
    # Available for body = term_height - 6 (app chrome)
    # News header = 5, so available for list+detail = term_height - 11
    min_article_rows = 7  # 5 content rows + 2 borders
    max_detail = state.term_height - 11 - min_article_rows

    # Clamp between reasonable bounds
    return max(8, min(lines, max_detail))


def render_news_fullscreen_view(state: AppState) -> Layout:
    """Render full-screen news view with selectable items."""
    layout = Layout()

    detail_height = _calculate_detail_height(state)

    layout.split_column(
        Layout(name="header", size=5),
        Layout(name="list"),
        Layout(name="detail", size=detail_height),
    )

    # Header with ticker and sentiment summary
    layout["header"].update(_render_news_header(state))

    # News list with selection
    layout["list"].update(_render_news_list(state))

    # Selected article detail
    layout["detail"].update(_render_news_detail(state))

    return layout


def _render_news_header(state: AppState) -> Panel:
    """Render news header with ticker and sentiment."""
    theme = get_theme()
    content = Text()

    # Ticker info
    content.append(f" {state.detail_ticker} ", style=f"bold {theme.text_bright} on {theme.primary}")
    if state.detail_company:
        content.append(f" {state.detail_company.name} ", style=theme.text)
    content.append(" - News & Sentiment\n", style=theme.text_muted)

    # Sentiment summary
    sentiment = state.detail_news_sentiment
    if sentiment:
        pos = sentiment.get("positive", 0)
        neg = sentiment.get("negative", 0)
        neu = sentiment.get("neutral", 0)
        total = pos + neg + neu

        content.append(" 30-Day Sentiment: ", style=f"bold {theme.info}")
        if pos > 0:
            content.append(f"Positive: {pos}", style=f"bold {theme.positive}")
            content.append("  ", style=theme.text_muted)
        if neg > 0:
            content.append(f"Negative: {neg}", style=f"bold {theme.negative}")
            content.append("  ", style=theme.text_muted)
        if neu > 0:
            content.append(f"Neutral: {neu}", style=theme.text_muted)

        # Sentiment bar
        if total > 0:
            bar_width = 30
            pos_width = int((pos / total) * bar_width)
            neg_width = int((neg / total) * bar_width)
            neu_width = bar_width - pos_width - neg_width
            content.append("\n ", style=theme.text_muted)
            content.append("[", style=theme.text_muted)
            content.append("+" * pos_width, style=theme.positive)
            content.append("-" * neg_width, style=theme.negative)
            content.append("=" * neu_width, style=theme.text_muted)
            content.append("]", style=theme.text_muted)

    return Panel(content, border_style=theme.border)


def _render_news_list(state: AppState) -> Panel:
    """Render scrollable news list with selection."""
    theme = get_theme()
    content = Text()

    if not state.detail_news:
        content.append(" No news articles available", style=theme.text_muted)
        return Panel(content, title=f"[{theme.header}]Articles[/]", border_style=theme.text_muted)

    visible_rows = state.get_visible_news_rows()
    start = state.news_scroll_offset
    end = start + visible_rows

    for i, article in enumerate(state.detail_news[start:end], start=start):
        is_selected = i == state.selected_news_idx

        # Selection indicator
        if is_selected:
            content.append(" > ", style=f"bold {theme.primary}")
        else:
            content.append("   ", style="")

        # Sentiment indicator
        if article.sentiment == "positive":
            content.append("[+] ", style=theme.positive)
        elif article.sentiment == "negative":
            content.append("[-] ", style=theme.negative)
        else:
            content.append("[~] ", style=theme.text_muted)

        # Date
        date_str = article.published_utc.strftime("%m/%d %H:%M")
        content.append(f"[{date_str}] ", style=theme.text_muted)

        # Title (truncated)
        title = article.title
        max_title_len = state.term_width - 30
        if len(title) > max_title_len:
            title = title[: max_title_len - 3] + "..."

        if is_selected:
            content.append(title, style=f"bold {theme.primary}")
        else:
            content.append(title, style=theme.text)

        content.append("\n", style=theme.text_muted)

    # Scroll indicators
    total = len(state.detail_news)
    scroll_info = ""
    if total > visible_rows:
        if start > 0:
            scroll_info += " ^"
        if end < total:
            scroll_info += " v"

    title = f"[{theme.header}]Articles ({len(state.detail_news)})[/]{scroll_info}"
    return Panel(content, title=title, border_style=theme.border)


def _render_news_detail(state: AppState) -> Panel:
    """Render selected article detail."""
    theme = get_theme()
    article = state.current_news_article()
    content = Text()

    if not article:
        content.append(" Select an article to view details", style=theme.text_muted)
        return Panel(content, title=f"[{theme.header}]Details[/]", border_style=theme.text_muted)

    # Title
    content.append(f" {article.title}", style=f"bold {theme.text}")
    content.append("\n", style=theme.text_muted)

    # Meta info
    if article.author:
        content.append(f" By: {article.author}", style=theme.text_muted)
        if article.publisher_name:
            content.append(f" | {article.publisher_name}", style=theme.text_muted)
        content.append("\n", style=theme.text_muted)
    elif article.publisher_name:
        content.append(f" Source: {article.publisher_name}", style=theme.text_muted)
        content.append("\n", style=theme.text_muted)

    # Date and sentiment
    date_str = article.published_utc.strftime("%Y-%m-%d %H:%M UTC")
    content.append(f" Published: {date_str}", style=theme.text_muted)
    if article.sentiment:
        style = (
            theme.positive
            if article.sentiment == "positive"
            else (theme.negative if article.sentiment == "negative" else theme.text_muted)
        )
        content.append(f"  |  Sentiment: {article.sentiment.capitalize()}", style=style)
    content.append("\n", style=theme.text_muted)

    # Description
    if article.description:
        desc = article.description
        if len(desc) > 200:
            desc = desc[:197] + "..."
        content.append("\n", style=theme.text_muted)
        content.append(f" {desc}", style=theme.text)
        content.append("\n", style=theme.text_muted)

    # URL hint
    if article.article_url:
        content.append("\n", style=theme.text_muted)
        content.append(" [Enter] Open in browser", style=theme.warning)

    return Panel(content, title=f"[{theme.header}]Details[/]", border_style=theme.text_muted)


# =============================================================================
# FUNDAMENTALS VIEW
# =============================================================================


def render_fundamentals_view(state: AppState) -> Layout:
    """Render the fundamentals view."""
    layout = Layout()

    layout.split_column(
        Layout(name="header", size=4),
        Layout(name="charts", size=6),
        Layout(name="table"),
    )

    # Header with search and tabs
    layout["header"].update(_render_fund_header(state))

    # Charts/sparklines summary
    layout["charts"].update(_render_fund_charts(state))

    # Body with data table
    layout["table"].update(_render_fund_table(state))

    return layout


def _render_fund_charts(state: AppState) -> Panel:
    """Render sparkline charts for fundamentals data."""
    theme = get_theme()
    content = Text()

    if state.fund_tab == FundamentalsTab.INCOME:
        # Income statement sparklines
        revenues = [inc.revenue for inc in reversed(state.fund_income)]
        net_incomes = [inc.net_income for inc in reversed(state.fund_income)]
        eps_values = [inc.eps for inc in reversed(state.fund_income)]

        # Row 1: Revenue and Net Income
        content.append(f" {'Revenue':<12}", style=theme.text_muted)
        content.append_text(render_sparkline(revenues, 20))
        rev_val = format_number(revenues[-1]) if revenues and revenues[-1] else "-"
        content.append(f" {rev_val:>8}", style=f"bold {theme.positive}")
        content.append("   ", style=theme.text_muted)
        content.append(f"{'Net Income':<12}", style=theme.text_muted)
        content.append_text(render_sparkline(net_incomes, 20))
        ni_val = format_number(net_incomes[-1]) if net_incomes and net_incomes[-1] else "-"
        content.append(f" {ni_val:>8}", style=f"bold {theme.positive}")
        content.append("\n", style=theme.text_muted)

        # Row 2: EPS
        content.append(f" {'EPS':<12}", style=theme.text_muted)
        content.append_text(render_sparkline(eps_values, 20))
        eps_val = f"${eps_values[-1]:.2f}" if eps_values and eps_values[-1] else "-"
        content.append(f" {eps_val:>8}", style=f"bold {theme.info}")

    elif state.fund_tab == FundamentalsTab.BALANCE:
        # Balance sheet sparklines
        assets = [bal.total_assets for bal in reversed(state.fund_balance)]
        equity = [bal.total_equity for bal in reversed(state.fund_balance)]
        cash = [bal.cash for bal in reversed(state.fund_balance)]

        # Row 1: Assets and Equity
        content.append(f" {'Assets':<12}", style=theme.text_muted)
        content.append_text(render_sparkline(assets, 20))
        assets_val = format_number(assets[-1]) if assets and assets[-1] else "-"
        content.append(f" {assets_val:>8}", style=f"bold {theme.positive}")
        content.append("   ", style=theme.text_muted)
        content.append(f"{'Equity':<12}", style=theme.text_muted)
        content.append_text(render_sparkline(equity, 20))
        equity_val = format_number(equity[-1]) if equity and equity[-1] else "-"
        content.append(f" {equity_val:>8}", style=f"bold {theme.positive}")
        content.append("\n", style=theme.text_muted)

        # Row 2: Cash
        content.append(f" {'Cash':<12}", style=theme.text_muted)
        content.append_text(render_sparkline(cash, 20))
        cash_val = format_number(cash[-1]) if cash and cash[-1] else "-"
        content.append(f" {cash_val:>8}", style=f"bold {theme.info}")

    elif state.fund_tab == FundamentalsTab.CASHFLOW:
        # Cash flow sparklines
        operating = [cf.operating_cash_flow for cf in reversed(state.fund_cashflow)]
        net_change = [cf.net_change for cf in reversed(state.fund_cashflow)]

        # Row 1: Operating CF and Net Change
        content.append(f" {'Operating CF':<12}", style=theme.text_muted)
        content.append_text(render_sparkline(operating, 20))
        op_val = format_number(operating[-1]) if operating and operating[-1] else "-"
        content.append(f" {op_val:>8}", style=f"bold {theme.positive}")
        content.append("   ", style=theme.text_muted)
        content.append(f"{'Net Change':<12}", style=theme.text_muted)
        content.append_text(render_sparkline(net_change, 20))
        nc_val = format_number(net_change[-1]) if net_change and net_change[-1] else "-"
        content.append(f" {nc_val:>8}", style=f"bold {theme.info}")

    if not content.plain:
        content.append(" No data available", style=theme.text_muted)

    return Panel(content, title=f"[{theme.header}]Trends[/]", border_style=theme.text_muted)


def _render_fund_header(state: AppState) -> Panel:
    """Render fundamentals header with ticker and tabs."""
    theme = get_theme()
    text = Text()

    # Ticker/company info
    if state.fund_company:
        text.append(f" {state.fund_ticker} ", style=f"bold {theme.primary}")
        text.append(f"{state.fund_company.name}", style=theme.text)
    else:
        text.append(" Press / to search for a ticker", style=theme.text_muted)

    text.append("\n")

    # Tabs
    tabs = [
        ("1", "Income Statement", FundamentalsTab.INCOME),
        ("2", "Balance Sheet", FundamentalsTab.BALANCE),
        ("3", "Cash Flow", FundamentalsTab.CASHFLOW),
    ]

    for key, name, tab in tabs:
        if state.fund_tab == tab:
            text.append(f" [{key}]{name} ", style=f"bold black on {theme.primary}")
        else:
            text.append(f" [{key}]{name} ", style=theme.text_muted)

    text.append("   ")
    tf = "Quarterly" if state.fund_quarterly else "Annual"
    text.append(f"[t] {tf}", style=theme.warning)

    return Panel(text, border_style=theme.border)


def _render_fund_table(state: AppState) -> Panel:
    """Render the fundamentals data table."""
    theme = get_theme()
    table = Table(
        show_header=True,
        header_style=f"bold {theme.header}",
        expand=True,
        row_styles=[theme.text, theme.text_muted],
        border_style=theme.border,
    )

    if state.fund_tab == FundamentalsTab.INCOME:
        table.add_column("Period", width=12)
        table.add_column("Revenue", justify="right")
        table.add_column("Gross Profit", justify="right")
        table.add_column("Op. Income", justify="right")
        table.add_column("Net Income", justify="right")
        table.add_column("EPS", justify="right")
        table.add_column("EBITDA", justify="right")

        for inc in state.fund_income:
            period = f"{inc.fiscal_year}"
            if inc.fiscal_quarter:
                period += f" Q{inc.fiscal_quarter}"
            table.add_row(
                period,
                format_number(inc.revenue),
                format_number(inc.gross_profit),
                format_number(inc.operating_income),
                format_number(inc.net_income),
                f"${inc.eps:.2f}" if inc.eps else "-",
                format_number(inc.ebitda),
            )

    elif state.fund_tab == FundamentalsTab.BALANCE:
        table.add_column("Period", width=12)
        table.add_column("Total Assets", justify="right")
        table.add_column("Total Liab.", justify="right")
        table.add_column("Total Equity", justify="right")
        table.add_column("Cash", justify="right")
        table.add_column("Total Debt", justify="right")

        for bal in state.fund_balance:
            period = f"{bal.fiscal_year}"
            if bal.fiscal_quarter:
                period += f" Q{bal.fiscal_quarter}"
            table.add_row(
                period,
                format_number(bal.total_assets),
                format_number(bal.total_liabilities),
                format_number(bal.total_equity),
                format_number(bal.cash),
                format_number(bal.total_debt),
            )

    elif state.fund_tab == FundamentalsTab.CASHFLOW:
        table.add_column("Period", width=12)
        table.add_column("Operating CF", justify="right")
        table.add_column("Investing CF", justify="right")
        table.add_column("Financing CF", justify="right")
        table.add_column("Net Change", justify="right")
        table.add_column("CapEx", justify="right")

        for cf in state.fund_cashflow:
            period = f"{cf.fiscal_year}"
            if cf.fiscal_quarter:
                period += f" Q{cf.fiscal_quarter}"
            table.add_row(
                period,
                format_number(cf.operating_cash_flow),
                format_number(cf.investing_cash_flow),
                format_number(cf.financing_cash_flow),
                format_number(cf.net_change),
                format_number(cf.capex),
            )

    title = {
        FundamentalsTab.INCOME: "Income Statement",
        FundamentalsTab.BALANCE: "Balance Sheet",
        FundamentalsTab.CASHFLOW: "Cash Flow Statement",
    }[state.fund_tab]

    return Panel(table, title=f"[{theme.header}]{title}[/]", border_style=theme.text_muted)


# =============================================================================
# ECONOMY VIEW
# =============================================================================


def render_economy_view(state: AppState) -> Layout:
    """Render the economy view."""
    layout = Layout()

    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="indicators", size=8),
        Layout(name="table"),
    )

    layout["header"].update(_render_econ_header(state))
    layout["indicators"].update(_render_econ_indicators(state))
    layout["table"].update(_render_econ_table(state))

    return layout


def _render_econ_header(state: AppState) -> Panel:
    """Render economy header with tabs."""
    theme = get_theme()
    text = Text()
    text.append(" ECONOMIC INDICATORS ", style=f"bold {theme.header}")
    text.append("  ")

    tabs = [
        ("1", "Treasury Yields", EconomyTab.YIELDS),
        ("2", "Inflation", EconomyTab.INFLATION),
        ("3", "Labor Market", EconomyTab.LABOR),
    ]

    for key, name, tab in tabs:
        if state.econ_tab == tab:
            text.append(f" [{key}]{name} ", style=f"bold black on {theme.primary}")
        else:
            text.append(f" [{key}]{name} ", style=theme.text_muted)

    return Panel(text, border_style=theme.border)


def _render_econ_indicators(state: AppState) -> Panel:
    """Render key economic indicators summary with sparklines."""
    theme = get_theme()
    content = Text()

    # Get time series data (reversed for sparkline - oldest to newest)
    y10_series = [y.yield_10y for y in reversed(state.econ_yields)] if state.econ_yields else []
    y2_series = [y.yield_2y for y in reversed(state.econ_yields)] if state.econ_yields else []
    cpi_series = [i.cpi_yoy for i in reversed(state.econ_inflation)] if state.econ_inflation else []
    unemp_series = (
        [lm.unemployment_rate for lm in reversed(state.econ_labor)] if state.econ_labor else []
    )

    # Get latest and previous values for trend
    y10 = state.econ_yields[0].yield_10y if state.econ_yields else None
    y10_prev = state.econ_yields[1].yield_10y if len(state.econ_yields) > 1 else None
    y2 = state.econ_yields[0].yield_2y if state.econ_yields else None
    y2_prev = state.econ_yields[1].yield_2y if len(state.econ_yields) > 1 else None
    cpi = state.econ_inflation[0].cpi_yoy if state.econ_inflation else None
    cpi_prev = state.econ_inflation[1].cpi_yoy if len(state.econ_inflation) > 1 else None
    unemp = state.econ_labor[0].unemployment_rate if state.econ_labor else None
    unemp_prev = state.econ_labor[1].unemployment_rate if len(state.econ_labor) > 1 else None

    # Row 1: 10Y Treasury and 2Y Treasury
    content.append(f" {'10Y Treasury':<13}", style=theme.text_muted)
    y10_val = f"{y10:.2f}%" if y10 else "-"
    content.append(f"{y10_val:>7}", style=f"bold {theme.positive}")
    content.append(" ", style=theme.text_muted)
    content.append_text(render_sparkline(y10_series, 12))
    content.append(" ", style=theme.text_muted)
    content.append_text(render_trend_indicator(y10, y10_prev, 9))
    content.append("  ", style=theme.text_muted)
    content.append(f"{'2Y Treasury':<13}", style=theme.text_muted)
    y2_val = f"{y2:.2f}%" if y2 else "-"
    content.append(f"{y2_val:>7}", style=f"bold {theme.positive}")
    content.append(" ", style=theme.text_muted)
    content.append_text(render_sparkline(y2_series, 12))
    content.append(" ", style=theme.text_muted)
    content.append_text(render_trend_indicator(y2, y2_prev, 9))
    content.append("\n", style=theme.text_muted)

    # Row 2: CPI and Unemployment
    content.append(f" {'CPI YoY':<13}", style=theme.text_muted)
    cpi_str, cpi_norm = format_rate_as_pct(cpi)
    cpi_prev_str, cpi_prev_norm = format_rate_as_pct(cpi_prev)
    content.append(f"{cpi_str:>7}", style=f"bold {theme.warning}")
    content.append(" ", style=theme.text_muted)
    # Normalize sparkline data
    cpi_spark_data = [format_rate_as_pct(c)[1] for c in cpi_series]
    content.append_text(render_sparkline(cpi_spark_data, 12))
    content.append(" ", style=theme.text_muted)
    content.append_text(render_trend_indicator(cpi_norm, cpi_prev_norm, 9))
    content.append("  ", style=theme.text_muted)
    content.append(f"{'Unemployment':<13}", style=theme.text_muted)
    unemp_str, unemp_norm = format_rate_as_pct(unemp)
    unemp_prev_str, unemp_prev_norm = format_rate_as_pct(unemp_prev)
    content.append(f"{unemp_str:>7}", style=f"bold {theme.info}")
    content.append(" ", style=theme.text_muted)
    # Normalize sparkline data
    unemp_spark_data = [format_rate_as_pct(u)[1] for u in unemp_series]
    content.append_text(render_sparkline(unemp_spark_data, 12))
    content.append(" ", style=theme.text_muted)
    content.append_text(render_trend_indicator(unemp_norm, unemp_prev_norm, 9))
    content.append("\n", style=theme.text_muted)

    # Yield curve inversion warning
    if y10 and y2 and y2 > y10:
        content.append("\n", style=theme.text_muted)
        content.append(" ⚠ YIELD CURVE INVERTED ", style=f"bold {theme.text} on {theme.negative}")
        content.append(f" (2Y: {y2:.2f}% > 10Y: {y10:.2f}%)", style=theme.negative)

    return Panel(
        content,
        title=f"[{theme.header}]Key Indicators with Trends[/]",
        border_style=theme.text_muted,
    )


def _render_econ_table(state: AppState) -> Panel:
    """Render economy data table based on selected tab."""
    theme = get_theme()
    table = Table(
        show_header=True,
        header_style=f"bold {theme.header}",
        expand=True,
        row_styles=[theme.text, theme.text_muted],
        border_style=theme.border,
    )

    if state.econ_tab == EconomyTab.YIELDS:
        table.add_column("Date", width=12)
        table.add_column("1M", justify="right")
        table.add_column("3M", justify="right")
        table.add_column("6M", justify="right")
        table.add_column("1Y", justify="right")
        table.add_column("2Y", justify="right")
        table.add_column("5Y", justify="right")
        table.add_column("10Y", justify="right")
        table.add_column("30Y", justify="right")

        for y in state.econ_yields:
            table.add_row(
                str(y.date),
                f"{y.yield_1m:.2f}" if y.yield_1m else "-",
                f"{y.yield_3m:.2f}" if y.yield_3m else "-",
                f"{y.yield_6m:.2f}" if y.yield_6m else "-",
                f"{y.yield_1y:.2f}" if y.yield_1y else "-",
                f"{y.yield_2y:.2f}" if y.yield_2y else "-",
                f"{y.yield_5y:.2f}" if y.yield_5y else "-",
                f"{y.yield_10y:.2f}" if y.yield_10y else "-",
                f"{y.yield_30y:.2f}" if y.yield_30y else "-",
            )
        title = "Treasury Yields History"

    elif state.econ_tab == EconomyTab.INFLATION:
        table.add_column("Date", width=12)
        table.add_column("CPI", justify="right")
        table.add_column("CPI Core", justify="right")
        table.add_column("CPI YoY", justify="right")
        table.add_column("PCE", justify="right")
        table.add_column("PCE Core", justify="right")

        for i in state.econ_inflation:
            cpi_yoy_str, _ = format_rate_as_pct(i.cpi_yoy)
            table.add_row(
                str(i.date),
                f"{i.cpi:.2f}" if i.cpi else "-",
                f"{i.cpi_core:.2f}" if i.cpi_core else "-",
                cpi_yoy_str,
                f"{i.pce:.2f}" if i.pce else "-",
                f"{i.pce_core:.2f}" if i.pce_core else "-",
            )
        title = "Inflation Data History"

    else:  # LABOR
        table.add_column("Date", width=12)
        table.add_column("Unemployment", justify="right")
        table.add_column("Participation", justify="right")
        table.add_column("Hourly Wages", justify="right")
        table.add_column("Job Openings", justify="right")

        for lm in state.econ_labor:
            unemp_str, _ = format_rate_as_pct(lm.unemployment_rate)
            partic_str, _ = format_rate_as_pct(lm.participation_rate)
            table.add_row(
                str(lm.date),
                unemp_str,
                partic_str,
                f"${lm.avg_hourly_earnings:.2f}" if lm.avg_hourly_earnings else "-",
                f"{lm.job_openings / 1000:.0f}K" if lm.job_openings else "-",
            )
        title = "Labor Market History"

    return Panel(table, title=f"[{theme.header}]{title}[/]", border_style=theme.text_muted)


# =============================================================================
# SETTINGS VIEW
# =============================================================================


def render_settings_view(state: AppState) -> Layout:
    """Render the settings view with horizontal tabs."""
    layout = Layout()

    layout.split_column(
        Layout(name="tabs", size=TAB_HEADER_HEIGHT),
        Layout(name="content"),
    )

    # Tab header
    layout["tabs"].update(_render_settings_tabs(state))

    # Settings content (single panel, no sidebar)
    layout["content"].update(_render_settings_content(state))

    return layout


def _render_settings_tabs(state: AppState) -> Panel:
    """Render settings category tabs."""
    theme = get_theme()

    tabs = [
        ("1", "Display", state.settings_category == SettingsCategory.DISPLAY),
        ("2", "Charts", state.settings_category == SettingsCategory.CHARTS),
        ("3", "Behavior", state.settings_category == SettingsCategory.BEHAVIOR),
        ("4", "API Keys", state.settings_category == SettingsCategory.API),
        ("5", "Users", state.settings_category == SettingsCategory.USERS),
    ]

    text = Text()
    text.append(" SETTINGS ", style=f"bold {theme.header}")
    text.append("  ")
    text.append_text(render_tabs(tabs))

    return Panel(text, border_style=theme.border, height=TAB_HEADER_HEIGHT)


# Settings definitions per category
SETTINGS_ITEMS = {
    SettingsCategory.DISPLAY: [
        (
            "theme",
            "Theme",
            "choice",
            [
                "default",
                "osaka-jade",
                "mono",
                "high-contrast",
                "dracula",
                "catppuccin",
                "gruvbox",
                "nord",
                "tokyo-night",
                "solarized",
                "one-dark",
            ],
        ),
        ("chart_period_days", "Chart Period (days)", "int", [30, 60, 90, 180, 365]),
        ("number_format", "Number Format", "choice", ["compact", "full"]),
        ("logo_enabled", "Show Company Logos", "bool", None),
        ("logo_width", "Logo Width (chars)", "int", [10, 20, 24, 28, 32, 36, 40]),
        ("logo_height", "Logo Height (lines)", "int", [5, 6, 8, 10, 12, 15, 18]),
    ],
    SettingsCategory.CHARTS: [
        ("chart_detail", "Chart Detail Level", "choice", ["compact", "normal", "detailed"]),
    ],
    SettingsCategory.BEHAVIOR: [
        ("fundamentals_timeframe", "Default Timeframe", "choice", ["quarterly", "annual"]),
    ],
    SettingsCategory.API: [
        ("zai_api_key", "Z.AI API Key", "secret", None),
    ],
    SettingsCategory.USERS: [],  # Handled specially in _render_settings_content
}


def _render_users_settings_panel(state: AppState) -> Panel:
    """Render user management info in the settings panel."""
    theme = get_theme()
    content = Text()

    # Current user info
    state.ensure_user()
    if state.current_user:
        content.append("\n Current User\n", style=f"bold {theme.info}")
        content.append(" " + "─" * 50 + "\n", style=theme.border)
        content.append(f"  Name: {state.current_user.name}\n", style=theme.text_bright)
        content.append(
            f"  Admin: {'Yes' if state.current_user.is_admin else 'No'}\n", style=theme.text_bright
        )
        content.append("\n")

    # Instructions
    content.append(" User Management\n", style=f"bold {theme.warning}")
    content.append(" " + "─" * 50 + "\n", style=theme.border)
    content.append("\n")
    content.append("  Press ", style=theme.text_muted)
    content.append("Ctrl+U", style=theme.warning)
    content.append(" to quickly switch between users\n", style=theme.text_muted)
    content.append("\n")

    if state.current_user and state.current_user.is_admin:
        content.append("  Press ", style=theme.text_muted)
        content.append("Ctrl+P", style=theme.warning)
        content.append(" to manage users (create, delete, rename)\n", style=theme.text_muted)
    else:
        content.append("  User management requires admin privileges\n", style=theme.text_muted)

    content.append("\n")
    content.append(" Features\n", style=f"bold {theme.info}")
    content.append(" " + "─" * 50 + "\n", style=theme.border)
    content.append("  • Each user has their own settings and watchlists\n", style=theme.text_bright)
    content.append("  • Admins can manage users and access all features\n", style=theme.text_bright)
    content.append("  • Switch users anytime without logging out\n", style=theme.text_bright)

    return Panel(
        content,
        title=f"[{theme.header}]User Management[/]",
        border_style=theme.border,
        padding=(0, 2),
    )


def _render_settings_content(state: AppState) -> Panel:
    """Render settings items for current category."""
    from sp500_tui.models.settings import SettingsManager

    theme = get_theme()

    # Special handling for USERS category
    if state.settings_category == SettingsCategory.USERS:
        return _render_users_settings_panel(state)

    items = SETTINGS_ITEMS.get(state.settings_category, [])

    content = Text()

    if not items:
        content.append_text(
            render_empty_state("No settings in this category.", "Use 1-5 to switch categories")
        )
        return Panel(content, title=f"[{theme.header}]Settings[/]", border_style=theme.text_muted)

    # If popup is open, show it instead
    if state.settings_popup_open:
        return _render_settings_popup(state)

    # Ensure user is loaded
    state.ensure_user()
    if not state.current_user:
        content.append_text(render_empty_state("No active user", "Cannot load settings"))
        return Panel(content, title=f"[{theme.header}]Settings[/]", border_style=theme.text_muted)

    content.append("\n")

    for i, (key, label, value_type, choices) in enumerate(items):
        # Get current value from database
        current_value = SettingsManager.get(state.current_user.id, key)

        # Format value for display
        if value_type == "bool":
            value_str = "On" if current_value else "Off"
        elif value_type == "secret":
            # Mask secret values
            if current_value:
                value_str = (
                    "*" * 8 + str(current_value)[-4:] if len(str(current_value)) > 4 else "****"
                )
            else:
                value_str = "(not set)"
        else:
            value_str = str(current_value) if current_value else "(not set)"

        is_selected = i == state.settings_selected_idx
        is_editing = is_selected and state.settings_editing

        if is_editing:
            content.append(f"  > {label}: ", style=f"bold {theme.warning}")
            content.append(
                f"[{state.settings_edit_value}]",
                style=f"bold {theme.selected_text} on {theme.selected}",
            )
            content.append("_", style="blink")
            content.append("\n")
        elif is_selected:
            content.append(f"  > {label}: ", style=COLORS["selected"])
            if choices and (value_type == "choice" or value_type == "int"):
                content.append("< ", style=f"{theme.info}")
                content.append(f"{value_str}", style=theme.text_bright)
                content.append(" >", style=f"{theme.info}")
            else:
                content.append(f"{value_str}", style=theme.text_bright)
            content.append("  [Enter to select]", style=theme.text_muted)
            content.append("\n")
        else:
            content.append(f"    {label}: ", style=theme.text_muted)
            content.append(f"{value_str}", style=theme.text)
            content.append("\n")

    content.append("\n")

    category_name = {
        SettingsCategory.DISPLAY: "Display Settings",
        SettingsCategory.CHARTS: "Chart Settings",
        SettingsCategory.BEHAVIOR: "Behavior Settings",
        SettingsCategory.API: "API Keys",
        SettingsCategory.USERS: "User Management",
    }[state.settings_category]

    return Panel(
        content,
        title=f"[{theme.header}]{category_name}[/]",
        title_align="left",
        border_style=theme.border,
    )


def _render_settings_popup(state: AppState) -> Panel:
    """Render the popup menu for selecting a choice."""
    theme = get_theme()
    content = Text()

    content.append(f" Select {state.settings_popup_label}:\n\n", style=f"bold {theme.header}")

    for i, choice in enumerate(state.settings_popup_choices):
        if i == state.settings_popup_idx:
            content.append(f"   > {choice}\n", style=f"{theme.selected_text} {theme.selected}")
        else:
            content.append(f"     {choice}\n", style=theme.text)

    content.append("\n", style=theme.text_muted)
    content.append(" [Up/Down] Navigate  [Enter] Select  [Esc] Cancel\n", style=theme.text_muted)

    return Panel(
        content,
        title=f"[{theme.accent}]{state.settings_popup_label}[/]",
        title_align="left",
        border_style=theme.accent,
    )


# =============================================================================
# USERS SETTINGS PANEL
# =============================================================================


def _render_users_settings_panel(state: AppState) -> Panel:
    """Render user management info in the settings panel."""
    theme = get_theme()
    content = Text()

    # Current user info
    state.ensure_user()
    if state.current_user:
        content.append("\n Current User\n", style=f"bold {theme.info}")
        content.append(" " + "─" * 50 + "\n", style=theme.border)
        content.append(f"  Name: {state.current_user.name}\n", style=theme.text_bright)
        content.append(
            f"  Admin: {'Yes' if state.current_user.is_admin else 'No'}\n", style=theme.text_bright
        )
        content.append("\n")

    # Instructions
    content.append(" User Management\n", style=f"bold {theme.warning}")
    content.append(" " + "─" * 50 + "\n", style=theme.border)
    content.append("\n")
    content.append("  Press ", style=theme.text_muted)
    content.append("Ctrl+U", style=theme.warning)
    content.append(" to quickly switch between users\n", style=theme.text_muted)
    content.append("\n")

    if state.current_user and state.current_user.is_admin:
        content.append("  Press ", style=theme.text_muted)
        content.append("Ctrl+P", style=theme.warning)
        content.append(" to manage users (create, delete, rename)\n", style=theme.text_muted)
    else:
        content.append("  User management requires admin privileges\n", style=theme.text_muted)

    content.append("\n")
    content.append(" Features\n", style=f"bold {theme.info}")
    content.append(" " + "─" * 50 + "\n", style=theme.border)
    content.append("  • Each user has their own settings and watchlists\n", style=theme.text_bright)
    content.append("  • Admins can manage users and access all features\n", style=theme.text_bright)
    content.append("  • Switch users anytime without logging out\n", style=theme.text_bright)

    return Panel(
        content,
        title=f"[{theme.header}]User Management[/]",
        border_style=theme.border,
        padding=(0, 2),
    )


# =============================================================================
# GLOSSARY VIEW
# =============================================================================


def render_glossary_view(state: AppState) -> Layout:
    """Render the glossary view."""
    layout = Layout()

    layout.split_row(
        Layout(name="sidebar", size=SIDEBAR_WIDTH),
        Layout(name="main"),
    )

    # Sidebar: term list
    layout["sidebar"].update(_render_glossary_sidebar(state))

    # Main: definition or loading/error state
    layout["main"].update(_render_glossary_definition(state))

    return layout


def _render_glossary_sidebar(state: AppState) -> Panel:
    """Render the glossary term list sidebar."""
    theme = get_theme()
    content = Text()

    # Search box
    if state.glossary_search:
        content.append(f" / {state.glossary_search}", style=theme.warning)
    else:
        content.append(" / Search...", style=theme.text_muted)
    content.append("\n\n", style=theme.text_muted)

    # Term list with scrolling
    visible_rows = state.get_visible_glossary_rows()
    start = state.glossary_scroll_offset
    end = start + visible_rows

    for i, term in enumerate(state.glossary_filtered[start:end], start=start):
        # Category prefix
        cat_abbrev = ""
        if term.category:
            cat_map = {
                "Valuation": "VAL",
                "Profitability": "PRF",
                "Liquidity": "LIQ",
                "Leverage": "LEV",
                "Cash Flow": "CF",
                "Dividends": "DIV",
                "Growth": "GRW",
                "Trading": "TRD",
                "User Added": "USR",
                "Related": "REL",
            }
            cat_abbrev = cat_map.get(term.category, term.category[:3].upper())

        # Indicator for cached definition
        cached = "*" if term.has_definition else " "

        line = f"{cached}[{cat_abbrev:>3}] {term.term}"

        if i == state.selected_term_idx and state.glossary_focus_sidebar:
            content.append(line + "\n", style=f"{theme.selected_text} {theme.selected}")
        elif i == state.selected_term_idx:
            content.append(line + "\n", style=theme.primary)
        else:
            content.append(
                line + "\n", style=theme.text_muted if not term.has_definition else theme.text
            )

    # Calculate scroll info using shared component
    total = len(state.glossary_filtered)
    scroll_info = render_scroll_indicator(start, total, visible_rows) if total > 0 else ""

    # Use focused panel title
    title = panel_title("TERMS", state.glossary_focus_sidebar, scroll_info)

    return Panel(
        content,
        title=title,
        title_align="left",
        border_style=theme.border_focus if state.glossary_focus_sidebar else theme.text_muted,
    )


def _render_glossary_definition(state: AppState) -> Panel:
    """Render the glossary definition panel."""
    theme = get_theme()
    # Show regeneration menu if active
    if state.glossary_show_regen_menu:
        return _render_regen_menu(state)

    # Loading state with streaming content
    if state.glossary_loading:
        content = Text()
        content.append(" Generating definition...", style=f"bold {theme.warning}")
        content.append("\n\n", style=theme.text_muted)

        # Spinner animation (will be static but indicates activity)
        content.append(" ", style=theme.text_muted)
        content.append("[", style=theme.text_muted)
        # Simple progress indicator
        content.append("=" * 20, style=theme.positive)
        content.append(">", style=f"bold {theme.positive}")
        content.append(" " * 10, style=theme.text_muted)
        content.append("]", style=theme.text_muted)
        content.append("\n\n", style=theme.text_muted)

        # Show streaming content as it arrives
        if state.glossary_stream_content:
            content.append(state.glossary_stream_content, style=theme.text)

        return Panel(
            content,
            title=f"[{theme.warning}]Generating...[/]",
            border_style=theme.warning,
        )

    # Error state
    if state.glossary_error:
        content = Text()
        content.append(" Error\n\n", style=f"bold {theme.negative}")
        content.append(f" {state.glossary_error}\n\n", style=theme.negative)
        content.append(" Press Enter to retry", style=theme.text_muted)

        return Panel(
            content,
            title=f"[{theme.negative}]Error[/]",
            border_style=theme.negative,
        )

    # No term selected
    term = state.current_glossary_term()
    if not term:
        content = Text()
        content.append("\n Select a term from the list\n", style=theme.text_muted)
        content.append(" Press Enter to generate definition\n", style=theme.text_muted)

        return Panel(
            content,
            title=f"[{theme.header}]Definition[/]",
            border_style=theme.text_muted,
        )

    # No definition yet
    definition = state.glossary_definition
    if not definition:
        content = Text()
        content.append(f"\n {term.term}\n\n", style=f"bold {theme.primary}")
        content.append(" No definition cached.\n", style=theme.text_muted)
        content.append(" Press Enter to generate.\n", style=theme.text_muted)

        return Panel(
            content,
            title=f"[{theme.header}]{term.term}[/]",
            border_style=theme.text_muted,
        )

    # Render the full definition
    content = Text()

    # Official definition
    content.append(" OFFICIAL DEFINITION\n", style=f"bold {theme.info}")
    content.append(f" {definition.official_definition}\n\n", style=theme.text)

    # Plain English
    content.append(" WHAT IT ACTUALLY MEANS\n", style=f"bold {theme.info}")
    content.append(f" {definition.plain_english}\n\n", style=theme.text)

    # Examples
    if definition.examples:
        content.append(" EXAMPLES\n", style=f"bold {theme.info}")
        for i, example in enumerate(definition.examples, 1):
            content.append(f" {i}. {example}\n", style=theme.text)
        content.append("\n", style=theme.text_muted)

    # Related terms with numbers for quick jump
    if definition.related_terms:
        content.append(" RELATED TERMS\n", style=f"bold {theme.info}")
        for i, related in enumerate(definition.related_terms[:5], 1):
            content.append(f" [{i}] ", style=theme.warning)
            content.append(f"{related}  ", style=theme.positive)
        content.append("\n\n", style=theme.text_muted)

    # Learn more links
    if definition.learn_more:
        content.append(" LEARN MORE\n", style=f"bold {theme.info}")
        for url in definition.learn_more[:3]:
            # Truncate long URLs
            display_url = url
            if len(display_url) > 60:
                display_url = display_url[:57] + "..."
            content.append(f" {display_url}\n", style=theme.info)

    # Show if regenerated with custom prompt
    if definition.custom_prompt:
        content.append("\n", style=theme.text_muted)
        content.append(f" [Customized: {definition.custom_prompt[:30]}...]", style=theme.text_muted)

    return Panel(
        content,
        title=f"[{theme.header}]{definition.term}[/]",
        border_style=theme.border if not state.glossary_focus_sidebar else theme.text_muted,
    )


def _render_regen_menu(state: AppState) -> Panel:
    """Render the regeneration options menu."""
    theme = get_theme()
    content = Text()
    content.append("\n Regenerate definition with:\n\n", style=f"bold {theme.warning}")

    options = [
        ("1", "More technical", "Use more technical language and formulas"),
        ("2", "Simpler explanation", "Make it easier for beginners"),
        ("3", "Add more examples", "Include 4-5 practical examples"),
        ("4", "Focus on practical use", "How investors use this metric"),
        ("c", "Custom instructions", "Enter your own prompt"),
    ]

    for key, label, desc in options:
        content.append(f"  [{key}] ", style=theme.warning)
        content.append(f"{label}\n", style=f"bold {theme.text}")
        content.append(f"      {desc}\n\n", style=theme.text_muted)

    content.append("\n  Press Esc to cancel", style=theme.text_muted)

    term = state.current_glossary_term()
    title = f"Regenerate: {term.term}" if term else "Regenerate"

    return Panel(
        content,
        title=f"[{theme.warning}]{title}[/]",
        border_style=theme.warning,
    )


# =============================================================================
# USER MANAGEMENT VIEWS
# =============================================================================


def render_user_management_view(state: AppState) -> Panel:
    """Render user management view (admin only)."""
    theme = get_theme()
    content = Text()

    if not state.current_user or not state.current_user.is_admin:
        content.append_text(
            render_empty_state("Admin access required", "Only admins can manage users")
        )
        return Panel(
            content, title=f"[{theme.header}]User Management[/]", border_style=theme.border
        )

    if not state.user_mgmt_users:
        content.append_text(render_empty_state("No users found", "Press 'n' to create a user"))
        return Panel(
            content, title=f"[{theme.header}]User Management[/]", border_style=theme.border
        )

    content.append("\n")
    content.append(f"  Total Users: {len(state.user_mgmt_users)}\n", style=theme.text_muted)
    content.append(f"  Current User: {state.current_user.name}\n\n", style=theme.info)

    for i, user in enumerate(state.user_mgmt_users):
        is_selected = i == state.user_mgmt_selected_idx
        is_current = user.id == state.current_user.id

        # User indicator
        if is_current:
            indicator = "→ "
            style = theme.positive
        else:
            indicator = "  "
            style = theme.text if not is_selected else COLORS["selected"]

        # Selection marker
        if is_selected:
            prefix = "> "
            name_style = f"bold {COLORS['selected']}"
        else:
            prefix = "  "
            name_style = style

        # Admin badge
        admin_badge = " [ADMIN]" if user.is_admin else ""

        content.append(f"{prefix}{indicator}{user.name}{admin_badge}", style=name_style)
        content.append(f"  (ID: {user.id})\n", style=theme.text_muted)

    content.append("\n")

    if state.user_mgmt_confirm_delete and state.user_mgmt_users:
        user = state.user_mgmt_users[state.user_mgmt_selected_idx]
        content.append(f"  ⚠ Delete user '{user.name}'? (y/n)\n", style=f"bold {theme.warning}")

    return Panel(
        content,
        title=f"[{theme.header}]User Management[/]",
        border_style=theme.border,
        subtitle=f"[{theme.text_muted}]↑↓:Select  Enter:Switch  n:New  d:Delete  t:Toggle Admin  r:Rename  Esc:Back[/]",
        subtitle_align="left",
    )


def render_user_switcher_view(state: AppState) -> Panel:
    """Render user switcher popup."""
    theme = get_theme()
    content = Text()

    if not state.user_switcher_users:
        content.append_text(render_empty_state("No users found", ""))
        return Panel(content, title=f"[{theme.header}]Switch User[/]", border_style=theme.border)

    content.append("\n")
    content.append("  Select a user to switch to:\n\n", style=f"bold {theme.header}")

    for i, user in enumerate(state.user_switcher_users):
        is_selected = i == state.user_switcher_selected_idx
        is_current = state.current_user and user.id == state.current_user.id

        if is_current:
            indicator = "✓ "
            style = theme.positive
        else:
            indicator = "  "
            style = theme.text

        if is_selected:
            prefix = "> "
            name_style = f"bold {COLORS['selected']}"
        else:
            prefix = "  "
            name_style = style

        admin_badge = " [ADMIN]" if user.is_admin else ""

        content.append(f"{prefix}{indicator}{user.name}{admin_badge}\n", style=name_style)

    content.append("\n")

    return Panel(
        content,
        title=f"[{theme.header}]Switch User[/]",
        border_style=theme.border,
        subtitle=f"[{theme.text_muted}]↑↓:Select  Enter:Switch  Esc:Cancel[/]",
        subtitle_align="left",
    )


# =============================================================================
# MAIN RENDER
# =============================================================================


def render_app(console: Console, state: AppState) -> None:
    """Render the complete application."""
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
        from sp500_tui.help import render_help_overlay
        from rich.align import Align

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
