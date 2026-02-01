"""View rendering using Rich."""

from rich.console import Console
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from sp500_tui.state import AppState, EconomyTab, FundamentalsTab, View


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
    if value is None:
        return Text("-")
    if value >= 0:
        return Text(f"+{value:.2f}", style="green")
    return Text(f"{value:.2f}", style="red")


def format_pct_change(value: float | None) -> Text:
    """Format a percentage change with color."""
    if value is None:
        return Text("-")
    if value >= 0:
        return Text(f"+{value:.2f}%", style="green")
    return Text(f"{value:.2f}%", style="red")


# =============================================================================
# HEADER / FOOTER
# =============================================================================


def render_header(state: AppState) -> Panel:
    """Render the top navigation header."""
    views = [
        ("F1", "Stocks", View.STOCKS),
        ("F2", "Fundamentals", View.FUNDAMENTALS),
        ("F3", "Economy", View.ECONOMY),
        ("F4", "Settings", View.SETTINGS),
        ("F5", "Glossary", View.GLOSSARY),
    ]

    parts = [Text(" S&P 500 ", style="bold green"), Text(" | ")]

    for key, name, view in views:
        # Stock detail and news fullscreen are sub-views of Stocks
        is_current = state.current_view == view or (
            view == View.STOCKS and state.current_view in (View.STOCK_DETAIL, View.NEWS_FULLSCREEN)
        )
        if is_current:
            parts.append(Text(f" {key}:{name} ", style="bold black on green"))
        else:
            parts.append(Text(f" {key}:{name} ", style="dim"))

    parts.append(Text(" | ", style="dim"))
    parts.append(Text("q:Quit  r:Refresh", style="dim"))

    header_text = Text()
    for part in parts:
        header_text.append_text(part)

    return Panel(header_text, style="on black", height=3, padding=(0, 1))


def render_footer(state: AppState) -> Panel:
    """Render the bottom status bar."""
    if state.input_mode:
        # Input mode
        text = Text()
        text.append(state.input_prompt, style="yellow")
        text.append(": ")
        text.append(state.input_value, style="bold")
        text.append("_", style="blink")
        text.append("  [Esc] Cancel", style="dim")
    elif state.message:
        # Status message
        style = "red" if state.message_error else "green"
        text = Text(state.message, style=style)
    else:
        # Default help based on current view
        text = _get_view_help(state)

    return Panel(text, style="on black", height=3, padding=(0, 1))


def _get_view_help(state: AppState) -> Text:
    """Get help text for current view."""
    text = Text()

    if state.current_view == View.STOCKS:
        text.append("Enter", style="yellow")
        text.append(":View  ")
        text.append("Tab", style="yellow")
        text.append(":Switch  ")
        text.append("n", style="yellow")
        text.append(":New WL  ")
        text.append("d", style="yellow")
        text.append(":Delete WL  ")
        text.append("a", style="yellow")
        text.append(":Add Stock  ")
        text.append("x", style="yellow")
        text.append(":Remove Stock")

    elif state.current_view == View.STOCK_DETAIL:
        text.append("Esc", style="yellow")
        text.append(":Back  ")
        text.append("a", style="yellow")
        text.append(":Add to Watchlist  ")
        text.append("n", style="yellow")
        text.append(":Toggle News  ")
        text.append("N", style="yellow")
        text.append(":Fullscreen News")

    elif state.current_view == View.NEWS_FULLSCREEN:
        text.append("Esc", style="yellow")
        text.append(":Back  ")
        text.append("Up/Down", style="yellow")
        text.append(":Navigate  ")
        text.append("Enter", style="yellow")
        text.append(":Open URL")

    elif state.current_view == View.FUNDAMENTALS:
        text.append("1", style="yellow")
        text.append(":Income  ")
        text.append("2", style="yellow")
        text.append(":Balance  ")
        text.append("3", style="yellow")
        text.append(":Cash Flow  ")
        text.append("t", style="yellow")
        text.append(":Toggle Q/A  ")
        text.append("/", style="yellow")
        text.append(":Search Ticker")

    elif state.current_view == View.ECONOMY:
        text.append("1", style="yellow")
        text.append(":Yields  ")
        text.append("2", style="yellow")
        text.append(":Inflation  ")
        text.append("3", style="yellow")
        text.append(":Labor")

    elif state.current_view == View.SETTINGS:
        text.append("Settings view - use arrow keys to navigate", style="dim")

    elif state.current_view == View.GLOSSARY:
        if state.glossary_show_regen_menu:
            text.append("1-4", style="yellow")
            text.append(":Select  ")
            text.append("c", style="yellow")
            text.append(":Custom  ")
            text.append("Esc", style="yellow")
            text.append(":Cancel")
        elif state.glossary_loading:
            text.append("Generating definition...", style="yellow")
        else:
            text.append("/", style="yellow")
            text.append(":Search  ")
            text.append("Enter", style="yellow")
            text.append(":Generate  ")
            text.append("g", style="yellow")
            text.append(":Regen  ")
            text.append("n", style="yellow")
            text.append(":Add  ")
            text.append("d", style="yellow")
            text.append(":Delete  ")
            text.append("1-5", style="yellow")
            text.append(":Related")

    return text


# =============================================================================
# STOCKS VIEW
# =============================================================================


def render_stocks_view(state: AppState) -> Layout:
    """Render the stocks/watchlist view."""
    layout = Layout()

    layout.split_row(
        Layout(name="sidebar", size=28),
        Layout(name="main"),
    )

    # Sidebar: watchlist list
    layout["sidebar"].update(_render_watchlist_sidebar(state))

    # Main: stock table
    layout["main"].update(_render_stock_table(state))

    return layout


def _render_watchlist_sidebar(state: AppState) -> Panel:
    """Render the watchlist sidebar."""
    lines = []
    visible_rows = state.get_visible_watchlist_rows()
    start = state.watchlist_scroll_offset
    end = start + visible_rows

    for i, wl in enumerate(state.watchlists[start:end], start=start):
        prefix = "* " if wl.is_default else "  "
        name = f"{prefix}{wl.name} ({wl.symbol_count})"

        if i == state.selected_watchlist_idx and state.focus_sidebar:
            lines.append(Text(name, style="bold green on dark_green"))
        elif i == state.selected_watchlist_idx:
            lines.append(Text(name, style="green"))
        else:
            lines.append(Text(name, style="dim"))

    content = Text("\n").join(lines) if lines else Text("No watchlists", style="dim")

    # Scroll indicators
    scroll_info = ""
    total = len(state.watchlists)
    if total > visible_rows:
        if start > 0:
            scroll_info += " [dim]^[/]"
        if end < total:
            scroll_info += " [dim]v[/]"

    help_text = Text("\n\n[n] New  [d] Delete", style="dim")
    full_content = Text()
    full_content.append_text(content)
    full_content.append_text(help_text)

    title = f"[yellow]WATCHLISTS[/]{scroll_info}"
    return Panel(
        full_content,
        title=title,
        title_align="left",
        border_style="green" if state.focus_sidebar else "dim",
        height=None,
    )


def _render_stock_table(state: AppState) -> Panel:
    """Render the stock table."""
    wl = state.current_watchlist()
    title = wl.name if wl else "No Watchlist"

    table = Table(
        show_header=True,
        header_style="bold yellow",
        expand=True,
        row_styles=["", "dim"],
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

    for i, stock in enumerate(state.watchlist_stocks[start:end], start=start):
        ticker = stock.ticker
        name = (stock.name or "")[:24]
        price = stock.price
        change = stock.change
        change_pct = stock.change_pct
        volume = stock.volume

        # Highlight selected row
        if i == state.selected_stock_idx and not state.focus_sidebar:
            style = "bold green on dark_green"
        else:
            style = ""

        table.add_row(
            Text(ticker, style=style or "green"),
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

    # Scroll indicators
    total = len(state.watchlist_stocks)
    scroll_info = ""
    if total > visible_rows:
        if start > 0:
            scroll_info += " ^"
        if end < total:
            scroll_info += " v"

    return Panel(
        table,
        title=f"[yellow]{title}[/] ({len(state.watchlist_stocks)} stocks){scroll_info}",
        title_align="left",
        border_style="green" if not state.focus_sidebar else "dim",
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
    layout["body"]["sidebar"].split_column(
        Layout(name="info", ratio=1),
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
    company = state.detail_company
    if not company:
        return Panel(Text(f"Loading {state.detail_ticker}...", style="yellow"))

    # Line 1: Ticker, Name, Sector
    text = Text()
    text.append(f" {company.ticker} ", style="bold white on dark_green")
    text.append(f" {company.name} ", style="bold white")
    text.append(f" {company.sector or 'Unknown'} ", style="dim")
    if company.exchange:
        text.append(f" [{company.exchange}]", style="dim cyan")

    # Line 2: Price and change
    text.append("\n ")
    if state.detail_prices:
        latest = state.detail_prices[0]
        text.append(f"${latest.close:.2f}", style="bold yellow")

        if len(state.detail_prices) > 1:
            prev = state.detail_prices[1]
            change = latest.close - prev.close
            pct = (change / prev.close * 100) if prev.close else 0
            if change >= 0:
                text.append(f"  +{change:.2f} (+{pct:.2f}%)", style="bold green")
            else:
                text.append(f"  {change:.2f} ({pct:.2f}%)", style="bold red")

        # Add today's OHLV
        text.append(f"   O:{latest.open:.2f}", style="dim")
        text.append(f"  H:{latest.high:.2f}", style="dim")
        text.append(f"  L:{latest.low:.2f}", style="dim")
        text.append(f"  V:{format_number(latest.volume, decimals=0)}", style="dim")

    # Line 3: 52-week range
    text.append("\n ")
    if state.detail_52w_low and state.detail_52w_high:
        text.append("52W: ", style="dim")
        text.append(f"${state.detail_52w_low:.2f}", style="red")
        text.append(" - ", style="dim")
        text.append(f"${state.detail_52w_high:.2f}", style="green")

        # Show position in range as a mini bar
        if state.detail_prices:
            current = state.detail_prices[0].close
            range_52w = state.detail_52w_high - state.detail_52w_low
            if range_52w > 0:
                pos = (current - state.detail_52w_low) / range_52w
                bar_width = 20
                filled = int(pos * bar_width)
                text.append("  [", style="dim")
                text.append("=" * filled, style="green")
                text.append("-" * (bar_width - filled), style="dim")
                text.append("]", style="dim")

    if state.detail_avg_volume:
        text.append(
            f"   Avg Vol: {format_number(state.detail_avg_volume, decimals=0)}", style="dim"
        )

    return Panel(text, border_style="green", padding=(0, 1))


def _render_price_chart(state: AppState) -> Panel:
    """Render ASCII price chart that fills available space."""
    if not state.detail_prices:
        return Panel(Text("No price data", style="dim"), title="[yellow]Price Chart[/]")

    # Use more price data and dynamic sizing
    prices = list(reversed(state.detail_prices))
    if not prices:
        return Panel(Text("No data", style="dim"), title="[yellow]Price Chart[/]")

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
                    # Green candle (up)
                    if c >= threshold_low and c <= threshold_high:
                        line_chars.append(("█", "green"))
                    elif h >= threshold_low and l <= threshold_high:
                        line_chars.append(("│", "green"))
                    else:
                        line_chars.append((" ", ""))
                else:
                    # Red candle (down)
                    if c >= threshold_low and c <= threshold_high:
                        line_chars.append(("█", "red"))
                    elif h >= threshold_low and l <= threshold_high:
                        line_chars.append(("│", "red"))
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
            chart_content.append("\n")
        chart_content.append_text(line)

    period = len(state.detail_prices)
    return Panel(
        chart_content,
        title=f"[yellow]{period}-Day Price Chart[/]",
        border_style="green",
    )


def _render_company_info(state: AppState) -> Panel:
    """Render company information panel with description."""
    company = state.detail_company

    content = Text()

    if company:
        # Company description
        if company.description:
            content.append(company.description, style="white")
            content.append("\n\n")

        # Key stats in a compact format
        stats = []
        if company.employees:
            stats.append(f"Employees: {company.employees:,}")
        if company.exchange:
            stats.append(f"Exchange: {company.exchange}")
        if company.cik:
            stats.append(f"CIK: {company.cik}")

        if stats:
            content.append(" | ".join(stats), style="dim")

        if company.homepage_url:
            content.append("\n")
            url = company.homepage_url
            if len(url) > 45:
                url = url[:42] + "..."
            content.append(url, style="dim cyan")

        if company.address:
            content.append("\n")
            content.append(company.address, style="dim")
    else:
        content.append("No company data", style="dim")

    return Panel(content, title="[yellow]About[/]", border_style="dim")


def _render_ratios(state: AppState) -> Panel:
    """Render key ratios in two columns."""
    ratios = state.detail_ratios

    table = Table(show_header=False, expand=True, box=None, padding=(0, 1))
    table.add_column("Metric", style="dim", width=12)
    table.add_column("Value", style="green", width=10)
    table.add_column("Metric", style="dim", width=12)
    table.add_column("Value", style="green", width=10)

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
        table.add_row(Text("No ratio data", style="dim"), "", "", "")

    return Panel(table, title="[yellow]Key Ratios[/]", border_style="dim")


def _render_news_pane(state: AppState) -> Panel:
    """Render news and sentiment pane."""
    content = Text()

    # Sentiment summary header
    sentiment = state.detail_news_sentiment
    if sentiment:
        pos = sentiment.get("positive", 0)
        neg = sentiment.get("negative", 0)
        neu = sentiment.get("neutral", 0)
        total = pos + neg + neu

        content.append(" SENTIMENT (30d): ", style="bold cyan")
        if pos > 0:
            content.append(f"+{pos}", style="bold green")
            content.append("  ")
        if neg > 0:
            content.append(f"-{neg}", style="bold red")
            content.append("  ")
        if neu > 0:
            content.append(f"~{neu}", style="dim")
            content.append("  ")

        # Sentiment bar
        if total > 0:
            bar_width = 20
            pos_width = int((pos / total) * bar_width)
            neg_width = int((neg / total) * bar_width)
            neu_width = bar_width - pos_width - neg_width
            content.append("[", style="dim")
            content.append("+" * pos_width, style="green")
            content.append("-" * neg_width, style="red")
            content.append("=" * neu_width, style="dim")
            content.append("]", style="dim")
        content.append("\n\n")

    # News headlines
    if state.detail_news:
        for article in state.detail_news[:6]:  # Show top 6 articles
            # Sentiment indicator
            if article.sentiment == "positive":
                content.append(" + ", style="green")
            elif article.sentiment == "negative":
                content.append(" - ", style="red")
            else:
                content.append(" ~ ", style="dim")

            # Date
            date_str = article.published_utc.strftime("%m/%d")
            content.append(f"[{date_str}] ", style="dim")

            # Title (truncated)
            title = article.title
            max_title_len = state.term_width - 25
            if len(title) > max_title_len:
                title = title[: max_title_len - 3] + "..."
            content.append(title, style="white")

            # Publisher
            if article.publisher_name:
                content.append(f" ({article.publisher_name})", style="dim")

            content.append("\n")
    else:
        content.append(" No recent news available", style="dim")

    title = "[yellow]News & Sentiment[/] [dim](n:toggle N:fullscreen)[/]"
    return Panel(content, title=title, border_style="dim")


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
    content = Text()

    # Ticker info
    content.append(f" {state.detail_ticker} ", style="bold white on dark_green")
    if state.detail_company:
        content.append(f" {state.detail_company.name} ", style="white")
    content.append(" - News & Sentiment\n", style="dim")

    # Sentiment summary
    sentiment = state.detail_news_sentiment
    if sentiment:
        pos = sentiment.get("positive", 0)
        neg = sentiment.get("negative", 0)
        neu = sentiment.get("neutral", 0)
        total = pos + neg + neu

        content.append(" 30-Day Sentiment: ", style="bold cyan")
        if pos > 0:
            content.append(f"Positive: {pos}", style="bold green")
            content.append("  ")
        if neg > 0:
            content.append(f"Negative: {neg}", style="bold red")
            content.append("  ")
        if neu > 0:
            content.append(f"Neutral: {neu}", style="dim")

        # Sentiment bar
        if total > 0:
            bar_width = 30
            pos_width = int((pos / total) * bar_width)
            neg_width = int((neg / total) * bar_width)
            neu_width = bar_width - pos_width - neg_width
            content.append("\n ")
            content.append("[", style="dim")
            content.append("+" * pos_width, style="green")
            content.append("-" * neg_width, style="red")
            content.append("=" * neu_width, style="dim")
            content.append("]", style="dim")

    return Panel(content, border_style="green")


def _render_news_list(state: AppState) -> Panel:
    """Render scrollable news list with selection."""
    content = Text()

    if not state.detail_news:
        content.append(" No news articles available", style="dim")
        return Panel(content, title="[yellow]Articles[/]", border_style="dim")

    visible_rows = state.get_visible_news_rows()
    start = state.news_scroll_offset
    end = start + visible_rows

    for i, article in enumerate(state.detail_news[start:end], start=start):
        is_selected = i == state.selected_news_idx

        # Selection indicator
        if is_selected:
            content.append(" > ", style="bold green")
        else:
            content.append("   ", style="")

        # Sentiment indicator
        if article.sentiment == "positive":
            content.append("[+] ", style="green")
        elif article.sentiment == "negative":
            content.append("[-] ", style="red")
        else:
            content.append("[~] ", style="dim")

        # Date
        date_str = article.published_utc.strftime("%m/%d %H:%M")
        content.append(f"[{date_str}] ", style="dim")

        # Title (truncated)
        title = article.title
        max_title_len = state.term_width - 30
        if len(title) > max_title_len:
            title = title[: max_title_len - 3] + "..."

        if is_selected:
            content.append(title, style="bold green")
        else:
            content.append(title, style="white")

        content.append("\n")

    # Scroll indicators
    total = len(state.detail_news)
    scroll_info = ""
    if total > visible_rows:
        if start > 0:
            scroll_info += " ^"
        if end < total:
            scroll_info += " v"

    title = f"[yellow]Articles ({len(state.detail_news)})[/]{scroll_info}"
    return Panel(content, title=title, border_style="green")


def _render_news_detail(state: AppState) -> Panel:
    """Render selected article detail."""
    article = state.current_news_article()
    content = Text()

    if not article:
        content.append(" Select an article to view details", style="dim")
        return Panel(content, title="[yellow]Details[/]", border_style="dim")

    # Title
    content.append(f" {article.title}\n", style="bold white")

    # Meta info
    if article.author:
        content.append(f" By: {article.author}", style="dim")
        if article.publisher_name:
            content.append(f" | {article.publisher_name}", style="dim")
        content.append("\n")
    elif article.publisher_name:
        content.append(f" Source: {article.publisher_name}\n", style="dim")

    # Date and sentiment
    date_str = article.published_utc.strftime("%Y-%m-%d %H:%M UTC")
    content.append(f" Published: {date_str}", style="dim")
    if article.sentiment:
        style = (
            "green"
            if article.sentiment == "positive"
            else ("red" if article.sentiment == "negative" else "dim")
        )
        content.append(f"  |  Sentiment: {article.sentiment.capitalize()}", style=style)
    content.append("\n")

    # Description
    if article.description:
        desc = article.description
        if len(desc) > 200:
            desc = desc[:197] + "..."
        content.append(f"\n {desc}\n", style="white")

    # URL hint
    if article.article_url:
        content.append("\n [Enter] Open in browser", style="yellow")

    return Panel(content, title="[yellow]Details[/]", border_style="dim")


# =============================================================================
# FUNDAMENTALS VIEW
# =============================================================================


def render_fundamentals_view(state: AppState) -> Layout:
    """Render the fundamentals view."""
    layout = Layout()

    layout.split_column(
        Layout(name="header", size=4),
        Layout(name="body"),
    )

    # Header with search and tabs
    layout["header"].update(_render_fund_header(state))

    # Body with data table
    layout["body"].update(_render_fund_table(state))

    return layout


def _render_fund_header(state: AppState) -> Panel:
    """Render fundamentals header with ticker and tabs."""
    text = Text()

    # Ticker/company info
    if state.fund_company:
        text.append(f" {state.fund_ticker} ", style="bold green")
        text.append(f"{state.fund_company.name}", style="white")
    else:
        text.append(" Press / to search for a ticker", style="dim")

    text.append("\n")

    # Tabs
    tabs = [
        ("1", "Income Statement", FundamentalsTab.INCOME),
        ("2", "Balance Sheet", FundamentalsTab.BALANCE),
        ("3", "Cash Flow", FundamentalsTab.CASHFLOW),
    ]

    for key, name, tab in tabs:
        if state.fund_tab == tab:
            text.append(f" [{key}]{name} ", style="bold black on green")
        else:
            text.append(f" [{key}]{name} ", style="dim")

    text.append("   ")
    tf = "Quarterly" if state.fund_quarterly else "Annual"
    text.append(f"[t] {tf}", style="yellow")

    return Panel(text, border_style="green")


def _render_fund_table(state: AppState) -> Panel:
    """Render the fundamentals data table."""
    table = Table(
        show_header=True,
        header_style="bold yellow",
        expand=True,
        row_styles=["", "dim"],
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

    return Panel(table, title=f"[yellow]{title}[/]", border_style="dim")


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
    text = Text()
    text.append(" ECONOMIC INDICATORS ", style="bold yellow")
    text.append("  ")

    tabs = [
        ("1", "Treasury Yields", EconomyTab.YIELDS),
        ("2", "Inflation", EconomyTab.INFLATION),
        ("3", "Labor Market", EconomyTab.LABOR),
    ]

    for key, name, tab in tabs:
        if state.econ_tab == tab:
            text.append(f" [{key}]{name} ", style="bold black on green")
        else:
            text.append(f" [{key}]{name} ", style="dim")

    return Panel(text, border_style="green")


def _render_econ_indicators(state: AppState) -> Panel:
    """Render key economic indicators summary."""
    table = Table(show_header=False, expand=True, box=None)

    # 4 columns for key metrics
    table.add_column("Metric", style="dim", width=15)
    table.add_column("Value", style="green", width=12)
    table.add_column("Metric", style="dim", width=15)
    table.add_column("Value", style="green", width=12)
    table.add_column("Metric", style="dim", width=15)
    table.add_column("Value", style="green", width=12)
    table.add_column("Metric", style="dim", width=15)
    table.add_column("Value", style="green", width=12)

    # Get latest values
    y10 = state.econ_yields[0].yield_10y if state.econ_yields else None
    y2 = state.econ_yields[0].yield_2y if state.econ_yields else None
    cpi = state.econ_inflation[0].cpi_yoy if state.econ_inflation else None
    unemp = state.econ_labor[0].unemployment_rate if state.econ_labor else None

    table.add_row(
        "10Y Treasury",
        f"{y10:.2f}%" if y10 else "-",
        "2Y Treasury",
        f"{y2:.2f}%" if y2 else "-",
        "CPI YoY",
        f"{cpi * 100:.1f}%" if cpi else "-",
        "Unemployment",
        f"{unemp * 100:.1f}%" if unemp else "-",
    )

    return Panel(table, title="[yellow]Latest Indicators[/]", border_style="dim")


def _render_econ_table(state: AppState) -> Panel:
    """Render economy data table based on selected tab."""
    table = Table(
        show_header=True,
        header_style="bold yellow",
        expand=True,
        row_styles=["", "dim"],
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
            table.add_row(
                str(i.date),
                f"{i.cpi:.2f}" if i.cpi else "-",
                f"{i.cpi_core:.2f}" if i.cpi_core else "-",
                f"{i.cpi_yoy * 100:.1f}%" if i.cpi_yoy else "-",
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
            table.add_row(
                str(lm.date),
                f"{lm.unemployment_rate * 100:.1f}%" if lm.unemployment_rate else "-",
                f"{lm.participation_rate * 100:.1f}%" if lm.participation_rate else "-",
                f"${lm.avg_hourly_earnings:.2f}" if lm.avg_hourly_earnings else "-",
                f"{lm.job_openings / 1000:.0f}K" if lm.job_openings else "-",
            )
        title = "Labor Market History"

    return Panel(table, title=f"[yellow]{title}[/]", border_style="dim")


# =============================================================================
# SETTINGS VIEW
# =============================================================================


def render_settings_view(state: AppState) -> Panel:
    """Render the settings view."""
    text = Text()
    text.append(" SETTINGS\n\n", style="bold yellow")
    text.append(" Settings management coming soon.\n", style="dim")
    text.append(" Current settings are stored in the database.\n", style="dim")

    return Panel(text, border_style="green")


# =============================================================================
# GLOSSARY VIEW
# =============================================================================


def render_glossary_view(state: AppState) -> Layout:
    """Render the glossary view."""
    layout = Layout()

    layout.split_row(
        Layout(name="sidebar", size=30),
        Layout(name="main"),
    )

    # Sidebar: term list
    layout["sidebar"].update(_render_glossary_sidebar(state))

    # Main: definition or loading/error state
    layout["main"].update(_render_glossary_definition(state))

    return layout


def _render_glossary_sidebar(state: AppState) -> Panel:
    """Render the glossary term list sidebar."""
    content = Text()

    # Search box
    if state.glossary_search:
        content.append(f" / {state.glossary_search}", style="yellow")
    else:
        content.append(" / Search...", style="dim")
    content.append("\n\n")

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
            content.append(line + "\n", style="bold green on dark_green")
        elif i == state.selected_term_idx:
            content.append(line + "\n", style="green")
        else:
            content.append(line + "\n", style="dim" if not term.has_definition else "white")

    # Scroll indicators
    total = len(state.glossary_filtered)
    scroll_info = ""
    if total > visible_rows:
        if start > 0:
            scroll_info += " ^"
        if end < total:
            scroll_info += " v"

    # Help text
    content.append("\n")
    content.append("[n] Add term  [d] Delete", style="dim")
    content.append("\n")
    content.append("* = cached definition", style="dim")

    title = f"[yellow]TERMS ({len(state.glossary_filtered)})[/]{scroll_info}"
    return Panel(
        content,
        title=title,
        title_align="left",
        border_style="green" if state.glossary_focus_sidebar else "dim",
    )


def _render_glossary_definition(state: AppState) -> Panel:
    """Render the glossary definition panel."""
    # Show regeneration menu if active
    if state.glossary_show_regen_menu:
        return _render_regen_menu(state)

    # Loading state with streaming content
    if state.glossary_loading:
        content = Text()
        content.append(" Generating definition...\n\n", style="bold yellow")

        # Spinner animation (will be static but indicates activity)
        content.append(" ", style="")
        content.append("[", style="dim")
        # Simple progress indicator
        content.append("=" * 20, style="green")
        content.append(">", style="bold green")
        content.append(" " * 10, style="dim")
        content.append("]", style="dim")
        content.append("\n\n")

        # Show streaming content as it arrives
        if state.glossary_stream_content:
            content.append(state.glossary_stream_content, style="white")

        return Panel(
            content,
            title="[yellow]Generating...[/]",
            border_style="yellow",
        )

    # Error state
    if state.glossary_error:
        content = Text()
        content.append(" Error\n\n", style="bold red")
        content.append(f" {state.glossary_error}\n\n", style="red")
        content.append(" Press Enter to retry", style="dim")

        return Panel(
            content,
            title="[red]Error[/]",
            border_style="red",
        )

    # No term selected
    term = state.current_glossary_term()
    if not term:
        content = Text()
        content.append("\n Select a term from the list\n", style="dim")
        content.append(" Press Enter to generate definition\n", style="dim")

        return Panel(
            content,
            title="[yellow]Definition[/]",
            border_style="dim",
        )

    # No definition yet
    definition = state.glossary_definition
    if not definition:
        content = Text()
        content.append(f"\n {term.term}\n\n", style="bold green")
        content.append(" No definition cached.\n", style="dim")
        content.append(" Press Enter to generate.\n", style="dim")

        return Panel(
            content,
            title=f"[yellow]{term.term}[/]",
            border_style="dim",
        )

    # Render the full definition
    content = Text()

    # Official definition
    content.append(" OFFICIAL DEFINITION\n", style="bold cyan")
    content.append(f" {definition.official_definition}\n\n", style="white")

    # Plain English
    content.append(" WHAT IT ACTUALLY MEANS\n", style="bold cyan")
    content.append(f" {definition.plain_english}\n\n", style="white")

    # Examples
    if definition.examples:
        content.append(" EXAMPLES\n", style="bold cyan")
        for i, example in enumerate(definition.examples, 1):
            content.append(f" {i}. {example}\n", style="white")
        content.append("\n")

    # Related terms with numbers for quick jump
    if definition.related_terms:
        content.append(" RELATED TERMS\n", style="bold cyan")
        for i, related in enumerate(definition.related_terms[:5], 1):
            content.append(f" [{i}] ", style="yellow")
            content.append(f"{related}  ", style="green")
        content.append("\n\n")

    # Learn more links
    if definition.learn_more:
        content.append(" LEARN MORE\n", style="bold cyan")
        for url in definition.learn_more[:3]:
            # Truncate long URLs
            display_url = url
            if len(display_url) > 60:
                display_url = display_url[:57] + "..."
            content.append(f" {display_url}\n", style="dim cyan")

    # Show if regenerated with custom prompt
    if definition.custom_prompt:
        content.append("\n")
        content.append(f" [Customized: {definition.custom_prompt[:30]}...]", style="dim")

    return Panel(
        content,
        title=f"[yellow]{definition.term}[/]",
        border_style="green" if not state.glossary_focus_sidebar else "dim",
    )


def _render_regen_menu(state: AppState) -> Panel:
    """Render the regeneration options menu."""
    content = Text()
    content.append("\n Regenerate definition with:\n\n", style="bold yellow")

    options = [
        ("1", "More technical", "Use more technical language and formulas"),
        ("2", "Simpler explanation", "Make it easier for beginners"),
        ("3", "Add more examples", "Include 4-5 practical examples"),
        ("4", "Focus on practical use", "How investors use this metric"),
        ("c", "Custom instructions", "Enter your own prompt"),
    ]

    for key, label, desc in options:
        content.append(f"  [{key}] ", style="yellow")
        content.append(f"{label}\n", style="bold white")
        content.append(f"      {desc}\n\n", style="dim")

    content.append("\n  Press Esc to cancel", style="dim")

    term = state.current_glossary_term()
    title = f"Regenerate: {term.term}" if term else "Regenerate"

    return Panel(
        content,
        title=f"[yellow]{title}[/]",
        border_style="yellow",
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

    # Footer
    layout["footer"].update(render_footer(state))

    # Move cursor to home and overwrite (no flicker)
    # Using ANSI escape: \033[H moves cursor to row 1, col 1
    print("\033[H", end="")
    console.print(layout)
