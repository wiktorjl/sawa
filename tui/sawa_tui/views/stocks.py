"""Stock list, detail, and news views."""

from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from sawa_tui.components import (
    SIDEBAR_WIDTH,
    panel_title,
    render_empty_state,
    render_scroll_indicator,
)
from sawa_tui.rendering.formatters import format_change, format_number, format_pct_change
from sawa_tui.state import AppState
from sawa_tui.themes import get_theme

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

    # Determine bottom panel: overview takes precedence over news
    if state.detail_overview_visible:
        # Layout with AI overview pane at bottom
        layout.split_column(
            Layout(name="header", size=6),
            Layout(name="body"),
            Layout(name="overview", size=18),
        )
    elif state.detail_show_news:
        # Layout with news pane at bottom
        layout.split_column(
            Layout(name="header", size=6),
            Layout(name="body"),
            Layout(name="news", size=12),
        )
    else:
        # Layout without bottom pane
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

    # Bottom panel (if visible)
    if state.detail_overview_visible:
        from sawa_tui.views.overview import render_overview_panel

        layout["overview"].update(render_overview_panel(state))
    elif state.detail_show_news:
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
            hi, lo, cl = highs[i], lows[i], closes[i]
            prev_c = closes[i - 1] if i > 0 else cl

            # Determine what to draw at this position
            if lo <= threshold_high and hi >= threshold_low:
                # Price range crosses this row
                if cl >= prev_c:
                    # Up candle
                    if cl >= threshold_low and cl <= threshold_high:
                        line_chars.append(("█", theme.positive))
                    elif hi >= threshold_low and lo <= threshold_high:
                        line_chars.append(("│", theme.positive))
                    else:
                        line_chars.append((" ", ""))
                else:
                    # Down candle
                    if cl >= threshold_low and cl <= threshold_high:
                        line_chars.append(("█", theme.negative))
                    elif hi >= threshold_low and lo <= threshold_high:
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
    """Render company information panel."""
    theme = get_theme()
    company = state.detail_company

    content = Text()

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


# =============================================================================
# NEWS FULLSCREEN VIEW
# =============================================================================


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
