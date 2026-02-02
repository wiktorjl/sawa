"""Screener view rendering."""

from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from sp500_tui.components import FOOTER_HEIGHT, HEADER_HEIGHT, render_scroll_indicator
from sp500_tui.state import AppState
from sp500_tui.themes import get_theme


def render_screener_view(state: AppState) -> Layout:
    """Render the stock screener view."""
    theme = get_theme()
    layout = Layout()

    # Split into search header and results table
    layout.split_column(
        Layout(name="search", size=3),
        Layout(name="results"),
    )

    # Search Bar
    search_text = Text()
    if state.screener_query:
        search_text.append(f"Query: {state.screener_query}", style=theme.text_bright)
    else:
        search_text.append(
            "Query: (Press / to filter, e.g. 'pe < 15 and yield > 0.03')", style=theme.text_muted
        )

    if state.screener_error:
        search_text.append(f"  {state.screener_error}", style=theme.negative)

    layout["search"].update(
        Panel(search_text, border_style=theme.border, title=f"[{theme.header}]Filter[/]")
    )

    # Results Table
    table = Table(
        show_header=True,
        header_style=f"bold {theme.header}",
        expand=True,
        row_styles=[theme.text, theme.text_muted],
        border_style=theme.border,
    )

    table.add_column("Ticker", width=8)
    table.add_column("Name", width=20)
    table.add_column("Sector", width=15)
    table.add_column("Price", justify="right", width=10)
    table.add_column("P/E", justify="right", width=8)
    table.add_column("Yield", justify="right", width=8)
    table.add_column("ROE", justify="right", width=8)
    table.add_column("Cap (B)", justify="right", width=10)

    # Scrolling
    results = state.screener_results
    visible_rows = state.get_visible_screener_rows()
    start = state.screener_scroll_offset
    end = start + visible_rows

    for i, item in enumerate(results[start:end], start=start):
        # Styles
        style = ""
        if i == state.screener_selected_idx:
            style = f"{theme.selected_text} {theme.selected}"

        # Formatting
        price = f"${item.price:.2f}" if item.price else "-"
        pe = f"{item.pe:.1f}" if item.pe else "-"
        dy = f"{item.dividend_yield * 100:.1f}%" if item.dividend_yield else "-"
        roe = f"{item.roe * 100:.1f}%" if item.roe else "-"
        cap = f"${item.market_cap / 1e9:.1f}B" if item.market_cap else "-"

        table.add_row(
            Text(item.ticker, style=style or theme.primary),
            Text(item.name[:18], style=style),
            Text(item.sector[:15], style=style),
            Text(price, style=style),
            Text(pe, style=style),
            Text(dy, style=style),
            Text(roe, style=style),
            Text(cap, style=style),
        )

    # Scroll info
    total = len(results)
    scroll_info = render_scroll_indicator(start, total, visible_rows) if total > 0 else ""

    layout["results"].update(
        Panel(
            table,
            title=f"[{theme.header}]Results ({total})[/] {scroll_info}",
            border_style=theme.border,
            title_align="left",
        )
    )

    return layout
