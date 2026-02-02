"""Economy view (Treasury Yields, Inflation, Labor Market)."""

from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from sp500_tui.rendering.formatters import (
    format_rate_as_pct,
    render_sparkline,
    render_trend_indicator,
)
from sp500_tui.state import AppState, EconomyTab
from sp500_tui.themes import get_theme


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
