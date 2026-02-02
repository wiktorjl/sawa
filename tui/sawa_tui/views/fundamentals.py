"""Fundamentals view (Income Statement, Balance Sheet, Cash Flow)."""

from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from sawa_tui.rendering.formatters import format_number, render_sparkline
from sawa_tui.state import AppState, FundamentalsTab
from sawa_tui.themes import get_theme


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
