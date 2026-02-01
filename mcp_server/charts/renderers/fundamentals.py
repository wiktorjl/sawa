"""Fundamentals chart renderer."""

from typing import Any

from ..config import ChartDetail
from ..core.formatters import format_large_number, format_percent
from ..core.layout import Layout, get_layout
from ..core.sparkline import Sparkline
from ..themes import Theme, get_theme
from ..widgets.box import Box
from ..widgets.trend import TrendIndicator


def render_fundamentals_chart(
    data: dict[str, Any],
    ticker: str,
    layout: Layout | None = None,
    theme: Theme | None = None,
) -> str:
    """
    Render a fundamentals chart.

    Args:
        data: Dictionary with balance_sheets, cash_flows, income_statements
        ticker: Stock ticker symbol
        layout: Layout configuration
        theme: Theme to use

    Returns:
        Formatted chart string
    """
    if not data:
        return f"No fundamentals data available for {ticker}"

    if layout is None:
        layout = get_layout()
    if theme is None:
        theme = get_theme()

    balance_sheets = data.get("balance_sheets", [])
    cash_flows = data.get("cash_flows", [])
    income_statements = data.get("income_statements", [])

    if not any([balance_sheets, cash_flows, income_statements]):
        return f"No fundamentals data available for {ticker}"

    if layout.detail == ChartDetail.COMPACT:
        return _render_compact(ticker, balance_sheets, cash_flows, income_statements, layout, theme)
    elif layout.detail == ChartDetail.NORMAL:
        return _render_normal(ticker, balance_sheets, cash_flows, income_statements, layout, theme)
    else:
        return _render_detailed(
            ticker, balance_sheets, cash_flows, income_statements, layout, theme
        )


def _render_compact(
    ticker: str,
    balance_sheets: list[dict],
    cash_flows: list[dict],
    income_statements: list[dict],
    layout: Layout,
    theme: Theme,
) -> str:
    """Render compact fundamentals summary."""
    parts = [ticker]

    # Latest income statement metrics
    if income_statements:
        latest = income_statements[0]  # Most recent (sorted DESC)
        revenue = latest.get("total_revenue")
        net_income = latest.get("net_income")
        if revenue:
            parts.append(f"Rev: {format_large_number(revenue, prefix='$')}")
        if net_income:
            parts.append(f"NI: {format_large_number(net_income, prefix='$')}")

    # Latest balance sheet metrics
    if balance_sheets:
        latest = balance_sheets[0]
        equity = latest.get("total_equity")
        cash = latest.get("cash_and_equivalents")
        if equity:
            parts.append(f"Equity: {format_large_number(equity, prefix='$')}")
        if cash:
            parts.append(f"Cash: {format_large_number(cash, prefix='$')}")

    return f" {theme.muted_text('|')} ".join(parts)


def _render_normal(
    ticker: str,
    balance_sheets: list[dict],
    cash_flows: list[dict],
    income_statements: list[dict],
    layout: Layout,
    theme: Theme,
) -> str:
    """Render normal fundamentals chart."""
    spark = Sparkline(theme)
    trend = TrendIndicator(theme)
    box = Box(theme, layout.width)

    lines = []

    # Determine timeframe
    if income_statements:
        latest = income_statements[0]
        timeframe = "Quarterly" if latest.get("fiscal_quarter") else "Annual"
        periods = len(income_statements)
    else:
        timeframe = "N/A"
        periods = 0

    sep = theme.muted_text("|")
    lines.append(f"{ticker} Fundamentals {sep} {timeframe} {sep} Last {periods} periods")
    lines.append("")

    # Income Statement section
    if income_statements:
        lines.append(theme.muted_text("INCOME STATEMENT"))

        revenues = [
            d.get("total_revenue") for d in reversed(income_statements) if d.get("total_revenue")
        ]
        net_incomes = [
            d.get("net_income") for d in reversed(income_statements) if d.get("net_income")
        ]

        latest = income_statements[0]
        revenue = latest.get("total_revenue")
        net_income = latest.get("net_income")
        gross_margin = latest.get("gross_margin")
        profit_margin = latest.get("profit_margin")

        if revenues:
            rev_spark = spark.render(revenues, 20)
            rev_trend = trend.render(_calc_change(revenues))
            rev_val = format_large_number(revenue, prefix="$")
            lines.append(f"Revenue:      {rev_val:<12} {rev_spark} {rev_trend}")

        if net_incomes:
            ni_spark = spark.render(net_incomes, 20)
            ni_trend = trend.render(_calc_change(net_incomes))
            ni_val = format_large_number(net_income, prefix="$")
            lines.append(f"Net Income:   {ni_val:<12} {ni_spark} {ni_trend}")

        if gross_margin is not None or profit_margin is not None:
            margins = []
            if gross_margin is not None:
                margins.append(f"Gross: {format_percent(gross_margin)}")
            if profit_margin is not None:
                margins.append(f"Net: {format_percent(profit_margin)}")
            lines.append(f"Margins:      {' | '.join(margins)}")

        lines.append("")

    # Balance Sheet section
    if balance_sheets:
        lines.append(theme.muted_text("BALANCE SHEET"))

        latest = balance_sheets[0]
        assets = latest.get("total_assets")
        liabilities = latest.get("total_liabilities")
        equity = latest.get("total_equity")
        cash = latest.get("cash_and_equivalents")

        lines.append(f"Assets:       {format_large_number(assets, prefix='$')}")
        lines.append(f"Liabilities:  {format_large_number(liabilities, prefix='$')}")
        lines.append(f"Equity:       {format_large_number(equity, prefix='$')}")
        lines.append(f"Cash:         {format_large_number(cash, prefix='$')}")

        lines.append("")

    # Cash Flow section
    if cash_flows:
        lines.append(theme.muted_text("CASH FLOW"))

        latest = cash_flows[0]
        operating = latest.get("operating_cash_flow")
        fcf = latest.get("free_cash_flow")

        lines.append(f"Operating:    {format_large_number(operating, prefix='$')}")
        lines.append(f"Free CF:      {format_large_number(fcf, prefix='$')}")

    return box.render(lines, title=f"{ticker} Fundamentals")


def _render_detailed(
    ticker: str,
    balance_sheets: list[dict],
    cash_flows: list[dict],
    income_statements: list[dict],
    layout: Layout,
    theme: Theme,
) -> str:
    """Render detailed fundamentals chart."""
    box = Box(theme, layout.width)

    lines = []

    # Determine timeframe
    if income_statements:
        latest = income_statements[0]
        timeframe = "Quarterly" if latest.get("fiscal_quarter") else "Annual"
        periods = len(income_statements)
    else:
        timeframe = "N/A"
        periods = 0

    lines.append(f"{theme.primary_text(ticker, bold=True)} Fundamentals")
    lines.append(f"{timeframe} {theme.muted_text('|')} Last {periods} periods")
    lines.append("")

    # Income Statement detailed
    if income_statements:
        lines.append(theme.muted_text("=" * 60))
        lines.append(theme.primary_text("INCOME STATEMENT", bold=True))
        lines.append(theme.muted_text("=" * 60))

        # Period headers
        period_headers = []
        for stmt in income_statements[:4]:  # Show up to 4 periods
            fy = stmt.get("fiscal_year", "")
            fq = stmt.get("fiscal_quarter", "")
            if fq:
                period_headers.append(f"Q{fq}'{str(fy)[-2:]}")
            else:
                period_headers.append(f"FY{str(fy)[-2:]}")

        header = f"{'Metric':<20} " + " ".join(f"{h:>12}" for h in period_headers)
        lines.append(header)
        lines.append(theme.muted_text("-" * len(header)))

        # Metrics
        metrics = [
            ("Revenue", "total_revenue"),
            ("Gross Profit", "gross_profit"),
            ("Operating Income", "operating_income"),
            ("Net Income", "net_income"),
            ("EPS (Diluted)", "diluted_eps"),
            ("EBITDA", "ebitda"),
        ]

        for label, key in metrics:
            values = [stmt.get(key) for stmt in income_statements[:4]]
            if key == "diluted_eps":
                formatted = [f"${v:.2f}" if v else "--" for v in values]
            else:
                formatted = [format_large_number(v, prefix="$") if v else "--" for v in values]
            line = f"{label:<20} " + " ".join(f"{f:>12}" for f in formatted)
            lines.append(line)

        # Margins
        lines.append("")
        lines.append(theme.muted_text("Margins"))
        margin_metrics = [
            ("Gross Margin", "gross_margin"),
            ("Operating Margin", "operating_margin"),
            ("Profit Margin", "profit_margin"),
        ]

        for label, key in margin_metrics:
            values = [stmt.get(key) for stmt in income_statements[:4]]
            formatted = [format_percent(v) if v else "--" for v in values]
            line = f"{label:<20} " + " ".join(f"{f:>12}" for f in formatted)
            lines.append(line)

        lines.append("")

    # Balance Sheet detailed
    if balance_sheets:
        lines.append(theme.muted_text("=" * 60))
        lines.append(theme.primary_text("BALANCE SHEET", bold=True))
        lines.append(theme.muted_text("=" * 60))

        latest = balance_sheets[0]
        assets = latest.get("total_assets", 0) or 0
        liabilities = latest.get("total_liabilities", 0) or 0
        equity = latest.get("total_equity", 0) or 0

        # Visual breakdown
        if assets > 0:
            liab_pct = liabilities / assets
            equity_pct = equity / assets

            lines.append(f"Assets:       {format_large_number(assets, prefix='$')}")
            liab_val = format_large_number(liabilities, prefix="$")
            lines.append(f"Liabilities:  {liab_val} ({liab_pct * 100:.1f}% of assets)")
            equity_val = format_large_number(equity, prefix="$")
            lines.append(f"Equity:       {equity_val} ({equity_pct * 100:.1f}% of assets)")

        # Liquidity
        cash = latest.get("cash_and_equivalents")
        current_assets = latest.get("total_current_assets")
        current_liabilities = latest.get("total_current_liabilities")

        lines.append("")
        lines.append(theme.muted_text("Liquidity"))
        lines.append(f"Cash:                 {format_large_number(cash, prefix='$')}")

        if current_assets and current_liabilities and current_liabilities > 0:
            current_ratio = current_assets / current_liabilities
            lines.append(f"Current Ratio:        {current_ratio:.2f}")

        lines.append("")

    # Cash Flow detailed
    if cash_flows:
        lines.append(theme.muted_text("=" * 60))
        lines.append(theme.primary_text("CASH FLOW", bold=True))
        lines.append(theme.muted_text("=" * 60))

        # Period headers (reuse from income if available)
        period_headers = []
        for cf in cash_flows[:4]:
            fy = cf.get("fiscal_year", "")
            fq = cf.get("fiscal_quarter", "")
            if fq:
                period_headers.append(f"Q{fq}'{str(fy)[-2:]}")
            else:
                period_headers.append(f"FY{str(fy)[-2:]}")

        header = f"{'Metric':<20} " + " ".join(f"{h:>12}" for h in period_headers)
        lines.append(header)
        lines.append(theme.muted_text("-" * len(header)))

        cf_metrics = [
            ("Operating CF", "operating_cash_flow"),
            ("Investing CF", "investing_cash_flow"),
            ("Financing CF", "financing_cash_flow"),
            ("CapEx", "capex"),
            ("Free Cash Flow", "free_cash_flow"),
        ]

        for label, key in cf_metrics:
            values = [cf.get(key) for cf in cash_flows[:4]]
            formatted = [format_large_number(v, prefix="$") if v else "--" for v in values]
            line = f"{label:<20} " + " ".join(f"{f:>12}" for f in formatted)
            lines.append(line)

    return box.render(lines, title=f"{ticker} Fundamentals")


def _calc_change(values: list[float]) -> float:
    """Calculate percentage change from first to last value."""
    if not values or len(values) < 2:
        return 0.0
    first = values[0]
    last = values[-1]
    if first == 0:
        return 0.0
    return ((last - first) / abs(first)) * 100
