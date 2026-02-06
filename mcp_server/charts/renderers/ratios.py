"""Financial ratios chart renderer."""

from typing import Any

from ..config import ChartDetail
from ..core.formatters import format_large_number, format_percent
from ..core.layout import Layout, get_layout
from ..core.sparkline import Sparkline
from ..themes import Theme, get_theme
from ..widgets.box import Box
from ..widgets.trend import TrendIndicator

# Arrow constant to avoid Python 3.10 f-string escape issues
ARROW = "\u2192"


def render_ratios_chart(
    data: list[dict[str, Any]],
    ticker: str,
    layout: Layout | None = None,
    theme: Theme | None = None,
) -> str:
    """
    Render a financial ratios chart.

    Args:
        data: List of ratio records
        ticker: Stock ticker symbol
        layout: Layout configuration
        theme: Theme to use

    Returns:
        Formatted chart string
    """
    if not data:
        return f"No ratio data available for {ticker}"

    if layout is None:
        layout = get_layout()
    if theme is None:
        theme = get_theme()

    # Extract key metrics
    metrics = _extract_metrics(data)

    if layout.detail == ChartDetail.COMPACT:
        return _render_compact(ticker, metrics, layout, theme)
    elif layout.detail == ChartDetail.NORMAL:
        return _render_normal(ticker, metrics, data, layout, theme)
    else:
        return _render_detailed(ticker, metrics, data, layout, theme)


def _extract_metrics(data: list[dict[str, Any]]) -> dict[str, Any]:
    """Extract key metrics from ratio data."""

    def get_series(key: str) -> list[float]:
        return [float(d[key]) for d in data if d.get(key) is not None]

    def get_stats(series: list[float]) -> dict[str, float | None]:
        if not series:
            return {"current": None, "min": None, "max": None, "first": None}
        return {
            "current": series[-1],
            "min": min(series),
            "max": max(series),
            "first": series[0],
        }

    return {
        "pe_ratio": get_stats(get_series("pe_ratio")),
        "pb_ratio": get_stats(get_series("pb_ratio")),
        "ps_ratio": get_stats(get_series("ps_ratio")),
        "debt_to_equity": get_stats(get_series("debt_to_equity")),
        "roe": get_stats(get_series("roe")),
        "roa": get_stats(get_series("roa")),
        "dividend_yield": get_stats(get_series("dividend_yield")),
        "eps": get_stats(get_series("eps")),
        "market_cap": get_stats(get_series("market_cap")),
        "ev": get_stats(get_series("ev")),
        "ev_to_ebitda": get_stats(get_series("ev_to_ebitda")),
        "price": get_stats(get_series("price")),
        "series": {
            "pe_ratio": get_series("pe_ratio"),
            "roe": get_series("roe"),
            "debt_to_equity": get_series("debt_to_equity"),
            "price": get_series("price"),
        },
        "dates": [d.get("date") for d in data],
    }


def _render_compact(
    ticker: str,
    metrics: dict[str, Any],
    layout: Layout,
    theme: Theme,
) -> str:
    """Render compact single-line ratios summary."""
    parts = [ticker]

    # Key metrics
    pe = metrics["pe_ratio"]["current"]
    roe = metrics["roe"]["current"]
    de = metrics["debt_to_equity"]["current"]

    if pe is not None:
        parts.append(f"P/E: {pe:.1f}")
    if roe is not None:
        parts.append(f"ROE: {format_percent(roe)}")
    if de is not None:
        parts.append(f"D/E: {de:.2f}")

    # Market cap
    mc = metrics["market_cap"]["current"]
    if mc is not None:
        parts.append(f"MCap: {format_large_number(mc, prefix='$')}")

    return f" {theme.muted_text('|')} ".join(parts)


def _render_normal(
    ticker: str,
    metrics: dict[str, Any],
    data: list[dict[str, Any]],
    layout: Layout,
    theme: Theme,
) -> str:
    """Render normal ratios chart with sparklines."""
    spark = Sparkline(theme)
    trend = TrendIndicator(theme)
    box = Box(theme, layout.width)

    # Date range
    dates = metrics["dates"]
    start_date = dates[0] if dates else "?"
    end_date = dates[-1] if dates else "?"

    lines = []

    # Header
    header = f"{ticker} Financial Ratios {theme.muted_text('|')} "
    header += f"{start_date} {theme.muted_text(ARROW)} {end_date}"
    lines.append(header)
    lines.append("")

    # Build metric rows with sparklines
    metric_rows = [
        ("P/E Ratio", "pe_ratio", lambda x: f"{x:.1f}" if x else "--"),
        ("P/B Ratio", "pb_ratio", lambda x: f"{x:.1f}" if x else "--"),
        ("ROE", "roe", lambda x: format_percent(x) if x else "--"),
        ("ROA", "roa", lambda x: format_percent(x) if x else "--"),
        ("Debt/Equity", "debt_to_equity", lambda x: f"{x:.2f}" if x else "--"),
        ("Div Yield", "dividend_yield", lambda x: format_percent(x) if x else "--"),
    ]

    label_width = max(len(row[0]) for row in metric_rows)

    for label, key, fmt in metric_rows:
        stats = metrics.get(key, {})
        current = stats.get("current")
        series = metrics["series"].get(key, [])

        # Format current value
        current_str = fmt(current)

        # Sparkline if we have series data
        if series and len(series) > 1:
            sparkline_str = spark.render(series, 20)

            # Trend
            first = stats.get("first")
            if first and current:
                change = ((current - first) / abs(first)) * 100 if first else 0
                trend_str = trend.render(change)
            else:
                trend_str = ""

            sep = theme.muted_text("|")
            line = (
                f"{label:<{label_width}} {sep} {current_str:>10} {sep} {sparkline_str} {trend_str}"
            )
        else:
            sep = theme.muted_text("|")
            line = f"{label:<{label_width}} {sep} {current_str:>10}"

        lines.append(line)

    lines.append("")

    # Valuation summary
    mc = metrics["market_cap"]["current"]
    ev = metrics["ev"]["current"]
    lines.append(theme.muted_text("VALUATION"))
    mc_str = format_large_number(mc, prefix="$")
    ev_str = format_large_number(ev, prefix="$")
    lines.append(f"Market Cap: {mc_str:<15} EV: {ev_str}")

    return box.render(lines, title=f"{ticker} Financial Ratios")


def _render_detailed(
    ticker: str,
    metrics: dict[str, Any],
    data: list[dict[str, Any]],
    layout: Layout,
    theme: Theme,
) -> str:
    """Render detailed ratios chart with full statistics."""
    spark = Sparkline(theme)
    trend = TrendIndicator(theme)
    box = Box(theme, layout.width)

    dates = metrics["dates"]
    start_date = dates[0] if dates else "?"
    end_date = dates[-1] if dates else "?"

    lines = []

    # Title
    title = f"{theme.primary_text(ticker, bold=True)} Financial Ratios"
    lines.append(title)
    lines.append(f"{start_date} {theme.muted_text(ARROW)} {end_date}")
    lines.append("")

    # Detailed metrics table
    lines.append(theme.muted_text("METRIC              CURRENT       MIN        MAX       TREND"))
    lines.append(theme.muted_text("-" * 70))

    metric_rows = [
        ("P/E Ratio", "pe_ratio", lambda x: f"{x:.1f}" if x else "--"),
        ("P/B Ratio", "pb_ratio", lambda x: f"{x:.1f}" if x else "--"),
        ("P/S Ratio", "ps_ratio", lambda x: f"{x:.1f}" if x else "--"),
        ("EV/EBITDA", "ev_to_ebitda", lambda x: f"{x:.1f}" if x else "--"),
        ("ROE", "roe", lambda x: format_percent(x) if x else "--"),
        ("ROA", "roa", lambda x: format_percent(x) if x else "--"),
        ("Debt/Equity", "debt_to_equity", lambda x: f"{x:.2f}" if x else "--"),
        ("Dividend Yield", "dividend_yield", lambda x: format_percent(x) if x else "--"),
        ("EPS", "eps", lambda x: f"${x:.2f}" if x else "--"),
    ]

    for label, key, fmt in metric_rows:
        stats = metrics.get(key, {})
        current = stats.get("current")
        min_val = stats.get("min")
        max_val = stats.get("max")
        first = stats.get("first")

        series = metrics["series"].get(key, [])

        # Calculate trend
        if first and current and first != 0:
            change = ((current - first) / abs(first)) * 100
            trend_str = trend.render(change)
        else:
            trend_str = ""

        # Sparkline
        if series and len(series) > 3:
            sparkline_str = spark.render(series, 15)
        else:
            sparkline_str = ""

        line = f"{label:<18} {fmt(current):>10} {fmt(min_val):>10} {fmt(max_val):>10} "
        line += f"{sparkline_str} {trend_str}"
        lines.append(line)

    lines.append("")
    lines.append(theme.muted_text("VALUATION SUMMARY"))

    mc = metrics["market_cap"]["current"]
    ev = metrics["ev"]["current"]
    lines.append(f"Market Cap:       {format_large_number(mc, prefix='$')}")
    lines.append(f"Enterprise Value: {format_large_number(ev, prefix='$')}")

    return box.render(lines, title=f"{ticker} Financial Ratios")
