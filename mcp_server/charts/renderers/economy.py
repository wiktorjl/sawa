"""Economy data chart renderer."""

from typing import Any

from ..config import ChartDetail
from ..core.formatters import format_large_number
from ..core.layout import Layout, get_layout
from ..core.sparkline import Sparkline
from ..themes import Theme, get_theme
from ..widgets.box import Box
from ..widgets.trend import TrendIndicator

# Unicode arrow for date ranges (can't use \u escape in f-strings on Python 3.10)
ARROW = "\u2192"


def render_economy_chart(
    data: list[dict[str, Any]],
    indicator_type: str,
    layout: Layout | None = None,
    theme: Theme | None = None,
) -> str:
    """
    Render an economy indicator chart.

    Args:
        data: List of indicator records
        indicator_type: Type of indicator (treasury_yields, inflation, etc.)
        layout: Layout configuration
        theme: Theme to use

    Returns:
        Formatted chart string
    """
    if not data:
        return f"No data available for {indicator_type}"

    if layout is None:
        layout = get_layout()
    if theme is None:
        theme = get_theme()

    renderers = {
        "treasury_yields": _render_treasury_yields,
        "inflation": _render_inflation,
        "inflation_expectations": _render_inflation_expectations,
        "labor_market": _render_labor_market,
    }

    renderer = renderers.get(indicator_type)
    if not renderer:
        return f"Unknown indicator type: {indicator_type}"

    return renderer(data, layout, theme)


def render_economy_dashboard(
    data: list[dict[str, Any]],
    layout: Layout | None = None,
    theme: Theme | None = None,
) -> str:
    """
    Render economy dashboard with all indicators.

    Args:
        data: List of dashboard records
        layout: Layout configuration
        theme: Theme to use

    Returns:
        Formatted dashboard string
    """
    if not data:
        return "No economy dashboard data available"

    if layout is None:
        layout = get_layout()
    if theme is None:
        theme = get_theme()

    if layout.detail == ChartDetail.COMPACT:
        return _render_dashboard_compact(data, layout, theme)
    elif layout.detail == ChartDetail.NORMAL:
        return _render_dashboard_normal(data, layout, theme)
    else:
        return _render_dashboard_detailed(data, layout, theme)


def _render_treasury_yields(
    data: list[dict[str, Any]],
    layout: Layout,
    theme: Theme,
) -> str:
    """Render treasury yields chart."""
    spark = Sparkline(theme)
    trend = TrendIndicator(theme)
    box = Box(theme, layout.width)

    lines = []

    # Date range
    dates = [d.get("date") for d in data]
    start_date = dates[0] if dates else "?"
    end_date = dates[-1] if dates else "?"

    lines.append(
        f"Treasury Yields {theme.muted_text('|')} {start_date} {theme.muted_text(ARROW)} {end_date}"
    )
    lines.append("")

    # Yield curve (latest values)
    latest = data[-1] if data else {}
    lines.append(theme.muted_text("YIELD CURVE (Latest)"))

    maturities = [
        ("1M", "yield_1_month"),
        ("3M", "yield_3_month"),
        ("6M", "yield_6_month"),
        ("1Y", "yield_1_year"),
        ("2Y", "yield_2_year"),
        ("5Y", "yield_5_year"),
        ("10Y", "yield_10_year"),
        ("30Y", "yield_30_year"),
    ]

    # Build yield curve visualization
    yields: list[tuple[str, float]] = [
        (name, float(latest[key])) for name, key in maturities if latest.get(key) is not None
    ]

    if yields:
        max_yield = max(y for _, y in yields)

        # Check for inversion (2Y > 10Y)
        y2 = latest.get("yield_2_year")
        y10 = latest.get("yield_10_year")
        if y2 and y10 and y2 > y10:
            msg = f"{theme.symbols.warning} YIELD CURVE INVERTED "
            msg += f"(2Y: {y2:.2f}% > 10Y: {y10:.2f}%)"
            lines.append(theme.warning_text(msg))
        lines.append("")

        # Simple bar representation
        for name, value in yields:
            if value is not None:
                bar_len = int((value / max_yield) * 20) if max_yield > 0 else 0
                bar = "\u2588" * bar_len
                bar = theme.colorize(bar, theme.colors.primary)
                lines.append(f"{name:>4} {value:>5.2f}% {bar}")

    lines.append("")

    # Time series for key maturities
    lines.append(theme.muted_text("TIME SERIES"))

    key_yields = [
        ("2Y", "yield_2_year"),
        ("10Y", "yield_10_year"),
        ("30Y", "yield_30_year"),
    ]

    for name, key in key_yields:
        series = [d.get(key) for d in data if d.get(key) is not None]
        if series:
            sparkline_str = spark.render(series, layout.sparkline_width)
            current = series[-1]
            change = ((series[-1] - series[0]) / series[0] * 100) if series[0] else 0
            trend_str = trend.render(change)
            lines.append(f"{name:>4} {current:>5.2f}% {sparkline_str} {trend_str}")

    # 10Y-2Y spread
    spreads = []
    for d in data:
        y2 = d.get("yield_2_year")
        y10 = d.get("yield_10_year")
        if y2 is not None and y10 is not None:
            spreads.append(y10 - y2)

    if spreads:
        lines.append("")
        lines.append(theme.muted_text("10Y-2Y SPREAD"))
        spread_spark = spark.render(spreads, layout.sparkline_width)
        current_spread = spreads[-1]
        spread_color = theme.colors.negative if current_spread < 0 else theme.colors.positive
        spread_str = theme.colorize(f"{current_spread:+.2f}%", spread_color)
        lines.append(f"Current: {spread_str} {spread_spark}")

    return box.render(lines, title="Treasury Yields")


def _render_inflation(
    data: list[dict[str, Any]],
    layout: Layout,
    theme: Theme,
) -> str:
    """Render inflation chart."""
    spark = Sparkline(theme)
    trend = TrendIndicator(theme)
    box = Box(theme, layout.width)

    lines = []

    dates = [d.get("date") for d in data]
    start_date = dates[0] if dates else "?"
    end_date = dates[-1] if dates else "?"

    lines.append(
        f"Inflation Data {theme.muted_text('|')} {start_date} {theme.muted_text(ARROW)} {end_date}"
    )
    lines.append("")

    # CPI metrics
    latest = data[-1] if data else {}

    metrics = [
        ("CPI", "cpi", False),
        ("Core CPI", "cpi_core", False),
        ("Inflation YoY", "inflation_yoy", True),
        ("PCE", "pce", False),
        ("Core PCE", "pce_core", False),
    ]

    for label, key, is_pct in metrics:
        series = [d.get(key) for d in data if d.get(key) is not None]
        if series:
            current = series[-1]
            sparkline_str = spark.render(series, layout.sparkline_width)
            change = ((series[-1] - series[0]) / series[0] * 100) if series[0] else 0
            trend_str = trend.render(change)

            if is_pct:
                current_str = f"{current:.1f}%"
            else:
                current_str = f"{current:.1f}"

            lines.append(f"{label:<15} {current_str:>8} {sparkline_str} {trend_str}")

    # Fed target comparison
    inflation_yoy = latest.get("inflation_yoy")
    if inflation_yoy is not None:
        lines.append("")
        fed_target = 2.0
        diff = inflation_yoy - fed_target
        if diff > 1:
            status = theme.warning_text(f"{theme.symbols.warning} Above target by {diff:.1f}%")
        elif diff > 0:
            status = theme.muted_text(f"Slightly above target (+{diff:.1f}%)")
        else:
            status = theme.positive_text(f"{theme.symbols.check} At or below target")
        lines.append(f"Fed 2% Target: {status}")

    return box.render(lines, title="Inflation")


def _render_inflation_expectations(
    data: list[dict[str, Any]],
    layout: Layout,
    theme: Theme,
) -> str:
    """Render inflation expectations chart."""
    spark = Sparkline(theme)
    trend = TrendIndicator(theme)
    box = Box(theme, layout.width)

    lines = []

    dates = [d.get("date") for d in data]
    start_date = dates[0] if dates else "?"
    end_date = dates[-1] if dates else "?"

    header = f"Inflation Expectations {theme.muted_text('|')} "
    header += f"{start_date} {theme.muted_text(ARROW)} {end_date}"
    lines.append(header)
    lines.append("")

    metrics = [
        ("5-Year", "market_5_year"),
        ("10-Year", "market_10_year"),
        ("5Y-10Y Forward", "forward_years_5_to_10"),
    ]

    for label, key in metrics:
        series = [d.get(key) for d in data if d.get(key) is not None]
        if series:
            current = series[-1]
            sparkline_str = spark.render(series, layout.sparkline_width)
            change = ((series[-1] - series[0]) / series[0] * 100) if series[0] else 0
            trend_str = trend.render(change)
            lines.append(f"{label:<15} {current:>5.2f}% {sparkline_str} {trend_str}")

    return box.render(lines, title="Inflation Expectations")


def _render_labor_market(
    data: list[dict[str, Any]],
    layout: Layout,
    theme: Theme,
) -> str:
    """Render labor market chart."""
    spark = Sparkline(theme)
    trend = TrendIndicator(theme)
    box = Box(theme, layout.width)

    lines = []

    dates = [d.get("date") for d in data]
    start_date = dates[0] if dates else "?"
    end_date = dates[-1] if dates else "?"

    lines.append(
        f"Labor Market {theme.muted_text('|')} {start_date} {theme.muted_text(ARROW)} {end_date}"
    )
    lines.append("")

    metrics = [
        ("Unemployment", "unemployment_rate", "%", 1),
        ("Participation", "labor_force_participation_rate", "%", 1),
        ("Hourly Earnings", "avg_hourly_earnings", "$", 2),
        ("Job Openings", "job_openings", "M", 1),
    ]

    for label, key, suffix, decimals in metrics:
        series: list[float] = [float(d[key]) for d in data if d.get(key) is not None]
        if series:
            current = series[-1]
            sparkline_str = spark.render(series, layout.sparkline_width)
            change = ((series[-1] - series[0]) / series[0] * 100) if series[0] else 0
            trend_str = trend.render(change)

            if suffix == "M":
                current_str = f"{current / 1_000_000:.1f}M"
            elif suffix == "$":
                current_str = f"${current:.2f}"
            else:
                current_str = f"{current:.{decimals}f}{suffix}"

            lines.append(f"{label:<15} {current_str:>10} {sparkline_str} {trend_str}")

    return box.render(lines, title="Labor Market")


def _render_dashboard_compact(
    data: list[dict[str, Any]],
    layout: Layout,
    theme: Theme,
) -> str:
    """Render compact economy dashboard."""
    latest = data[-1] if data else {}

    parts = ["ECONOMY"]

    y10 = latest.get("yield_10_year")
    if y10:
        parts.append(f"10Y: {y10:.2f}%")

    inflation = latest.get("inflation_yoy")
    if inflation:
        parts.append(f"CPI: {inflation:.1f}%")

    unemp = latest.get("unemployment_rate")
    if unemp:
        parts.append(f"Unemp: {unemp:.1f}%")

    return f" {theme.muted_text('|')} ".join(parts)


def _render_dashboard_normal(
    data: list[dict[str, Any]],
    layout: Layout,
    theme: Theme,
) -> str:
    """Render normal economy dashboard."""
    spark = Sparkline(theme)
    trend = TrendIndicator(theme)
    box = Box(theme, layout.width)

    lines = []

    latest = data[-1] if data else {}
    latest_date = latest.get("date", "?")

    lines.append(f"ECONOMY DASHBOARD {theme.muted_text('|')} as of {latest_date}")
    lines.append("")

    # Interest rates
    lines.append(theme.muted_text("INTEREST RATES"))

    for label, key in [("10Y Treasury", "yield_10_year"), ("30Y Treasury", "yield_30_year")]:
        series = [d.get(key) for d in data if d.get(key) is not None]
        if series:
            current = series[-1]
            sparkline_str = spark.render(series, 15)
            change = ((series[-1] - series[0]) / series[0] * 100) if series[0] else 0
            trend_str = trend.render(change)
            lines.append(f"  {label:<12} {current:>5.2f}% {sparkline_str} {trend_str}")

    lines.append("")

    # Inflation
    lines.append(theme.muted_text("INFLATION"))
    series = [d.get("inflation_yoy") for d in data if d.get("inflation_yoy") is not None]
    if series:
        current = series[-1]
        sparkline_str = spark.render(series, 15)
        change = current - series[0] if series[0] else 0
        trend_str = trend.render(change)
        lines.append(f"  CPI YoY      {current:>5.1f}% {sparkline_str} {trend_str}")

    lines.append("")

    # Labor
    lines.append(theme.muted_text("LABOR MARKET"))
    series = [d.get("unemployment_rate") for d in data if d.get("unemployment_rate") is not None]
    if series:
        current = series[-1]
        sparkline_str = spark.render(series, 15)
        change = current - series[0] if series[0] else 0
        trend_str = trend.render(change)
        lines.append(f"  Unemployment {current:>5.1f}% {sparkline_str} {trend_str}")

    return box.render(lines, title="Economy Dashboard")


def _render_dashboard_detailed(
    data: list[dict[str, Any]],
    layout: Layout,
    theme: Theme,
) -> str:
    """Render detailed economy dashboard."""
    spark = Sparkline(theme)
    trend = TrendIndicator(theme)
    box = Box(theme, layout.width)

    lines = []

    latest = data[-1] if data else {}
    latest_date = latest.get("date", "?")

    lines.append(theme.primary_text("ECONOMY DASHBOARD", bold=True))
    lines.append(f"As of {latest_date} {theme.muted_text('|')} Last {len(data)} data points")
    lines.append("")

    # Detailed table header
    lines.append(
        theme.muted_text(f"{'Indicator':<20} {'Value':>10} {'Trend (Period)':<25} {'Change':>10}")
    )
    lines.append(theme.muted_text("-" * 70))

    metrics = [
        ("10Y Treasury", "yield_10_year", lambda x: f"{x:.2f}%"),
        ("30Y Treasury", "yield_30_year", lambda x: f"{x:.2f}%"),
        ("CPI YoY", "inflation_yoy", lambda x: f"{x:.1f}%"),
        ("Infl Exp 5Y", "inflation_expectation_5y", lambda x: f"{x:.2f}%"),
        ("Infl Exp 10Y", "inflation_expectation_10y", lambda x: f"{x:.2f}%"),
        ("Unemployment", "unemployment_rate", lambda x: f"{x:.1f}%"),
        ("Job Openings", "job_openings", lambda x: format_large_number(x)),
    ]

    for label, key, fmt in metrics:
        series = [d.get(key) for d in data if d.get(key) is not None]
        if series:
            current = series[-1]
            sparkline_str = spark.render(series, 20)

            if series[0] and series[0] != 0:
                change = ((series[-1] - series[0]) / abs(series[0])) * 100
            else:
                change = 0

            trend_str = trend.render(change)

            lines.append(f"{label:<20} {fmt(current):>10} {sparkline_str:<25} {trend_str:>10}")

    return box.render(lines, title="Economy Dashboard")
