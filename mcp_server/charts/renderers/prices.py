"""Price chart renderer."""

from typing import Any

from ..config import ChartDetail
from ..core.formatters import format_currency, format_date_range, format_large_number
from ..core.layout import Layout, get_layout
from ..core.sparkline import Sparkline
from ..themes import Theme, get_theme
from ..widgets.box import Box
from ..widgets.trend import TrendIndicator

# Unicode arrow for date ranges (can't use \u escape in f-strings on Python 3.10)
ARROW = "\u2192"


def render_price_chart(
    data: list[dict[str, Any]],
    ticker: str,
    layout: Layout | None = None,
    theme: Theme | None = None,
) -> str:
    """
    Render a price chart for stock data.

    Args:
        data: List of price records with date, open, high, low, close, volume
        ticker: Stock ticker symbol
        layout: Layout configuration (None = auto)
        theme: Theme to use (None = default)

    Returns:
        Formatted chart string
    """
    if not data:
        return f"No price data available for {ticker}"

    if layout is None:
        layout = get_layout()
    if theme is None:
        theme = get_theme()

    # Extract data
    dates = [d.get("date") for d in data]
    closes = [d.get("close") for d in data if d.get("close") is not None]
    volumes = [d.get("volume") for d in data if d.get("volume") is not None]
    highs = [d.get("high") for d in data if d.get("high") is not None]
    lows = [d.get("low") for d in data if d.get("low") is not None]

    if not closes:
        return f"No valid price data for {ticker}"

    # Calculate stats
    first_close = closes[0]
    last_close = closes[-1]
    high_price = max(highs) if highs else max(closes)
    low_price = min(lows) if lows else min(closes)
    avg_volume = sum(volumes) / len(volumes) if volumes else 0

    change = last_close - first_close
    change_pct = (change / first_close * 100) if first_close else 0

    # Build chart based on detail level
    if layout.detail == ChartDetail.COMPACT:
        return _render_compact(
            ticker,
            dates,
            closes,
            volumes,
            first_close,
            last_close,
            change,
            change_pct,
            high_price,
            low_price,
            avg_volume,
            layout,
            theme,
        )
    elif layout.detail == ChartDetail.NORMAL:
        return _render_normal(
            ticker,
            dates,
            closes,
            volumes,
            first_close,
            last_close,
            change,
            change_pct,
            high_price,
            low_price,
            avg_volume,
            layout,
            theme,
        )
    else:
        return _render_detailed(
            ticker,
            dates,
            closes,
            volumes,
            first_close,
            last_close,
            change,
            change_pct,
            high_price,
            low_price,
            avg_volume,
            layout,
            theme,
        )


def _render_compact(
    ticker: str,
    dates: list,
    closes: list[float],
    volumes: list[int],
    first_close: float,
    last_close: float,
    change: float,
    change_pct: float,
    high_price: float,
    low_price: float,
    avg_volume: float,
    layout: Layout,
    theme: Theme,
) -> str:
    """Render compact single-line price chart."""
    spark = Sparkline(theme)
    trend = TrendIndicator(theme)

    # Build sparkline
    sparkline_str = spark.render(closes, layout.sparkline_width)

    # Build trend indicator
    trend_str = trend.render(change_pct)

    # Format prices
    first_str = format_currency(first_close)
    last_str = format_currency(last_close)

    # Build line
    sep = theme.muted_text("|")
    arrow = theme.muted_text(ARROW)
    line = f"{ticker} {sep} {first_str}{arrow}{last_str} {sep} {sparkline_str} "
    line += f"{sep} {trend_str} {sep} Vol: {format_large_number(avg_volume)} avg"

    return line


def _render_normal(
    ticker: str,
    dates: list,
    closes: list[float],
    volumes: list[int],
    first_close: float,
    last_close: float,
    change: float,
    change_pct: float,
    high_price: float,
    low_price: float,
    avg_volume: float,
    layout: Layout,
    theme: Theme,
) -> str:
    """Render normal price chart with box."""
    spark = Sparkline(theme)
    trend = TrendIndicator(theme)
    box = Box(theme, layout.width)

    # Date range
    start_date = dates[0] if dates else "?"
    end_date = dates[-1] if dates else "?"
    date_range = format_date_range(start_date, end_date)

    lines = []

    # Header
    header = (
        f"{ticker} {theme.muted_text('|')} {date_range} {theme.muted_text('|')} {len(dates)} days"
    )
    lines.append(header)

    lines.append("")

    # Price sparkline with range
    price_spark = spark.render(closes, layout.sparkline_width)
    first_str = format_currency(first_close)
    last_str = format_currency(last_close)
    trend_str = trend.render(change_pct)

    sep = theme.muted_text("|")
    price_line = f"Price {sep} {first_str} {price_spark} {last_str} {sep} {trend_str}"
    lines.append(price_line)

    # Volume sparkline
    if volumes:
        vol_spark = spark.render(volumes, layout.sparkline_width)
        min_vol = format_large_number(min(volumes))
        max_vol = format_large_number(max(volumes))
        vol_line = f"Vol   {sep} {min_vol} {vol_spark} {max_vol} {sep} "
        vol_line += f"Avg: {format_large_number(avg_volume)}"
        lines.append(vol_line)

    lines.append("")

    # Summary stats
    summary = f"Open: {format_currency(first_close)} {sep} High: {format_currency(high_price)} "
    summary += f"{sep} Low: {format_currency(low_price)} {sep} Close: {format_currency(last_close)}"
    lines.append(summary)

    return box.render(lines, title=f"{ticker} Stock Price")


def _render_detailed(
    ticker: str,
    dates: list,
    closes: list[float],
    volumes: list[int],
    first_close: float,
    last_close: float,
    change: float,
    change_pct: float,
    high_price: float,
    low_price: float,
    avg_volume: float,
    layout: Layout,
    theme: Theme,
) -> str:
    """Render detailed price chart with full statistics."""
    spark = Sparkline(theme)
    trend = TrendIndicator(theme)
    box = Box(theme, layout.width)

    # Date range
    start_date = dates[0] if dates else "?"
    end_date = dates[-1] if dates else "?"
    date_range = format_date_range(start_date, end_date)

    lines = []

    # Title line
    sep = theme.muted_text("|")
    title_line = f"{theme.primary_text(ticker, bold=True)} {sep} {date_range} {sep} "
    title_line += f"{len(dates)} trading days"
    lines.append(title_line)

    lines.append("")
    lines.append(theme.muted_text("PRICE"))

    # Price sparkline
    price_spark = spark.render(closes, layout.sparkline_width)
    first_str = format_currency(first_close)
    last_str = format_currency(last_close)
    price_range = f"{first_str} {price_spark} {last_str}"
    lines.append(price_range)

    lines.append("")
    lines.append(theme.muted_text("VOLUME"))

    # Volume sparkline
    if volumes:
        vol_spark = spark.render(volumes, layout.sparkline_width)
        lines.append(vol_spark)

    lines.append("")
    lines.append(theme.muted_text("STATISTICS"))

    # Detailed stats table
    change_str = trend.render_change(first_close, last_close, prefix="$")
    max_vol_str = format_large_number(max(volumes)) if volumes else "--"
    range_str = format_currency(high_price - low_price)

    lines.append(
        f"Open:       {format_currency(first_close):<12} "
        f"Close:      {format_currency(last_close):<12} Change:     {change_str}"
    )
    lines.append(
        f"High:       {format_currency(high_price):<12} "
        f"Low:        {format_currency(low_price):<12} Range:      {range_str}"
    )
    lines.append(f"Avg Volume: {format_large_number(avg_volume):<12} Max Volume: {max_vol_str:<12}")

    # Volatility indicator if we have enough data
    if len(closes) > 5:
        # Simple volatility as % range
        volatility = ((high_price - low_price) / first_close) * 100
        lines.append("")
        lines.append(f"Volatility: {volatility:.1f}% (price range as % of open)")

    return box.render(lines, title=f"{ticker} Stock Price")
