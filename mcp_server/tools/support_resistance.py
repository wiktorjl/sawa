"""Support and resistance level detection tool."""

import logging
from datetime import date, timedelta
from typing import Any

from ..database import execute_query

logger = logging.getLogger(__name__)


def calculate_support_resistance_levels(
    ticker: str,
    lookback_days: int = 90,
    max_levels: int = 5,
    method: str = "cluster",
) -> dict[str, Any]:
    """
    Calculate support and resistance levels for a stock.

    Args:
        ticker: Stock ticker symbol (e.g., "AAPL")
        lookback_days: Number of trading days to analyze (default: 90)
        max_levels: Maximum number of levels to return (default: 5)
        method: Detection method - "pivot", "cluster", or "volume"

    Returns:
        Dictionary with ticker, method, current_price, and levels list
    """
    ticker = ticker.upper()
    lookback_days = min(max(lookback_days, 5), 500)
    max_levels = min(max(max_levels, 1), 20)

    if method not in ("pivot", "cluster", "volume"):
        raise ValueError(f"Invalid method: '{method}'. Must be pivot, cluster, or volume")

    end_date = date.today()
    start_date = end_date - timedelta(days=int(lookback_days * 1.5))

    prices = _fetch_prices(ticker, start_date.isoformat(), end_date.isoformat())

    if not prices:
        return {
            "ticker": ticker,
            "method": method,
            "current_price": None,
            "lookback_days": lookback_days,
            "data_points": 0,
            "levels": [],
            "error": f"No price data found for {ticker}",
        }

    prices = prices[-lookback_days:]
    current_price = float(prices[-1]["close"])

    if method == "pivot":
        levels = _pivot_point_levels(prices, max_levels)
    elif method == "cluster":
        levels = _cluster_levels(prices, max_levels)
    else:
        levels = _volume_levels(prices, max_levels)

    for level in levels:
        level["type"] = "support" if level["price"] < current_price else "resistance"

    levels.sort(key=lambda x: x["strength"], reverse=True)
    levels = levels[:max_levels]

    return {
        "ticker": ticker,
        "method": method,
        "current_price": current_price,
        "lookback_days": lookback_days,
        "data_points": len(prices),
        "levels": levels,
    }


def _fetch_prices(ticker: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
    """Fetch OHLCV data from stock_prices table."""
    sql = """
        SELECT date, open, high, low, close, volume
        FROM stock_prices
        WHERE ticker = %(ticker)s
            AND date >= %(start_date)s
            AND date <= %(end_date)s
        ORDER BY date ASC
    """
    return execute_query(sql, {
        "ticker": ticker,
        "start_date": start_date,
        "end_date": end_date,
    })


def _pivot_point_levels(
    prices: list[dict[str, Any]], max_levels: int
) -> list[dict[str, Any]]:
    """
    Classic pivot point calculation from recent high/low/close.

    Uses the most recent trading day's H/L/C to compute:
    - Pivot Point (PP) = (H + L + C) / 3
    - Support 1 (S1) = 2*PP - H
    - Resistance 1 (R1) = 2*PP - L
    - Support 2 (S2) = PP - (H - L)
    - Resistance 2 (R2) = PP + (H - L)
    """
    latest = prices[-1]
    high = float(latest["high"])
    low = float(latest["low"])
    close = float(latest["close"])

    pp = (high + low + close) / 3.0
    r1 = 2.0 * pp - low
    s1 = 2.0 * pp - high
    r2 = pp + (high - low)
    s2 = pp - (high - low)

    raw_levels = [
        ("pivot", pp),
        ("resistance_1", r1),
        ("support_1", s1),
        ("resistance_2", r2),
        ("support_2", s2),
    ]

    levels = []
    for label, price_val in raw_levels:
        test_count = _count_touches(prices, price_val)
        last_touch = _find_last_touch(prices, price_val)
        strength = _calculate_pivot_strength(label, test_count)

        levels.append({
            "price": round(price_val, 2),
            "strength": round(strength, 2),
            "test_count": test_count,
            "last_touch": last_touch,
            "label": label,
        })

    return levels[:max_levels]


def _cluster_levels(
    prices: list[dict[str, Any]], max_levels: int
) -> list[dict[str, Any]]:
    """
    Price clustering method: find price zones with multiple touches.

    Groups price extremes (highs and lows) into clusters within a
    percentage threshold, then ranks clusters by number of touches.
    """
    extremes = []
    for row in prices:
        extremes.append(float(row["high"]))
        extremes.append(float(row["low"]))

    if not extremes:
        return []

    price_range = max(extremes) - min(extremes)
    if price_range == 0:
        return []

    cluster_pct = 0.015
    cluster_threshold = price_range * cluster_pct

    extremes.sort()
    clusters: list[list[float]] = []
    current_cluster: list[float] = [extremes[0]]

    for price_val in extremes[1:]:
        if price_val - current_cluster[-1] <= cluster_threshold:
            current_cluster.append(price_val)
        else:
            clusters.append(current_cluster)
            current_cluster = [price_val]
    clusters.append(current_cluster)

    cluster_levels = []
    for cluster in clusters:
        if len(cluster) < 2:
            continue
        avg_price = sum(cluster) / len(cluster)
        touch_count = len(cluster)
        last_touch = _find_last_touch(prices, avg_price, tolerance_pct=cluster_pct)
        strength = min(touch_count / (len(prices) * 0.1), 1.0)

        cluster_levels.append({
            "price": round(avg_price, 2),
            "strength": round(strength, 2),
            "test_count": touch_count,
            "last_touch": last_touch,
            "label": "cluster",
        })

    cluster_levels.sort(key=lambda x: x["strength"], reverse=True)
    return cluster_levels[:max_levels]


def _volume_levels(
    prices: list[dict[str, Any]], max_levels: int
) -> list[dict[str, Any]]:
    """
    Volume-based support/resistance: find price levels with highest volume.

    Bins price range into buckets, accumulates volume per bucket,
    then returns the top volume nodes as support/resistance levels.
    """
    if not prices:
        return []

    price_min = min(float(row["low"]) for row in prices)
    price_max = max(float(row["high"]) for row in prices)

    if price_max == price_min:
        return []

    num_bins = max(20, len(prices) // 3)
    bin_width = (price_max - price_min) / num_bins

    volume_bins: dict[int, float] = {}
    for row in prices:
        mid_price = (float(row["high"]) + float(row["low"])) / 2.0
        volume = float(row["volume"]) if row["volume"] else 0
        bin_idx = int((mid_price - price_min) / bin_width)
        bin_idx = min(bin_idx, num_bins - 1)
        volume_bins[bin_idx] = volume_bins.get(bin_idx, 0) + volume

    if not volume_bins:
        return []

    max_volume = max(volume_bins.values())
    if max_volume == 0:
        return []

    sorted_bins = sorted(volume_bins.items(), key=lambda x: x[1], reverse=True)

    levels = []
    for bin_idx, vol in sorted_bins:
        price_val = price_min + (bin_idx + 0.5) * bin_width
        strength = vol / max_volume
        test_count = _count_touches(prices, price_val, tolerance_pct=bin_width / price_val)
        last_touch = _find_last_touch(prices, price_val, tolerance_pct=bin_width / price_val)

        levels.append({
            "price": round(price_val, 2),
            "strength": round(strength, 2),
            "test_count": test_count,
            "last_touch": last_touch,
            "label": "volume_node",
        })

        if len(levels) >= max_levels:
            break

    return levels


def _count_touches(
    prices: list[dict[str, Any]],
    level: float,
    tolerance_pct: float = 0.01,
) -> int:
    """Count how many times price touched a level within tolerance."""
    tolerance = level * tolerance_pct
    count = 0
    for row in prices:
        high = float(row["high"])
        low = float(row["low"])
        if low - tolerance <= level <= high + tolerance:
            count += 1
    return count


def _find_last_touch(
    prices: list[dict[str, Any]],
    level: float,
    tolerance_pct: float = 0.01,
) -> str | None:
    """Find the most recent date price touched a level."""
    tolerance = level * tolerance_pct
    for row in reversed(prices):
        high = float(row["high"])
        low = float(row["low"])
        if low - tolerance <= level <= high + tolerance:
            return str(row["date"])
    return None


def _calculate_pivot_strength(label: str, test_count: int) -> float:
    """Calculate strength score for a pivot level."""
    base_strength = {
        "pivot": 0.8,
        "resistance_1": 0.6,
        "support_1": 0.6,
        "resistance_2": 0.4,
        "support_2": 0.4,
    }
    base = base_strength.get(label, 0.5)
    touch_bonus = min(test_count * 0.05, 0.2)
    return min(base + touch_bonus, 1.0)
