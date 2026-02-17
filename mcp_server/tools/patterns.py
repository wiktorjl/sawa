"""Candlestick and chart pattern detection tools for the MCP server."""

import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from ..database import execute_query

logger = logging.getLogger(__name__)

# Pattern direction constants
BULLISH = "bullish"
BEARISH = "bearish"
NEUTRAL = "neutral"


def _to_float(val: Any) -> float:
    """Convert Decimal or other numeric types to float."""
    if isinstance(val, Decimal):
        return float(val)
    return float(val)


def _body(open_: float, close: float) -> float:
    """Absolute body size (close - open)."""
    return abs(close - open_)


def _upper_shadow(open_: float, high: float, close: float) -> float:
    """Upper shadow length."""
    return high - max(open_, close)


def _lower_shadow(open_: float, low: float, close: float) -> float:
    """Lower shadow length."""
    return min(open_, close) - low


def _candle_range(high: float, low: float) -> float:
    """Full candle range (high - low)."""
    return high - low


def _is_bullish(open_: float, close: float) -> bool:
    """True if close > open."""
    return close > open_


def _is_bearish(open_: float, close: float) -> bool:
    """True if close < open."""
    return close < open_


def _is_doji(
    open_: float, high: float, low: float, close: float, threshold: float = 0.05,
) -> bool:
    """True if body is very small relative to the candle range."""
    cr = _candle_range(high, low)
    if cr == 0:
        return True
    return _body(open_, close) / cr < threshold


def detect_patterns(candles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Detect candlestick patterns in OHLCV data.

    Args:
        candles: List of dicts with keys: date, open, high, low, close, volume
                 Must be sorted by date ascending.

    Returns:
        List of detected patterns, each with:
        - date: Date the pattern completed
        - pattern: Pattern name
        - direction: 'bullish', 'bearish', or 'neutral'
        - reliability: Score from 1 to 3 (1=low, 2=moderate, 3=high)
        - description: Brief explanation of the pattern
    """
    patterns: list[dict[str, Any]] = []
    n = len(candles)

    if n < 1:
        return patterns

    for i in range(n):
        o = _to_float(candles[i]["open"])
        h = _to_float(candles[i]["high"])
        lo = _to_float(candles[i]["low"])
        c = _to_float(candles[i]["close"])
        cr = _candle_range(h, lo)
        d = candles[i]["date"]

        if cr == 0:
            continue

        body = _body(o, c)
        upper = _upper_shadow(o, h, c)
        lower = _lower_shadow(o, lo, c)

        # --- Single candle patterns ---

        # Doji
        if _is_doji(o, h, lo, c, threshold=0.05):
            patterns.append({
                "date": d,
                "pattern": "doji",
                "direction": NEUTRAL,
                "reliability": 1,
                "description": "Indecision; open and close nearly equal",
            })

        # Hammer: small body at top, long lower shadow (>=2x body), tiny upper shadow
        if (
            body > 0
            and lower >= 2 * body
            and upper <= body * 0.3
            and body / cr < 0.4
        ):
            patterns.append({
                "date": d,
                "pattern": "hammer",
                "direction": BULLISH,
                "reliability": 2,
                "description": (
                    "Small body at top with long lower shadow; "
                    "bullish reversal signal"
                ),
            })

        # Inverted Hammer: small body at bottom, long upper shadow, tiny lower shadow
        if (
            body > 0
            and upper >= 2 * body
            and lower <= body * 0.3
            and body / cr < 0.4
        ):
            patterns.append({
                "date": d,
                "pattern": "inverted_hammer",
                "direction": BULLISH,
                "reliability": 1,
                "description": (
                    "Small body at bottom with long upper shadow; "
                    "potential bullish reversal"
                ),
            })

        # Shooting Star: same shape as inverted hammer but after uptrend
        if i >= 3:
            prev_closes = [
                _to_float(candles[j]["close"]) for j in range(i - 3, i)
            ]
            uptrend = all(
                prev_closes[j] < prev_closes[j + 1]
                for j in range(len(prev_closes) - 1)
            )
            if (
                uptrend
                and body > 0
                and upper >= 2 * body
                and lower <= body * 0.3
                and body / cr < 0.4
            ):
                patterns.append({
                    "date": d,
                    "pattern": "shooting_star",
                    "direction": BEARISH,
                    "reliability": 2,
                    "description": (
                        "Long upper shadow after uptrend; "
                        "bearish reversal"
                    ),
                })

        # Hanging Man: hammer shape after uptrend
        if i >= 3:
            prev_closes = [
                _to_float(candles[j]["close"]) for j in range(i - 3, i)
            ]
            uptrend = all(
                prev_closes[j] < prev_closes[j + 1]
                for j in range(len(prev_closes) - 1)
            )
            if (
                uptrend
                and body > 0
                and lower >= 2 * body
                and upper <= body * 0.3
                and body / cr < 0.4
            ):
                patterns.append({
                    "date": d,
                    "pattern": "hanging_man",
                    "direction": BEARISH,
                    "reliability": 2,
                    "description": (
                        "Hammer shape after uptrend; "
                        "potential bearish reversal"
                    ),
                })

        # --- Two candle patterns (require i >= 1) ---
        if i >= 1:
            po = _to_float(candles[i - 1]["open"])
            pc = _to_float(candles[i - 1]["close"])
            prev_body = _body(po, pc)

            # Bullish Engulfing: bearish candle followed by larger bullish candle
            if (
                _is_bearish(po, pc)
                and _is_bullish(o, c)
                and o <= pc
                and c >= po
                and body > prev_body
            ):
                patterns.append({
                    "date": d,
                    "pattern": "bullish_engulfing",
                    "direction": BULLISH,
                    "reliability": 3,
                    "description": (
                        "Bullish candle engulfs prior bearish candle; "
                        "strong bullish reversal"
                    ),
                })

            # Bearish Engulfing: bullish candle followed by larger bearish candle
            if (
                _is_bullish(po, pc)
                and _is_bearish(o, c)
                and o >= pc
                and c <= po
                and body > prev_body
            ):
                patterns.append({
                    "date": d,
                    "pattern": "bearish_engulfing",
                    "direction": BEARISH,
                    "reliability": 3,
                    "description": (
                        "Bearish candle engulfs prior bullish candle; "
                        "strong bearish reversal"
                    ),
                })

        # --- Three candle patterns (require i >= 2) ---
        if i >= 2:
            # Candle at i-2
            o2 = _to_float(candles[i - 2]["open"])
            c2 = _to_float(candles[i - 2]["close"])
            # Candle at i-1 (middle)
            o1 = _to_float(candles[i - 1]["open"])
            c1 = _to_float(candles[i - 1]["close"])

            body2 = _body(o2, c2)
            body1 = _body(o1, c1)
            body0 = body

            # Morning Star: bearish, small body (star), bullish
            if (
                _is_bearish(o2, c2)
                and body2 > 0
                and body1 < body2 * 0.4
                and _is_bullish(o, c)
                and body0 > body2 * 0.4
                and c > (o2 + c2) / 2
            ):
                patterns.append({
                    "date": d,
                    "pattern": "morning_star",
                    "direction": BULLISH,
                    "reliability": 3,
                    "description": (
                        "Three-candle bullish reversal: "
                        "bearish, small star, bullish"
                    ),
                })

            # Evening Star: bullish, small body (star), bearish
            if (
                _is_bullish(o2, c2)
                and body2 > 0
                and body1 < body2 * 0.4
                and _is_bearish(o, c)
                and body0 > body2 * 0.4
                and c < (o2 + c2) / 2
            ):
                patterns.append({
                    "date": d,
                    "pattern": "evening_star",
                    "direction": BEARISH,
                    "reliability": 3,
                    "description": (
                        "Three-candle bearish reversal: "
                        "bullish, small star, bearish"
                    ),
                })

            # Three White Soldiers: three consecutive bullish with higher closes
            if (
                _is_bullish(o2, c2)
                and _is_bullish(o1, c1)
                and _is_bullish(o, c)
                and c1 > c2
                and c > c1
                and o1 > o2
                and o > o1
                and body2 > 0
                and body1 > 0
                and body0 > 0
                # Each candle opens within prior body
                and o1 <= c2
                and o1 >= o2
                and o <= c1
                and o >= o1
            ):
                patterns.append({
                    "date": d,
                    "pattern": "three_white_soldiers",
                    "direction": BULLISH,
                    "reliability": 3,
                    "description": (
                        "Three bullish candles with higher closes; "
                        "strong bullish signal"
                    ),
                })

            # Three Black Crows: three consecutive bearish with lower closes
            if (
                _is_bearish(o2, c2)
                and _is_bearish(o1, c1)
                and _is_bearish(o, c)
                and c1 < c2
                and c < c1
                and o1 < o2
                and o < o1
                and body2 > 0
                and body1 > 0
                and body0 > 0
                # Each candle opens within prior body
                and o1 >= c2
                and o1 <= o2
                and o >= c1
                and o <= o1
            ):
                patterns.append({
                    "date": d,
                    "pattern": "three_black_crows",
                    "direction": BEARISH,
                    "reliability": 3,
                    "description": (
                        "Three bearish candles with lower closes; "
                        "strong bearish signal"
                    ),
                })

    return patterns


# All supported pattern names for filtering
SUPPORTED_PATTERNS = {
    "doji",
    "hammer",
    "inverted_hammer",
    "shooting_star",
    "hanging_man",
    "bullish_engulfing",
    "bearish_engulfing",
    "morning_star",
    "evening_star",
    "three_white_soldiers",
    "three_black_crows",
}


def detect_candlestick_patterns(
    ticker: str,
    days: int = 30,
    patterns_to_detect: list[str] | None = None,
) -> dict[str, Any]:
    """Detect candlestick patterns for a stock ticker.

    Args:
        ticker: Stock ticker symbol (e.g., "AAPL")
        days: Number of trading days to analyze (default: 30, max: 252)
        patterns_to_detect: Optional list of specific pattern names to detect.
                           If None, detect all patterns.

    Returns:
        Dict with:
        - ticker: The ticker symbol
        - days_analyzed: Number of days of data analyzed
        - patterns_found: Number of patterns detected
        - patterns: List of pattern dicts
        - summary: Dict of pattern counts by direction
    """
    days = min(days, 252)
    ticker = ticker.upper()

    # Validate patterns_to_detect if provided
    if patterns_to_detect:
        invalid = set(patterns_to_detect) - SUPPORTED_PATTERNS
        if invalid:
            raise ValueError(
                f"Unknown pattern(s): {', '.join(sorted(invalid))}. "
                f"Supported: {', '.join(sorted(SUPPORTED_PATTERNS))}"
            )

    # Extra lookback for trend detection (3 candles before analysis window)
    lookback_days = days + 10
    end_date = date.today()
    start_date = end_date - timedelta(days=int(lookback_days * 1.6))

    query = """
        SELECT date, open, high, low, close, volume
        FROM stock_prices
        WHERE ticker = %(ticker)s
            AND date >= %(start_date)s
            AND date <= %(end_date)s
        ORDER BY date ASC
        LIMIT %(limit)s
    """

    params = {
        "ticker": ticker,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "limit": lookback_days,
    }

    candles = execute_query(query, params)

    if not candles:
        return {
            "ticker": ticker,
            "days_analyzed": 0,
            "patterns_found": 0,
            "patterns": [],
            "summary": {"bullish": 0, "bearish": 0, "neutral": 0},
        }

    # Detect all patterns
    all_patterns = detect_patterns(candles)

    # Only return patterns within the requested analysis window
    if len(candles) > days:
        cutoff_date = candles[-days]["date"]
        all_patterns = [
            p for p in all_patterns if p["date"] >= cutoff_date
        ]

    # Filter by specific patterns if requested
    if patterns_to_detect:
        pattern_set = set(patterns_to_detect)
        all_patterns = [
            p for p in all_patterns if p["pattern"] in pattern_set
        ]

    # Sort by date descending (most recent first)
    all_patterns.sort(key=lambda p: str(p["date"]), reverse=True)

    # Build summary
    summary = {"bullish": 0, "bearish": 0, "neutral": 0}
    for p in all_patterns:
        summary[p["direction"]] += 1

    return {
        "ticker": ticker,
        "days_analyzed": min(len(candles), days),
        "patterns_found": len(all_patterns),
        "patterns": all_patterns,
        "summary": summary,
    }


# =============================================================================
# Chart Pattern Detection
# =============================================================================


def _find_peaks_troughs(
    highs: list[float],
    lows: list[float],
    order: int = 5,
) -> tuple[list[int], list[int]]:
    """Find local peaks and troughs in price data.

    Uses a simple local max/min approach: a peak at index i means
    highs[i] is the maximum within [i-order, i+order].

    Args:
        highs: List of high prices
        lows: List of low prices
        order: Number of bars on each side to compare

    Returns:
        Tuple of (peak_indices, trough_indices)
    """
    n = len(highs)
    peaks: list[int] = []
    troughs: list[int] = []

    for i in range(order, n - order):
        # Check peak
        is_peak = True
        for j in range(i - order, i + order + 1):
            if j != i and highs[j] >= highs[i]:
                is_peak = False
                break
        if is_peak:
            peaks.append(i)

        # Check trough
        is_trough = True
        for j in range(i - order, i + order + 1):
            if j != i and lows[j] <= lows[i]:
                is_trough = False
                break
        if is_trough:
            troughs.append(i)

    return peaks, troughs


def _linear_regression(
    x_vals: list[float], y_vals: list[float],
) -> tuple[float, float, float]:
    """Simple linear regression returning slope, intercept, and R-squared.

    Args:
        x_vals: X values (typically index positions)
        y_vals: Y values (prices)

    Returns:
        (slope, intercept, r_squared)
    """
    n = len(x_vals)
    if n < 2:
        return 0.0, y_vals[0] if y_vals else 0.0, 0.0

    sum_x = sum(x_vals)
    sum_y = sum(y_vals)
    sum_xy = sum(x * y for x, y in zip(x_vals, y_vals))
    sum_x2 = sum(x * x for x in x_vals)

    denom = n * sum_x2 - sum_x * sum_x
    if abs(denom) < 1e-10:
        return 0.0, sum_y / n, 0.0

    slope = (n * sum_xy - sum_x * sum_y) / denom
    intercept = (sum_y - slope * sum_x) / n

    # R-squared
    y_mean = sum_y / n
    ss_tot = sum((y - y_mean) ** 2 for y in y_vals)
    ss_res = sum((y - (slope * x + intercept)) ** 2 for x, y in zip(x_vals, y_vals))

    r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0
    return slope, intercept, r_squared


def _price_tolerance(prices: list[float], pct: float = 0.015) -> float:
    """Calculate a tolerance threshold as a percentage of price range."""
    if not prices:
        return 0.0
    avg = sum(prices) / len(prices)
    return avg * pct


def detect_chart_patterns_from_data(
    candles: list[dict[str, Any]],
    min_pattern_days: int = 10,
) -> list[dict[str, Any]]:
    """Detect chart patterns from OHLCV data.

    Args:
        candles: OHLCV data sorted by date ascending
        min_pattern_days: Minimum days for a pattern to be valid

    Returns:
        List of detected chart patterns with details
    """
    n = len(candles)
    if n < min_pattern_days:
        return []

    highs = [_to_float(c["high"]) for c in candles]
    lows = [_to_float(c["low"]) for c in candles]
    closes = [_to_float(c["close"]) for c in candles]

    patterns: list[dict[str, Any]] = []

    # Adjust peak/trough detection order based on data length
    order = max(3, min(7, n // 10))
    peaks, troughs = _find_peaks_troughs(highs, lows, order=order)

    # Double top/bottom need >=2 peaks or >=2 troughs + >=1 of the other
    if len(peaks) >= 2 and len(troughs) >= 1:
        _detect_double_top(
            candles, highs, lows, closes, peaks, troughs, patterns,
        )

    if len(troughs) >= 2 and len(peaks) >= 1:
        _detect_double_bottom(
            candles, highs, lows, closes, peaks, troughs, patterns,
        )

    # Head and shoulders need >=3 peaks/troughs
    if len(peaks) >= 3 and len(troughs) >= 2:
        _detect_head_shoulders(
            candles, highs, lows, closes, peaks, troughs, patterns,
        )

    if len(troughs) >= 3 and len(peaks) >= 2:
        _detect_inverse_head_shoulders(
            candles, highs, lows, closes, peaks, troughs, patterns,
        )

    # Triangles need >=2 peaks and >=2 troughs
    if len(peaks) >= 2 and len(troughs) >= 2:
        _detect_ascending_triangle(
            candles, highs, lows, closes, peaks, troughs, patterns,
            min_pattern_days,
        )
        _detect_descending_triangle(
            candles, highs, lows, closes, peaks, troughs, patterns,
            min_pattern_days,
        )

    # Channels work on raw price data, no peak requirement
    _detect_channels(
        candles, highs, lows, closes, patterns, min_pattern_days,
    )

    return patterns


def _detect_double_top(
    candles: list[dict],
    highs: list[float],
    lows: list[float],
    closes: list[float],
    peaks: list[int],
    troughs: list[int],
    patterns: list[dict],
) -> None:
    """Detect double top patterns."""
    tol = _price_tolerance(highs)

    for i in range(len(peaks) - 1):
        p1, p2 = peaks[i], peaks[i + 1]
        if p2 - p1 < 5:
            continue

        h1, h2 = highs[p1], highs[p2]
        if abs(h1 - h2) > tol:
            continue

        # Find trough between peaks
        mid_troughs = [t for t in troughs if p1 < t < p2]
        if not mid_troughs:
            continue

        neckline_idx = min(mid_troughs, key=lambda t: lows[t])
        neckline = lows[neckline_idx]
        pattern_height = max(h1, h2) - neckline

        if pattern_height <= 0:
            continue

        target = neckline - pattern_height

        patterns.append({
            "date": candles[p2]["date"],
            "pattern": "double_top",
            "direction": BEARISH,
            "reliability": 2,
            "formation_start": candles[p1]["date"],
            "formation_end": candles[p2]["date"],
            "neckline": round(neckline, 2),
            "target_price": round(target, 2),
            "pattern_height": round(pattern_height, 2),
            "description": (
                f"Double top at ~{round(max(h1, h2), 2)}, "
                f"neckline {round(neckline, 2)}, "
                f"target {round(target, 2)}"
            ),
        })


def _detect_double_bottom(
    candles: list[dict],
    highs: list[float],
    lows: list[float],
    closes: list[float],
    peaks: list[int],
    troughs: list[int],
    patterns: list[dict],
) -> None:
    """Detect double bottom patterns."""
    tol = _price_tolerance(lows)

    for i in range(len(troughs) - 1):
        t1, t2 = troughs[i], troughs[i + 1]
        if t2 - t1 < 5:
            continue

        lo1, lo2 = lows[t1], lows[t2]
        if abs(lo1 - lo2) > tol:
            continue

        # Find peak between troughs
        mid_peaks = [p for p in peaks if t1 < p < t2]
        if not mid_peaks:
            continue

        neckline_idx = max(mid_peaks, key=lambda p: highs[p])
        neckline = highs[neckline_idx]
        pattern_height = neckline - min(lo1, lo2)

        if pattern_height <= 0:
            continue

        target = neckline + pattern_height

        patterns.append({
            "date": candles[t2]["date"],
            "pattern": "double_bottom",
            "direction": BULLISH,
            "reliability": 2,
            "formation_start": candles[t1]["date"],
            "formation_end": candles[t2]["date"],
            "neckline": round(neckline, 2),
            "target_price": round(target, 2),
            "pattern_height": round(pattern_height, 2),
            "description": (
                f"Double bottom at ~{round(min(lo1, lo2), 2)}, "
                f"neckline {round(neckline, 2)}, "
                f"target {round(target, 2)}"
            ),
        })


def _detect_head_shoulders(
    candles: list[dict],
    highs: list[float],
    lows: list[float],
    closes: list[float],
    peaks: list[int],
    troughs: list[int],
    patterns: list[dict],
) -> None:
    """Detect head and shoulders (bearish) patterns."""
    tol = _price_tolerance(highs)

    for i in range(len(peaks) - 2):
        left, head, right = peaks[i], peaks[i + 1], peaks[i + 2]

        # Head must be highest
        if highs[head] <= highs[left] or highs[head] <= highs[right]:
            continue

        # Shoulders should be roughly equal
        if abs(highs[left] - highs[right]) > tol * 2:
            continue

        # Find troughs between peaks for neckline
        left_troughs = [t for t in troughs if left < t < head]
        right_troughs = [t for t in troughs if head < t < right]
        if not left_troughs or not right_troughs:
            continue

        lt = min(left_troughs, key=lambda t: lows[t])
        rt = min(right_troughs, key=lambda t: lows[t])
        neckline = (lows[lt] + lows[rt]) / 2
        pattern_height = highs[head] - neckline

        if pattern_height <= 0:
            continue

        target = neckline - pattern_height

        patterns.append({
            "date": candles[right]["date"],
            "pattern": "head_and_shoulders",
            "direction": BEARISH,
            "reliability": 3,
            "formation_start": candles[left]["date"],
            "formation_end": candles[right]["date"],
            "neckline": round(neckline, 2),
            "target_price": round(target, 2),
            "pattern_height": round(pattern_height, 2),
            "description": (
                f"Head at {round(highs[head], 2)}, "
                f"neckline {round(neckline, 2)}, "
                f"target {round(target, 2)}"
            ),
        })


def _detect_inverse_head_shoulders(
    candles: list[dict],
    highs: list[float],
    lows: list[float],
    closes: list[float],
    peaks: list[int],
    troughs: list[int],
    patterns: list[dict],
) -> None:
    """Detect inverse head and shoulders (bullish) patterns."""
    tol = _price_tolerance(lows)

    for i in range(len(troughs) - 2):
        left, head, right = troughs[i], troughs[i + 1], troughs[i + 2]

        # Head must be lowest
        if lows[head] >= lows[left] or lows[head] >= lows[right]:
            continue

        # Shoulders should be roughly equal
        if abs(lows[left] - lows[right]) > tol * 2:
            continue

        # Find peaks between troughs for neckline
        left_peaks = [p for p in peaks if left < p < head]
        right_peaks = [p for p in peaks if head < p < right]
        if not left_peaks or not right_peaks:
            continue

        lp = max(left_peaks, key=lambda p: highs[p])
        rp = max(right_peaks, key=lambda p: highs[p])
        neckline = (highs[lp] + highs[rp]) / 2
        pattern_height = neckline - lows[head]

        if pattern_height <= 0:
            continue

        target = neckline + pattern_height

        patterns.append({
            "date": candles[right]["date"],
            "pattern": "inverse_head_and_shoulders",
            "direction": BULLISH,
            "reliability": 3,
            "formation_start": candles[left]["date"],
            "formation_end": candles[right]["date"],
            "neckline": round(neckline, 2),
            "target_price": round(target, 2),
            "pattern_height": round(pattern_height, 2),
            "description": (
                f"Head at {round(lows[head], 2)}, "
                f"neckline {round(neckline, 2)}, "
                f"target {round(target, 2)}"
            ),
        })


def _detect_ascending_triangle(
    candles: list[dict],
    highs: list[float],
    lows: list[float],
    closes: list[float],
    peaks: list[int],
    troughs: list[int],
    patterns: list[dict],
    min_days: int = 10,
) -> None:
    """Detect ascending triangle: flat resistance, rising support."""
    if len(peaks) < 2 or len(troughs) < 2:
        return

    tol = _price_tolerance(highs)

    # Look for 2+ peaks at similar level (flat resistance)
    for i in range(len(peaks) - 1):
        p1, p2 = peaks[i], peaks[i + 1]
        if p2 - p1 < min_days:
            continue

        if abs(highs[p1] - highs[p2]) > tol:
            continue

        resistance = (highs[p1] + highs[p2]) / 2

        # Check for rising troughs between/around these peaks
        relevant_troughs = [
            t for t in troughs if p1 <= t <= p2
        ]
        if len(relevant_troughs) < 2:
            continue

        trough_prices = [lows[t] for t in relevant_troughs]
        if not all(
            trough_prices[j] < trough_prices[j + 1]
            for j in range(len(trough_prices) - 1)
        ):
            continue

        support_low = trough_prices[0]
        pattern_height = resistance - support_low

        if pattern_height <= 0:
            continue

        target = resistance + pattern_height

        patterns.append({
            "date": candles[p2]["date"],
            "pattern": "ascending_triangle",
            "direction": BULLISH,
            "reliability": 2,
            "formation_start": candles[p1]["date"],
            "formation_end": candles[p2]["date"],
            "resistance": round(resistance, 2),
            "target_price": round(target, 2),
            "pattern_height": round(pattern_height, 2),
            "description": (
                f"Flat resistance at {round(resistance, 2)} "
                f"with rising support, target {round(target, 2)}"
            ),
        })


def _detect_descending_triangle(
    candles: list[dict],
    highs: list[float],
    lows: list[float],
    closes: list[float],
    peaks: list[int],
    troughs: list[int],
    patterns: list[dict],
    min_days: int = 10,
) -> None:
    """Detect descending triangle: flat support, falling resistance."""
    if len(troughs) < 2 or len(peaks) < 2:
        return

    tol = _price_tolerance(lows)

    # Look for 2+ troughs at similar level (flat support)
    for i in range(len(troughs) - 1):
        t1, t2 = troughs[i], troughs[i + 1]
        if t2 - t1 < min_days:
            continue

        if abs(lows[t1] - lows[t2]) > tol:
            continue

        support = (lows[t1] + lows[t2]) / 2

        # Check for falling peaks between these troughs
        relevant_peaks = [
            p for p in peaks if t1 <= p <= t2
        ]
        if len(relevant_peaks) < 2:
            continue

        peak_prices = [highs[p] for p in relevant_peaks]
        if not all(
            peak_prices[j] > peak_prices[j + 1]
            for j in range(len(peak_prices) - 1)
        ):
            continue

        resistance_high = peak_prices[0]
        pattern_height = resistance_high - support

        if pattern_height <= 0:
            continue

        target = support - pattern_height

        patterns.append({
            "date": candles[t2]["date"],
            "pattern": "descending_triangle",
            "direction": BEARISH,
            "reliability": 2,
            "formation_start": candles[t1]["date"],
            "formation_end": candles[t2]["date"],
            "support": round(support, 2),
            "target_price": round(target, 2),
            "pattern_height": round(pattern_height, 2),
            "description": (
                f"Flat support at {round(support, 2)} "
                f"with falling resistance, target {round(target, 2)}"
            ),
        })


def _detect_channels(
    candles: list[dict],
    highs: list[float],
    lows: list[float],
    closes: list[float],
    patterns: list[dict],
    min_days: int = 10,
) -> None:
    """Detect ascending and descending price channels."""
    n = len(candles)
    # Sliding window for channel detection
    window = max(min_days, 20)
    if n < window:
        return

    for start in range(0, n - window + 1, max(1, window // 2)):
        end = min(start + window, n)
        seg_highs = highs[start:end]
        seg_lows = lows[start:end]
        x_vals = [float(i) for i in range(len(seg_highs))]

        h_slope, h_int, h_r2 = _linear_regression(x_vals, seg_highs)
        l_slope, l_int, l_r2 = _linear_regression(x_vals, seg_lows)

        # Both lines need good fit
        if h_r2 < 0.7 or l_r2 < 0.7:
            continue

        # Slopes should be roughly parallel (similar direction and magnitude)
        if abs(h_slope) < 1e-6 and abs(l_slope) < 1e-6:
            continue

        # Check parallel: slopes within 50% of each other
        avg_slope = (h_slope + l_slope) / 2
        if abs(avg_slope) < 1e-6:
            continue
        slope_diff_pct = abs(h_slope - l_slope) / abs(avg_slope)
        if slope_diff_pct > 0.5:
            continue

        # Channel width should be meaningful
        channel_width = h_int - l_int
        avg_price = sum(closes[start:end]) / len(closes[start:end])
        if channel_width / avg_price < 0.02:
            continue

        if avg_slope > 0:
            direction = BULLISH
            pattern_name = "ascending_channel"
            desc_dir = "ascending"
        else:
            direction = BEARISH
            pattern_name = "descending_channel"
            desc_dir = "descending"

        # Project upper/lower bounds at end of pattern
        upper_end = h_slope * (end - start - 1) + h_int
        lower_end = l_slope * (end - start - 1) + l_int

        patterns.append({
            "date": candles[end - 1]["date"],
            "pattern": pattern_name,
            "direction": direction,
            "reliability": 2,
            "formation_start": candles[start]["date"],
            "formation_end": candles[end - 1]["date"],
            "upper_bound": round(upper_end, 2),
            "lower_bound": round(lower_end, 2),
            "slope": round(avg_slope, 4),
            "r_squared": round((h_r2 + l_r2) / 2, 3),
            "description": (
                f"Price {desc_dir} channel, "
                f"upper {round(upper_end, 2)}, "
                f"lower {round(lower_end, 2)}"
            ),
        })


# All chart pattern names
SUPPORTED_CHART_PATTERNS = {
    "double_top",
    "double_bottom",
    "head_and_shoulders",
    "inverse_head_and_shoulders",
    "ascending_triangle",
    "descending_triangle",
    "ascending_channel",
    "descending_channel",
}


def detect_chart_patterns(
    ticker: str,
    lookback_days: int = 60,
    min_pattern_days: int = 10,
) -> dict[str, Any]:
    """Detect chart patterns for a stock ticker.

    Args:
        ticker: Stock ticker symbol
        lookback_days: Days of data to analyze (default: 60, max: 504)
        min_pattern_days: Minimum formation period in days (default: 10)

    Returns:
        Dict with ticker, patterns list, and summary
    """
    lookback_days = min(lookback_days, 504)
    min_pattern_days = max(5, min(min_pattern_days, lookback_days // 2))
    ticker = ticker.upper()

    end_date = date.today()
    start_date = end_date - timedelta(days=int(lookback_days * 1.6))

    query = """
        SELECT date, open, high, low, close, volume
        FROM stock_prices
        WHERE ticker = %(ticker)s
            AND date >= %(start_date)s
            AND date <= %(end_date)s
        ORDER BY date ASC
        LIMIT %(limit)s
    """

    params = {
        "ticker": ticker,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "limit": lookback_days,
    }

    candles = execute_query(query, params)

    if not candles:
        return {
            "ticker": ticker,
            "days_analyzed": 0,
            "patterns_found": 0,
            "patterns": [],
        }

    found = detect_chart_patterns_from_data(candles, min_pattern_days)

    # Sort by date descending
    found.sort(key=lambda p: str(p["date"]), reverse=True)

    return {
        "ticker": ticker,
        "days_analyzed": len(candles),
        "patterns_found": len(found),
        "patterns": found,
    }
