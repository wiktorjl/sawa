"""Volume profile and analysis MCP tools.

Provides tools for volume-by-price analysis, volume anomaly detection,
and advanced volume indicators (OBV, CMF, A/D line, VWAP).
"""

import logging
from datetime import date, timedelta
from typing import Any

from ..database import execute_query

logger = logging.getLogger(__name__)


def get_volume_profile(
    ticker: str,
    lookback_days: int = 30,
    price_bins: int = 20,
) -> dict[str, Any]:
    """
    Get volume distribution by price level (volume profile).

    Bins price data into levels and sums volume at each level to identify
    areas of high/low liquidity. Calculates POC (point of control) and
    value area (70% of total volume).

    Args:
        ticker: Stock ticker symbol (e.g., "AAPL")
        lookback_days: Number of trading days to analyze (default: 30, max: 252)
        price_bins: Number of price bins (default: 20, max: 50)

    Returns:
        Dict with:
        - ticker: Stock symbol
        - period_start/end: Date range analyzed
        - total_volume: Total volume in period
        - price_low/high: Price range
        - bin_size: Price range per bin
        - poc: Point of control (price level with most volume)
        - value_area_high/low: 70% volume range bounds
        - bins: List of {price_low, price_high, price_mid, volume, pct_of_total}
    """
    lookback_days = min(max(lookback_days, 1), 252)
    price_bins = min(max(price_bins, 5), 50)

    start_date = (date.today() - timedelta(days=int(lookback_days * 1.5))).isoformat()

    query = """
        SELECT date, high, low, close, volume
        FROM stock_prices
        WHERE ticker = %(ticker)s
            AND date >= %(start_date)s
        ORDER BY date ASC
    """

    rows = execute_query(query, {"ticker": ticker.upper(), "start_date": start_date})

    # Take only the last lookback_days rows
    rows = rows[-lookback_days:]

    if not rows:
        return {
            "ticker": ticker.upper(),
            "error": "No price data found for the specified period",
            "bins": [],
        }

    # Compute price range
    price_low = min(float(r["low"]) for r in rows)
    price_high = max(float(r["high"]) for r in rows)

    if price_high == price_low:
        price_high = price_low + 0.01

    bin_size = (price_high - price_low) / price_bins

    # Initialize bins
    bins: list[dict[str, Any]] = []
    for i in range(price_bins):
        b_low = price_low + i * bin_size
        b_high = price_low + (i + 1) * bin_size
        bins.append({
            "price_low": round(b_low, 2),
            "price_high": round(b_high, 2),
            "price_mid": round((b_low + b_high) / 2, 2),
            "volume": 0,
        })

    # Distribute volume across bins using typical price approach:
    # For each bar, allocate volume proportionally to bins the bar spans
    total_volume = 0
    for row in rows:
        bar_low = float(row["low"])
        bar_high = float(row["high"])
        bar_volume = int(row["volume"])
        total_volume += bar_volume

        if bar_high == bar_low:
            # All volume goes to one bin
            idx = min(int((bar_low - price_low) / bin_size), price_bins - 1)
            bins[idx]["volume"] += bar_volume
            continue

        # Distribute volume proportionally across bins the bar spans
        for i, b in enumerate(bins):
            overlap_low = max(bar_low, b["price_low"])
            overlap_high = min(bar_high, b["price_high"])
            if overlap_high > overlap_low:
                fraction = (overlap_high - overlap_low) / (bar_high - bar_low)
                bins[i]["volume"] += int(bar_volume * fraction)

    # Calculate percentages and find POC
    poc_bin = None
    max_vol = 0
    for b in bins:
        b["pct_of_total"] = round(b["volume"] / total_volume * 100, 2) if total_volume else 0
        if b["volume"] > max_vol:
            max_vol = b["volume"]
            poc_bin = b

    # Calculate value area (70% of total volume, expanding from POC)
    value_area_volume = int(total_volume * 0.7)
    if poc_bin:
        poc_idx = bins.index(poc_bin)
        accumulated = poc_bin["volume"]
        va_low_idx = poc_idx
        va_high_idx = poc_idx

        while accumulated < value_area_volume:
            expand_down = bins[va_low_idx - 1]["volume"] if va_low_idx > 0 else -1
            expand_up = bins[va_high_idx + 1]["volume"] if va_high_idx < len(bins) - 1 else -1

            if expand_down == -1 and expand_up == -1:
                break

            if expand_down >= expand_up:
                va_low_idx -= 1
                accumulated += bins[va_low_idx]["volume"]
            else:
                va_high_idx += 1
                accumulated += bins[va_high_idx]["volume"]

        va_low = bins[va_low_idx]["price_low"]
        va_high = bins[va_high_idx]["price_high"]
    else:
        va_low = price_low
        va_high = price_high

    return {
        "ticker": ticker.upper(),
        "period_start": str(rows[0]["date"]),
        "period_end": str(rows[-1]["date"]),
        "trading_days": len(rows),
        "total_volume": total_volume,
        "price_low": round(price_low, 2),
        "price_high": round(price_high, 2),
        "bin_size": round(bin_size, 2),
        "poc": {
            "price": poc_bin["price_mid"] if poc_bin else None,
            "volume": poc_bin["volume"] if poc_bin else None,
        },
        "value_area_high": round(va_high, 2),
        "value_area_low": round(va_low, 2),
        "bins": bins,
    }


def detect_volume_anomalies(
    ticker: str,
    lookback_days: int = 90,
    threshold_multiplier: float = 2.0,
) -> dict[str, Any]:
    """
    Detect volume spikes, drops, and price-volume divergences.

    Compares daily volume to a 20-day moving average to flag outliers.
    Also detects price-volume divergences (price up on low volume or
    price down on high volume).

    Args:
        ticker: Stock ticker symbol (e.g., "AAPL")
        lookback_days: Number of trading days to analyze (default: 90, max: 252)
        threshold_multiplier: Volume must be this many times above/below MA
                              to be flagged (default: 2.0)

    Returns:
        Dict with:
        - ticker: Stock symbol
        - period_start/end: Date range
        - avg_volume: Average daily volume
        - volume_spikes: List of dates with unusually high volume
        - volume_drops: List of dates with unusually low volume
        - divergences: List of price-volume divergences
        - summary: Counts of each anomaly type
    """
    lookback_days = min(max(lookback_days, 20), 252)
    threshold_multiplier = max(threshold_multiplier, 1.1)

    # Fetch extra days for moving average warmup
    fetch_days = lookback_days + 30
    start_date = (date.today() - timedelta(days=int(fetch_days * 1.5))).isoformat()

    query = """
        SELECT date, open, high, low, close, volume
        FROM stock_prices
        WHERE ticker = %(ticker)s
            AND date >= %(start_date)s
        ORDER BY date ASC
    """

    rows = execute_query(query, {"ticker": ticker.upper(), "start_date": start_date})

    if len(rows) < 21:
        return {
            "ticker": ticker.upper(),
            "error": "Insufficient data for volume analysis (need at least 21 days)",
            "volume_spikes": [],
            "volume_drops": [],
            "divergences": [],
        }

    # Calculate 20-day volume MA for each day
    volumes = [int(r["volume"]) for r in rows]
    closes = [float(r["close"]) for r in rows]

    volume_spikes: list[dict[str, Any]] = []
    volume_drops: list[dict[str, Any]] = []
    divergences: list[dict[str, Any]] = []

    # Only analyze the last lookback_days (after warmup)
    analysis_start = max(20, len(rows) - lookback_days)

    for i in range(analysis_start, len(rows)):
        # 20-day volume MA
        vol_ma = sum(volumes[i - 20 : i]) / 20
        if vol_ma == 0:
            continue

        vol_ratio = volumes[i] / vol_ma
        price_change_pct = (
            ((closes[i] - closes[i - 1]) / closes[i - 1] * 100)
            if closes[i - 1] > 0
            else 0
        )

        day_info = {
            "date": str(rows[i]["date"]),
            "volume": volumes[i],
            "volume_ma_20": round(vol_ma),
            "volume_ratio": round(vol_ratio, 2),
            "close": closes[i],
            "price_change_pct": round(price_change_pct, 2),
        }

        # Volume spike
        if vol_ratio >= threshold_multiplier:
            volume_spikes.append(day_info)

        # Volume drop (inverse threshold)
        if vol_ratio <= 1.0 / threshold_multiplier:
            volume_drops.append(day_info)

        # Price-volume divergences
        # Bearish divergence: price up significantly but volume declining
        if price_change_pct > 1.0 and vol_ratio < 0.7:
            divergences.append({
                **day_info,
                "type": "bearish",
                "description": "Price rising on declining volume",
            })
        # Bullish divergence: price down significantly but volume declining
        elif price_change_pct < -1.0 and vol_ratio < 0.7:
            divergences.append({
                **day_info,
                "type": "bullish",
                "description": "Price falling on declining volume",
            })
        # Climactic buying: big price up on huge volume
        elif price_change_pct > 2.0 and vol_ratio > threshold_multiplier:
            divergences.append({
                **day_info,
                "type": "climactic_buying",
                "description": "Large price gain on very high volume",
            })
        # Climactic selling: big price down on huge volume
        elif price_change_pct < -2.0 and vol_ratio > threshold_multiplier:
            divergences.append({
                **day_info,
                "type": "climactic_selling",
                "description": "Large price drop on very high volume",
            })

    # Compute analysis period info
    analysis_rows = rows[analysis_start:]
    avg_volume = round(sum(volumes[analysis_start:]) / len(analysis_rows)) if analysis_rows else 0

    return {
        "ticker": ticker.upper(),
        "period_start": str(analysis_rows[0]["date"]) if analysis_rows else None,
        "period_end": str(analysis_rows[-1]["date"]) if analysis_rows else None,
        "trading_days": len(analysis_rows),
        "avg_volume": avg_volume,
        "threshold_multiplier": threshold_multiplier,
        "volume_spikes": volume_spikes,
        "volume_drops": volume_drops,
        "divergences": divergences,
        "summary": {
            "spike_count": len(volume_spikes),
            "drop_count": len(volume_drops),
            "divergence_count": len(divergences),
            "bearish_divergences": sum(1 for d in divergences if d["type"] == "bearish"),
            "bullish_divergences": sum(1 for d in divergences if d["type"] == "bullish"),
            "climactic_events": sum(
                1 for d in divergences if d["type"].startswith("climactic")
            ),
        },
    }


def get_advanced_volume_indicators(
    ticker: str,
    lookback_days: int = 60,
) -> dict[str, Any]:
    """
    Calculate advanced volume indicators: OBV, CMF, A/D line, VWAP.

    Args:
        ticker: Stock ticker symbol (e.g., "AAPL")
        lookback_days: Number of trading days (default: 60, max: 252)

    Returns:
        Dict with:
        - ticker: Stock symbol
        - period_start/end: Date range
        - indicators: List of daily records with obv, cmf_20, ad_line, vwap
        - latest: Most recent indicator values
        - signals: Interpretation of current indicator values
    """
    lookback_days = min(max(lookback_days, 5), 252)

    # Fetch extra days for warmup (CMF needs 20-day window)
    fetch_days = lookback_days + 25
    start_date = (date.today() - timedelta(days=int(fetch_days * 1.5))).isoformat()

    query = """
        SELECT date, open, high, low, close, volume
        FROM stock_prices
        WHERE ticker = %(ticker)s
            AND date >= %(start_date)s
        ORDER BY date ASC
    """

    rows = execute_query(query, {"ticker": ticker.upper(), "start_date": start_date})

    if len(rows) < 2:
        return {
            "ticker": ticker.upper(),
            "error": "Insufficient data for volume indicator calculation",
            "indicators": [],
        }

    # Pre-extract data
    highs = [float(r["high"]) for r in rows]
    lows = [float(r["low"]) for r in rows]
    closes = [float(r["close"]) for r in rows]
    volumes = [int(r["volume"]) for r in rows]

    n = len(rows)

    # Calculate OBV (On-Balance Volume)
    obv = [0] * n
    obv[0] = volumes[0]
    for i in range(1, n):
        if closes[i] > closes[i - 1]:
            obv[i] = obv[i - 1] + volumes[i]
        elif closes[i] < closes[i - 1]:
            obv[i] = obv[i - 1] - volumes[i]
        else:
            obv[i] = obv[i - 1]

    # Calculate A/D (Accumulation/Distribution) line
    ad_line = [0.0] * n
    for i in range(n):
        hl_range = highs[i] - lows[i]
        if hl_range > 0:
            mfm = ((closes[i] - lows[i]) - (highs[i] - closes[i])) / hl_range
        else:
            mfm = 0.0
        mfv = mfm * volumes[i]
        ad_line[i] = (ad_line[i - 1] + mfv) if i > 0 else mfv

    # Calculate CMF (Chaikin Money Flow) - 20-day window
    cmf_period = 20
    cmf = [None] * n
    for i in range(cmf_period - 1, n):
        window_mfv_sum = 0.0
        window_vol_sum = 0
        for j in range(i - cmf_period + 1, i + 1):
            hl_range = highs[j] - lows[j]
            if hl_range > 0:
                mfm = ((closes[j] - lows[j]) - (highs[j] - closes[j])) / hl_range
            else:
                mfm = 0.0
            window_mfv_sum += mfm * volumes[j]
            window_vol_sum += volumes[j]

        cmf[i] = round(window_mfv_sum / window_vol_sum, 4) if window_vol_sum > 0 else 0.0

    # Calculate VWAP (cumulative for the period)
    cum_tp_vol = 0.0
    cum_vol = 0
    vwap = [None] * n
    for i in range(n):
        typical_price = (highs[i] + lows[i] + closes[i]) / 3
        cum_tp_vol += typical_price * volumes[i]
        cum_vol += volumes[i]
        vwap[i] = round(cum_tp_vol / cum_vol, 2) if cum_vol > 0 else None

    # Build output records (only last lookback_days)
    output_start = max(0, n - lookback_days)
    indicators = []
    for i in range(output_start, n):
        indicators.append({
            "date": str(rows[i]["date"]),
            "close": closes[i],
            "volume": volumes[i],
            "obv": obv[i],
            "ad_line": round(ad_line[i], 2),
            "cmf_20": cmf[i],
            "vwap": vwap[i],
        })

    # Generate signals from latest values
    latest = indicators[-1] if indicators else {}
    signals = _generate_volume_signals(indicators, closes[output_start:])

    return {
        "ticker": ticker.upper(),
        "period_start": indicators[0]["date"] if indicators else None,
        "period_end": indicators[-1]["date"] if indicators else None,
        "trading_days": len(indicators),
        "latest": latest,
        "signals": signals,
        "indicators": indicators,
    }


def _generate_volume_signals(
    indicators: list[dict[str, Any]],
    closes: list[float],
) -> list[dict[str, str]]:
    """Generate interpretation signals from volume indicators."""
    if len(indicators) < 5:
        return []

    signals = []
    latest = indicators[-1]

    # OBV trend
    obv_values = [r["obv"] for r in indicators[-10:]]
    if len(obv_values) >= 5:
        obv_trend = obv_values[-1] - obv_values[0]
        price_trend = closes[-1] - closes[-min(10, len(closes))] if len(closes) >= 2 else 0

        if obv_trend > 0 and price_trend > 0:
            signals.append({
                "indicator": "OBV",
                "signal": "bullish",
                "description": "OBV rising with price - confirms uptrend",
            })
        elif obv_trend > 0 and price_trend < 0:
            signals.append({
                "indicator": "OBV",
                "signal": "bullish_divergence",
                "description": "OBV rising while price falling - potential reversal up",
            })
        elif obv_trend < 0 and price_trend > 0:
            signals.append({
                "indicator": "OBV",
                "signal": "bearish_divergence",
                "description": "OBV falling while price rising - potential reversal down",
            })
        elif obv_trend < 0 and price_trend < 0:
            signals.append({
                "indicator": "OBV",
                "signal": "bearish",
                "description": "OBV falling with price - confirms downtrend",
            })

    # CMF signal
    if latest.get("cmf_20") is not None:
        cmf_val = latest["cmf_20"]
        if cmf_val > 0.1:
            signals.append({
                "indicator": "CMF",
                "signal": "strong_buying",
                "description": f"CMF at {cmf_val:.4f} - strong buying pressure",
            })
        elif cmf_val > 0:
            signals.append({
                "indicator": "CMF",
                "signal": "mild_buying",
                "description": f"CMF at {cmf_val:.4f} - mild buying pressure",
            })
        elif cmf_val < -0.1:
            signals.append({
                "indicator": "CMF",
                "signal": "strong_selling",
                "description": f"CMF at {cmf_val:.4f} - strong selling pressure",
            })
        else:
            signals.append({
                "indicator": "CMF",
                "signal": "mild_selling",
                "description": f"CMF at {cmf_val:.4f} - mild selling pressure",
            })

    # VWAP signal
    if latest.get("vwap") is not None and latest.get("close"):
        vwap_val = latest["vwap"]
        close_val = latest["close"]
        if close_val > vwap_val:
            signals.append({
                "indicator": "VWAP",
                "signal": "above_vwap",
                "description": f"Price ({close_val:.2f}) above VWAP ({vwap_val:.2f}) - bullish",
            })
        else:
            signals.append({
                "indicator": "VWAP",
                "signal": "below_vwap",
                "description": f"Price ({close_val:.2f}) below VWAP ({vwap_val:.2f}) - bearish",
            })

    return signals
