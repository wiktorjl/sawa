"""Momentum and squeeze indicator MCP tools.

Provides TTM Squeeze detection using Bollinger Bands and Keltner Channels,
plus advanced momentum indicators (ADX, DMI, ROC, Stochastic, Williams %R).
"""

import logging
from datetime import date, timedelta
from typing import Any

from ..database import execute_query

logger = logging.getLogger(__name__)

# TTM Squeeze parameters
BB_PERIOD = 20
BB_STD_MULT = 2.0
KC_PERIOD = 20
KC_ATR_MULT = 1.5
MOMENTUM_PERIOD = 12

# Advanced momentum parameters
ADX_PERIOD = 14
STOCHASTIC_K_PERIOD = 14
STOCHASTIC_D_PERIOD = 3
ROC_PERIOD = 12
WILLIAMS_R_PERIOD = 14


def get_squeeze_indicators(
    ticker: str,
    lookback_days: int = 60,
) -> dict[str, Any]:
    """
    Calculate TTM Squeeze indicators for a ticker.

    The TTM Squeeze detects periods of low volatility (Bollinger Bands inside
    Keltner Channels), which often precede significant price moves.

    Args:
        ticker: Stock ticker symbol (e.g., "AAPL")
        lookback_days: Number of trading days to return (default: 60, max: 252)

    Returns:
        Dictionary with:
        - ticker: Stock symbol
        - lookback_days: Number of days returned
        - current_squeeze: Whether squeeze is currently active
        - current_momentum: Latest momentum value
        - current_direction: "bullish", "bearish", or "neutral"
        - squeeze_fired: True if squeeze just released (was on, now off)
        - data: Time series list with per-day squeeze indicators
    """
    lookback_days = min(lookback_days, 252)

    # We need extra days for the calculation window (BB/KC need ~20 days,
    # plus ATR needs 14, so fetch extra buffer)
    extra_days = BB_PERIOD + 14 + 10
    total_calendar_days = int((lookback_days + extra_days) * 1.5)
    start_date = (date.today() - timedelta(days=total_calendar_days)).isoformat()

    query = """
        WITH price_data AS (
            SELECT
                sp.date,
                sp.close,
                sp.high,
                sp.low,
                sp.volume,
                ti.bb_upper,
                ti.bb_middle,
                ti.bb_lower,
                ti.atr_14,
                ti.ema_50,
                ti.sma_20,
                ti.macd_line,
                ti.macd_signal,
                ti.macd_histogram,
                ti.rsi_14
            FROM stock_prices sp
            LEFT JOIN technical_indicators ti
                ON sp.ticker = ti.ticker AND sp.date = ti.date
            WHERE sp.ticker = %(ticker)s
                AND sp.date >= %(start_date)s
            ORDER BY sp.date ASC
        ),
        squeeze_calc AS (
            SELECT
                date,
                close,
                high,
                low,
                volume,
                bb_upper,
                bb_middle,
                bb_lower,
                atr_14,
                ema_50,
                sma_20,
                macd_histogram,
                rsi_14,
                -- Bollinger Band width: (upper - lower) / middle
                CASE WHEN bb_middle > 0
                    THEN ROUND(((bb_upper - bb_lower) / bb_middle * 100)::numeric, 4)
                    ELSE NULL
                END AS bb_width_pct,
                -- Keltner Channel bounds: using EMA-50 as center, ATR * multiplier
                -- Standard KC uses 20-period EMA, but we use sma_20 (BB middle)
                -- as a compatible center for direct comparison
                ROUND((sma_20 + atr_14 * %(kc_mult)s)::numeric, 4) AS kc_upper,
                ROUND((sma_20 - atr_14 * %(kc_mult)s)::numeric, 4) AS kc_lower,
                -- Keltner Channel width
                CASE WHEN sma_20 > 0 AND atr_14 IS NOT NULL
                    THEN ROUND(((atr_14 * %(kc_mult)s * 2) / sma_20 * 100)::numeric, 4)
                    ELSE NULL
                END AS kc_width_pct,
                -- Squeeze condition: BB inside KC (low volatility)
                CASE WHEN bb_upper IS NOT NULL AND atr_14 IS NOT NULL AND sma_20 > 0
                    THEN bb_upper < (sma_20 + atr_14 * %(kc_mult)s)
                         AND bb_lower > (sma_20 - atr_14 * %(kc_mult)s)
                    ELSE NULL
                END AS squeeze_on,
                -- Momentum: using close relative to midpoint of
                -- highest high and lowest low over momentum period,
                -- combined with a linear regression approximation.
                -- Simplified: use (close - avg of highest high and lowest low)
                -- over momentum_period as the momentum oscillator.
                close - (
                    (MAX(high) OVER (
                        ORDER BY date
                        ROWS BETWEEN %(mom_period)s PRECEDING AND CURRENT ROW
                    ) + MIN(low) OVER (
                        ORDER BY date
                        ROWS BETWEEN %(mom_period)s PRECEDING AND CURRENT ROW
                    )) / 2.0
                    + sma_20
                ) / 2.0 AS momentum,
                ROW_NUMBER() OVER (ORDER BY date DESC) AS rn
            FROM price_data
            WHERE bb_upper IS NOT NULL
                AND atr_14 IS NOT NULL
                AND sma_20 IS NOT NULL
        )
        SELECT
            date,
            close,
            ROUND(bb_upper::numeric, 2) AS bb_upper,
            ROUND(bb_middle::numeric, 2) AS bb_middle,
            ROUND(bb_lower::numeric, 2) AS bb_lower,
            bb_width_pct,
            ROUND(kc_upper::numeric, 2) AS kc_upper,
            ROUND(kc_lower::numeric, 2) AS kc_lower,
            kc_width_pct,
            squeeze_on,
            ROUND(momentum::numeric, 4) AS momentum,
            ROUND(atr_14::numeric, 4) AS atr,
            rsi_14,
            macd_histogram
        FROM squeeze_calc
        WHERE rn <= %(lookback_days)s
        ORDER BY date ASC
    """

    params = {
        "ticker": ticker.upper(),
        "start_date": start_date,
        "kc_mult": KC_ATR_MULT,
        "mom_period": MOMENTUM_PERIOD,
        "lookback_days": lookback_days,
    }

    rows = execute_query(query, params)

    if not rows:
        return {
            "ticker": ticker.upper(),
            "lookback_days": lookback_days,
            "current_squeeze": None,
            "current_momentum": None,
            "current_direction": "neutral",
            "squeeze_fired": False,
            "data": [],
        }

    # Derive direction signals from momentum series
    data = []
    prev_momentum = None
    for row in rows:
        momentum = row.get("momentum")
        squeeze = row.get("squeeze_on")

        # Direction: bullish if momentum positive and rising, bearish if negative and falling
        if momentum is not None and prev_momentum is not None:
            mom_float = float(momentum)
            prev_float = float(prev_momentum)
            if mom_float > 0 and mom_float > prev_float:
                direction = "bullish"
            elif mom_float < 0 and mom_float < prev_float:
                direction = "bearish"
            elif mom_float > 0:
                direction = "weakening_bullish"
            elif mom_float < 0:
                direction = "weakening_bearish"
            else:
                direction = "neutral"
        else:
            direction = "neutral"

        data.append({
            "date": row["date"],
            "close": row["close"],
            "bb_upper": row["bb_upper"],
            "bb_middle": row["bb_middle"],
            "bb_lower": row["bb_lower"],
            "bb_width_pct": row["bb_width_pct"],
            "kc_upper": row["kc_upper"],
            "kc_lower": row["kc_lower"],
            "kc_width_pct": row["kc_width_pct"],
            "squeeze_on": squeeze,
            "momentum": momentum,
            "direction": direction,
            "atr": row["atr"],
            "rsi_14": row["rsi_14"],
            "macd_histogram": row["macd_histogram"],
        })

        prev_momentum = momentum

    # Current state (last row)
    latest = data[-1]
    second_latest = data[-2] if len(data) >= 2 else None

    # Squeeze fired = was on, now off
    squeeze_fired = False
    if second_latest and second_latest["squeeze_on"] is True and latest["squeeze_on"] is False:
        squeeze_fired = True

    return {
        "ticker": ticker.upper(),
        "lookback_days": len(data),
        "current_squeeze": latest["squeeze_on"],
        "current_momentum": latest["momentum"],
        "current_direction": latest["direction"],
        "squeeze_fired": squeeze_fired,
        "data": data,
    }


def get_momentum_indicators(
    ticker: str,
    lookback_days: int = 60,
) -> dict[str, Any]:
    """
    Calculate advanced momentum indicators from price data.

    Computes ADX (trend strength), DMI (+DI/-DI), Rate of Change,
    Stochastic Oscillator (%K/%D), and Williams %R using SQL window
    functions over OHLCV price data.

    Args:
        ticker: Stock ticker symbol (e.g., "AAPL")
        lookback_days: Number of trading days to return (default: 60, max: 252)

    Returns:
        Dictionary with:
        - ticker: Stock symbol
        - lookback_days: Number of days returned
        - current: Latest values for all indicators
        - signals: Interpretation of current values
        - data: Time series list with per-day indicator values
    """
    lookback_days = min(lookback_days, 252)

    # Extra buffer for calculation warmup (ADX needs 2x its period)
    extra_days = ADX_PERIOD * 3
    total_calendar_days = int((lookback_days + extra_days) * 1.5)
    start_date = (date.today() - timedelta(days=total_calendar_days)).isoformat()

    query = """
        WITH price_data AS (
            SELECT
                date,
                high,
                low,
                close,
                volume,
                LAG(high) OVER (ORDER BY date) AS prev_high,
                LAG(low) OVER (ORDER BY date) AS prev_low,
                LAG(close) OVER (ORDER BY date) AS prev_close,
                -- For ROC
                LAG(close, %(roc_period)s) OVER (ORDER BY date) AS close_n_ago
            FROM stock_prices
            WHERE ticker = %(ticker)s
                AND date >= %(start_date)s
            ORDER BY date ASC
        ),
        tr_dm AS (
            SELECT
                date,
                high,
                low,
                close,
                volume,
                prev_close,
                close_n_ago,
                -- True Range
                GREATEST(
                    high - low,
                    ABS(high - prev_close),
                    ABS(low - prev_close)
                ) AS tr,
                -- Directional Movement
                CASE WHEN (high - prev_high) > (prev_low - low)
                         AND (high - prev_high) > 0
                    THEN high - prev_high
                    ELSE 0
                END AS plus_dm,
                CASE WHEN (prev_low - low) > (high - prev_high)
                         AND (prev_low - low) > 0
                    THEN prev_low - low
                    ELSE 0
                END AS minus_dm,
                -- Stochastic: highest high and lowest low over K period
                MAX(high) OVER (
                    ORDER BY date
                    ROWS BETWEEN %(stoch_k)s PRECEDING AND CURRENT ROW
                ) AS highest_high_k,
                MIN(low) OVER (
                    ORDER BY date
                    ROWS BETWEEN %(stoch_k)s PRECEDING AND CURRENT ROW
                ) AS lowest_low_k,
                -- Williams %R: highest high and lowest low over period
                MAX(high) OVER (
                    ORDER BY date
                    ROWS BETWEEN %(williams_period)s PRECEDING AND CURRENT ROW
                ) AS highest_high_w,
                MIN(low) OVER (
                    ORDER BY date
                    ROWS BETWEEN %(williams_period)s PRECEDING AND CURRENT ROW
                ) AS lowest_low_w,
                ROW_NUMBER() OVER (ORDER BY date) AS rn
            FROM price_data
            WHERE prev_close IS NOT NULL
        ),
        smoothed AS (
            SELECT
                date,
                high,
                low,
                close,
                volume,
                tr,
                plus_dm,
                minus_dm,
                -- Smoothed TR and DM (using SMA as approximation of Wilder smoothing)
                AVG(tr) OVER (
                    ORDER BY date
                    ROWS BETWEEN %(adx_period)s PRECEDING AND CURRENT ROW
                ) AS atr_smooth,
                AVG(plus_dm) OVER (
                    ORDER BY date
                    ROWS BETWEEN %(adx_period)s PRECEDING AND CURRENT ROW
                ) AS plus_dm_smooth,
                AVG(minus_dm) OVER (
                    ORDER BY date
                    ROWS BETWEEN %(adx_period)s PRECEDING AND CURRENT ROW
                ) AS minus_dm_smooth,
                -- Stochastic %K (raw)
                CASE WHEN (highest_high_k - lowest_low_k) > 0
                    THEN ((close - lowest_low_k) / (highest_high_k - lowest_low_k)) * 100
                    ELSE 50.0
                END AS stoch_k_raw,
                -- Williams %R
                CASE WHEN (highest_high_w - lowest_low_w) > 0
                    THEN ((highest_high_w - close) / (highest_high_w - lowest_low_w)) * -100
                    ELSE -50.0
                END AS williams_r,
                -- ROC
                CASE WHEN close_n_ago > 0
                    THEN ((close - close_n_ago) / close_n_ago) * 100
                    ELSE NULL
                END AS roc,
                rn
            FROM tr_dm
        ),
        indicators AS (
            SELECT
                date,
                close,
                volume,
                -- +DI and -DI
                CASE WHEN atr_smooth > 0
                    THEN ROUND((plus_dm_smooth / atr_smooth * 100)::numeric, 2)
                    ELSE NULL
                END AS plus_di,
                CASE WHEN atr_smooth > 0
                    THEN ROUND((minus_dm_smooth / atr_smooth * 100)::numeric, 2)
                    ELSE NULL
                END AS minus_di,
                -- DX (for ADX calculation)
                CASE WHEN (plus_dm_smooth + minus_dm_smooth) > 0 AND atr_smooth > 0
                    THEN ABS(plus_dm_smooth - minus_dm_smooth)
                         / (plus_dm_smooth + minus_dm_smooth) * 100
                    ELSE NULL
                END AS dx,
                -- Stochastic %K (smoothed with D period SMA)
                ROUND(
                    AVG(stoch_k_raw) OVER (
                        ORDER BY date
                        ROWS BETWEEN %(stoch_d)s PRECEDING AND CURRENT ROW
                    )::numeric, 2
                ) AS stoch_k,
                -- Stochastic %D (SMA of %K)
                ROUND(
                    AVG(
                        AVG(stoch_k_raw) OVER (
                            ORDER BY date
                            ROWS BETWEEN %(stoch_d)s PRECEDING AND CURRENT ROW
                        )
                    ) OVER (
                        ORDER BY date
                        ROWS BETWEEN %(stoch_d)s PRECEDING AND CURRENT ROW
                    )::numeric, 2
                ) AS stoch_d,
                -- Williams %R
                ROUND(williams_r::numeric, 2) AS williams_r,
                -- ROC
                ROUND(roc::numeric, 2) AS roc,
                rn,
                ROW_NUMBER() OVER (ORDER BY date DESC) AS rn_desc
            FROM smoothed
            WHERE rn > %(adx_period)s
        )
        SELECT
            date,
            close,
            plus_di,
            minus_di,
            -- ADX: smoothed average of DX
            ROUND(
                AVG(dx) OVER (
                    ORDER BY date
                    ROWS BETWEEN %(adx_period)s PRECEDING AND CURRENT ROW
                )::numeric, 2
            ) AS adx,
            stoch_k,
            stoch_d,
            williams_r,
            roc
        FROM indicators
        WHERE rn_desc <= %(lookback_days)s
        ORDER BY date ASC
    """

    params = {
        "ticker": ticker.upper(),
        "start_date": start_date,
        "adx_period": ADX_PERIOD,
        "stoch_k": STOCHASTIC_K_PERIOD,
        "stoch_d": STOCHASTIC_D_PERIOD,
        "williams_period": WILLIAMS_R_PERIOD,
        "roc_period": ROC_PERIOD,
        "lookback_days": lookback_days,
    }

    rows = execute_query(query, params)

    if not rows:
        return {
            "ticker": ticker.upper(),
            "lookback_days": lookback_days,
            "current": None,
            "signals": {},
            "data": [],
        }

    data = []
    for row in rows:
        data.append({
            "date": row["date"],
            "close": row["close"],
            "adx": row["adx"],
            "plus_di": row["plus_di"],
            "minus_di": row["minus_di"],
            "stoch_k": row["stoch_k"],
            "stoch_d": row["stoch_d"],
            "williams_r": row["williams_r"],
            "roc": row["roc"],
        })

    latest = data[-1]

    # Generate signals from current values
    signals = _interpret_momentum(latest)

    return {
        "ticker": ticker.upper(),
        "lookback_days": len(data),
        "current": latest,
        "signals": signals,
        "data": data,
    }


def _interpret_momentum(values: dict[str, Any]) -> dict[str, str]:
    """Derive trading signals from momentum indicator values.

    Args:
        values: Dict with adx, plus_di, minus_di, stoch_k, stoch_d,
                williams_r, roc.

    Returns:
        Dict mapping signal name to interpretation string.
    """
    signals: dict[str, str] = {}

    adx = values.get("adx")
    plus_di = values.get("plus_di")
    minus_di = values.get("minus_di")
    stoch_k = values.get("stoch_k")
    williams_r = values.get("williams_r")
    roc = values.get("roc")

    # ADX trend strength
    if adx is not None:
        adx_f = float(adx)
        if adx_f >= 25:
            signals["trend_strength"] = "strong"
        elif adx_f >= 20:
            signals["trend_strength"] = "moderate"
        else:
            signals["trend_strength"] = "weak"

    # DMI direction
    if plus_di is not None and minus_di is not None:
        if float(plus_di) > float(minus_di):
            signals["dmi_direction"] = "bullish"
        elif float(minus_di) > float(plus_di):
            signals["dmi_direction"] = "bearish"
        else:
            signals["dmi_direction"] = "neutral"

    # Stochastic overbought/oversold
    if stoch_k is not None:
        stoch_f = float(stoch_k)
        if stoch_f >= 80:
            signals["stochastic"] = "overbought"
        elif stoch_f <= 20:
            signals["stochastic"] = "oversold"
        else:
            signals["stochastic"] = "neutral"

    # Williams %R overbought/oversold
    if williams_r is not None:
        wr_f = float(williams_r)
        if wr_f >= -20:
            signals["williams_r"] = "overbought"
        elif wr_f <= -80:
            signals["williams_r"] = "oversold"
        else:
            signals["williams_r"] = "neutral"

    # ROC momentum
    if roc is not None:
        roc_f = float(roc)
        if roc_f > 5:
            signals["roc_momentum"] = "strong_bullish"
        elif roc_f > 0:
            signals["roc_momentum"] = "bullish"
        elif roc_f < -5:
            signals["roc_momentum"] = "strong_bearish"
        elif roc_f < 0:
            signals["roc_momentum"] = "bearish"
        else:
            signals["roc_momentum"] = "neutral"

    return signals
