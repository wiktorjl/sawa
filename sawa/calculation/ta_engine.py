"""Technical indicator calculation engine using ta-lib.

Calculates 20 technical indicators from OHLCV price data.
Handles NaN values for insufficient data and validates bounded indicators.
"""

import logging
import math
from datetime import date
from decimal import Decimal
from typing import Any

import numpy as np

try:
    import talib
except ImportError:
    talib = None  # type: ignore

from sawa.domain.technical_indicators import TechnicalIndicators

logger = logging.getLogger(__name__)

# Validation bounds for indicators
INDICATOR_BOUNDS: dict[str, tuple[float | None, float | None]] = {
    "rsi_14": (0.0, 100.0),
    "rsi_21": (0.0, 100.0),
    "atr_14": (0.0, None),
    "volume_sma_20": (0.0, None),
    "volume_ratio": (0.0, None),
}

# Minimum periods required for each indicator
MIN_PERIODS: dict[str, int] = {
    "sma_5": 5,
    "sma_10": 10,
    "sma_20": 20,
    "sma_50": 50,
    "ema_12": 12,
    "ema_26": 26,
    "ema_50": 50,
    "vwap": 1,
    "rsi_14": 14,
    "rsi_21": 21,
    "macd_line": 26,
    "macd_signal": 35,  # 26 + 9
    "macd_histogram": 35,
    "bb_upper": 20,
    "bb_middle": 20,
    "bb_lower": 20,
    "atr_14": 14,
    "obv": 1,
    "volume_sma_20": 20,
    "volume_ratio": 20,
}


def validate_indicator(
    name: str,
    value: float,
    log: logging.Logger | None = None,
) -> float | None:
    """Validate indicator value with hard limits and soft clamping.

    - NaN values return None (insufficient data)
    - Values >1% outside bounds raise ValueError (calculation bug)
    - Values <1% outside bounds are clamped with warning (floating-point error)

    Args:
        name: Indicator name for logging
        value: Calculated value
        log: Logger instance

    Returns:
        Validated value, or None for NaN

    Raises:
        ValueError: If value is significantly outside valid bounds
    """
    log = log or logger

    # Handle NaN (insufficient data)
    if math.isnan(value):
        return None

    # Handle infinity
    if math.isinf(value):
        log.warning(f"Infinite value for {name}, returning None")
        return None

    # Get bounds for this indicator
    bounds = INDICATOR_BOUNDS.get(name)
    if not bounds:
        return value

    min_val, max_val = bounds

    # Check minimum bound
    if min_val is not None and value < min_val:
        deviation = abs(value - min_val)
        # Use absolute tolerance for values near zero
        tolerance = max(abs(min_val) * 0.01, 0.01)

        if deviation > tolerance:
            log.error(f"{name} value {value} far below min {min_val}")
            raise ValueError(f"Invalid {name}: {value} (min: {min_val})")

        log.warning(f"Clamping {name} from {value:.6f} to {min_val}")
        return min_val

    # Check maximum bound
    if max_val is not None and value > max_val:
        deviation = abs(value - max_val)
        tolerance = max(abs(max_val) * 0.01, 0.01)

        if deviation > tolerance:
            log.error(f"{name} value {value} far above max {max_val}")
            raise ValueError(f"Invalid {name}: {value} (max: {max_val})")

        log.warning(f"Clamping {name} from {value:.6f} to {max_val}")
        return max_val

    return value


def _to_decimal(value: float | None, precision: int = 4) -> Decimal | None:
    """Convert float to Decimal with specified precision."""
    if value is None or math.isnan(value) or math.isinf(value):
        return None
    return Decimal(str(round(value, precision)))


def _to_int(value: float | None) -> int | None:
    """Convert float to int."""
    if value is None or math.isnan(value) or math.isinf(value):
        return None
    return int(round(value))


def calculate_indicators_for_ticker(
    ticker: str,
    prices: list[dict[str, Any]],
    log: logging.Logger | None = None,
) -> list[TechnicalIndicators]:
    """Calculate all 20 technical indicators for one ticker.

    Args:
        ticker: Stock symbol
        prices: List of price dicts with keys: date, open, high, low, close, volume
                Must be sorted by date ascending
        log: Logger instance

    Returns:
        List of TechnicalIndicators, one per date in prices
    """
    log = log or logger

    if talib is None:
        raise ImportError(
            "ta-lib is required for technical indicator calculation. "
            "Install with: pip install TA-Lib (requires C library)"
        )

    if not prices:
        return []

    # Convert to numpy arrays
    dates = [p["date"] for p in prices]
    # open_prices reserved for future indicators (e.g., candlestick patterns)
    _open_prices = np.array([float(p["open"]) for p in prices], dtype=np.float64)  # noqa: F841
    high_prices = np.array([float(p["high"]) for p in prices], dtype=np.float64)
    low_prices = np.array([float(p["low"]) for p in prices], dtype=np.float64)
    close_prices = np.array([float(p["close"]) for p in prices], dtype=np.float64)
    volumes = np.array([float(p["volume"]) for p in prices], dtype=np.float64)

    # Calculate all indicators
    # Trend
    sma_5 = talib.SMA(close_prices, timeperiod=5)
    sma_10 = talib.SMA(close_prices, timeperiod=10)
    sma_20 = talib.SMA(close_prices, timeperiod=20)
    sma_50 = talib.SMA(close_prices, timeperiod=50)
    ema_12 = talib.EMA(close_prices, timeperiod=12)
    ema_26 = talib.EMA(close_prices, timeperiod=26)
    ema_50 = talib.EMA(close_prices, timeperiod=50)

    # VWAP (cumulative - we calculate a rolling approximation)
    # True VWAP resets daily, but for daily data we use cumulative typical price * volume
    typical_price = (high_prices + low_prices + close_prices) / 3
    cum_tp_vol = np.cumsum(typical_price * volumes)
    cum_vol = np.cumsum(volumes)
    # Avoid division by zero
    with np.errstate(divide="ignore", invalid="ignore"):
        vwap = np.where(cum_vol > 0, cum_tp_vol / cum_vol, np.nan)

    # Momentum
    rsi_14 = talib.RSI(close_prices, timeperiod=14)
    rsi_21 = talib.RSI(close_prices, timeperiod=21)
    macd_line, macd_signal, macd_hist = talib.MACD(
        close_prices, fastperiod=12, slowperiod=26, signalperiod=9
    )

    # Volatility
    bb_upper, bb_middle, bb_lower = talib.BBANDS(
        close_prices, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0
    )
    atr_14 = talib.ATR(high_prices, low_prices, close_prices, timeperiod=14)

    # Volume
    obv = talib.OBV(close_prices, volumes)
    volume_sma_20 = talib.SMA(volumes, timeperiod=20)

    # Volume ratio (today / 20-day avg)
    with np.errstate(divide="ignore", invalid="ignore"):
        volume_ratio = np.where(volume_sma_20 > 0, volumes / volume_sma_20, np.nan)

    # Build results
    results: list[TechnicalIndicators] = []

    for i, price_date in enumerate(dates):
        # Convert date if it's a string
        if isinstance(price_date, str):
            price_date = date.fromisoformat(price_date)

        try:
            indicators = TechnicalIndicators(
                ticker=ticker,
                date=price_date,
                # Trend
                sma_5=_to_decimal(validate_indicator("sma_5", sma_5[i], log)),
                sma_10=_to_decimal(validate_indicator("sma_10", sma_10[i], log)),
                sma_20=_to_decimal(validate_indicator("sma_20", sma_20[i], log)),
                sma_50=_to_decimal(validate_indicator("sma_50", sma_50[i], log)),
                ema_12=_to_decimal(validate_indicator("ema_12", ema_12[i], log)),
                ema_26=_to_decimal(validate_indicator("ema_26", ema_26[i], log)),
                ema_50=_to_decimal(validate_indicator("ema_50", ema_50[i], log)),
                vwap=_to_decimal(validate_indicator("vwap", vwap[i], log)),
                # Momentum
                rsi_14=_to_decimal(validate_indicator("rsi_14", rsi_14[i], log), precision=6),
                rsi_21=_to_decimal(validate_indicator("rsi_21", rsi_21[i], log), precision=6),
                macd_line=_to_decimal(validate_indicator("macd_line", macd_line[i], log)),
                macd_signal=_to_decimal(validate_indicator("macd_signal", macd_signal[i], log)),
                macd_histogram=_to_decimal(validate_indicator("macd_histogram", macd_hist[i], log)),
                # Volatility
                bb_upper=_to_decimal(validate_indicator("bb_upper", bb_upper[i], log)),
                bb_middle=_to_decimal(validate_indicator("bb_middle", bb_middle[i], log)),
                bb_lower=_to_decimal(validate_indicator("bb_lower", bb_lower[i], log)),
                atr_14=_to_decimal(validate_indicator("atr_14", atr_14[i], log)),
                # Volume
                obv=_to_int(validate_indicator("obv", obv[i], log)),
                volume_sma_20=_to_int(validate_indicator("volume_sma_20", volume_sma_20[i], log)),
                volume_ratio=_to_decimal(
                    validate_indicator("volume_ratio", volume_ratio[i], log), precision=6
                ),
            )
            results.append(indicators)
        except ValueError as e:
            log.error(f"Validation error for {ticker} on {price_date}: {e}")
            # Skip this date if validation fails
            continue

    return results


def get_required_lookback_days() -> int:
    """Get the number of calendar days needed for lookback.

    Returns calendar days (trading days * 1.4 for weekends/holidays).
    Based on longest indicator period (SMA-50 = 50 trading days).
    """
    max_period = max(MIN_PERIODS.values())
    return int(max_period * 1.5)  # ~75 calendar days for 50 trading days
