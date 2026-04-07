"""Stage 1 stock character classification engine.

Classifies a single stock as range-bound, trending, or boom-bust based on
multi-window Hurst exponent analysis (via DFA) with secondary confirmation
from ADX, OLS regression R-squared, or volatility-of-volatility.

This module is stateless -- it takes price data in and returns a
CharacterClassification domain object (or None if unclassifiable).
"""

import logging
import math
from datetime import date
from decimal import Decimal

import numpy as np
from statsmodels.api import OLS, add_constant

from sawa.calculation.hurst import (
    compute_adx,
    compute_hurst_for_windows,
    compute_vol_of_vol,
)
from sawa.calculation.stock_character_config import (
    ADX_PERIOD,
    ADX_RANGE_MAX,
    HURST_RANGE_THRESHOLD,
    HURST_TREND_THRESHOLD,
    MIN_HISTORY_DAYS,
    REGRESSION_R2_MIN,
    VOL_OF_VOL_THRESHOLD,
    WINDOW_1YR,
    WINDOW_2YR,
    WINDOW_FULL,
)
from sawa.domain.stock_character import CharacterClassification

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _to_decimal(value: float | None, precision: int = 4) -> Decimal | None:
    """Convert float to Decimal with specified precision.

    Returns None for None, NaN, or infinite values.
    """
    if value is None or math.isnan(value) or math.isinf(value):
        return None
    return Decimal(str(round(value, precision)))


def _extract_ohlcv_arrays(
    prices: list[dict],
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Extract high, low, close, volume numpy arrays from price dicts.

    Converts None values to NaN so downstream numpy routines handle them
    gracefully.

    Returns:
        ``(high, low, close, volume)`` arrays of float64.
    """
    high = np.array(
        [float(p["high"]) if p.get("high") is not None else np.nan for p in prices],
        dtype=np.float64,
    )
    low = np.array(
        [float(p["low"]) if p.get("low") is not None else np.nan for p in prices],
        dtype=np.float64,
    )
    close = np.array(
        [float(p["close"]) if p.get("close") is not None else np.nan for p in prices],
        dtype=np.float64,
    )
    volume = np.array(
        [float(p["volume"]) if p.get("volume") is not None else np.nan for p in prices],
        dtype=np.float64,
    )
    return high, low, close, volume


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def _classify_hurst(hurst_values: list[float]) -> tuple[str | None, bool]:
    """Determine candidate character class from Hurst exponent values.

    A stock is only "inconsistent" (boom-bust) when windows genuinely disagree
    on direction — i.e. at least one window says range (< 0.45) AND another
    says trend (> 0.55).  When some windows show a clear signal and others sit
    in the random-walk noise band, the stock *leans* in that direction rather
    than being called inconsistent.

    Args:
        hurst_values: List of valid (non-NaN) Hurst exponents across windows.

    Returns:
        ``(candidate_class, unanimous)`` where *unanimous* is True when every
        window agrees on the direction (all below or all above threshold).
        This feeds into confidence scoring — unanimous agreement can reach
        HIGH, a lean caps at MEDIUM.
    """
    has_range = any(h < HURST_RANGE_THRESHOLD for h in hurst_values)
    has_trend = any(h > HURST_TREND_THRESHOLD for h in hurst_values)
    all_range = all(h < HURST_RANGE_THRESHOLD for h in hurst_values)
    all_trend = all(h > HURST_TREND_THRESHOLD for h in hurst_values)

    if all_range:
        return "range_bound", True
    if all_trend:
        return "trending", True

    # Genuinely inconsistent: some windows say range, others say trend
    if has_range and has_trend:
        return "boom_bust", True

    # Leaning: clear signal in some windows, noise band in others
    if has_range and not has_trend:
        return "range_bound", False
    if has_trend and not has_range:
        return "trending", False

    # All in random-walk zone [0.45, 0.55]
    return None, False


def _confirm_range_bound(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
) -> tuple[bool, float, str]:
    """Secondary confirmation for range-bound candidate via ADX.

    Returns:
        ``(confirmed, adx_avg, confidence)`` where *confidence* is
        ``'HIGH'``, ``'MEDIUM'``, or ``'NONE'`` (failed confirmation).
    """
    adx_values = compute_adx(high, low, close, period=ADX_PERIOD)
    adx_avg = float(np.nanmean(adx_values))

    if math.isnan(adx_avg):
        logger.debug("ADX avg is NaN; confirmation fails")
        return False, adx_avg, "NONE"

    logger.debug("Range-bound ADX avg=%.2f (threshold=%d)", adx_avg, ADX_RANGE_MAX)

    if adx_avg >= ADX_RANGE_MAX:
        return False, adx_avg, "NONE"
    if adx_avg < ADX_RANGE_MAX * 0.8:
        return True, adx_avg, "HIGH"
    return True, adx_avg, "MEDIUM"


def _confirm_trending(close_prices: np.ndarray) -> tuple[bool, float, str]:
    """Secondary confirmation for trending candidate via OLS regression R-squared.

    Fits ``log(close)`` against a linear time index and checks R-squared.

    Returns:
        ``(confirmed, r2, confidence)``
    """
    log_prices = np.log(close_prices)

    # Guard against non-finite log prices (e.g. zero or negative close)
    if not np.all(np.isfinite(log_prices)):
        logger.debug("Non-finite log prices; trending confirmation fails")
        return False, float("nan"), "NONE"

    x = add_constant(np.arange(len(log_prices)))
    model = OLS(log_prices, x).fit()
    r2 = float(model.rsquared)

    logger.debug("Trending OLS R²=%.4f (threshold=%.2f)", r2, REGRESSION_R2_MIN)

    if r2 < REGRESSION_R2_MIN:
        return False, r2, "NONE"
    if r2 > REGRESSION_R2_MIN + 0.1:
        return True, r2, "HIGH"
    return True, r2, "MEDIUM"


def _confirm_boom_bust(close_prices: np.ndarray) -> tuple[bool, float, str]:
    """Secondary confirmation for boom-bust candidate via vol-of-vol.

    Returns:
        ``(confirmed, vol_of_vol, confidence)``
    """
    vov = compute_vol_of_vol(close_prices)

    logger.debug("Boom-bust vol-of-vol=%.4f (threshold=%.2f)", vov, VOL_OF_VOL_THRESHOLD)

    if math.isnan(vov) or vov <= VOL_OF_VOL_THRESHOLD:
        return False, vov, "NONE"
    if vov > VOL_OF_VOL_THRESHOLD * 1.2:
        return True, vov, "HIGH"
    return True, vov, "MEDIUM"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def classify_stock(
    ticker: str,
    prices: list[dict],
    run_date: date | None = None,
) -> CharacterClassification | None:
    """Classify a single stock's behavioral character.

    Args:
        ticker: Stock symbol.
        prices: List of price dicts with keys ``date``, ``open``, ``high``,
                ``low``, ``close``, ``volume``.  Must be sorted by date
                ascending and should contain full available history.
        run_date: Date to stamp the classification (defaults to today).

    Returns:
        A :class:`CharacterClassification` if the stock is classifiable with
        HIGH or MEDIUM confidence, or ``None`` if unclassifiable.
    """
    run_date = run_date or date.today()

    # ------------------------------------------------------------------
    # Step 1: Validate data
    # ------------------------------------------------------------------
    if not prices:
        logger.info("%s: no price data provided", ticker)
        return None

    if len(prices) < MIN_HISTORY_DAYS:
        logger.info(
            "%s: insufficient history (%d days, need %d)",
            ticker,
            len(prices),
            MIN_HISTORY_DAYS,
        )
        return None

    survivorship_flag = len(prices) >= WINDOW_FULL

    high, low, close_prices, _volume = _extract_ohlcv_arrays(prices)

    # Drop leading/trailing NaN close prices
    if not np.all(np.isfinite(close_prices)):
        finite_mask = np.isfinite(close_prices)
        if not np.any(finite_mask):
            logger.info("%s: all close prices are NaN", ticker)
            return None
        # Keep only the longest contiguous run from the end (most recent data)
        last_valid = len(close_prices) - 1
        first_valid = 0
        for i in range(len(close_prices) - 1, -1, -1):
            if not finite_mask[i]:
                break
            first_valid = i
        close_prices = close_prices[first_valid : last_valid + 1]
        high = high[first_valid : last_valid + 1]
        low = low[first_valid : last_valid + 1]

        if len(close_prices) < MIN_HISTORY_DAYS:
            logger.info(
                "%s: insufficient finite close prices (%d after trimming)",
                ticker,
                len(close_prices),
            )
            return None

    # Guard against zero or negative prices which break log returns
    if np.any(close_prices <= 0):
        logger.info("%s: zero or negative close prices detected", ticker)
        return None

    # ------------------------------------------------------------------
    # Step 2: Compute Hurst exponents
    # ------------------------------------------------------------------
    windows = [WINDOW_FULL, WINDOW_2YR, WINDOW_1YR]
    hurst_results = compute_hurst_for_windows(close_prices, windows)

    # ------------------------------------------------------------------
    # Step 3: Classify based on Hurst consistency
    # ------------------------------------------------------------------
    valid_hursts = [(h, r2) for h, r2 in hurst_results if not math.isnan(h)]

    if len(valid_hursts) < 2:
        logger.info(
            "%s: only %d valid Hurst windows (need >= 2)",
            ticker,
            len(valid_hursts),
        )
        return None

    hurst_values = [h for h, _ in valid_hursts]
    candidate_class, unanimous = _classify_hurst(hurst_values)

    if candidate_class is None:
        logger.info(
            "%s: all Hurst values in random-walk zone %s",
            ticker,
            [f"{h:.4f}" for h in hurst_values],
        )
        return None

    # ------------------------------------------------------------------
    # Step 4: Secondary confirmation
    # ------------------------------------------------------------------
    adx_avg: float = float("nan")
    r2: float = float("nan")
    vol_of_vol: float = float("nan")

    if candidate_class == "range_bound":
        confirmed, adx_avg, confidence = _confirm_range_bound(high, low, close_prices)
    elif candidate_class == "trending":
        confirmed, r2, confidence = _confirm_trending(close_prices)
    else:  # boom_bust
        confirmed, vol_of_vol, confidence = _confirm_boom_bust(close_prices)

    if not confirmed:
        logger.info(
            "%s: %s candidate failed secondary confirmation",
            ticker,
            candidate_class,
        )
        return None

    # Non-unanimous Hurst agreement caps confidence at MEDIUM
    if not unanimous and confidence == "HIGH":
        confidence = "MEDIUM"

    # ------------------------------------------------------------------
    # Step 5 & 6: Build result
    # ------------------------------------------------------------------
    logger.info(
        "%s: classified as %s (%s%s) | hurst_3yr=%s hurst_2yr=%s hurst_1yr=%s",
        ticker,
        candidate_class,
        confidence,
        "" if unanimous else " [leaning]",
        f"{hurst_results[0][0]:.4f}" if not math.isnan(hurst_results[0][0]) else "nan",
        f"{hurst_results[1][0]:.4f}" if not math.isnan(hurst_results[1][0]) else "nan",
        f"{hurst_results[2][0]:.4f}" if not math.isnan(hurst_results[2][0]) else "nan",
    )

    return CharacterClassification(
        ticker=ticker,
        run_date=run_date,
        character=candidate_class,
        confidence=confidence,
        hurst_3yr=_to_decimal(hurst_results[0][0]),
        hurst_2yr=_to_decimal(hurst_results[1][0]),
        hurst_1yr=_to_decimal(hurst_results[2][0]),
        adx_avg=_to_decimal(adx_avg) if candidate_class == "range_bound" else None,
        regression_r2=_to_decimal(r2) if candidate_class == "trending" else None,
        vol_of_vol=_to_decimal(vol_of_vol) if candidate_class == "boom_bust" else None,
        survivorship_flag=survivorship_flag,
    )
