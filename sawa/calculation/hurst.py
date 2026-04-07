"""Hurst exponent estimation via Detrended Fluctuation Analysis (DFA).

DFA is preferred over classical R/S (rescaled range) analysis for financial
return series because it handles non-stationarity better. R/S analysis assumes
the series is stationary and can produce biased estimates when local trends or
level shifts are present -- both common in financial data. DFA explicitly
detrends within each window, making the exponent estimate robust to
polynomial trends embedded in the price series.

Interpretation of the Hurst exponent H:
    H < 0.5  -> anti-persistent / mean-reverting (range-bound)
    H ~ 0.5  -> random walk (no memory)
    H > 0.5  -> persistent / trending

Also provides ADX (Average Directional Index) and volatility-of-volatility
helpers used by the stock-character classification pipeline.
"""

import logging

import numpy as np

from sawa.calculation.stock_character_config import (
    DFA_MAX_SCALE_RATIO,
    DFA_MIN_SCALE,
    DFA_NUM_SCALES,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DFA core
# ---------------------------------------------------------------------------


def compute_dfa(
    returns: np.ndarray,
    min_scale: int = DFA_MIN_SCALE,
    max_scale_ratio: float = DFA_MAX_SCALE_RATIO,
    num_scales: int = DFA_NUM_SCALES,
) -> tuple[float, float]:
    """Compute Hurst exponent via Detrended Fluctuation Analysis.

    Args:
        returns: 1-D array of log returns.
        min_scale: Minimum box size (number of observations per window).
        max_scale_ratio: Maximum scale expressed as a fraction of the series
            length.
        num_scales: Number of log-spaced scales to evaluate between
            *min_scale* and *max_scale*.

    Returns:
        ``(hurst_exponent, r_squared)`` where *r_squared* measures the
        quality of the log-log linear fit.

    Raises:
        ValueError: If *returns* has fewer than ``min_scale * 4`` elements.
    """
    returns = np.asarray(returns, dtype=np.float64)
    n = len(returns)

    if n < min_scale * 4:
        raise ValueError(
            f"Series too short ({n} elements) for DFA with min_scale={min_scale}. "
            f"Need at least {min_scale * 4}."
        )

    # Guard against constant / zero-variance series
    if np.nanstd(returns) == 0.0:
        logger.warning("Zero-variance return series; returning H=0.5, R²=0.0")
        return (0.5, 0.0)

    # Step 1: build the cumulative profile
    mean_x = np.nanmean(returns)
    profile = np.cumsum(returns - mean_x)

    # Step 2: determine scales
    max_scale = int(n * max_scale_ratio)
    if max_scale < min_scale:
        max_scale = min_scale
    scales = np.unique(
        np.logspace(np.log10(min_scale), np.log10(max_scale), num=num_scales).astype(
            int
        )
    )
    # Ensure every scale is at least min_scale
    scales = scales[scales >= min_scale]

    if len(scales) < 2:
        logger.warning("Not enough distinct scales; returning H=0.5, R²=0.0")
        return (0.5, 0.0)

    # Step 3: compute fluctuation function F(n) for each scale
    fluctuations = np.empty(len(scales), dtype=np.float64)

    for idx, scale in enumerate(scales):
        num_windows = n // scale
        if num_windows == 0:
            fluctuations[idx] = np.nan
            continue

        rms_values = np.empty(num_windows, dtype=np.float64)
        x_range = np.arange(scale, dtype=np.float64)

        for w in range(num_windows):
            segment = profile[w * scale : (w + 1) * scale]
            # Linear detrend (polynomial order 1)
            coeffs = np.polyfit(x_range, segment, 1)
            trend = np.polyval(coeffs, x_range)
            residuals = segment - trend
            rms_values[w] = np.sqrt(np.mean(residuals**2))

        fluctuations[idx] = np.mean(rms_values)

    # Remove any NaN entries
    valid = ~np.isnan(fluctuations)
    scales = scales[valid]
    fluctuations = fluctuations[valid]

    if len(scales) < 2:
        logger.warning("All fluctuation values are NaN; returning H=0.5, R²=0.0")
        return (0.5, 0.0)

    # Remove zero-fluctuation entries (can happen for very small constant segments)
    nonzero = fluctuations > 0
    scales = scales[nonzero]
    fluctuations = fluctuations[nonzero]

    if len(scales) < 2:
        logger.warning("All fluctuation values are zero; returning H=0.5, R²=0.0")
        return (0.5, 0.0)

    # Step 4: log-log linear fit  -> slope = Hurst exponent
    log_scales = np.log(scales.astype(np.float64))
    log_fluct = np.log(fluctuations)

    coeffs = np.polyfit(log_scales, log_fluct, 1)
    hurst = float(coeffs[0])

    # R² of the fit
    predicted = np.polyval(coeffs, log_scales)
    ss_res = np.sum((log_fluct - predicted) ** 2)
    ss_tot = np.sum((log_fluct - np.mean(log_fluct)) ** 2)
    r_squared = float(1.0 - ss_res / ss_tot) if ss_tot > 0 else 0.0

    logger.debug("DFA Hurst=%.4f  R²=%.4f  (n=%d, scales=%d)", hurst, r_squared, n, len(scales))
    return (hurst, r_squared)


# ---------------------------------------------------------------------------
# Multi-window Hurst
# ---------------------------------------------------------------------------


def compute_hurst_for_windows(
    close_prices: np.ndarray,
    windows: list[int],
) -> list[tuple[float, float]]:
    """Compute Hurst exponent over multiple trailing windows.

    For each window size the function takes the most recent *window* closing
    prices, converts them to log returns, and runs DFA.

    Args:
        close_prices: Array of closing prices (full history, oldest first).
        windows: List of window sizes in trading days,
            e.g. ``[756, 504, 252]``.

    Returns:
        List of ``(hurst, r_squared)`` tuples, one per window.
        Returns ``(nan, nan)`` for a window if there is not enough data.
    """
    close_prices = np.asarray(close_prices, dtype=np.float64)
    results: list[tuple[float, float]] = []

    for w in windows:
        if len(close_prices) < w:
            logger.info(
                "Not enough data for %d-day window (have %d prices)", w, len(close_prices)
            )
            results.append((float("nan"), float("nan")))
            continue

        window_prices = close_prices[-w:]
        log_returns = np.log(window_prices[1:] / window_prices[:-1])

        # Guard against NaN / inf in returns (e.g. zero prices)
        if not np.all(np.isfinite(log_returns)):
            logger.warning("Non-finite log returns in %d-day window; skipping", w)
            results.append((float("nan"), float("nan")))
            continue

        try:
            hurst, r2 = compute_dfa(log_returns)
            results.append((hurst, r2))
        except ValueError as exc:
            logger.warning("DFA failed for %d-day window: %s", w, exc)
            results.append((float("nan"), float("nan")))

    return results


# ---------------------------------------------------------------------------
# ADX (Average Directional Index) — pure numpy, Wilder's smoothing
# ---------------------------------------------------------------------------


def compute_adx(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    period: int = 20,
) -> np.ndarray:
    """Compute Average Directional Index (ADX).

    Pure numpy implementation using Wilder's smoothing (no ta-lib dependency).

    Args:
        high: Array of high prices.
        low: Array of low prices.
        close: Array of closing prices.
        period: ADX look-back period (default 20).

    Returns:
        Array of ADX values with the same length as the input arrays.
        The first ``period * 2`` values will be NaN because ADX needs
        *period* bars for the initial DI smoothing and another *period*
        bars for the ADX smoothing itself.
    """
    high = np.asarray(high, dtype=np.float64)
    low = np.asarray(low, dtype=np.float64)
    close = np.asarray(close, dtype=np.float64)
    n = len(high)

    adx = np.full(n, np.nan, dtype=np.float64)

    if n < period * 2 + 1:
        logger.warning(
            "Not enough bars (%d) for ADX with period %d (need %d)",
            n,
            period,
            period * 2 + 1,
        )
        return adx

    # --- True Range, +DM, -DM (bar-by-bar) ---
    tr = np.empty(n, dtype=np.float64)
    plus_dm = np.empty(n, dtype=np.float64)
    minus_dm = np.empty(n, dtype=np.float64)

    tr[0] = np.nan
    plus_dm[0] = np.nan
    minus_dm[0] = np.nan

    for i in range(1, n):
        h_l = high[i] - low[i]
        h_cp = abs(high[i] - close[i - 1])
        l_cp = abs(low[i] - close[i - 1])
        tr[i] = max(h_l, h_cp, l_cp)

        up_move = high[i] - high[i - 1]
        down_move = low[i - 1] - low[i]

        plus_dm[i] = up_move if (up_move > down_move and up_move > 0) else 0.0
        minus_dm[i] = down_move if (down_move > up_move and down_move > 0) else 0.0

    # --- Wilder's smoothing for ATR, +DM14, -DM14 ---
    # First value: simple sum of the first *period* bars (bars 1..period)
    atr_smooth = np.sum(tr[1 : period + 1])
    plus_dm_smooth = np.sum(plus_dm[1 : period + 1])
    minus_dm_smooth = np.sum(minus_dm[1 : period + 1])

    # Arrays to hold smoothed +DI and -DI (and DX)
    plus_di = np.full(n, np.nan, dtype=np.float64)
    minus_di = np.full(n, np.nan, dtype=np.float64)
    dx = np.full(n, np.nan, dtype=np.float64)

    # First smoothed DI at index *period*
    idx = period  # index where we have the first smoothed value
    if atr_smooth != 0.0:
        plus_di[idx] = 100.0 * plus_dm_smooth / atr_smooth
        minus_di[idx] = 100.0 * minus_dm_smooth / atr_smooth
    else:
        plus_di[idx] = 0.0
        minus_di[idx] = 0.0

    di_sum = plus_di[idx] + minus_di[idx]
    dx[idx] = 100.0 * abs(plus_di[idx] - minus_di[idx]) / di_sum if di_sum != 0.0 else 0.0

    # Continue Wilder's smoothing for subsequent bars
    for i in range(period + 1, n):
        atr_smooth = atr_smooth - atr_smooth / period + tr[i]
        plus_dm_smooth = plus_dm_smooth - plus_dm_smooth / period + plus_dm[i]
        minus_dm_smooth = minus_dm_smooth - minus_dm_smooth / period + minus_dm[i]

        if atr_smooth != 0.0:
            plus_di[i] = 100.0 * plus_dm_smooth / atr_smooth
            minus_di[i] = 100.0 * minus_dm_smooth / atr_smooth
        else:
            plus_di[i] = 0.0
            minus_di[i] = 0.0

        di_sum = plus_di[i] + minus_di[i]
        dx[i] = 100.0 * abs(plus_di[i] - minus_di[i]) / di_sum if di_sum != 0.0 else 0.0

    # --- Wilder's smoothing for ADX itself ---
    # First ADX = simple average of DX over the next *period* DX values
    first_adx_idx = period * 2
    if first_adx_idx >= n:
        return adx

    adx_val = np.nanmean(dx[period : period * 2 + 1])
    adx[first_adx_idx] = adx_val

    for i in range(first_adx_idx + 1, n):
        adx_val = (adx_val * (period - 1) + dx[i]) / period
        adx[i] = adx_val

    return adx


# ---------------------------------------------------------------------------
# Volatility-of-volatility
# ---------------------------------------------------------------------------


def compute_vol_of_vol(
    close_prices: np.ndarray,
    rolling_window: int = 30,
) -> float:
    """Compute volatility-of-volatility (dispersion of rolling realised vol).

    ``vol_of_vol = std(rolling_vol) / mean(rolling_vol)``

    where *rolling_vol* is the standard deviation of daily log returns
    computed over a rolling window.

    Args:
        close_prices: Array of closing prices (oldest first).
        rolling_window: Window size for the rolling volatility estimate.

    Returns:
        Coefficient of variation of rolling volatility.  Returns ``nan``
        if there is insufficient data.
    """
    close_prices = np.asarray(close_prices, dtype=np.float64)

    if len(close_prices) < rolling_window + 2:
        logger.warning(
            "Not enough prices (%d) for vol-of-vol with window %d",
            len(close_prices),
            rolling_window,
        )
        return float("nan")

    log_returns = np.log(close_prices[1:] / close_prices[:-1])

    if not np.all(np.isfinite(log_returns)):
        logger.warning("Non-finite log returns in vol-of-vol calculation")
        return float("nan")

    # Rolling standard deviation of log returns
    num_windows = len(log_returns) - rolling_window + 1
    rolling_vol = np.empty(num_windows, dtype=np.float64)
    for i in range(num_windows):
        rolling_vol[i] = np.std(log_returns[i : i + rolling_window], ddof=1)

    mean_vol = np.mean(rolling_vol)
    if mean_vol == 0.0:
        logger.warning("Mean rolling vol is zero; returning nan")
        return float("nan")

    vol_of_vol = float(np.std(rolling_vol, ddof=1) / mean_vol)
    logger.debug("Vol-of-vol=%.4f (window=%d)", vol_of_vol, rolling_window)
    return vol_of_vol
