"""Stage 2 stock character baseline computation engine.

Computes "what normal looks like" for a classified stock over its full
price history.  This is a stateless per-stock function that takes price
data and a Stage 1 classification, then returns a CharacterBaseline
domain object with all applicable fields populated.
"""

import logging
import math
from datetime import date
from decimal import Decimal
from typing import cast

import numpy as np
from statsmodels.api import OLS, add_constant  # type: ignore[import-not-found,import-untyped]

from sawa.calculation.stock_character import _extract_ohlcv_arrays, _to_decimal
from sawa.calculation.stock_character_config import (
    CORRELATION_WINDOW,
    HVN_VOLUME_PERCENTILE,
    LVN_VOLUME_PERCENTILE,
    RANGE_HIGH_PERCENTILE,
    RANGE_LOW_PERCENTILE,
    SMA_BOUNCE_CONFIRM_DAYS,
    SMA_BOUNCE_MIN_MOVE_PCT,
    SMA_TOUCH_PROXIMITY_PCT,
    VOLUME_PROFILE_BUCKETS,
)
from sawa.domain.stock_character import CharacterBaseline, CharacterClassification

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _compute_atr(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    period: int = 14,
) -> np.ndarray:
    """Compute Average True Range series using pure numpy.

    Returns an array the same length as *close* with the first
    ``period`` values set to NaN (not enough data for rolling mean).
    """
    prev_close = np.empty_like(close)
    prev_close[0] = np.nan
    prev_close[1:] = close[:-1]

    tr = np.maximum(
        high - low,
        np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)),
    )

    # Rolling mean of true range
    atr = np.full_like(tr, np.nan)
    cumsum = np.nancumsum(tr)
    atr[period - 1 :] = (cumsum[period - 1 :] - np.concatenate([[0], cumsum[:-period]])) / period
    return cast(np.ndarray, atr)


def compute_recent_atr(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    window: int,
) -> float | None:
    """Mean true range over the most recent *window* bars (``recent_ATR_10d``).

    Shared by Stage 3 (detection) and Stage 4 (scorecard) so the resulting
    ``atr_ratio`` is identical in both -- COMPRESSION/EXPANSION flags and the
    scorecard must agree.  True range uses each bar's *actual* prior close
    (the bar before the window is consulted when available), matching the
    canonical TR used by :func:`_compute_atr`.

    Returns ``None`` when fewer than 2 bars are available.
    """
    n = len(close)
    if n < 2:
        return None

    # Use up to `window` recent bars, but the previous close for the first
    # bar in the window is taken from just before the window when it exists.
    span = min(window, n - 1)
    recent_high = high[-span:]
    recent_low = low[-span:]
    prev_close = close[-(span + 1) : -1]

    tr = np.maximum(
        recent_high - recent_low,
        np.maximum(
            np.abs(recent_high - prev_close),
            np.abs(recent_low - prev_close),
        ),
    )
    if len(tr) == 0 or np.all(np.isnan(tr)):
        return None
    return float(np.nanmean(tr))


def _rolling_mean(arr: np.ndarray, window: int) -> np.ndarray:
    """Simple rolling mean via cumulative sum.  First ``window-1`` values are NaN."""
    out = np.full_like(arr, np.nan, dtype=np.float64)
    cumsum = np.nancumsum(arr)
    out[window - 1 :] = (
        cumsum[window - 1 :] - np.concatenate([[0.0], cumsum[: len(arr) - window]])
    ) / window
    return out


def _rolling_correlation(
    x: np.ndarray,
    y: np.ndarray,
    window: int,
) -> np.ndarray:
    """Rolling Pearson correlation between *x* and *y*.

    Returns an array with NaN for the first ``window-1`` elements.
    """
    n = len(x)
    corr = np.full(n, np.nan, dtype=np.float64)

    if n < window:
        return corr

    for i in range(window - 1, n):
        xw = x[i - window + 1 : i + 1]
        yw = y[i - window + 1 : i + 1]
        mask = np.isfinite(xw) & np.isfinite(yw)
        if mask.sum() < window // 2:
            continue
        xm = xw[mask]
        ym = yw[mask]
        if np.std(xm) == 0 or np.std(ym) == 0:
            continue
        corr[i] = np.corrcoef(xm, ym)[0, 1]
    return corr


def _align_returns(
    stock_prices: list[dict],
    bench_prices: list[dict],
) -> tuple[np.ndarray, np.ndarray]:
    """Inner-join stock and benchmark on date, return aligned daily return arrays."""
    bench_map: dict[str, float] = {}
    for p in bench_prices:
        d = p.get("date")
        c = p.get("close")
        if d is not None and c is not None:
            bench_map[str(d)] = float(c)

    aligned_stock: list[float] = []
    aligned_bench: list[float] = []
    prev_stock: float | None = None
    prev_bench: float | None = None

    for p in stock_prices:
        d = str(p.get("date"))
        c = p.get("close")
        if c is None:
            prev_stock = None
            continue
        sc = float(c)
        bc = bench_map.get(d)
        if bc is None:
            prev_stock = sc
            continue
        if (
            prev_stock is not None
            and prev_bench is not None
            and prev_stock != 0
            and prev_bench != 0
        ):
            aligned_stock.append(sc / prev_stock - 1.0)
            aligned_bench.append(bc / prev_bench - 1.0)
        prev_stock = sc
        prev_bench = bc

    return np.array(aligned_stock, dtype=np.float64), np.array(aligned_bench, dtype=np.float64)


def _compute_benchmark_correlation(
    stock_prices: list[dict],
    bench_prices: list[dict],
    window: int,
) -> tuple[float, float]:
    """Compute rolling correlation stats between stock and benchmark.

    Returns:
        ``(mean, std)`` of the rolling correlation series.
        Both are NaN if insufficient data.
    """
    if not bench_prices:
        return float("nan"), float("nan")

    stock_ret, bench_ret = _align_returns(stock_prices, bench_prices)

    if len(stock_ret) < window:
        return float("nan"), float("nan")

    rc = _rolling_correlation(stock_ret, bench_ret, window)
    valid = rc[np.isfinite(rc)]

    if len(valid) == 0:
        return float("nan"), float("nan")

    return float(np.mean(valid)), float(np.std(valid))


def _compute_volume_profile(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    volume: np.ndarray,
    n_buckets: int,
) -> tuple[tuple[Decimal, ...], tuple[Decimal, ...]]:
    """Compute HVN and LVN levels from a volume-at-price profile.

    Returns:
        ``(hvn_levels, lvn_levels)`` as tuples of Decimals.
    """
    typical = (high + low + close) / 3.0
    finite_mask = np.isfinite(typical) & np.isfinite(volume) & (volume > 0)
    typical = typical[finite_mask]
    vol = volume[finite_mask]

    if len(typical) < 2:
        return (), ()

    price_min = float(np.min(typical))
    price_max = float(np.max(typical))
    if price_max == price_min:
        return (), ()

    bucket_width = (price_max - price_min) / n_buckets
    bucket_volumes = np.zeros(n_buckets, dtype=np.float64)
    bucket_midpoints = np.array(
        [price_min + (i + 0.5) * bucket_width for i in range(n_buckets)],
        dtype=np.float64,
    )

    # Assign each day's volume to its bucket
    indices = np.clip(
        ((typical - price_min) / bucket_width).astype(int),
        0,
        n_buckets - 1,
    )
    for idx, v in zip(indices, vol):
        bucket_volumes[idx] += v

    total_vol = bucket_volumes.sum()
    if total_vol == 0:
        return (), ()

    normalized = bucket_volumes / total_vol

    hvn_threshold = np.percentile(normalized[normalized > 0], HVN_VOLUME_PERCENTILE)
    lvn_threshold = np.percentile(normalized[normalized > 0], LVN_VOLUME_PERCENTILE)

    hvn_candidates = tuple(
        _to_decimal(float(bucket_midpoints[i]), 2)
        for i in range(n_buckets)
        if normalized[i] >= hvn_threshold
    )
    lvn_candidates = tuple(
        _to_decimal(float(bucket_midpoints[i]), 2)
        for i in range(n_buckets)
        if 0 < normalized[i] <= lvn_threshold
    )

    # Filter out Nones (shouldn't happen but be safe)
    hvn_levels: tuple[Decimal, ...] = tuple(v for v in hvn_candidates if v is not None)
    lvn_levels: tuple[Decimal, ...] = tuple(v for v in lvn_candidates if v is not None)

    return hvn_levels, lvn_levels


def _compute_typical_cycle_days(
    close: np.ndarray,
    order: int = 10,
) -> float | None:
    """Estimate typical cycle length from peak/trough spacing.

    Uses a simple local extrema detection: a point is a local max (min) if
    it is the highest (lowest) within *order* bars on each side.

    Returns the average distance between consecutive extremes, or None if
    fewer than 3 extremes are found.
    """
    n = len(close)
    if n < 2 * order + 1:
        return None

    extremes: list[int] = []
    for i in range(order, n - order):
        window = close[i - order : i + order + 1]
        if close[i] == np.max(window) or close[i] == np.min(window):
            # Avoid double-counting adjacent extremes
            if not extremes or i - extremes[-1] >= order:
                extremes.append(i)

    if len(extremes) < 3:
        return None

    diffs = np.diff(extremes)
    return float(np.mean(diffs))


def _compute_ols_regression(
    close: np.ndarray,
) -> tuple[float, float, float, float, float, float]:
    """Fit OLS on log-prices and return regression statistics.

    Returns:
        ``(slope, intercept, r2, residuals_std, residuals_2std, expected_price_today)``
        All NaN on failure.
    """
    nan_result = (float("nan"),) * 6

    log_prices = np.log(close)
    if not np.all(np.isfinite(log_prices)):
        return nan_result  # type: ignore[return-value]

    design_matrix = add_constant(np.arange(len(log_prices)))
    try:
        model = OLS(log_prices, design_matrix).fit()
    except Exception:
        logger.debug("OLS regression failed", exc_info=True)
        return nan_result  # type: ignore[return-value]

    slope = float(model.params[1])
    intercept = float(model.params[0])
    r2 = float(model.rsquared)
    res_std = float(np.std(model.resid))
    res_2std = 2.0 * res_std
    expected = float(np.exp(intercept + slope * (len(close) - 1)))

    return slope, intercept, r2, res_std, res_2std, expected


def _compute_sma_adherence(
    close: np.ndarray,
    sma_period: int,
    touch_pct: float,
    bounce_days: int,
    bounce_min_pct: float,
) -> float:
    """Compute touch-and-bounce adherence ratio for a given SMA.

    Returns the fraction of SMA "touches" that resulted in a confirmed
    bounce.  Returns 0.0 if there are no touches.
    """
    if len(close) <= sma_period:
        return 0.0

    sma = _rolling_mean(close, sma_period)

    touches = 0
    bounces = 0
    i = sma_period  # start after SMA is valid
    while i < len(close):
        if np.isnan(sma[i]):
            i += 1
            continue
        # Check if price is within touch_pct of SMA
        if abs(close[i] - sma[i]) / sma[i] <= touch_pct:
            touches += 1
            # Look ahead bounce_days to see if price bounced
            future_idx = min(i + bounce_days, len(close) - 1)
            move_pct = abs(close[future_idx] - sma[i]) / sma[i]
            if move_pct >= bounce_min_pct:
                bounces += 1
            # Skip ahead to avoid counting same touch multiple times
            i += bounce_days
        else:
            i += 1

    return bounces / touches if touches > 0 else 0.0


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compute_baseline(
    ticker: str,
    prices: list[dict],
    classification: CharacterClassification,
    benchmark_prices: dict[str, list[dict]],
    run_date: date | None = None,
) -> CharacterBaseline:
    """Compute baseline statistics for a classified stock.

    Args:
        ticker: Stock symbol.
        prices: Full OHLCV history for this ticker (sorted by date ascending).
            Each dict has keys: date, open, high, low, close, volume.
        classification: Stage 1 classification result.
        benchmark_prices: Dict of benchmark ticker -> price list.
            e.g. ``{"SPY": [...], "GLD": [...], "TLT": [...]}``.
        run_date: Date to stamp (defaults to today).

    Returns:
        CharacterBaseline with all applicable fields populated.
        Fields that cannot be computed are left as None.
    """
    run_date = run_date or date.today()
    character = classification.character

    # Extract arrays
    high, low, close, volume = _extract_ohlcv_arrays(prices)

    # ------------------------------------------------------------------
    # 1. ATR baseline
    # ------------------------------------------------------------------
    atr_baseline_val: float | None = None
    atr_pct_baseline_val: float | None = None

    try:
        atr_series = _compute_atr(high, low, close, period=14)
        valid_atr = atr_series[np.isfinite(atr_series)]
        if len(valid_atr) > 0:
            atr_baseline_val = float(np.mean(valid_atr))
            # Normalized: mean of (atr / close) where both are valid
            valid_mask = np.isfinite(atr_series) & np.isfinite(close) & (close > 0)
            if np.any(valid_mask):
                atr_pct_baseline_val = float(np.mean(atr_series[valid_mask] / close[valid_mask]))
    except Exception:
        logger.debug("%s: ATR computation failed", ticker, exc_info=True)

    # ------------------------------------------------------------------
    # 2. Benchmark correlations
    # ------------------------------------------------------------------
    spy_corr_mean = float("nan")
    spy_corr_std = float("nan")
    gld_corr_mean = float("nan")
    tlt_corr_mean = float("nan")

    try:
        spy_prices = benchmark_prices.get("SPY", [])
        spy_corr_mean, spy_corr_std = _compute_benchmark_correlation(
            prices, spy_prices, CORRELATION_WINDOW
        )
    except Exception:
        logger.debug("%s: SPY correlation failed", ticker, exc_info=True)

    try:
        gld_prices = benchmark_prices.get("GLD", [])
        gld_corr_mean, _ = _compute_benchmark_correlation(
            prices, gld_prices, CORRELATION_WINDOW
        )
    except Exception:
        logger.debug("%s: GLD correlation failed", ticker, exc_info=True)

    try:
        tlt_prices = benchmark_prices.get("TLT", [])
        tlt_corr_mean, _ = _compute_benchmark_correlation(
            prices, tlt_prices, CORRELATION_WINDOW
        )
    except Exception:
        logger.debug("%s: TLT correlation failed", ticker, exc_info=True)

    # ------------------------------------------------------------------
    # 3. Volume SMA20
    # ------------------------------------------------------------------
    volume_sma20_val: int | None = None
    try:
        vol_sma = _rolling_mean(volume, 20)
        valid_vol_sma = vol_sma[np.isfinite(vol_sma)]
        if len(valid_vol_sma) > 0:
            volume_sma20_val = int(round(float(np.mean(valid_vol_sma))))
    except Exception:
        logger.debug("%s: volume SMA20 failed", ticker, exc_info=True)

    # ------------------------------------------------------------------
    # 4 & 5 & 6. Range-bound specific
    # ------------------------------------------------------------------
    range_high_val: float | None = None
    range_low_val: float | None = None
    range_midpoint_val: float | None = None
    hvn_levels: tuple[Decimal, ...] | None = None
    lvn_levels: tuple[Decimal, ...] | None = None
    typical_cycle: float | None = None

    if character == "range_bound":
        try:
            finite_close = close[np.isfinite(close)]
            if len(finite_close) > 0:
                range_high_val = float(np.percentile(finite_close, RANGE_HIGH_PERCENTILE))
                range_low_val = float(np.percentile(finite_close, RANGE_LOW_PERCENTILE))
                range_midpoint_val = (range_high_val + range_low_val) / 2.0
        except Exception:
            logger.debug("%s: range bounds failed", ticker, exc_info=True)

        try:
            hvn_levels, lvn_levels = _compute_volume_profile(
                high, low, close, volume, VOLUME_PROFILE_BUCKETS
            )
        except Exception:
            logger.debug("%s: volume profile failed", ticker, exc_info=True)
            hvn_levels = None
            lvn_levels = None

        try:
            finite_close = close[np.isfinite(close)]
            typical_cycle = _compute_typical_cycle_days(finite_close)
        except Exception:
            logger.debug("%s: cycle detection failed", ticker, exc_info=True)

    # ------------------------------------------------------------------
    # 7. Trending specific: OLS regression
    # ------------------------------------------------------------------
    reg_slope: float | None = None
    reg_intercept: float | None = None
    reg_r2: float | None = None
    reg_res_std: float | None = None
    reg_res_2std: float | None = None
    reg_expected: float | None = None

    if character == "trending":
        try:
            finite_close = close[np.isfinite(close)]
            if len(finite_close) > 1 and np.all(finite_close > 0):
                slope, intercept, r2, res_std, res_2std, expected = _compute_ols_regression(
                    finite_close
                )
                if not math.isnan(slope):
                    reg_slope = slope
                    reg_intercept = intercept
                    reg_r2 = r2
                    reg_res_std = res_std
                    reg_res_2std = res_2std
                    reg_expected = expected
        except Exception:
            logger.debug("%s: OLS regression failed", ticker, exc_info=True)

    # ------------------------------------------------------------------
    # 8. SMA adherence (all stocks)
    # ------------------------------------------------------------------
    sma_150_ratio: float | None = None
    sma_200_ratio: float | None = None

    try:
        finite_close = close[np.isfinite(close)]
        if len(finite_close) > 200:
            sma_150_ratio = _compute_sma_adherence(
                finite_close,
                150,
                SMA_TOUCH_PROXIMITY_PCT,
                SMA_BOUNCE_CONFIRM_DAYS,
                SMA_BOUNCE_MIN_MOVE_PCT,
            )
            sma_200_ratio = _compute_sma_adherence(
                finite_close,
                200,
                SMA_TOUCH_PROXIMITY_PCT,
                SMA_BOUNCE_CONFIRM_DAYS,
                SMA_BOUNCE_MIN_MOVE_PCT,
            )
        elif len(finite_close) > 150:
            sma_150_ratio = _compute_sma_adherence(
                finite_close,
                150,
                SMA_TOUCH_PROXIMITY_PCT,
                SMA_BOUNCE_CONFIRM_DAYS,
                SMA_BOUNCE_MIN_MOVE_PCT,
            )
    except Exception:
        logger.debug("%s: SMA adherence failed", ticker, exc_info=True)

    # ------------------------------------------------------------------
    # Assemble result
    # ------------------------------------------------------------------
    baseline = CharacterBaseline(
        ticker=ticker,
        run_date=run_date,
        character=character,
        # Common
        atr_baseline=_to_decimal(atr_baseline_val),
        atr_pct_baseline=_to_decimal(atr_pct_baseline_val),
        spy_corr_90d_mean=_to_decimal(spy_corr_mean),
        spy_corr_90d_std=_to_decimal(spy_corr_std),
        gld_corr_90d_mean=_to_decimal(gld_corr_mean),
        tlt_corr_90d_mean=_to_decimal(tlt_corr_mean),
        volume_sma20=volume_sma20_val,
        # Range-bound
        range_high=_to_decimal(range_high_val, 2),
        range_low=_to_decimal(range_low_val, 2),
        range_midpoint=_to_decimal(range_midpoint_val, 2),
        hvn_levels=hvn_levels if hvn_levels else None,
        lvn_levels=lvn_levels if lvn_levels else None,
        typical_cycle_days=_to_decimal(typical_cycle, 1),
        volume_profile_source="daily_approximation",
        # Trending
        regression_slope=_to_decimal(reg_slope, 8),
        regression_intercept=_to_decimal(reg_intercept, 8),
        regression_r2=_to_decimal(reg_r2),
        residuals_std=_to_decimal(reg_res_std),
        residuals_2std=_to_decimal(reg_res_2std),
        expected_price_today=_to_decimal(reg_expected, 2),
        # SMA adherence
        sma_150_adherence_ratio=_to_decimal(sma_150_ratio),
        sma_200_adherence_ratio=_to_decimal(sma_200_ratio),
    )

    logger.info(
        "%s: baseline computed | character=%s atr=%.4f atr_pct=%.4f spy_corr=%.4f vol_sma20=%s",
        ticker,
        character,
        atr_baseline_val or 0.0,
        atr_pct_baseline_val or 0.0,
        spy_corr_mean if not (spy_corr_mean != spy_corr_mean) else 0.0,  # NaN check
        volume_sma20_val,
    )

    return baseline
