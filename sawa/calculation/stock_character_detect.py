"""Stage 3 atypical behavior detection engine.

Detects atypical behavior by comparing the recent window (last 10 days)
against the baseline computed in Stage 2.  Returns a list of CharacterFlag
instances describing every active deviation.

This module is stateless -- it takes price data, a classification, and a
baseline and returns flags.
"""

import logging
import math
from datetime import date

import numpy as np

from sawa.calculation.stock_character import _extract_ohlcv_arrays, _to_decimal
from sawa.calculation.stock_character_baseline import compute_recent_atr
from sawa.calculation.stock_character_config import (
    COMPRESSION_RATIO,
    DECORRELATION_STDDEV,
    EXPANSION_RATIO,
    EXTREMUM_HIGH_PCT,
    EXTREMUM_LOW_PCT,
    HVN_PROXIMITY_PCT,
    RECENT_WINDOW_DAYS,
    SIGMA_THRESHOLD,
    SMA_ADHERENCE_MIN_RATIO,
    SMA_PROXIMITY_PCT,
    VOLUME_DROUGHT_RATIO,
    VOLUME_SPIKE_RATIO,
)
from sawa.domain.stock_character import (
    CharacterBaseline,
    CharacterClassification,
    CharacterFlag,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_flag(
    ticker: str,
    run_date: date,
    flag_name: str,
    value: float,
    threshold: float,
) -> CharacterFlag:
    """Build a single CharacterFlag with safely-rounded Decimal values."""
    return CharacterFlag(
        ticker=ticker,
        run_date=run_date,
        flag=flag_name,
        value=_to_decimal(value, 6),
        threshold=_to_decimal(threshold, 6),
    )


def _percentileofscore(arr: np.ndarray, score: float) -> float:
    """Compute the percentile rank of *score* within *arr*.

    Uses the 'weak' method: percentage of values in *arr* that are <= *score*.
    Equivalent to ``scipy.stats.percentileofscore(arr, score, kind='weak')``.
    """
    if len(arr) == 0:
        return 0.0
    return float(np.sum(arr <= score) / len(arr) * 100.0)


def _align_returns_by_date(
    prices: list[dict],
    benchmark_prices: list[dict],
    window: int,
) -> tuple[np.ndarray, np.ndarray] | None:
    """Align recent *window*-day log-returns of stock and benchmark by date.

    Returns ``(stock_returns, benchmark_returns)`` arrays of matching length,
    or ``None`` if fewer than *window* overlapping dates exist.
    """
    # Build date-indexed close maps (last `window + 1` entries at most)
    stock_map: dict[date, float] = {}
    for p in prices[-(window + 20) :]:
        d = p["date"]
        c = p.get("close")
        if c is not None:
            stock_map[d] = float(c)

    bench_map: dict[date, float] = {}
    for p in benchmark_prices[-(window + 20) :]:
        d = p["date"]
        c = p.get("close")
        if c is not None:
            bench_map[d] = float(c)

    # Overlapping dates, sorted
    common = sorted(stock_map.keys() & bench_map.keys())
    if len(common) < window + 1:
        return None

    common = common[-(window + 1) :]

    stock_close = np.array([stock_map[d] for d in common])
    bench_close = np.array([bench_map[d] for d in common])

    stock_ret = np.diff(np.log(stock_close))
    bench_ret = np.diff(np.log(bench_close))

    return stock_ret, bench_ret


# ---------------------------------------------------------------------------
# Flag detectors
# ---------------------------------------------------------------------------


def _detect_range_bound_flags(
    ticker: str,
    run_date: date,
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    baseline: CharacterBaseline,
    current_price: float,
    recent_atr: float,
) -> list[CharacterFlag]:
    """Flags specific to range-bound stocks."""
    flags: list[CharacterFlag] = []

    # 1. EXTREMUM_HIGH
    price_pct = _percentileofscore(close, current_price)
    if price_pct > EXTREMUM_HIGH_PCT:
        flags.append(
            _make_flag(ticker, run_date, "EXTREMUM_HIGH", price_pct, EXTREMUM_HIGH_PCT)
        )

    # 2. EXTREMUM_LOW
    if price_pct < EXTREMUM_LOW_PCT:
        flags.append(
            _make_flag(ticker, run_date, "EXTREMUM_LOW", price_pct, EXTREMUM_LOW_PCT)
        )

    # 3. COMPRESSION
    if baseline.atr_baseline and float(baseline.atr_baseline) > 0:
        atr_ratio = recent_atr / float(baseline.atr_baseline)
        if atr_ratio < COMPRESSION_RATIO:
            flags.append(
                _make_flag(ticker, run_date, "COMPRESSION", atr_ratio, COMPRESSION_RATIO)
            )

        # 4. EXPANSION
        if atr_ratio > EXPANSION_RATIO:
            flags.append(
                _make_flag(ticker, run_date, "EXPANSION", atr_ratio, EXPANSION_RATIO)
            )

    # 5. AT_HVN
    if baseline.hvn_levels:
        for level in baseline.hvn_levels:
            lf = float(level)
            if lf > 0 and abs(current_price - lf) / lf <= HVN_PROXIMITY_PCT:
                flags.append(
                    _make_flag(ticker, run_date, "AT_HVN", current_price, lf)
                )
                break

    # 6. IN_LVN
    if baseline.lvn_levels:
        for level in baseline.lvn_levels:
            lf = float(level)
            if lf > 0 and abs(current_price - lf) / lf <= HVN_PROXIMITY_PCT:
                flags.append(
                    _make_flag(ticker, run_date, "IN_LVN", current_price, lf)
                )
                break

    return flags


def _detect_trending_flags(
    ticker: str,
    run_date: date,
    close: np.ndarray,
    baseline: CharacterBaseline,
    current_price: float,
    recent_atr: float,
) -> list[CharacterFlag]:
    """Flags specific to trending stocks."""
    flags: list[CharacterFlag] = []

    # 7/8. ABOVE_2SIGMA / BELOW_2SIGMA
    if baseline.expected_price_today and baseline.residuals_std:
        expected = float(baseline.expected_price_today)
        residuals_std = float(baseline.residuals_std)
        if expected > 0 and residuals_std > 0:
            # Work in log space for consistency with the OLS
            log_current = np.log(current_price)
            log_expected = np.log(expected)
            sigma_dist = (log_current - log_expected) / residuals_std

            if sigma_dist > SIGMA_THRESHOLD:
                flags.append(
                    _make_flag(
                        ticker, run_date, "ABOVE_2SIGMA", sigma_dist, SIGMA_THRESHOLD
                    )
                )
            elif sigma_dist < -SIGMA_THRESHOLD:
                flags.append(
                    _make_flag(
                        ticker, run_date, "BELOW_2SIGMA", sigma_dist, -SIGMA_THRESHOLD
                    )
                )

    # 9. SLOPE_BREAK
    if baseline.regression_slope is not None and len(close) >= RECENT_WINDOW_DAYS:
        recent_log = np.log(close[-RECENT_WINDOW_DAYS:])
        if np.all(np.isfinite(recent_log)):
            x = np.arange(RECENT_WINDOW_DAYS)
            recent_slope = float(np.polyfit(x, recent_log, 1)[0])
            hist_slope = float(baseline.regression_slope)
            if (recent_slope > 0) != (hist_slope > 0):
                flags.append(
                    _make_flag(
                        ticker, run_date, "SLOPE_BREAK", recent_slope, hist_slope
                    )
                )

    # 10. VOL_SPIKE (same as EXPANSION for range stocks)
    if baseline.atr_baseline and float(baseline.atr_baseline) > 0:
        atr_ratio = recent_atr / float(baseline.atr_baseline)
        if atr_ratio > EXPANSION_RATIO:
            flags.append(
                _make_flag(ticker, run_date, "VOL_SPIKE", atr_ratio, EXPANSION_RATIO)
            )

    return flags


def _detect_common_flags(
    ticker: str,
    run_date: date,
    prices: list[dict],
    close: np.ndarray,
    volume: np.ndarray,
    baseline: CharacterBaseline,
    benchmark_prices: dict[str, list[dict]],
) -> list[CharacterFlag]:
    """Flags shared across all character types."""
    flags: list[CharacterFlag] = []

    # 11. DECORRELATION_SPY
    spy_prices = benchmark_prices.get("SPY", [])
    if (
        spy_prices
        and baseline.spy_corr_90d_mean is not None
        and baseline.spy_corr_90d_std is not None
    ):
        spy_corr_std = float(baseline.spy_corr_90d_std)
        if spy_corr_std > 0:
            aligned = _align_returns_by_date(
                prices, spy_prices, RECENT_WINDOW_DAYS
            )
            if aligned is not None:
                stock_ret, spy_ret = aligned
                # Correlation of recent returns
                if len(stock_ret) >= 2:
                    corr_matrix = np.corrcoef(stock_ret, spy_ret)
                    recent_corr = float(corr_matrix[0, 1])
                    if not math.isnan(recent_corr):
                        baseline_mean = float(baseline.spy_corr_90d_mean)
                        deviation = abs(recent_corr - baseline_mean) / spy_corr_std
                        if deviation > DECORRELATION_STDDEV:
                            flags.append(
                                _make_flag(
                                    ticker,
                                    run_date,
                                    "DECORRELATION_SPY",
                                    deviation,
                                    DECORRELATION_STDDEV,
                                )
                            )

    # 12/13. VOLUME_SPIKE / VOLUME_DROUGHT
    if baseline.volume_sma20 and baseline.volume_sma20 > 0:
        recent_vol = float(np.nanmean(volume[-RECENT_WINDOW_DAYS:]))
        vol_ratio = recent_vol / float(baseline.volume_sma20)

        if vol_ratio > VOLUME_SPIKE_RATIO:
            flags.append(
                _make_flag(
                    ticker, run_date, "VOLUME_SPIKE", vol_ratio, VOLUME_SPIKE_RATIO
                )
            )
        elif vol_ratio < VOLUME_DROUGHT_RATIO:
            flags.append(
                _make_flag(
                    ticker,
                    run_date,
                    "VOLUME_DROUGHT",
                    vol_ratio,
                    VOLUME_DROUGHT_RATIO,
                )
            )

    return flags


def _detect_sma_flags(
    ticker: str,
    run_date: date,
    close: np.ndarray,
    classification: CharacterClassification,
    baseline: CharacterBaseline,
    current_price: float,
) -> list[CharacterFlag]:
    """SMA proximity and crossover flags."""
    flags: list[CharacterFlag] = []

    # 14. AT_200SMA
    if len(close) >= 200:
        sma_200 = float(np.mean(close[-200:]))
        if (
            baseline.sma_200_adherence_ratio is not None
            and float(baseline.sma_200_adherence_ratio) >= SMA_ADHERENCE_MIN_RATIO
            and sma_200 > 0
        ):
            if abs(current_price - sma_200) / sma_200 <= SMA_PROXIMITY_PCT:
                flags.append(
                    _make_flag(ticker, run_date, "AT_200SMA", current_price, sma_200)
                )

        # 16. BELOW_200SMA -- trending stocks that historically stay above 200 SMA
        if classification.character == "trending":
            # Compute SMA200 for each of the last RECENT_WINDOW_DAYS days
            n = len(close)
            recent_closes = close[-RECENT_WINDOW_DAYS:]
            recent_sma200s = np.array(
                [
                    float(np.mean(close[max(0, i - 199) : i + 1]))
                    for i in range(n - RECENT_WINDOW_DAYS, n)
                ]
            )
            any_below = bool(np.any(recent_closes < recent_sma200s))

            if any_below:
                # Check that price was above SMA200 just before the window
                pre_idx = n - RECENT_WINDOW_DAYS - 1
                if pre_idx >= 199:
                    sma200_before = float(
                        np.mean(close[pre_idx - 199 : pre_idx + 1])
                    )
                    if close[pre_idx] >= sma200_before:
                        flags.append(
                            _make_flag(
                                ticker,
                                run_date,
                                "BELOW_200SMA",
                                float(recent_closes[-1]),
                                float(recent_sma200s[-1]),
                            )
                        )

    # 15. AT_150SMA
    if len(close) >= 150:
        sma_150 = float(np.mean(close[-150:]))
        if (
            baseline.sma_150_adherence_ratio is not None
            and float(baseline.sma_150_adherence_ratio) >= SMA_ADHERENCE_MIN_RATIO
            and sma_150 > 0
        ):
            if abs(current_price - sma_150) / sma_150 <= SMA_PROXIMITY_PCT:
                flags.append(
                    _make_flag(ticker, run_date, "AT_150SMA", current_price, sma_150)
                )

    return flags


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def detect_flags(
    ticker: str,
    prices: list[dict],
    classification: CharacterClassification,
    baseline: CharacterBaseline,
    benchmark_prices: dict[str, list[dict]],
    run_date: date | None = None,
) -> list[CharacterFlag]:
    """Detect atypical behavior flags for a classified stock.

    Args:
        ticker: Stock symbol
        prices: Full OHLCV history (sorted by date ascending).
            Each dict has keys: date, open, high, low, close, volume
        classification: Stage 1 result
        baseline: Stage 2 result
        benchmark_prices: {"SPY": [...], "GLD": [...], "TLT": [...]}
        run_date: Date to stamp flags (defaults to today)

    Returns:
        List of CharacterFlag instances (may be empty if nothing atypical).
    """
    run_date = run_date or date.today()

    try:
        # ------------------------------------------------------------------
        # Step 1: Extract arrays, validate minimums
        # ------------------------------------------------------------------
        if not prices or len(prices) < RECENT_WINDOW_DAYS + 1:
            logger.info(
                "%s: insufficient price data for flag detection (%d bars)",
                ticker,
                len(prices) if prices else 0,
            )
            return []

        high, low, close, volume = _extract_ohlcv_arrays(prices)

        if not np.all(np.isfinite(close[-RECENT_WINDOW_DAYS:])):
            logger.info("%s: NaN close prices in recent window", ticker)
            return []

        current_price = float(close[-1])
        if current_price <= 0:
            logger.info("%s: non-positive current price", ticker)
            return []

        recent_atr = compute_recent_atr(high, low, close, RECENT_WINDOW_DAYS)
        if recent_atr is None:
            # All recent true ranges were NaN: keep NaN so atr_ratio comparisons
            # are False and no COMPRESSION/EXPANSION/VOL_SPIKE flag fires.
            recent_atr = float("nan")

        flags: list[CharacterFlag] = []
        character = classification.character

        # ------------------------------------------------------------------
        # Step 2: Common flags (decorrelation, volume)
        # ------------------------------------------------------------------
        flags.extend(
            _detect_common_flags(
                ticker,
                run_date,
                prices,
                close,
                volume,
                baseline,
                benchmark_prices,
            )
        )

        # ------------------------------------------------------------------
        # Step 3: Character-specific flags
        # ------------------------------------------------------------------
        if character == "range_bound":
            flags.extend(
                _detect_range_bound_flags(
                    ticker,
                    run_date,
                    close,
                    high,
                    low,
                    baseline,
                    current_price,
                    recent_atr,
                )
            )
        elif character == "trending":
            flags.extend(
                _detect_trending_flags(
                    ticker,
                    run_date,
                    close,
                    baseline,
                    current_price,
                    recent_atr,
                )
            )

        # ------------------------------------------------------------------
        # Step 4: SMA flags (all character types)
        # ------------------------------------------------------------------
        flags.extend(
            _detect_sma_flags(
                ticker,
                run_date,
                close,
                classification,
                baseline,
                current_price,
            )
        )

        logger.info(
            "%s: detected %d flag(s): %s",
            ticker,
            len(flags),
            ", ".join(f.flag for f in flags) if flags else "(none)",
        )
        return flags

    except Exception:
        logger.exception("%s: error during flag detection", ticker)
        return []
