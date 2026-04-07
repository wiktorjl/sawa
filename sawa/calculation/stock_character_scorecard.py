"""Stage 4 stock character scorecard builder.

Aggregates outputs from Stages 1-3 (classification, baseline, detection) into
a single :class:`CharacterScorecard` and provides a convenience
:func:`analyze_stock` function that runs the full pipeline.
"""

import logging
import math
from datetime import date
from decimal import Decimal

import numpy as np
from scipy.stats import percentileofscore

from sawa.calculation.stock_character import _to_decimal, _extract_ohlcv_arrays, classify_stock
from sawa.calculation.stock_character_baseline import compute_baseline
from sawa.calculation.stock_character_detect import detect_flags
from sawa.calculation.stock_character_config import (
    RECENT_WINDOW_DAYS,
    HVN_PROXIMITY_PCT,
    SMA_ADHERENCE_MIN_RATIO,
)
from sawa.domain.stock_character import (
    CharacterBaseline,
    CharacterClassification,
    CharacterFlag,
    CharacterScorecard,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _compute_recent_atr(prices: list[dict], window: int) -> float | None:
    """Compute mean true range over the last *window* trading days.

    Returns None if there are fewer than 2 prices in the window.
    """
    recent = prices[-window:] if len(prices) >= window else prices
    if len(recent) < 2:
        return None

    high, low, close, _ = _extract_ohlcv_arrays(recent)
    # True range: max(high-low, |high-prev_close|, |low-prev_close|)
    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    tr = np.maximum(
        high - low,
        np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)),
    )
    # Skip the first element (no valid previous close)
    tr_valid = tr[1:]
    if len(tr_valid) == 0 or np.all(np.isnan(tr_valid)):
        return None
    return float(np.nanmean(tr_valid))


def _compute_spy_corr_recent(
    prices: list[dict],
    spy_prices: list[dict],
    window: int,
) -> float | None:
    """Compute Pearson correlation of stock vs SPY daily returns over recent window.

    Aligns by date, computes log returns, then correlates.
    Returns None if insufficient overlap.
    """
    # Build date -> close maps for the recent window
    stock_recent = prices[-window:] if len(prices) >= window else prices
    spy_recent = spy_prices[-window:] if len(spy_prices) >= window else spy_prices

    stock_by_date = {p["date"]: float(p["close"]) for p in stock_recent if p.get("close") is not None}
    spy_by_date = {p["date"]: float(p["close"]) for p in spy_recent if p.get("close") is not None}

    common_dates = sorted(set(stock_by_date) & set(spy_by_date))
    if len(common_dates) < 5:
        return None

    stock_closes = np.array([stock_by_date[d] for d in common_dates], dtype=np.float64)
    spy_closes = np.array([spy_by_date[d] for d in common_dates], dtype=np.float64)

    # Guard against zero / negative prices
    if np.any(stock_closes <= 0) or np.any(spy_closes <= 0):
        return None

    stock_rets = np.diff(np.log(stock_closes))
    spy_rets = np.diff(np.log(spy_closes))

    if len(stock_rets) < 3:
        return None

    # Handle constant series
    if np.std(stock_rets) == 0 or np.std(spy_rets) == 0:
        return None

    corr = float(np.corrcoef(stock_rets, spy_rets)[0, 1])
    if math.isnan(corr):
        return None
    return corr


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_scorecard(
    ticker: str,
    prices: list[dict],
    classification: CharacterClassification,
    baseline: CharacterBaseline,
    flags: list[CharacterFlag],
    benchmark_prices: dict[str, list[dict]],
    run_date: date | None = None,
) -> CharacterScorecard:
    """Build a scorecard from Stage 1-3 outputs.

    Args:
        ticker: Stock symbol
        prices: Full OHLCV history
        classification: Stage 1 result
        baseline: Stage 2 result
        flags: Stage 3 result (list of flags)
        benchmark_prices: {"SPY": [...], ...}
        run_date: Date to stamp

    Returns:
        CharacterScorecard
    """
    run_date = run_date or date.today()

    # --- Current price ---
    current_price_float: float | None = None
    if prices:
        last = prices[-1]
        if last.get("close") is not None:
            current_price_float = float(last["close"])

    current_price = _to_decimal(current_price_float)

    # --- Price percentile (range_bound only) ---
    price_percentile: Decimal | None = None
    if classification.character == "range_bound" and current_price_float is not None:
        _, _, close_arr, _ = _extract_ohlcv_arrays(prices)
        finite_closes = close_arr[np.isfinite(close_arr)]
        if len(finite_closes) > 0:
            pct = percentileofscore(finite_closes, current_price_float, kind="rank")
            price_percentile = _to_decimal(pct, 2)

    # --- Sigma distance (trending only) ---
    sigma_distance: Decimal | None = None
    if (
        classification.character == "trending"
        and current_price_float is not None
        and current_price_float > 0
        and baseline.regression_slope is not None
        and baseline.regression_intercept is not None
        and baseline.residuals_std is not None
    ):
        residuals_std_f = float(baseline.residuals_std)
        if residuals_std_f > 0:
            # Expected log price from regression at last index
            n = len(prices)
            expected_log = float(baseline.regression_intercept) + float(baseline.regression_slope) * (n - 1)
            actual_log = math.log(current_price_float)
            sigma_dist = (actual_log - expected_log) / residuals_std_f
            sigma_distance = _to_decimal(sigma_dist, 4)

    # --- Flag count and flag names ---
    flag_count = len(flags)
    flag_names = tuple(sorted(f.flag for f in flags))

    # --- ATR ratio ---
    atr_ratio: Decimal | None = None
    recent_atr = _compute_recent_atr(prices, RECENT_WINDOW_DAYS)
    if recent_atr is not None and baseline.atr_baseline is not None:
        baseline_atr_f = float(baseline.atr_baseline)
        if baseline_atr_f > 0:
            atr_ratio = _to_decimal(recent_atr / baseline_atr_f, 4)

    # --- SPY correlations ---
    spy_corr_recent: Decimal | None = None
    spy_prices = benchmark_prices.get("SPY")
    if spy_prices:
        corr_val = _compute_spy_corr_recent(prices, spy_prices, RECENT_WINDOW_DAYS)
        spy_corr_recent = _to_decimal(corr_val, 4)

    spy_corr_baseline = baseline.spy_corr_90d_mean

    # --- HVN / LVN flags ---
    at_hvn = any(f.flag == "AT_HVN" for f in flags)
    in_lvn = any(f.flag == "IN_LVN" for f in flags)

    # --- Notes ---
    notes: str | None = None
    if not classification.survivorship_flag:
        notes = "survivorship_bias: incomplete history"

    scorecard = CharacterScorecard(
        ticker=ticker,
        run_date=run_date,
        character=classification.character,
        confidence=classification.confidence,
        current_price=current_price,
        price_percentile=price_percentile,
        sigma_distance=sigma_distance,
        flag_count=flag_count,
        flags=flag_names,
        atr_ratio=atr_ratio,
        spy_corr_recent=spy_corr_recent,
        spy_corr_baseline=spy_corr_baseline,
        at_hvn=at_hvn,
        in_lvn=in_lvn,
        notes=notes,
    )

    logger.info(
        "%s: scorecard built | character=%s confidence=%s price=%s "
        "flags=%d atr_ratio=%s spy_corr_recent=%s at_hvn=%s in_lvn=%s",
        ticker,
        scorecard.character,
        scorecard.confidence,
        scorecard.current_price,
        scorecard.flag_count,
        scorecard.atr_ratio,
        scorecard.spy_corr_recent,
        scorecard.at_hvn,
        scorecard.in_lvn,
    )

    return scorecard


def analyze_stock(
    ticker: str,
    prices: list[dict],
    benchmark_prices: dict[str, list[dict]],
    run_date: date | None = None,
) -> dict | None:
    """Run the full Stage 1-4 pipeline for a single stock.

    Convenience function that chains classify -> baseline -> detect -> scorecard.

    Args:
        ticker: Stock symbol
        prices: Full OHLCV history
        benchmark_prices: {"SPY": [...], "GLD": [...], "TLT": [...]}
        run_date: Date to stamp all outputs

    Returns:
        Dict with keys: classification, baseline, flags, scorecard.
        Returns None if stock is unclassifiable (Stage 1 returns None).
    """
    run_date = run_date or date.today()

    classification = classify_stock(ticker, prices, run_date)
    if classification is None:
        return None

    baseline = compute_baseline(ticker, prices, classification, benchmark_prices, run_date)
    flags = detect_flags(ticker, prices, classification, baseline, benchmark_prices, run_date)
    scorecard = build_scorecard(ticker, prices, classification, baseline, flags, benchmark_prices, run_date)

    return {
        "classification": classification,
        "baseline": baseline,
        "flags": flags,
        "scorecard": scorecard,
    }
