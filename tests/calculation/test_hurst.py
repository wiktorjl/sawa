"""Parity tests for the pure-numpy stock-character helpers."""

import numpy as np
import pytest

from sawa.calculation.hurst import compute_adx

try:
    import talib

    TALIB_AVAILABLE = True
except ImportError:
    TALIB_AVAILABLE = False


@pytest.mark.skipif(not TALIB_AVAILABLE, reason="ta-lib not installed")
@pytest.mark.parametrize("flat", [False, True])
def test_compute_adx_matches_talib_warmup_and_values(flat: bool) -> None:
    period = 14
    count = 90
    if flat:
        high = low = close = np.full(count, 100.0, dtype=np.float64)
    else:
        x = np.arange(count, dtype=np.float64)
        close = 100.0 + np.sin(x / 3.0) * 2.0 + x * 0.15
        high = close + 1.0 + (x % 4) * 0.1
        low = close - 1.0 - (x % 3) * 0.1

    actual = compute_adx(high, low, close, period=period)
    expected = talib.ADX(high, low, close, timeperiod=period)

    first_valid = period * 2 - 1
    assert np.isnan(actual[:first_valid]).all()
    assert np.isfinite(actual[first_valid])
    np.testing.assert_allclose(actual, expected, rtol=1e-12, atol=1e-12, equal_nan=True)


@pytest.mark.skipif(not TALIB_AVAILABLE, reason="ta-lib not installed")
def test_compute_adx_emits_at_first_possible_bar() -> None:
    period = 5
    close = np.array([10, 11, 10, 12, 11, 13, 12, 14, 13, 15], dtype=np.float64)
    high = close + 1.0
    low = close - 1.0

    actual = compute_adx(high, low, close, period=period)
    expected = talib.ADX(high, low, close, timeperiod=period)

    assert len(actual) == period * 2
    assert np.isnan(actual[:-1]).all()
    assert actual[-1] == pytest.approx(expected[-1], rel=1e-12, abs=1e-12)
