"""Tests for technical indicator calculation engine."""

from datetime import date
from decimal import Decimal

import pytest

from sawa.calculation.ta_engine import (
    INDICATOR_BOUNDS,
    MIN_PERIODS,
    get_required_lookback_days,
    validate_indicator,
)
from sawa.domain.technical_indicators import TechnicalIndicators


class TestValidateIndicator:
    """Tests for validate_indicator function."""

    def test_nan_returns_none(self):
        """NaN values should return None."""
        result = validate_indicator("rsi_14", float("nan"))
        assert result is None

    def test_inf_returns_none(self):
        """Infinite values should return None."""
        result = validate_indicator("sma_5", float("inf"))
        assert result is None

        result = validate_indicator("sma_5", float("-inf"))
        assert result is None

    def test_valid_rsi_passes(self):
        """Valid RSI values should pass through."""
        result = validate_indicator("rsi_14", 50.0)
        assert result == 50.0

        result = validate_indicator("rsi_14", 0.0)
        assert result == 0.0

        result = validate_indicator("rsi_14", 100.0)
        assert result == 100.0

    def test_rsi_clamped_within_tolerance(self):
        """RSI slightly outside bounds should be clamped."""
        # Slightly below 0 (within 1% tolerance)
        result = validate_indicator("rsi_14", -0.005)
        assert result == 0.0

        # Slightly above 100 (within 1% tolerance)
        result = validate_indicator("rsi_14", 100.5)
        assert result == 100.0

    def test_rsi_far_outside_bounds_raises(self):
        """RSI far outside bounds should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid rsi_14"):
            validate_indicator("rsi_14", -5.0)

        with pytest.raises(ValueError, match="Invalid rsi_14"):
            validate_indicator("rsi_14", 105.0)

    def test_unbounded_indicator_passes(self):
        """Unbounded indicators should pass any value."""
        result = validate_indicator("sma_5", 1000.0)
        assert result == 1000.0

        result = validate_indicator("macd_line", -50.0)
        assert result == -50.0

    def test_volume_ratio_non_negative(self):
        """Volume ratio must be >= 0."""
        result = validate_indicator("volume_ratio", 0.0)
        assert result == 0.0

        result = validate_indicator("volume_ratio", 5.0)
        assert result == 5.0

        # Slightly negative within tolerance
        result = validate_indicator("volume_ratio", -0.005)
        assert result == 0.0

        # Far negative raises
        with pytest.raises(ValueError, match="Invalid volume_ratio"):
            validate_indicator("volume_ratio", -1.0)


class TestIndicatorBounds:
    """Tests for indicator bounds configuration."""

    def test_rsi_bounds(self):
        """RSI should be bounded 0-100."""
        assert INDICATOR_BOUNDS["rsi_14"] == (0.0, 100.0)
        assert INDICATOR_BOUNDS["rsi_21"] == (0.0, 100.0)

    def test_atr_non_negative(self):
        """ATR should be >= 0."""
        assert INDICATOR_BOUNDS["atr_14"] == (0.0, None)

    def test_volume_non_negative(self):
        """Volume indicators should be >= 0."""
        assert INDICATOR_BOUNDS["volume_sma_20"] == (0.0, None)
        assert INDICATOR_BOUNDS["volume_ratio"] == (0.0, None)


class TestMinPeriods:
    """Tests for minimum periods configuration."""

    def test_sma_periods(self):
        """SMA periods should match their names."""
        assert MIN_PERIODS["sma_5"] == 5
        assert MIN_PERIODS["sma_10"] == 10
        assert MIN_PERIODS["sma_20"] == 20
        assert MIN_PERIODS["sma_50"] == 50

    def test_ema_periods(self):
        """EMA periods should match their names."""
        assert MIN_PERIODS["ema_12"] == 12
        assert MIN_PERIODS["ema_26"] == 26
        assert MIN_PERIODS["ema_50"] == 50

    def test_rsi_periods(self):
        """RSI periods should match their names."""
        assert MIN_PERIODS["rsi_14"] == 14
        assert MIN_PERIODS["rsi_21"] == 21

    def test_macd_periods(self):
        """MACD signal requires slow period + signal period."""
        assert MIN_PERIODS["macd_line"] == 26
        assert MIN_PERIODS["macd_signal"] == 35  # 26 + 9


class TestGetRequiredLookbackDays:
    """Tests for lookback calculation."""

    def test_lookback_exceeds_max_period(self):
        """Lookback should exceed the max indicator period."""
        lookback = get_required_lookback_days()
        max_period = max(MIN_PERIODS.values())
        assert lookback > max_period

    def test_lookback_accounts_for_weekends(self):
        """Lookback should account for weekends/holidays (~1.5x)."""
        lookback = get_required_lookback_days()
        max_period = max(MIN_PERIODS.values())
        # Should be roughly 1.4-1.5x the max period
        assert lookback >= int(max_period * 1.4)
        assert lookback <= int(max_period * 1.6)


class TestTechnicalIndicatorsModel:
    """Tests for TechnicalIndicators dataclass."""

    def test_ticker_normalized_to_uppercase(self):
        """Ticker should be normalized to uppercase."""
        ti = TechnicalIndicators(ticker="aapl", date=date(2024, 1, 15))
        assert ti.ticker == "AAPL"

    def test_default_none_values(self):
        """All indicator fields should default to None."""
        ti = TechnicalIndicators(ticker="AAPL", date=date(2024, 1, 15))
        assert ti.sma_5 is None
        assert ti.rsi_14 is None
        assert ti.obv is None

    def test_column_names_match_fields(self):
        """column_names() should return all field names."""
        columns = TechnicalIndicators.column_names()
        assert "ticker" in columns
        assert "date" in columns
        assert "sma_5" in columns
        assert "rsi_14" in columns
        assert "obv" in columns
        assert len(columns) == 27  # 2 keys + 25 indicators

    def test_to_tuple_order(self):
        """to_tuple() should return values in column order."""
        ti = TechnicalIndicators(
            ticker="AAPL",
            date=date(2024, 1, 15),
            sma_5=Decimal("150.00"),
            rsi_14=Decimal("45.5"),
        )
        t = ti.to_tuple()
        assert t[0] == "AAPL"
        assert t[1] == date(2024, 1, 15)
        assert t[2] == Decimal("150.00")  # sma_5


# Tests requiring ta-lib (marked to skip if not installed)
try:
    import talib  # noqa: F401

    TALIB_AVAILABLE = True
except ImportError:
    TALIB_AVAILABLE = False


@pytest.mark.skipif(not TALIB_AVAILABLE, reason="ta-lib not installed")
class TestCalculateIndicators:
    """Tests for calculate_indicators_for_ticker (requires ta-lib)."""

    def test_basic_calculation(self):
        """Calculate indicators for a simple price series."""
        from sawa.calculation.ta_engine import calculate_indicators_for_ticker

        # Generate 60 days of price data
        prices = []
        base_price = 100.0
        for i in range(60):
            d = date(2024, 1, 1) + __import__("datetime").timedelta(days=i)
            # Skip weekends (simple approximation)
            if d.weekday() >= 5:
                continue
            prices.append(
                {
                    "date": d,
                    "open": base_price + i * 0.1,
                    "high": base_price + i * 0.1 + 1.0,
                    "low": base_price + i * 0.1 - 1.0,
                    "close": base_price + i * 0.1 + 0.5,
                    "volume": 1000000 + i * 10000,
                }
            )

        results = calculate_indicators_for_ticker("AAPL", prices)

        assert len(results) == len(prices)
        assert all(r.ticker == "AAPL" for r in results)

        # First few results should have some NULLs (insufficient data)
        assert results[0].sma_50 is None  # Need 50 days
        assert results[0].sma_5 is None  # Need 5 days (first day has only 1)

        # Last result should have all indicators (enough data)
        last = results[-1]
        assert last.sma_5 is not None
        assert last.sma_10 is not None
        assert last.ema_12 is not None
        assert last.rsi_14 is not None

    def test_rsi_in_valid_range(self):
        """RSI should always be between 0 and 100."""
        from sawa.calculation.ta_engine import calculate_indicators_for_ticker

        # Trending up prices (should give high RSI)
        prices = []
        for i in range(30):
            d = date(2024, 1, 1) + __import__("datetime").timedelta(days=i)
            prices.append(
                {
                    "date": d,
                    "open": 100 + i * 2,
                    "high": 100 + i * 2 + 1,
                    "low": 100 + i * 2 - 0.5,
                    "close": 100 + i * 2 + 0.5,
                    "volume": 1000000,
                }
            )

        results = calculate_indicators_for_ticker("TEST", prices)

        for r in results:
            if r.rsi_14 is not None:
                assert 0 <= float(r.rsi_14) <= 100
