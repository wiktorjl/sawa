"""Tests for momentum/squeeze indicator tools."""

from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import patch

from mcp_server.tools.momentum import (
    ADX_PERIOD,
    BB_PERIOD,
    BB_STD_MULT,
    KC_ATR_MULT,
    KC_PERIOD,
    MOMENTUM_PERIOD,
    ROC_PERIOD,
    STOCHASTIC_D_PERIOD,
    STOCHASTIC_K_PERIOD,
    WILLIAMS_R_PERIOD,
    _interpret_momentum,
    get_momentum_indicators,
    get_squeeze_indicators,
)


def _make_row(
    d: date,
    close: float = 150.0,
    high: float = 152.0,
    low: float = 148.0,
    bb_upper: float = 155.0,
    bb_middle: float = 150.0,
    bb_lower: float = 145.0,
    atr_14: float = 3.0,
    sma_20: float = 150.0,
    ema_50: float = 148.0,
    rsi_14: float = 55.0,
    macd_histogram: float = 0.5,
    volume: int = 1000000,
    squeeze_on: bool = False,
) -> dict:
    """Build a mock DB row for squeeze calculation.

    The actual squeeze_on is calculated in SQL, but for testing the
    Python post-processing we provide pre-calculated data.
    """
    # Calculate KC bounds using same formula as the SQL
    kc_upper = round(sma_20 + atr_14 * KC_ATR_MULT, 4)
    kc_lower = round(sma_20 - atr_14 * KC_ATR_MULT, 4)

    # BB width
    bb_width_pct = round((bb_upper - bb_lower) / bb_middle * 100, 4) if bb_middle > 0 else None

    # KC width
    kc_width_pct = round((atr_14 * KC_ATR_MULT * 2) / sma_20 * 100, 4) if sma_20 > 0 else None

    # Momentum (simplified - matches SQL window function output)
    midpoint = (high + low) / 2.0
    momentum = round(close - (midpoint + sma_20) / 2.0, 4)

    # Squeeze detection
    is_squeeze = bb_upper < kc_upper and bb_lower > kc_lower

    return {
        "date": d,
        "close": Decimal(str(close)),
        "bb_upper": Decimal(str(round(bb_upper, 2))),
        "bb_middle": Decimal(str(round(bb_middle, 2))),
        "bb_lower": Decimal(str(round(bb_lower, 2))),
        "bb_width_pct": Decimal(str(bb_width_pct)) if bb_width_pct else None,
        "kc_upper": Decimal(str(round(kc_upper, 2))),
        "kc_lower": Decimal(str(round(kc_lower, 2))),
        "kc_width_pct": Decimal(str(kc_width_pct)) if kc_width_pct else None,
        "squeeze_on": is_squeeze if not squeeze_on else squeeze_on,
        "momentum": Decimal(str(momentum)),
        "atr": Decimal(str(atr_14)),
        "rsi_14": Decimal(str(rsi_14)),
        "macd_histogram": Decimal(str(macd_histogram)),
    }


def _generate_squeeze_series(
    days: int = 10,
    squeeze_days: list[int] | None = None,
    base_close: float = 150.0,
    trend: float = 0.5,
) -> list[dict]:
    """Generate a time series of squeeze data rows.

    Args:
        days: Number of days to generate
        squeeze_days: Indices where squeeze is on (BB inside KC)
        base_close: Starting close price
        trend: Daily price trend
    """
    if squeeze_days is None:
        squeeze_days = []

    rows = []
    start = date.today() - timedelta(days=days)
    for i in range(days):
        d = start + timedelta(days=i)
        close = base_close + i * trend
        sma_20 = close - 1.0

        if i in squeeze_days:
            # Narrow BB (inside KC): squeeze on
            bb_upper = sma_20 + 2.0
            bb_lower = sma_20 - 2.0
            atr_14 = 3.0  # KC range = 3 * 1.5 = 4.5 each side
        else:
            # Wide BB (outside KC): squeeze off
            bb_upper = sma_20 + 8.0
            bb_lower = sma_20 - 8.0
            atr_14 = 3.0  # KC range = 4.5 each side, BB range = 8 each side

        row = _make_row(
            d=d,
            close=close,
            high=close + 2.0,
            low=close - 2.0,
            bb_upper=bb_upper,
            bb_middle=sma_20,
            bb_lower=bb_lower,
            atr_14=atr_14,
            sma_20=sma_20,
        )
        # Override squeeze_on to match our intent
        row["squeeze_on"] = i in squeeze_days
        rows.append(row)

    return rows


class TestGetSqueezeIndicators:
    """Tests for get_squeeze_indicators function."""

    @patch("mcp_server.tools.momentum.execute_query")
    def test_empty_result(self, mock_query):
        """Returns empty structure when no data available."""
        mock_query.return_value = []

        result = get_squeeze_indicators("AAPL")

        assert result["ticker"] == "AAPL"
        assert result["current_squeeze"] is None
        assert result["current_momentum"] is None
        assert result["current_direction"] == "neutral"
        assert result["squeeze_fired"] is False
        assert result["data"] == []

    @patch("mcp_server.tools.momentum.execute_query")
    def test_basic_structure(self, mock_query):
        """Result has all required fields."""
        rows = _generate_squeeze_series(days=5)
        mock_query.return_value = rows

        result = get_squeeze_indicators("AAPL", lookback_days=60)

        assert "ticker" in result
        assert "lookback_days" in result
        assert "current_squeeze" in result
        assert "current_momentum" in result
        assert "current_direction" in result
        assert "squeeze_fired" in result
        assert "data" in result

    @patch("mcp_server.tools.momentum.execute_query")
    def test_data_fields(self, mock_query):
        """Each data point has all expected fields."""
        rows = _generate_squeeze_series(days=5)
        mock_query.return_value = rows

        result = get_squeeze_indicators("AAPL")
        assert len(result["data"]) == 5

        expected_fields = {
            "date", "close", "bb_upper", "bb_middle", "bb_lower",
            "bb_width_pct", "kc_upper", "kc_lower", "kc_width_pct",
            "squeeze_on", "momentum", "direction", "atr",
            "rsi_14", "macd_histogram",
        }
        for point in result["data"]:
            assert set(point.keys()) == expected_fields

    @patch("mcp_server.tools.momentum.execute_query")
    def test_ticker_uppercased(self, mock_query):
        """Ticker is normalized to uppercase."""
        mock_query.return_value = _generate_squeeze_series(days=3)

        result = get_squeeze_indicators("aapl")
        assert result["ticker"] == "AAPL"

    @patch("mcp_server.tools.momentum.execute_query")
    def test_lookback_capped_at_252(self, mock_query):
        """Lookback days cannot exceed 252."""
        mock_query.return_value = []

        get_squeeze_indicators("AAPL", lookback_days=500)

        # Verify the params passed to execute_query
        call_args = mock_query.call_args
        params = call_args[1] if call_args[1] else call_args[0][1]
        assert params["lookback_days"] == 252

    @patch("mcp_server.tools.momentum.execute_query")
    def test_squeeze_on_detection(self, mock_query):
        """Squeeze on is detected when BB is inside KC."""
        rows = _generate_squeeze_series(days=5, squeeze_days=[3, 4])
        mock_query.return_value = rows

        result = get_squeeze_indicators("AAPL")

        # Last day (index 4) is squeeze on
        assert result["current_squeeze"] is True
        assert result["data"][3]["squeeze_on"] is True
        assert result["data"][4]["squeeze_on"] is True
        # Earlier days not in squeeze
        assert result["data"][0]["squeeze_on"] is False

    @patch("mcp_server.tools.momentum.execute_query")
    def test_squeeze_off_detection(self, mock_query):
        """Squeeze off when BB wider than KC."""
        rows = _generate_squeeze_series(days=5, squeeze_days=[])
        mock_query.return_value = rows

        result = get_squeeze_indicators("AAPL")

        assert result["current_squeeze"] is False
        for point in result["data"]:
            assert point["squeeze_on"] is False

    @patch("mcp_server.tools.momentum.execute_query")
    def test_squeeze_fired(self, mock_query):
        """Squeeze fired when transitioning from on to off."""
        # Squeeze on for days 2,3 then off for day 4
        rows = _generate_squeeze_series(days=5, squeeze_days=[2, 3])
        mock_query.return_value = rows

        result = get_squeeze_indicators("AAPL")

        assert result["squeeze_fired"] is True
        assert result["current_squeeze"] is False

    @patch("mcp_server.tools.momentum.execute_query")
    def test_squeeze_not_fired_when_still_on(self, mock_query):
        """Squeeze not fired when still in squeeze."""
        rows = _generate_squeeze_series(days=5, squeeze_days=[3, 4])
        mock_query.return_value = rows

        result = get_squeeze_indicators("AAPL")

        assert result["squeeze_fired"] is False
        assert result["current_squeeze"] is True

    @patch("mcp_server.tools.momentum.execute_query")
    def test_squeeze_not_fired_when_never_on(self, mock_query):
        """Squeeze not fired when it was never on."""
        rows = _generate_squeeze_series(days=5, squeeze_days=[])
        mock_query.return_value = rows

        result = get_squeeze_indicators("AAPL")

        assert result["squeeze_fired"] is False

    @patch("mcp_server.tools.momentum.execute_query")
    def test_direction_bullish(self, mock_query):
        """Bullish direction when momentum positive and rising."""
        rows = _generate_squeeze_series(days=4, base_close=150.0, trend=1.0)
        # Set increasing positive momentum
        for i, row in enumerate(rows):
            row["momentum"] = Decimal(str(1.0 + i * 0.5))
        mock_query.return_value = rows

        result = get_squeeze_indicators("AAPL")

        # Last point should be bullish (positive and rising)
        assert result["current_direction"] == "bullish"

    @patch("mcp_server.tools.momentum.execute_query")
    def test_direction_bearish(self, mock_query):
        """Bearish direction when momentum negative and falling."""
        rows = _generate_squeeze_series(days=4)
        # Set decreasing negative momentum
        for i, row in enumerate(rows):
            row["momentum"] = Decimal(str(-1.0 - i * 0.5))
        mock_query.return_value = rows

        result = get_squeeze_indicators("AAPL")

        assert result["current_direction"] == "bearish"

    @patch("mcp_server.tools.momentum.execute_query")
    def test_direction_weakening_bullish(self, mock_query):
        """Weakening bullish when momentum positive but falling."""
        rows = _generate_squeeze_series(days=4)
        # Positive but declining momentum
        for i, row in enumerate(rows):
            row["momentum"] = Decimal(str(3.0 - i * 0.5))
        mock_query.return_value = rows

        result = get_squeeze_indicators("AAPL")

        assert result["current_direction"] == "weakening_bullish"

    @patch("mcp_server.tools.momentum.execute_query")
    def test_direction_weakening_bearish(self, mock_query):
        """Weakening bearish when momentum negative but rising."""
        rows = _generate_squeeze_series(days=4)
        # Negative but rising momentum
        for i, row in enumerate(rows):
            row["momentum"] = Decimal(str(-3.0 + i * 0.5))
        mock_query.return_value = rows

        result = get_squeeze_indicators("AAPL")

        assert result["current_direction"] == "weakening_bearish"

    @patch("mcp_server.tools.momentum.execute_query")
    def test_direction_neutral_single_point(self, mock_query):
        """Direction is neutral with only one data point."""
        rows = _generate_squeeze_series(days=1)
        mock_query.return_value = rows

        result = get_squeeze_indicators("AAPL")

        assert result["current_direction"] == "neutral"

    @patch("mcp_server.tools.momentum.execute_query")
    def test_query_params(self, mock_query):
        """Verify correct parameters are passed to execute_query."""
        mock_query.return_value = []

        get_squeeze_indicators("MSFT", lookback_days=30)

        call_args = mock_query.call_args
        params = call_args[1] if call_args[1] else call_args[0][1]
        assert params["ticker"] == "MSFT"
        assert params["lookback_days"] == 30
        assert params["kc_mult"] == KC_ATR_MULT
        assert params["mom_period"] == MOMENTUM_PERIOD
        assert "start_date" in params

    @patch("mcp_server.tools.momentum.execute_query")
    def test_lookback_days_count(self, mock_query):
        """Result lookback_days matches actual data length."""
        rows = _generate_squeeze_series(days=10)
        mock_query.return_value = rows

        result = get_squeeze_indicators("AAPL", lookback_days=60)

        assert result["lookback_days"] == 10

    @patch("mcp_server.tools.momentum.execute_query")
    def test_data_ordered_by_date(self, mock_query):
        """Data points should be in ascending date order."""
        rows = _generate_squeeze_series(days=5)
        mock_query.return_value = rows

        result = get_squeeze_indicators("AAPL")
        dates = [point["date"] for point in result["data"]]

        assert dates == sorted(dates)


class TestSqueezeConstants:
    """Tests for module-level constants."""

    def test_bb_period(self):
        assert BB_PERIOD == 20

    def test_bb_std_mult(self):
        assert BB_STD_MULT == 2.0

    def test_kc_period(self):
        assert KC_PERIOD == 20

    def test_kc_atr_mult(self):
        assert KC_ATR_MULT == 1.5

    def test_momentum_period(self):
        assert MOMENTUM_PERIOD == 12


# --- Advanced Momentum Indicators Tests ---


def _make_momentum_row(
    d: date,
    close: float = 150.0,
    adx: float = 25.0,
    plus_di: float = 30.0,
    minus_di: float = 20.0,
    stoch_k: float = 50.0,
    stoch_d: float = 48.0,
    williams_r: float = -50.0,
    roc: float = 2.0,
) -> dict:
    """Build a mock DB row for momentum indicators."""
    return {
        "date": d,
        "close": Decimal(str(close)),
        "adx": Decimal(str(adx)),
        "plus_di": Decimal(str(plus_di)),
        "minus_di": Decimal(str(minus_di)),
        "stoch_k": Decimal(str(stoch_k)),
        "stoch_d": Decimal(str(stoch_d)),
        "williams_r": Decimal(str(williams_r)),
        "roc": Decimal(str(roc)),
    }


def _generate_momentum_series(
    days: int = 10,
    base_close: float = 150.0,
    trend: float = 0.5,
) -> list[dict]:
    """Generate a time series of momentum indicator rows."""
    rows = []
    start = date.today() - timedelta(days=days)
    for i in range(days):
        d = start + timedelta(days=i)
        close = base_close + i * trend
        rows.append(_make_momentum_row(
            d=d,
            close=close,
            adx=20.0 + i * 0.5,
            plus_di=25.0 + i * 0.3,
            minus_di=22.0 - i * 0.2,
            stoch_k=40.0 + i * 3.0,
            stoch_d=38.0 + i * 3.0,
            williams_r=-60.0 + i * 3.0,
            roc=1.0 + i * 0.3,
        ))
    return rows


class TestGetMomentumIndicators:
    """Tests for get_momentum_indicators function."""

    @patch("mcp_server.tools.momentum.execute_query")
    def test_empty_result(self, mock_query):
        """Returns empty structure when no data available."""
        mock_query.return_value = []

        result = get_momentum_indicators("AAPL")

        assert result["ticker"] == "AAPL"
        assert result["current"] is None
        assert result["signals"] == {}
        assert result["data"] == []

    @patch("mcp_server.tools.momentum.execute_query")
    def test_basic_structure(self, mock_query):
        """Result has all required fields."""
        mock_query.return_value = _generate_momentum_series(days=5)

        result = get_momentum_indicators("AAPL")

        assert "ticker" in result
        assert "lookback_days" in result
        assert "current" in result
        assert "signals" in result
        assert "data" in result

    @patch("mcp_server.tools.momentum.execute_query")
    def test_data_fields(self, mock_query):
        """Each data point has all expected fields."""
        mock_query.return_value = _generate_momentum_series(days=5)

        result = get_momentum_indicators("AAPL")

        expected_fields = {
            "date", "close", "adx", "plus_di", "minus_di",
            "stoch_k", "stoch_d", "williams_r", "roc",
        }
        for point in result["data"]:
            assert set(point.keys()) == expected_fields

    @patch("mcp_server.tools.momentum.execute_query")
    def test_ticker_uppercased(self, mock_query):
        """Ticker is normalized to uppercase."""
        mock_query.return_value = _generate_momentum_series(days=3)

        result = get_momentum_indicators("msft")
        assert result["ticker"] == "MSFT"

    @patch("mcp_server.tools.momentum.execute_query")
    def test_lookback_capped_at_252(self, mock_query):
        """Lookback days cannot exceed 252."""
        mock_query.return_value = []

        get_momentum_indicators("AAPL", lookback_days=500)

        call_args = mock_query.call_args
        params = call_args[1] if call_args[1] else call_args[0][1]
        assert params["lookback_days"] == 252

    @patch("mcp_server.tools.momentum.execute_query")
    def test_current_is_latest(self, mock_query):
        """Current should be the last data point."""
        rows = _generate_momentum_series(days=5)
        mock_query.return_value = rows

        result = get_momentum_indicators("AAPL")

        assert result["current"]["date"] == result["data"][-1]["date"]
        assert result["current"]["close"] == result["data"][-1]["close"]

    @patch("mcp_server.tools.momentum.execute_query")
    def test_signals_populated(self, mock_query):
        """Signals should be generated from current values."""
        mock_query.return_value = _generate_momentum_series(days=5)

        result = get_momentum_indicators("AAPL")

        assert "trend_strength" in result["signals"]
        assert "dmi_direction" in result["signals"]
        assert "stochastic" in result["signals"]
        assert "williams_r" in result["signals"]
        assert "roc_momentum" in result["signals"]

    @patch("mcp_server.tools.momentum.execute_query")
    def test_query_params(self, mock_query):
        """Verify correct parameters are passed to execute_query."""
        mock_query.return_value = []

        get_momentum_indicators("GOOGL", lookback_days=30)

        call_args = mock_query.call_args
        params = call_args[1] if call_args[1] else call_args[0][1]
        assert params["ticker"] == "GOOGL"
        assert params["lookback_days"] == 30
        assert params["adx_period"] == ADX_PERIOD
        assert params["stoch_k"] == STOCHASTIC_K_PERIOD
        assert params["stoch_d"] == STOCHASTIC_D_PERIOD
        assert params["williams_period"] == WILLIAMS_R_PERIOD
        assert params["roc_period"] == ROC_PERIOD

    @patch("mcp_server.tools.momentum.execute_query")
    def test_lookback_days_count(self, mock_query):
        """Result lookback_days matches actual data length."""
        mock_query.return_value = _generate_momentum_series(days=7)

        result = get_momentum_indicators("AAPL", lookback_days=60)
        assert result["lookback_days"] == 7

    @patch("mcp_server.tools.momentum.execute_query")
    def test_data_ordered_by_date(self, mock_query):
        """Data points should be in ascending date order."""
        mock_query.return_value = _generate_momentum_series(days=5)

        result = get_momentum_indicators("AAPL")
        dates = [point["date"] for point in result["data"]]
        assert dates == sorted(dates)


class TestInterpretMomentum:
    """Tests for _interpret_momentum signal generation."""

    def test_strong_trend(self):
        signals = _interpret_momentum({"adx": Decimal("30"), "plus_di": None,
                                        "minus_di": None, "stoch_k": None,
                                        "williams_r": None, "roc": None})
        assert signals["trend_strength"] == "strong"

    def test_moderate_trend(self):
        signals = _interpret_momentum({"adx": Decimal("22"), "plus_di": None,
                                        "minus_di": None, "stoch_k": None,
                                        "williams_r": None, "roc": None})
        assert signals["trend_strength"] == "moderate"

    def test_weak_trend(self):
        signals = _interpret_momentum({"adx": Decimal("15"), "plus_di": None,
                                        "minus_di": None, "stoch_k": None,
                                        "williams_r": None, "roc": None})
        assert signals["trend_strength"] == "weak"

    def test_dmi_bullish(self):
        signals = _interpret_momentum({"adx": None, "plus_di": Decimal("30"),
                                        "minus_di": Decimal("20"), "stoch_k": None,
                                        "williams_r": None, "roc": None})
        assert signals["dmi_direction"] == "bullish"

    def test_dmi_bearish(self):
        signals = _interpret_momentum({"adx": None, "plus_di": Decimal("15"),
                                        "minus_di": Decimal("25"), "stoch_k": None,
                                        "williams_r": None, "roc": None})
        assert signals["dmi_direction"] == "bearish"

    def test_dmi_neutral(self):
        signals = _interpret_momentum({"adx": None, "plus_di": Decimal("25"),
                                        "minus_di": Decimal("25"), "stoch_k": None,
                                        "williams_r": None, "roc": None})
        assert signals["dmi_direction"] == "neutral"

    def test_stochastic_overbought(self):
        signals = _interpret_momentum({"adx": None, "plus_di": None,
                                        "minus_di": None, "stoch_k": Decimal("85"),
                                        "williams_r": None, "roc": None})
        assert signals["stochastic"] == "overbought"

    def test_stochastic_oversold(self):
        signals = _interpret_momentum({"adx": None, "plus_di": None,
                                        "minus_di": None, "stoch_k": Decimal("15"),
                                        "williams_r": None, "roc": None})
        assert signals["stochastic"] == "oversold"

    def test_stochastic_neutral(self):
        signals = _interpret_momentum({"adx": None, "plus_di": None,
                                        "minus_di": None, "stoch_k": Decimal("50"),
                                        "williams_r": None, "roc": None})
        assert signals["stochastic"] == "neutral"

    def test_williams_r_overbought(self):
        signals = _interpret_momentum({"adx": None, "plus_di": None,
                                        "minus_di": None, "stoch_k": None,
                                        "williams_r": Decimal("-10"), "roc": None})
        assert signals["williams_r"] == "overbought"

    def test_williams_r_oversold(self):
        signals = _interpret_momentum({"adx": None, "plus_di": None,
                                        "minus_di": None, "stoch_k": None,
                                        "williams_r": Decimal("-90"), "roc": None})
        assert signals["williams_r"] == "oversold"

    def test_williams_r_neutral(self):
        signals = _interpret_momentum({"adx": None, "plus_di": None,
                                        "minus_di": None, "stoch_k": None,
                                        "williams_r": Decimal("-50"), "roc": None})
        assert signals["williams_r"] == "neutral"

    def test_roc_strong_bullish(self):
        signals = _interpret_momentum({"adx": None, "plus_di": None,
                                        "minus_di": None, "stoch_k": None,
                                        "williams_r": None, "roc": Decimal("8")})
        assert signals["roc_momentum"] == "strong_bullish"

    def test_roc_bullish(self):
        signals = _interpret_momentum({"adx": None, "plus_di": None,
                                        "minus_di": None, "stoch_k": None,
                                        "williams_r": None, "roc": Decimal("3")})
        assert signals["roc_momentum"] == "bullish"

    def test_roc_strong_bearish(self):
        signals = _interpret_momentum({"adx": None, "plus_di": None,
                                        "minus_di": None, "stoch_k": None,
                                        "williams_r": None, "roc": Decimal("-8")})
        assert signals["roc_momentum"] == "strong_bearish"

    def test_roc_bearish(self):
        signals = _interpret_momentum({"adx": None, "plus_di": None,
                                        "minus_di": None, "stoch_k": None,
                                        "williams_r": None, "roc": Decimal("-3")})
        assert signals["roc_momentum"] == "bearish"

    def test_roc_neutral(self):
        signals = _interpret_momentum({"adx": None, "plus_di": None,
                                        "minus_di": None, "stoch_k": None,
                                        "williams_r": None, "roc": Decimal("0")})
        assert signals["roc_momentum"] == "neutral"

    def test_all_none_returns_empty(self):
        """When all indicators are None, no signals generated."""
        signals = _interpret_momentum({"adx": None, "plus_di": None,
                                        "minus_di": None, "stoch_k": None,
                                        "williams_r": None, "roc": None})
        assert signals == {}

    def test_full_signal_set(self):
        """All signals present with complete data."""
        signals = _interpret_momentum({
            "adx": Decimal("28"),
            "plus_di": Decimal("32"),
            "minus_di": Decimal("18"),
            "stoch_k": Decimal("75"),
            "williams_r": Decimal("-25"),
            "roc": Decimal("4"),
        })
        assert len(signals) == 5
        assert signals["trend_strength"] == "strong"
        assert signals["dmi_direction"] == "bullish"
        assert signals["stochastic"] == "neutral"
        assert signals["williams_r"] == "neutral"
        assert signals["roc_momentum"] == "bullish"


class TestMomentumConstants:
    """Tests for advanced momentum constants."""

    def test_adx_period(self):
        assert ADX_PERIOD == 14

    def test_stochastic_k_period(self):
        assert STOCHASTIC_K_PERIOD == 14

    def test_stochastic_d_period(self):
        assert STOCHASTIC_D_PERIOD == 3

    def test_roc_period(self):
        assert ROC_PERIOD == 12

    def test_williams_r_period(self):
        assert WILLIAMS_R_PERIOD == 14
