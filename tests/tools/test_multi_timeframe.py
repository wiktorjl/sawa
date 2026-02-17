"""Tests for multi-timeframe analysis tools."""

from datetime import date, timedelta
from unittest.mock import patch

import pytest

from mcp_server.tools.multi_timeframe import (
    _analyze_daily_signals,
    _calculate_alignment,
    _compute_beta,
    _compute_rs_trend,
    _compute_signals_from_candles,
    _overall_direction,
    _rsi_signal,
    _sample_rs_line,
    _simple_ema,
    _simple_macd,
    _simple_rsi,
    calculate_relative_strength,
    get_multi_timeframe_alignment,
    get_weekly_monthly_candles,
)

# -- Helper data factories --


def _make_candles(count: int, start_price: float = 100.0, trend: float = 0.5):
    """Generate a list of candle dicts with a simple uptrend/downtrend."""
    base = date(2024, 1, 1)
    candles = []
    for i in range(count):
        price = start_price + i * trend
        candles.append({
            "period_start": base + timedelta(weeks=i),
            "open": price,
            "high": price + 2,
            "low": price - 2,
            "close": price + 1,
            "volume": 1000000 + i * 10000,
        })
    return candles


def _make_price_rows(count: int, ticker_start: float, bench_start: float,
                     ticker_trend: float = 0.5, bench_trend: float = 0.3):
    """Generate paired price rows for RS calculations."""
    base = date(2024, 6, 1)
    rows = []
    for i in range(count):
        rows.append({
            "date": base + timedelta(days=i),
            "ticker_close": ticker_start + i * ticker_trend,
            "benchmark_close": bench_start + i * bench_trend,
        })
    return rows


# -- Tests for _simple_rsi --


class TestSimpleRSI:
    def test_returns_none_insufficient_data(self):
        assert _simple_rsi([100, 101, 102], period=14) is None

    def test_all_gains_returns_100(self):
        closes = [100 + i for i in range(20)]
        rsi = _simple_rsi(closes, period=14)
        assert rsi == 100.0

    def test_all_losses_returns_0(self):
        closes = [200 - i for i in range(20)]
        rsi = _simple_rsi(closes, period=14)
        assert rsi is not None
        assert rsi == pytest.approx(0.0, abs=0.01)

    def test_mixed_returns_between_0_and_100(self):
        closes = [100 + (i % 2) * 2 for i in range(20)]
        rsi = _simple_rsi(closes, period=14)
        assert rsi is not None
        assert 0 < rsi < 100

    def test_custom_period(self):
        closes = [100 + i * 0.5 for i in range(10)]
        rsi = _simple_rsi(closes, period=5)
        assert rsi is not None
        assert rsi == 100.0


# -- Tests for _simple_ema --


class TestSimpleEMA:
    def test_single_value(self):
        assert _simple_ema([50.0], 10) == 50.0

    def test_fewer_values_than_period(self):
        values = [10.0, 20.0, 30.0]
        assert _simple_ema(values, 5) == 20.0

    def test_exact_period_returns_sma(self):
        values = [10.0, 20.0, 30.0, 40.0, 50.0]
        assert _simple_ema(values, 5) == 30.0

    def test_ema_responds_to_trend(self):
        values = list(range(1, 21))
        ema = _simple_ema([float(v) for v in values], 10)
        sma = sum(values[-10:]) / 10
        assert ema >= sma - 1


# -- Tests for _simple_macd --


class TestSimpleMACD:
    def test_returns_none_insufficient_data(self):
        closes = [100.0] * 30
        assert _simple_macd(closes) is None

    def test_returns_tuple_of_three(self):
        closes = [100 + i * 0.5 for i in range(50)]
        result = _simple_macd(closes)
        assert result is not None
        line, signal, histogram = result
        assert isinstance(line, float)
        assert isinstance(signal, float)
        assert isinstance(histogram, float)
        assert histogram == pytest.approx(line - signal)

    def test_uptrend_positive_macd(self):
        closes = [100 + i * 2 for i in range(50)]
        result = _simple_macd(closes)
        assert result is not None
        line, _, _ = result
        assert line > 0

    def test_downtrend_negative_macd(self):
        closes = [200 - i * 2 for i in range(50)]
        result = _simple_macd(closes)
        assert result is not None
        line, _, _ = result
        assert line < 0


# -- Tests for _rsi_signal --


class TestRSISignal:
    def test_overbought(self):
        assert _rsi_signal(75) == "overbought"
        assert _rsi_signal(70) == "overbought"

    def test_oversold(self):
        assert _rsi_signal(25) == "oversold"
        assert _rsi_signal(30) == "oversold"

    def test_bullish(self):
        assert _rsi_signal(55) == "bullish"
        assert _rsi_signal(50) == "bullish"

    def test_bearish(self):
        assert _rsi_signal(45) == "bearish"
        assert _rsi_signal(31) == "bearish"


# -- Tests for _overall_direction --


class TestOverallDirection:
    def test_all_bullish(self):
        signals = {
            "sma_trend": {"signal": "bullish"},
            "rsi": {"signal": "bullish"},
            "macd": {"signal": "bullish"},
        }
        assert _overall_direction(signals) == "bullish"

    def test_all_bearish(self):
        signals = {
            "sma_trend": {"signal": "bearish"},
            "rsi": {"signal": "bearish"},
            "macd": {"signal": "bearish"},
        }
        assert _overall_direction(signals) == "bearish"

    def test_two_of_three_bullish(self):
        signals = {
            "sma_trend": {"signal": "bullish"},
            "rsi": {"signal": "bearish"},
            "macd": {"signal": "bullish"},
        }
        assert _overall_direction(signals) == "bullish"

    def test_two_of_three_bearish(self):
        signals = {
            "sma_trend": {"signal": "bearish"},
            "rsi": {"signal": "bearish"},
            "macd": {"signal": "bullish"},
        }
        assert _overall_direction(signals) == "bearish"

    def test_even_split_is_neutral(self):
        signals = {
            "sma_trend": {"signal": "bullish"},
            "rsi": {"signal": "bearish"},
        }
        assert _overall_direction(signals) == "neutral"

    def test_no_signals(self):
        assert _overall_direction({}) == "unknown"

    def test_overbought_counts_as_bullish(self):
        signals = {
            "sma_trend": {"signal": "bearish"},
            "rsi": {"signal": "overbought"},
            "macd": {"signal": "bullish"},
        }
        assert _overall_direction(signals) == "bullish"


# -- Tests for _analyze_daily_signals --


class TestAnalyzeDailySignals:
    def test_bullish_daily(self):
        data = {
            "price": 150.0,
            "sma_20": 140.0,
            "sma_50": 135.0,
            "sma_200": 130.0,
            "rsi_14": 55.0,
            "macd_line": 1.5,
            "macd_signal": 1.0,
            "macd_histogram": 0.5,
        }
        result = _analyze_daily_signals(data)
        assert result["direction"] == "bullish"
        assert result["sma_trend"]["signal"] == "bullish"
        assert result["sma_trend"]["above_sma_20"] is True
        assert result["rsi"]["signal"] == "bullish"
        assert result["macd"]["signal"] == "bullish"

    def test_bearish_daily(self):
        data = {
            "price": 100.0,
            "sma_20": 110.0,
            "sma_50": 120.0,
            "sma_200": 130.0,
            "rsi_14": 25.0,
            "macd_line": -2.0,
            "macd_signal": -1.0,
            "macd_histogram": -1.0,
        }
        result = _analyze_daily_signals(data)
        assert result["direction"] == "bearish"
        assert result["sma_trend"]["signal"] == "bearish"

    def test_missing_indicators(self):
        data = {"price": 100.0}
        result = _analyze_daily_signals(data)
        assert result["price"] == 100.0
        assert "sma_trend" not in result
        assert "rsi" not in result


# -- Tests for _compute_signals_from_candles --


class TestComputeSignalsFromCandles:
    def test_uptrend_candles(self):
        candles = _make_candles(60, start_price=100, trend=1.0)
        result = _compute_signals_from_candles(candles)
        assert result["price"] > 0
        assert "sma_trend" in result
        assert "rsi" in result
        assert result["direction"] in ("bullish", "neutral", "bearish")

    def test_insufficient_data(self):
        result = _compute_signals_from_candles([{"close": 100}])
        assert result == {"price": 100.0}

    def test_empty_candles(self):
        result = _compute_signals_from_candles([])
        assert result == {}

    def test_macd_present_with_enough_data(self):
        candles = _make_candles(50, start_price=100, trend=0.5)
        result = _compute_signals_from_candles(candles)
        assert "macd" in result

    def test_no_macd_with_insufficient_data(self):
        candles = _make_candles(20, start_price=100, trend=0.5)
        result = _compute_signals_from_candles(candles)
        assert "macd" not in result


# -- Tests for _calculate_alignment --


class TestCalculateAlignment:
    def test_all_bullish(self):
        result = {
            "timeframes": {
                "daily": {"direction": "bullish"},
                "weekly": {"direction": "bullish"},
                "monthly": {"direction": "bullish"},
            },
        }
        _calculate_alignment(result)
        assert result["alignment_score"] == 100
        assert result["trend_consistency"] is True
        assert "aligned bullish" in result["summary"]

    def test_all_bearish(self):
        result = {
            "timeframes": {
                "daily": {"direction": "bearish"},
                "weekly": {"direction": "bearish"},
                "monthly": {"direction": "bearish"},
            },
        }
        _calculate_alignment(result)
        assert result["alignment_score"] == -100
        assert result["trend_consistency"] is True

    def test_mixed_signals(self):
        result = {
            "timeframes": {
                "daily": {"direction": "bullish"},
                "weekly": {"direction": "bearish"},
                "monthly": {"direction": "neutral"},
            },
        }
        _calculate_alignment(result)
        assert result["trend_consistency"] is False
        assert "Mixed" in result["summary"]
        assert -100 <= result["alignment_score"] <= 100

    def test_empty_timeframes(self):
        result = {"timeframes": {}}
        _calculate_alignment(result)
        assert "alignment_score" not in result


# -- Tests for _compute_rs_trend --


class TestComputeRSTrend:
    def test_improving(self):
        values = [100, 100, 100, 100, 110, 110, 110, 110]
        assert _compute_rs_trend(values) == "improving"

    def test_declining(self):
        values = [110, 110, 110, 110, 100, 100, 100, 100]
        assert _compute_rs_trend(values) == "declining"

    def test_flat(self):
        values = [100.0, 100.1, 100.0, 100.1, 100.0, 100.1, 100.0, 100.1]
        assert _compute_rs_trend(values) == "flat"

    def test_insufficient_data(self):
        assert _compute_rs_trend([100, 101]) == "insufficient_data"


# -- Tests for _compute_beta --


class TestComputeBeta:
    def test_insufficient_data(self):
        assert _compute_beta([100, 101], [200, 201]) is None

    def test_perfectly_correlated_same_pct_returns(self):
        # Both grow at same rate -> beta = 1
        base_t = 100.0
        base_b = 200.0
        rate = 1.01  # 1% per day
        ticker = [base_t * rate ** i for i in range(20)]
        bench = [base_b * rate ** i for i in range(20)]
        beta = _compute_beta(ticker, bench)
        assert beta is not None
        assert beta == pytest.approx(1.0, abs=0.01)

    def test_double_volatility(self):
        # Build prices from varying returns where ticker = 2x benchmark
        bench_rets = [0.01, -0.005, 0.008, -0.003, 0.012, -0.007, 0.009,
                      -0.004, 0.006, -0.002, 0.011, -0.008, 0.007, -0.001,
                      0.005, -0.006, 0.010, -0.003, 0.004]
        bench = [100.0]
        ticker = [100.0]
        for r in bench_rets:
            bench.append(bench[-1] * (1 + r))
            ticker.append(ticker[-1] * (1 + 2 * r))
        beta = _compute_beta(ticker, bench)
        assert beta is not None
        assert beta == pytest.approx(2.0, abs=0.05)

    def test_inverse_returns(self):
        # Ticker moves opposite to benchmark
        bench_rets = [0.01, -0.005, 0.008, -0.003, 0.012, -0.007, 0.009,
                      -0.004, 0.006, -0.002, 0.011, -0.008, 0.007, -0.001,
                      0.005, -0.006, 0.010, -0.003, 0.004]
        bench = [100.0]
        ticker = [100.0]
        for r in bench_rets:
            bench.append(bench[-1] * (1 + r))
            ticker.append(ticker[-1] * (1 - r))
        beta = _compute_beta(ticker, bench)
        assert beta is not None
        assert beta < 0

    def test_zero_variance_returns_none(self):
        # Benchmark constant -> variance=0 -> None
        ticker = [100 + i for i in range(10)]
        bench = [200.0] * 10
        beta = _compute_beta(ticker, bench)
        assert beta is None


# -- Tests for _sample_rs_line --


class TestSampleRSLine:
    def test_samples_down_to_max_points(self):
        n = 100
        base = date(2024, 1, 1)
        dates = [base + timedelta(days=i) for i in range(n)]
        rs_line = [100.0] * n
        t_norm = [100.0] * n
        b_norm = [100.0] * n
        result = _sample_rs_line(dates, rs_line, t_norm, b_norm, max_points=10)
        assert len(result) <= 12

    def test_always_includes_last_point(self):
        base = date(2024, 1, 1)
        dates = [base + timedelta(days=i) for i in range(50)]
        rs_line = [100.0 + i for i in range(50)]
        t_norm = [100.0] * 50
        b_norm = [100.0] * 50
        result = _sample_rs_line(dates, rs_line, t_norm, b_norm, max_points=5)
        assert result[-1]["date"] == str(dates[-1])

    def test_skips_none_values(self):
        base = date(2024, 1, 1)
        dates = [base + timedelta(days=i) for i in range(5)]
        rs_line = [None, 100.0, None, 102.0, 103.0]
        t_norm = [100.0] * 5
        b_norm = [100.0] * 5
        result = _sample_rs_line(dates, rs_line, t_norm, b_norm, max_points=50)
        for entry in result:
            assert entry["rs_ratio"] is not None


# -- Tests for get_weekly_monthly_candles (mocked DB) --


class TestGetWeeklyMonthlyCandles:
    @patch("mcp_server.tools.multi_timeframe.execute_query")
    def test_weekly_default_periods(self, mock_query):
        mock_query.return_value = []
        get_weekly_monthly_candles("AAPL", timeframe="weekly")

        mock_query.assert_called_once()
        _, kwargs = mock_query.call_args
        params = kwargs.get("params") or mock_query.call_args[0][1]
        assert params["ticker"] == "AAPL"
        assert params["periods"] == 52

    @patch("mcp_server.tools.multi_timeframe.execute_query")
    def test_monthly_default_periods(self, mock_query):
        mock_query.return_value = []
        get_weekly_monthly_candles("MSFT", timeframe="monthly")

        mock_query.assert_called_once()
        _, kwargs = mock_query.call_args
        params = kwargs.get("params") or mock_query.call_args[0][1]
        assert params["ticker"] == "MSFT"
        assert params["periods"] == 12

    @patch("mcp_server.tools.multi_timeframe.execute_query")
    def test_custom_periods_clamped(self, mock_query):
        mock_query.return_value = []
        get_weekly_monthly_candles("AAPL", timeframe="weekly", periods=9999)

        _, kwargs = mock_query.call_args
        params = kwargs.get("params") or mock_query.call_args[0][1]
        assert params["periods"] == 520

    @patch("mcp_server.tools.multi_timeframe.execute_query")
    def test_ticker_uppercased(self, mock_query):
        mock_query.return_value = []
        get_weekly_monthly_candles("aapl", timeframe="weekly")

        _, kwargs = mock_query.call_args
        params = kwargs.get("params") or mock_query.call_args[0][1]
        assert params["ticker"] == "AAPL"

    @patch("mcp_server.tools.multi_timeframe.execute_query")
    def test_returns_query_result(self, mock_query):
        mock_data = [
            {
                "period_start": date(2025, 1, 6),
                "open": 100,
                "high": 110,
                "low": 95,
                "close": 105,
                "volume": 5000000,
                "trading_days": 5,
                "change_pct": 5.0,
            }
        ]
        mock_query.return_value = mock_data
        result = get_weekly_monthly_candles("AAPL")
        assert result == mock_data


# -- Tests for get_multi_timeframe_alignment (mocked DB) --


class TestGetMultiTimeframeAlignment:
    @patch("mcp_server.tools.multi_timeframe.execute_query")
    def test_no_data_returns_error(self, mock_query):
        mock_query.return_value = []
        result = get_multi_timeframe_alignment("AAPL")
        assert result["ticker"] == "AAPL"
        assert "error" in result

    @patch("mcp_server.tools.multi_timeframe._fetch_aggregated_candles")
    @patch("mcp_server.tools.multi_timeframe.execute_query")
    def test_daily_only(self, mock_query, mock_fetch_candles):
        mock_query.return_value = [{
            "date": date(2025, 1, 15),
            "price": 150.0,
            "sma_20": 145.0,
            "sma_50": 140.0,
            "sma_200": 130.0,
            "rsi_14": 55.0,
            "macd_line": 1.5,
            "macd_signal": 1.0,
            "macd_histogram": 0.5,
        }]
        mock_fetch_candles.return_value = []

        result = get_multi_timeframe_alignment("AAPL", timeframes=["daily"])
        assert "daily" in result["timeframes"]
        assert result["timeframes"]["daily"]["direction"] == "bullish"

    @patch("mcp_server.tools.multi_timeframe._fetch_aggregated_candles")
    @patch("mcp_server.tools.multi_timeframe.execute_query")
    def test_all_timeframes_with_data(self, mock_query, mock_fetch_candles):
        mock_query.return_value = [{
            "date": date(2025, 1, 15),
            "price": 150.0,
            "sma_20": 145.0,
            "sma_50": 140.0,
            "sma_200": 130.0,
            "rsi_14": 55.0,
            "macd_line": 1.5,
            "macd_signal": 1.0,
            "macd_histogram": 0.5,
        }]

        mock_fetch_candles.return_value = _make_candles(60, start_price=100, trend=1.0)

        result = get_multi_timeframe_alignment("AAPL")
        assert "daily" in result["timeframes"]
        assert "weekly" in result["timeframes"]
        assert "monthly" in result["timeframes"]
        assert "alignment_score" in result
        assert -100 <= result["alignment_score"] <= 100

    @patch("mcp_server.tools.multi_timeframe._fetch_aggregated_candles")
    @patch("mcp_server.tools.multi_timeframe.execute_query")
    def test_ticker_uppercased(self, mock_query, mock_fetch_candles):
        mock_query.return_value = [{
            "date": date(2025, 1, 15),
            "price": 150.0,
            "sma_20": 145.0,
            "sma_50": 140.0,
            "sma_200": 130.0,
            "rsi_14": 55.0,
            "macd_line": 1.5,
            "macd_signal": 1.0,
            "macd_histogram": 0.5,
        }]
        mock_fetch_candles.return_value = []

        result = get_multi_timeframe_alignment("aapl", timeframes=["daily"])
        assert result["ticker"] == "AAPL"


# -- Tests for calculate_relative_strength (mocked DB) --


class TestCalculateRelativeStrength:
    @patch("mcp_server.tools.multi_timeframe.execute_query")
    def test_insufficient_data(self, mock_query):
        mock_query.return_value = [
            {"date": date(2025, 1, 2), "ticker_close": 100, "benchmark_close": 200}
        ]
        result = calculate_relative_strength("AAPL")
        assert "error" in result

    @patch("mcp_server.tools.multi_timeframe.execute_query")
    def test_basic_outperformance(self, mock_query):
        rows = _make_price_rows(60, ticker_start=100, bench_start=200,
                                ticker_trend=0.2, bench_trend=0.1)
        mock_query.return_value = rows

        result = calculate_relative_strength("AAPL", "SPY", lookback_days=90)
        assert result["ticker"] == "AAPL"
        assert result["benchmark"] == "SPY"
        assert result["ticker_return"] > result["benchmark_return"]
        assert result["relative_return"] > 0
        assert result["rs_trend"] in ("improving", "flat", "declining", "insufficient_data")
        assert result["beta"] is not None
        assert 0 <= result["outperformance_pct"] <= 100
        assert len(result["rs_line"]) > 0

    @patch("mcp_server.tools.multi_timeframe.execute_query")
    def test_underperformance(self, mock_query):
        rows = _make_price_rows(60, ticker_start=100, bench_start=200,
                                ticker_trend=0.0, bench_trend=0.5)
        mock_query.return_value = rows

        result = calculate_relative_strength("AAPL", "SPY")
        assert result["ticker_return"] < result["benchmark_return"]
        assert result["relative_return"] < 0
        assert result["rs_trend"] == "declining"

    @patch("mcp_server.tools.multi_timeframe.execute_query")
    def test_lookback_clamped(self, mock_query):
        mock_query.return_value = []
        result = calculate_relative_strength("AAPL", lookback_days=9999)
        assert "error" in result

    @patch("mcp_server.tools.multi_timeframe.execute_query")
    def test_tickers_uppercased(self, mock_query):
        mock_query.return_value = []
        result = calculate_relative_strength("aapl", benchmark="spy")
        assert result["ticker"] == "AAPL"
        assert result["benchmark"] == "SPY"

    @patch("mcp_server.tools.multi_timeframe.execute_query")
    def test_rs_line_contains_samples(self, mock_query):
        rows = _make_price_rows(100, ticker_start=100, bench_start=200,
                                ticker_trend=0.3, bench_trend=0.2)
        mock_query.return_value = rows

        result = calculate_relative_strength("AAPL", "SPY")
        rs_line = result["rs_line"]
        assert len(rs_line) > 0
        for entry in rs_line:
            assert "date" in entry
            assert "rs_ratio" in entry
            assert "ticker_normalized" in entry
            assert "benchmark_normalized" in entry

    @patch("mcp_server.tools.multi_timeframe.execute_query")
    def test_period_info(self, mock_query):
        rows = _make_price_rows(30, ticker_start=100, bench_start=200)
        mock_query.return_value = rows

        result = calculate_relative_strength("AAPL")
        assert "period" in result
        assert "start" in result["period"]
        assert "end" in result["period"]
        assert result["period"]["trading_days"] == 29
