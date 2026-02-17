"""Tests for volume analysis MCP tools."""

from datetime import date, timedelta
from unittest.mock import patch

from mcp_server.tools.volume_analysis import (
    _generate_volume_signals,
    detect_volume_anomalies,
    get_advanced_volume_indicators,
    get_volume_profile,
)


def _make_price_rows(
    n: int = 60,
    base_price: float = 100.0,
    base_volume: int = 1_000_000,
    start_date: date | None = None,
    price_pattern: str = "flat",
    volume_pattern: str = "normal",
) -> list[dict]:
    """Generate synthetic price data for testing.

    Args:
        n: Number of rows to generate
        base_price: Starting price
        base_volume: Base daily volume
        start_date: First date (defaults to n trading days ago)
        price_pattern: "flat", "up", "down", or "volatile"
        volume_pattern: "normal", "spike_at_end", "declining"
    """
    if start_date is None:
        start_date = date.today() - timedelta(days=int(n * 1.5))

    rows = []
    price = base_price
    d = start_date

    for i in range(n):
        # Price movement
        if price_pattern == "up":
            price = base_price + i * 0.5
        elif price_pattern == "down":
            price = base_price - i * 0.3
        elif price_pattern == "volatile":
            price = base_price + (5 if i % 2 == 0 else -5)
        # else "flat"

        # Volume pattern
        vol = base_volume
        if volume_pattern == "spike_at_end" and i >= n - 3:
            vol = base_volume * 5
        elif volume_pattern == "declining":
            vol = max(base_volume // (1 + i // 10), 100_000)

        high = price + 1.0
        low = price - 1.0
        open_p = price - 0.2

        rows.append({
            "date": d,
            "open": open_p,
            "high": high,
            "low": low,
            "close": price,
            "volume": vol,
        })
        d += timedelta(days=1)
        # Skip weekends
        while d.weekday() >= 5:
            d += timedelta(days=1)

    return rows


class TestGetVolumeProfile:
    """Tests for get_volume_profile."""

    @patch("mcp_server.tools.volume_analysis.execute_query")
    def test_basic_volume_profile(self, mock_query):
        """Volume profile returns bins with POC and value area."""
        rows = _make_price_rows(30, base_price=100.0)
        mock_query.return_value = rows

        result = get_volume_profile("AAPL", lookback_days=30, price_bins=10)

        assert result["ticker"] == "AAPL"
        assert len(result["bins"]) == 10
        assert result["total_volume"] > 0
        assert result["poc"]["price"] is not None
        assert result["poc"]["volume"] > 0
        assert result["value_area_low"] <= result["value_area_high"]
        assert result["price_low"] <= result["price_high"]

        # Bins should cover the full price range
        assert result["bins"][0]["price_low"] == result["price_low"]
        assert result["bins"][-1]["price_high"] == result["price_high"]

    @patch("mcp_server.tools.volume_analysis.execute_query")
    def test_volume_profile_pct_sums_to_100(self, mock_query):
        """Bin percentages should approximately sum to 100."""
        rows = _make_price_rows(30, base_price=50.0)
        mock_query.return_value = rows

        result = get_volume_profile("MSFT", lookback_days=30, price_bins=20)

        total_pct = sum(b["pct_of_total"] for b in result["bins"])
        # Allow small rounding error due to integer volume distribution
        assert 95.0 <= total_pct <= 105.0

    @patch("mcp_server.tools.volume_analysis.execute_query")
    def test_volume_profile_empty_data(self, mock_query):
        """Returns error dict when no data available."""
        mock_query.return_value = []

        result = get_volume_profile("XYZ")

        assert result["ticker"] == "XYZ"
        assert "error" in result
        assert result["bins"] == []

    @patch("mcp_server.tools.volume_analysis.execute_query")
    def test_volume_profile_clamps_params(self, mock_query):
        """Parameters are clamped to valid ranges."""
        rows = _make_price_rows(10)
        mock_query.return_value = rows

        # lookback_days clamped to 252
        result = get_volume_profile("AAPL", lookback_days=500, price_bins=100)
        assert len(result["bins"]) == 50  # clamped to max

    @patch("mcp_server.tools.volume_analysis.execute_query")
    def test_volume_profile_flat_price(self, mock_query):
        """Handles case where all prices are the same."""
        rows = []
        d = date.today() - timedelta(days=10)
        for i in range(5):
            rows.append({
                "date": d + timedelta(days=i),
                "open": 100.0,
                "high": 100.0,
                "low": 100.0,
                "close": 100.0,
                "volume": 1_000_000,
            })
        mock_query.return_value = rows

        result = get_volume_profile("FLAT", lookback_days=5, price_bins=10)

        # Should not crash even with zero range
        assert result["ticker"] == "FLAT"
        assert len(result["bins"]) == 10

    @patch("mcp_server.tools.volume_analysis.execute_query")
    def test_volume_profile_value_area_contains_poc(self, mock_query):
        """Value area should contain the POC price."""
        rows = _make_price_rows(30, base_price=100.0)
        mock_query.return_value = rows

        result = get_volume_profile("AAPL", lookback_days=30, price_bins=20)

        poc_price = result["poc"]["price"]
        assert result["value_area_low"] <= poc_price <= result["value_area_high"]

    @patch("mcp_server.tools.volume_analysis.execute_query")
    def test_ticker_normalized_to_uppercase(self, mock_query):
        """Ticker in result should always be uppercase."""
        mock_query.return_value = _make_price_rows(10)

        result = get_volume_profile("aapl")
        assert result["ticker"] == "AAPL"


class TestDetectVolumeAnomalies:
    """Tests for detect_volume_anomalies."""

    @patch("mcp_server.tools.volume_analysis.execute_query")
    def test_detects_volume_spike(self, mock_query):
        """Should detect days where volume is above threshold."""
        rows = _make_price_rows(60, volume_pattern="spike_at_end")
        mock_query.return_value = rows

        result = detect_volume_anomalies("AAPL", lookback_days=60, threshold_multiplier=2.0)

        assert result["ticker"] == "AAPL"
        assert len(result["volume_spikes"]) > 0
        assert result["summary"]["spike_count"] > 0

        # Verify spike entries have required fields
        spike = result["volume_spikes"][0]
        assert "date" in spike
        assert "volume" in spike
        assert "volume_ratio" in spike
        assert spike["volume_ratio"] >= 2.0

    @patch("mcp_server.tools.volume_analysis.execute_query")
    def test_detects_volume_drops(self, mock_query):
        """Should detect days with unusually low volume."""
        # Create data with a very low volume day
        rows = _make_price_rows(60)
        # Set a few days to very low volume
        for i in range(45, 48):
            rows[i]["volume"] = 50_000  # much lower than 1M base

        mock_query.return_value = rows

        result = detect_volume_anomalies("AAPL", lookback_days=40, threshold_multiplier=2.0)

        assert result["ticker"] == "AAPL"
        assert len(result["volume_drops"]) > 0

    @patch("mcp_server.tools.volume_analysis.execute_query")
    def test_insufficient_data(self, mock_query):
        """Returns error when not enough data."""
        mock_query.return_value = _make_price_rows(10)

        result = detect_volume_anomalies("XYZ", lookback_days=90)

        assert "error" in result
        assert result["volume_spikes"] == []
        assert result["volume_drops"] == []
        assert result["divergences"] == []

    @patch("mcp_server.tools.volume_analysis.execute_query")
    def test_detects_climactic_selling(self, mock_query):
        """Should detect big price drop on high volume."""
        rows = _make_price_rows(60, base_price=100.0)
        # Simulate a crash day
        crash_idx = 55
        rows[crash_idx]["close"] = rows[crash_idx - 1]["close"] * 0.95  # -5%
        rows[crash_idx]["low"] = rows[crash_idx]["close"] - 1
        rows[crash_idx]["volume"] = 5_000_000  # 5x normal

        mock_query.return_value = rows

        result = detect_volume_anomalies("AAPL", lookback_days=40, threshold_multiplier=2.0)

        climactic = [d for d in result["divergences"] if d["type"] == "climactic_selling"]
        assert len(climactic) > 0

    @patch("mcp_server.tools.volume_analysis.execute_query")
    def test_threshold_clamped_minimum(self, mock_query):
        """Threshold multiplier should be at least 1.1."""
        rows = _make_price_rows(60)
        mock_query.return_value = rows

        result = detect_volume_anomalies("AAPL", threshold_multiplier=0.5)

        assert result["threshold_multiplier"] == 1.1

    @patch("mcp_server.tools.volume_analysis.execute_query")
    def test_summary_counts_match(self, mock_query):
        """Summary counts should match list lengths."""
        rows = _make_price_rows(60, volume_pattern="spike_at_end")
        mock_query.return_value = rows

        result = detect_volume_anomalies("AAPL", lookback_days=60)

        assert result["summary"]["spike_count"] == len(result["volume_spikes"])
        assert result["summary"]["drop_count"] == len(result["volume_drops"])
        assert result["summary"]["divergence_count"] == len(result["divergences"])

    @patch("mcp_server.tools.volume_analysis.execute_query")
    def test_no_anomalies_on_steady_volume(self, mock_query):
        """Steady volume should produce no spikes or drops."""
        rows = _make_price_rows(60, base_volume=1_000_000)
        mock_query.return_value = rows

        result = detect_volume_anomalies("AAPL", lookback_days=30, threshold_multiplier=2.0)

        assert result["summary"]["spike_count"] == 0
        assert result["summary"]["drop_count"] == 0


class TestGetAdvancedVolumeIndicators:
    """Tests for get_advanced_volume_indicators."""

    @patch("mcp_server.tools.volume_analysis.execute_query")
    def test_basic_indicators(self, mock_query):
        """Should calculate OBV, A/D line, CMF, and VWAP."""
        rows = _make_price_rows(60, price_pattern="up")
        mock_query.return_value = rows

        result = get_advanced_volume_indicators("AAPL", lookback_days=30)

        assert result["ticker"] == "AAPL"
        assert len(result["indicators"]) == 30
        assert result["latest"] is not None

        latest = result["latest"]
        assert "obv" in latest
        assert "ad_line" in latest
        assert "cmf_20" in latest
        assert "vwap" in latest

    @patch("mcp_server.tools.volume_analysis.execute_query")
    def test_obv_increases_on_up_days(self, mock_query):
        """OBV should increase when price rises."""
        rows = _make_price_rows(40, price_pattern="up")
        mock_query.return_value = rows

        result = get_advanced_volume_indicators("AAPL", lookback_days=30)

        indicators = result["indicators"]
        # OBV should generally be increasing in an uptrend
        first_obv = indicators[0]["obv"]
        last_obv = indicators[-1]["obv"]
        assert last_obv > first_obv

    @patch("mcp_server.tools.volume_analysis.execute_query")
    def test_obv_decreases_on_down_days(self, mock_query):
        """OBV should decrease when price falls."""
        rows = _make_price_rows(40, price_pattern="down")
        mock_query.return_value = rows

        result = get_advanced_volume_indicators("AAPL", lookback_days=30)

        indicators = result["indicators"]
        first_obv = indicators[0]["obv"]
        last_obv = indicators[-1]["obv"]
        assert last_obv < first_obv

    @patch("mcp_server.tools.volume_analysis.execute_query")
    def test_cmf_has_values_after_warmup(self, mock_query):
        """CMF requires 20-day warmup, should have values."""
        rows = _make_price_rows(60)
        mock_query.return_value = rows

        result = get_advanced_volume_indicators("AAPL", lookback_days=30)

        # All output indicators should have CMF values since warmup data is available
        for ind in result["indicators"]:
            assert ind["cmf_20"] is not None

    @patch("mcp_server.tools.volume_analysis.execute_query")
    def test_vwap_is_positive(self, mock_query):
        """VWAP should be positive."""
        rows = _make_price_rows(60, base_price=100.0)
        mock_query.return_value = rows

        result = get_advanced_volume_indicators("AAPL", lookback_days=30)

        for ind in result["indicators"]:
            assert ind["vwap"] is not None
            assert ind["vwap"] > 0

    @patch("mcp_server.tools.volume_analysis.execute_query")
    def test_insufficient_data(self, mock_query):
        """Returns error when not enough data."""
        mock_query.return_value = [
            {
                "date": date.today(), "open": 100, "high": 101,
                "low": 99, "close": 100, "volume": 1000,
            }
        ]

        result = get_advanced_volume_indicators("XYZ")

        assert "error" in result
        assert result["indicators"] == []

    @patch("mcp_server.tools.volume_analysis.execute_query")
    def test_signals_generated(self, mock_query):
        """Should generate interpretation signals."""
        rows = _make_price_rows(60, price_pattern="up")
        mock_query.return_value = rows

        result = get_advanced_volume_indicators("AAPL", lookback_days=30)

        assert "signals" in result
        assert len(result["signals"]) > 0

        for signal in result["signals"]:
            assert "indicator" in signal
            assert "signal" in signal
            assert "description" in signal

    @patch("mcp_server.tools.volume_analysis.execute_query")
    def test_lookback_clamp(self, mock_query):
        """lookback_days should be clamped to [5, 252]."""
        rows = _make_price_rows(10)
        mock_query.return_value = rows

        result = get_advanced_volume_indicators("AAPL", lookback_days=1)
        # Clamped to 5, but only 10 rows available, so result limited by data
        assert result["ticker"] == "AAPL"

    @patch("mcp_server.tools.volume_analysis.execute_query")
    def test_ad_line_calculation(self, mock_query):
        """A/D line should be calculated correctly for known values."""
        # Create simple test: close at high means max accumulation
        rows = []
        d = date.today() - timedelta(days=10)
        for i in range(5):
            rows.append({
                "date": d + timedelta(days=i),
                "open": 99.0,
                "high": 102.0,
                "low": 98.0,
                "close": 102.0,  # Close at high -> MFM = 1.0
                "volume": 1_000_000,
            })
        mock_query.return_value = rows

        result = get_advanced_volume_indicators("TEST", lookback_days=5)

        # When close == high, MFM = ((close-low) - (high-close)) / (high-low)
        # = ((102-98) - (102-102)) / (102-98) = 4/4 = 1.0
        # A/D line should be accumulating (positive and growing)
        indicators = result["indicators"]
        assert indicators[-1]["ad_line"] > 0


class TestGenerateVolumeSignals:
    """Tests for _generate_volume_signals helper."""

    def test_empty_indicators(self):
        """Returns empty signals for insufficient data."""
        signals = _generate_volume_signals([], [])
        assert signals == []

    def test_bullish_obv_trend(self):
        """OBV rising with price rising should produce bullish signal."""
        indicators = []
        for i in range(10):
            indicators.append({
                "date": str(date.today() - timedelta(days=10 - i)),
                "close": 100 + i,
                "volume": 1_000_000,
                "obv": 1_000_000 * (i + 1),
                "ad_line": 500_000 * i,
                "cmf_20": 0.15,
                "vwap": 104.0,
            })
        closes = [100 + i for i in range(10)]

        signals = _generate_volume_signals(indicators, closes)

        obv_signals = [s for s in signals if s["indicator"] == "OBV"]
        assert len(obv_signals) == 1
        assert obv_signals[0]["signal"] == "bullish"

    def test_cmf_strong_buying(self):
        """CMF > 0.1 should signal strong buying."""
        indicators = []
        for i in range(10):
            indicators.append({
                "date": str(date.today() - timedelta(days=10 - i)),
                "close": 100.0,
                "volume": 1_000_000,
                "obv": 1_000_000,
                "ad_line": 500_000,
                "cmf_20": 0.2,
                "vwap": 99.0,
            })
        closes = [100.0] * 10

        signals = _generate_volume_signals(indicators, closes)

        cmf_signals = [s for s in signals if s["indicator"] == "CMF"]
        assert len(cmf_signals) == 1
        assert cmf_signals[0]["signal"] == "strong_buying"

    def test_vwap_above_signal(self):
        """Price above VWAP should produce above_vwap signal."""
        indicators = []
        for i in range(10):
            indicators.append({
                "date": str(date.today() - timedelta(days=10 - i)),
                "close": 110.0,
                "volume": 1_000_000,
                "obv": 1_000_000,
                "ad_line": 500_000,
                "cmf_20": 0.05,
                "vwap": 100.0,
            })
        closes = [110.0] * 10

        signals = _generate_volume_signals(indicators, closes)

        vwap_signals = [s for s in signals if s["indicator"] == "VWAP"]
        assert len(vwap_signals) == 1
        assert vwap_signals[0]["signal"] == "above_vwap"

    def test_vwap_below_signal(self):
        """Price below VWAP should produce below_vwap signal."""
        indicators = []
        for i in range(10):
            indicators.append({
                "date": str(date.today() - timedelta(days=10 - i)),
                "close": 90.0,
                "volume": 1_000_000,
                "obv": 1_000_000,
                "ad_line": 500_000,
                "cmf_20": -0.05,
                "vwap": 100.0,
            })
        closes = [90.0] * 10

        signals = _generate_volume_signals(indicators, closes)

        vwap_signals = [s for s in signals if s["indicator"] == "VWAP"]
        assert len(vwap_signals) == 1
        assert vwap_signals[0]["signal"] == "below_vwap"

    def test_cmf_strong_selling(self):
        """CMF < -0.1 should signal strong selling."""
        indicators = []
        for i in range(10):
            indicators.append({
                "date": str(date.today() - timedelta(days=10 - i)),
                "close": 100.0,
                "volume": 1_000_000,
                "obv": 1_000_000,
                "ad_line": -500_000,
                "cmf_20": -0.2,
                "vwap": 101.0,
            })
        closes = [100.0] * 10

        signals = _generate_volume_signals(indicators, closes)

        cmf_signals = [s for s in signals if s["indicator"] == "CMF"]
        assert len(cmf_signals) == 1
        assert cmf_signals[0]["signal"] == "strong_selling"
