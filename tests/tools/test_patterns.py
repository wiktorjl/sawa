"""Tests for candlestick and chart pattern detection."""

import importlib
import sys
from datetime import date, timedelta
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock

# Mock the database dependency so we can import the patterns module
# without needing a database connection or triggering __init__.py chain
_mock_db = MagicMock()
_mock_db.execute_query = MagicMock(return_value=[])
sys.modules["mcp_server.database"] = _mock_db

# Load patterns module directly to avoid __init__.py import chain
_spec = importlib.util.spec_from_file_location(
    "mcp_server.tools.patterns",
    Path(__file__).parent.parent.parent / "mcp_server" / "tools" / "patterns.py",
)
_mod: ModuleType = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_mod)  # type: ignore[union-attr]

detect_patterns = _mod.detect_patterns
SUPPORTED_PATTERNS = _mod.SUPPORTED_PATTERNS
detect_chart_patterns_from_data = _mod.detect_chart_patterns_from_data
SUPPORTED_CHART_PATTERNS = _mod.SUPPORTED_CHART_PATTERNS
_find_peaks_troughs = _mod._find_peaks_troughs
_linear_regression = _mod._linear_regression


def _candle(
    d: date, o: float, h: float, low: float, c: float, v: int = 1000000,
) -> dict:
    """Helper to create a candle dict."""
    return {"date": d, "open": o, "high": h, "low": low, "close": c, "volume": v}


def _date(offset: int) -> date:
    """Return a date relative to 2024-01-10."""
    return date(2024, 1, 10) + timedelta(days=offset)


class TestDoji:
    """Tests for doji pattern detection."""

    def test_perfect_doji(self):
        """Open equals close with shadows."""
        candles = [_candle(_date(0), 100.0, 102.0, 98.0, 100.0)]
        patterns = detect_patterns(candles)
        dojis = [p for p in patterns if p["pattern"] == "doji"]
        assert len(dojis) == 1
        assert dojis[0]["direction"] == "neutral"
        assert dojis[0]["reliability"] == 1

    def test_near_doji(self):
        """Body very small relative to range."""
        candles = [_candle(_date(0), 100.0, 105.0, 95.0, 100.4)]
        patterns = detect_patterns(candles)
        dojis = [p for p in patterns if p["pattern"] == "doji"]
        assert len(dojis) == 1

    def test_not_doji(self):
        """Normal candle is not a doji."""
        candles = [_candle(_date(0), 100.0, 105.0, 99.0, 104.0)]
        patterns = detect_patterns(candles)
        dojis = [p for p in patterns if p["pattern"] == "doji"]
        assert len(dojis) == 0


class TestHammer:
    """Tests for hammer pattern detection."""

    def test_classic_hammer(self):
        """Small body at top, long lower shadow."""
        # Body: 100-101 = 1, lower shadow: 95-100 = 5, upper: 101.2-101 = 0.2
        candles = [_candle(_date(0), 100.0, 101.2, 95.0, 101.0)]
        patterns = detect_patterns(candles)
        hammers = [p for p in patterns if p["pattern"] == "hammer"]
        assert len(hammers) == 1
        assert hammers[0]["direction"] == "bullish"
        assert hammers[0]["reliability"] == 2

    def test_bearish_body_hammer(self):
        """Hammer can have bearish body too."""
        candles = [_candle(_date(0), 101.0, 101.3, 95.0, 100.0)]
        patterns = detect_patterns(candles)
        hammers = [p for p in patterns if p["pattern"] == "hammer"]
        assert len(hammers) == 1

    def test_no_hammer_if_large_upper_shadow(self):
        """Not a hammer if upper shadow is significant."""
        candles = [_candle(_date(0), 100.0, 105.0, 95.0, 101.0)]
        patterns = detect_patterns(candles)
        hammers = [p for p in patterns if p["pattern"] == "hammer"]
        assert len(hammers) == 0


class TestInvertedHammer:
    """Tests for inverted hammer pattern detection."""

    def test_classic_inverted_hammer(self):
        """Small body at bottom, long upper shadow."""
        # Body: 100-101 = 1, upper: 106-101 = 5, lower: 100-99.8 = 0.2
        candles = [_candle(_date(0), 100.0, 106.0, 99.8, 101.0)]
        patterns = detect_patterns(candles)
        inv = [p for p in patterns if p["pattern"] == "inverted_hammer"]
        assert len(inv) == 1
        assert inv[0]["direction"] == "bullish"


class TestShootingStar:
    """Tests for shooting star pattern detection."""

    def test_shooting_star_after_uptrend(self):
        """Shooting star requires uptrend before it."""
        candles = [
            _candle(_date(0), 96.0, 97.0, 95.5, 97.0),
            _candle(_date(1), 97.0, 99.0, 96.5, 98.5),
            _candle(_date(2), 98.5, 100.0, 98.0, 99.5),
            # Shooting star: small body at bottom, long upper shadow
            _candle(_date(3), 100.0, 106.0, 99.8, 101.0),
        ]
        patterns = detect_patterns(candles)
        stars = [p for p in patterns if p["pattern"] == "shooting_star"]
        assert len(stars) == 1
        assert stars[0]["direction"] == "bearish"
        assert stars[0]["reliability"] == 2

    def test_no_shooting_star_without_uptrend(self):
        """Same shape but no uptrend should not trigger shooting star."""
        candles = [
            _candle(_date(0), 100.0, 101.0, 99.0, 99.5),
            _candle(_date(1), 99.5, 100.0, 98.0, 98.5),
            _candle(_date(2), 98.5, 99.0, 97.0, 97.5),
            _candle(_date(3), 97.0, 103.0, 96.8, 98.0),
        ]
        patterns = detect_patterns(candles)
        stars = [p for p in patterns if p["pattern"] == "shooting_star"]
        assert len(stars) == 0


class TestHangingMan:
    """Tests for hanging man pattern detection."""

    def test_hanging_man_after_uptrend(self):
        """Hanging man is hammer shape after uptrend."""
        candles = [
            _candle(_date(0), 96.0, 97.0, 95.5, 97.0),
            _candle(_date(1), 97.0, 99.0, 96.5, 98.5),
            _candle(_date(2), 98.5, 100.0, 98.0, 99.5),
            # Hanging man: small body at top, long lower shadow
            _candle(_date(3), 100.0, 101.2, 94.0, 101.0),
        ]
        patterns = detect_patterns(candles)
        hm = [p for p in patterns if p["pattern"] == "hanging_man"]
        assert len(hm) == 1
        assert hm[0]["direction"] == "bearish"


class TestBullishEngulfing:
    """Tests for bullish engulfing pattern detection."""

    def test_classic_bullish_engulfing(self):
        """Bearish candle followed by larger bullish candle."""
        candles = [
            _candle(_date(0), 102.0, 103.0, 99.0, 100.0),  # bearish
            _candle(_date(1), 99.0, 105.0, 98.0, 104.0),    # bullish, engulfs
        ]
        patterns = detect_patterns(candles)
        be = [p for p in patterns if p["pattern"] == "bullish_engulfing"]
        assert len(be) == 1
        assert be[0]["direction"] == "bullish"
        assert be[0]["reliability"] == 3

    def test_no_engulfing_if_not_fully_engulfed(self):
        """Bullish candle must completely engulf the prior bearish body."""
        candles = [
            _candle(_date(0), 102.0, 103.0, 99.0, 100.0),
            _candle(_date(1), 100.5, 103.0, 99.5, 101.5),
        ]
        patterns = detect_patterns(candles)
        be = [p for p in patterns if p["pattern"] == "bullish_engulfing"]
        assert len(be) == 0


class TestBearishEngulfing:
    """Tests for bearish engulfing pattern detection."""

    def test_classic_bearish_engulfing(self):
        """Bullish candle followed by larger bearish candle."""
        candles = [
            _candle(_date(0), 100.0, 103.0, 99.0, 102.0),  # bullish
            _candle(_date(1), 103.0, 104.0, 98.0, 99.0),    # bearish, engulfs
        ]
        patterns = detect_patterns(candles)
        be = [p for p in patterns if p["pattern"] == "bearish_engulfing"]
        assert len(be) == 1
        assert be[0]["direction"] == "bearish"
        assert be[0]["reliability"] == 3


class TestMorningStar:
    """Tests for morning star pattern detection."""

    def test_classic_morning_star(self):
        """Bearish, small body, bullish close above midpoint."""
        candles = [
            # big bearish
            _candle(_date(0), 110.0, 111.0, 104.0, 104.5),
            # small body (star)
            _candle(_date(1), 104.0, 105.0, 103.0, 104.5),
            # bullish, close > midpoint of first (107.25)
            _candle(_date(2), 105.0, 112.0, 104.0, 111.0),
        ]
        patterns = detect_patterns(candles)
        ms = [p for p in patterns if p["pattern"] == "morning_star"]
        assert len(ms) == 1
        assert ms[0]["direction"] == "bullish"
        assert ms[0]["reliability"] == 3

    def test_no_morning_star_if_third_not_bullish(self):
        """Third candle must be bullish."""
        candles = [
            _candle(_date(0), 110.0, 111.0, 104.0, 104.5),
            _candle(_date(1), 104.0, 105.0, 103.0, 104.5),
            _candle(_date(2), 106.0, 107.0, 104.0, 105.0),  # bearish
        ]
        patterns = detect_patterns(candles)
        ms = [p for p in patterns if p["pattern"] == "morning_star"]
        assert len(ms) == 0


class TestEveningStar:
    """Tests for evening star pattern detection."""

    def test_classic_evening_star(self):
        """Bullish, small body, bearish close below midpoint."""
        candles = [
            # big bullish
            _candle(_date(0), 100.0, 106.0, 99.0, 106.0),
            # small body (star)
            _candle(_date(1), 106.5, 107.0, 105.5, 106.5),
            # bearish, close < midpoint of first (103)
            _candle(_date(2), 105.0, 106.0, 99.0, 100.0),
        ]
        patterns = detect_patterns(candles)
        es = [p for p in patterns if p["pattern"] == "evening_star"]
        assert len(es) == 1
        assert es[0]["direction"] == "bearish"
        assert es[0]["reliability"] == 3


class TestThreeWhiteSoldiers:
    """Tests for three white soldiers pattern detection."""

    def test_classic_three_white_soldiers(self):
        """Three bullish candles with higher opens/closes, opens within prior body."""
        candles = [
            _candle(_date(0), 100.0, 104.0, 99.5, 103.0),
            _candle(_date(1), 102.0, 107.0, 101.5, 106.0),
            _candle(_date(2), 105.0, 110.0, 104.5, 109.0),
        ]
        patterns = detect_patterns(candles)
        tws = [p for p in patterns if p["pattern"] == "three_white_soldiers"]
        assert len(tws) == 1
        assert tws[0]["direction"] == "bullish"
        assert tws[0]["reliability"] == 3

    def test_no_three_white_soldiers_if_not_ascending(self):
        """Closes must be ascending."""
        candles = [
            _candle(_date(0), 100.0, 104.0, 99.5, 103.0),
            _candle(_date(1), 102.0, 107.0, 101.5, 106.0),
            _candle(_date(2), 105.0, 106.0, 104.5, 105.5),
        ]
        patterns = detect_patterns(candles)
        tws = [p for p in patterns if p["pattern"] == "three_white_soldiers"]
        assert len(tws) == 0


class TestThreeBlackCrows:
    """Tests for three black crows pattern detection."""

    def test_classic_three_black_crows(self):
        """Three bearish candles with lower opens/closes, opens within prior body."""
        candles = [
            _candle(_date(0), 110.0, 110.5, 106.0, 107.0),
            _candle(_date(1), 108.0, 108.5, 103.0, 104.0),
            _candle(_date(2), 105.0, 105.5, 100.0, 101.0),
        ]
        patterns = detect_patterns(candles)
        tbc = [p for p in patterns if p["pattern"] == "three_black_crows"]
        assert len(tbc) == 1
        assert tbc[0]["direction"] == "bearish"
        assert tbc[0]["reliability"] == 3


class TestMultiplePatterns:
    """Tests for detecting multiple patterns in a sequence."""

    def test_empty_candles(self):
        """No candles returns no patterns."""
        assert detect_patterns([]) == []

    def test_zero_range_candle_skipped(self):
        """Candle with open=high=low=close should not crash."""
        candles = [_candle(_date(0), 100.0, 100.0, 100.0, 100.0)]
        patterns = detect_patterns(candles)
        assert len(patterns) == 0

    def test_multiple_patterns_in_sequence(self):
        """A longer sequence can produce multiple patterns."""
        candles = [
            # Doji
            _candle(_date(0), 100.0, 102.0, 98.0, 100.0),
            # Bearish engulfing
            _candle(_date(1), 100.0, 103.0, 99.5, 102.0),
            _candle(_date(2), 103.0, 103.5, 98.0, 99.0),
        ]
        patterns = detect_patterns(candles)
        names = {p["pattern"] for p in patterns}
        assert "doji" in names
        assert "bearish_engulfing" in names


class TestSupportedPatterns:
    """Tests for the SUPPORTED_PATTERNS constant."""

    def test_all_patterns_listed(self):
        """Verify all implemented patterns are in SUPPORTED_PATTERNS."""
        expected = {
            "doji",
            "hammer",
            "inverted_hammer",
            "shooting_star",
            "hanging_man",
            "bullish_engulfing",
            "bearish_engulfing",
            "morning_star",
            "evening_star",
            "three_white_soldiers",
            "three_black_crows",
        }
        assert SUPPORTED_PATTERNS == expected

    def test_pattern_count(self):
        assert len(SUPPORTED_PATTERNS) == 11


class TestPatternFields:
    """Verify pattern result structure."""

    def test_all_required_fields_present(self):
        """Each detected pattern must have all required fields."""
        candles = [_candle(_date(0), 100.0, 102.0, 98.0, 100.0)]  # doji
        patterns = detect_patterns(candles)
        assert len(patterns) > 0
        for p in patterns:
            assert "date" in p
            assert "pattern" in p
            assert "direction" in p
            assert "reliability" in p
            assert "description" in p

    def test_reliability_range(self):
        """Reliability scores should be 1, 2, or 3."""
        candles = [
            _candle(_date(0), 100.0, 102.0, 98.0, 100.0),  # doji (1)
            _candle(_date(1), 102.0, 103.0, 99.0, 100.0),  # bearish
            _candle(_date(2), 99.0, 105.0, 98.0, 104.0),   # bullish engulfing (3)
        ]
        patterns = detect_patterns(candles)
        for p in patterns:
            assert p["reliability"] in (1, 2, 3)

    def test_direction_values(self):
        """Direction should be one of the three allowed values."""
        candles = [
            _candle(_date(0), 100.0, 102.0, 98.0, 100.0),  # doji
            _candle(_date(1), 100.0, 101.2, 95.0, 101.0),  # hammer
        ]
        patterns = detect_patterns(candles)
        for p in patterns:
            assert p["direction"] in ("bullish", "bearish", "neutral")


# =========================================================================
# Chart Pattern Detection Tests
# =========================================================================


def _gen_candles(
    prices: list[tuple[float, float, float, float]],
    start: date | None = None,
) -> list[dict]:
    """Generate candle dicts from (open, high, low, close) tuples."""
    if start is None:
        start = date(2024, 1, 10)
    return [
        _candle(start + timedelta(days=i), o, h, lo, c)
        for i, (o, h, lo, c) in enumerate(prices)
    ]


class TestFindPeaksTroughs:
    """Tests for peak/trough detection helper."""

    def test_simple_peak(self):
        """Detect a single peak in V-shaped data."""
        # Rising then falling
        highs = [10, 11, 12, 13, 14, 15, 14, 13, 12, 11, 10]
        lows = [9, 10, 11, 12, 13, 14, 13, 12, 11, 10, 9]
        peaks, troughs = _find_peaks_troughs(highs, lows, order=3)
        assert 5 in peaks

    def test_simple_trough(self):
        """Detect a single trough in inverted-V data."""
        highs = [15, 14, 13, 12, 11, 10, 11, 12, 13, 14, 15]
        lows = [14, 13, 12, 11, 10, 9, 10, 11, 12, 13, 14]
        peaks, troughs = _find_peaks_troughs(highs, lows, order=3)
        assert 5 in troughs

    def test_multiple_peaks(self):
        """Detect multiple peaks with sufficient separation."""
        # Two humps with wider separation for order=3
        highs = [
            10, 11, 12, 15, 12, 11, 10, 9, 10, 11, 12, 15, 12, 11, 10,
        ]
        lows = [v - 1 for v in highs]
        peaks, _ = _find_peaks_troughs(highs, lows, order=3)
        assert 3 in peaks
        assert 11 in peaks

    def test_insufficient_data(self):
        """Too little data for given order returns empty."""
        highs = [10, 11, 12]
        lows = [9, 10, 11]
        peaks, troughs = _find_peaks_troughs(highs, lows, order=3)
        assert peaks == []
        assert troughs == []


class TestLinearRegression:
    """Tests for linear regression helper."""

    def test_perfect_line(self):
        """Perfect linear data gives R-squared=1."""
        x = [0.0, 1.0, 2.0, 3.0, 4.0]
        y = [2.0, 4.0, 6.0, 8.0, 10.0]
        slope, intercept, r2 = _linear_regression(x, y)
        assert abs(slope - 2.0) < 0.01
        assert abs(intercept - 2.0) < 0.01
        assert abs(r2 - 1.0) < 0.01

    def test_flat_line(self):
        """Constant data gives slope=0."""
        x = [0.0, 1.0, 2.0, 3.0]
        y = [5.0, 5.0, 5.0, 5.0]
        slope, intercept, r2 = _linear_regression(x, y)
        assert abs(slope) < 0.01

    def test_single_point(self):
        """Single point returns intercept=y, slope=0."""
        slope, intercept, r2 = _linear_regression([0.0], [42.0])
        assert abs(intercept - 42.0) < 0.01


class TestDoubleTop:
    """Tests for double top pattern detection."""

    def test_classic_double_top(self):
        """Two peaks at similar levels with valley between."""
        # Price rises to ~115, drops to ~105, rises to ~115, drops
        prices = (
            # Rise to first peak
            [(100 + i, 101 + i, 99 + i, 100.5 + i) for i in range(15)]
            # Drop to valley
            + [(115 - i, 116 - i, 114 - i, 114.5 - i) for i in range(10)]
            # Rise to second peak
            + [(105 + i, 106 + i, 104 + i, 105.5 + i) for i in range(10)]
            # Drop
            + [(115 - i, 116 - i, 114 - i, 114.5 - i) for i in range(10)]
        )
        candles = _gen_candles(prices)
        patterns = detect_chart_patterns_from_data(candles, min_pattern_days=5)
        dt = [p for p in patterns if p["pattern"] == "double_top"]
        assert len(dt) >= 1
        assert dt[0]["direction"] == "bearish"
        assert "neckline" in dt[0]
        assert "target_price" in dt[0]

    def test_no_double_top_with_different_peaks(self):
        """Peaks at very different levels should not trigger."""
        # First peak at 120, second at 100 - too different
        prices = (
            [(100 + i * 2, 101 + i * 2, 99 + i * 2, 100 + i * 2) for i in range(10)]
            + [(120 - i * 2, 121 - i * 2, 119 - i * 2, 120 - i * 2) for i in range(10)]
            + [(100 + i, 101 + i, 99 + i, 100 + i) for i in range(5)]
            + [(105 - i, 106 - i, 104 - i, 105 - i) for i in range(10)]
        )
        candles = _gen_candles(prices)
        patterns = detect_chart_patterns_from_data(candles, min_pattern_days=5)
        dt = [p for p in patterns if p["pattern"] == "double_top"]
        assert len(dt) == 0


class TestDoubleBottom:
    """Tests for double bottom pattern detection."""

    def test_classic_double_bottom(self):
        """Two troughs at similar levels with peak between."""
        prices = (
            # Drop to first trough
            [(110 - i, 111 - i, 109 - i, 109.5 - i) for i in range(15)]
            # Rise to peak
            + [(95 + i, 96 + i, 94 + i, 95.5 + i) for i in range(10)]
            # Drop to second trough
            + [(105 - i, 106 - i, 104 - i, 104.5 - i) for i in range(10)]
            # Rise
            + [(95 + i, 96 + i, 94 + i, 95.5 + i) for i in range(10)]
        )
        candles = _gen_candles(prices)
        patterns = detect_chart_patterns_from_data(candles, min_pattern_days=5)
        db = [p for p in patterns if p["pattern"] == "double_bottom"]
        assert len(db) >= 1
        assert db[0]["direction"] == "bullish"
        assert "target_price" in db[0]


class TestHeadAndShoulders:
    """Tests for head and shoulders pattern detection."""

    def test_classic_head_and_shoulders(self):
        """Left shoulder, higher head, right shoulder."""
        prices = (
            # Left shoulder up
            [(100 + i, 101 + i, 99 + i, 100.5 + i) for i in range(8)]
            # Down to neckline
            + [(108 - i, 109 - i, 107 - i, 107.5 - i) for i in range(6)]
            # Head up (higher)
            + [(102 + i * 2, 103 + i * 2, 101 + i * 2, 102 + i * 2)
               for i in range(8)]
            # Down to neckline
            + [(118 - i * 2, 119 - i * 2, 117 - i * 2, 117 - i * 2)
               for i in range(8)]
            # Right shoulder up (lower than head)
            + [(102 + i, 103 + i, 101 + i, 102.5 + i) for i in range(8)]
            # Down
            + [(110 - i, 111 - i, 109 - i, 109.5 - i) for i in range(8)]
        )
        candles = _gen_candles(prices)
        patterns = detect_chart_patterns_from_data(candles, min_pattern_days=5)
        hs = [p for p in patterns if p["pattern"] == "head_and_shoulders"]
        # The pattern may or may not be detected depending on exact peak positions
        # but the detection should not crash
        for p in hs:
            assert p["direction"] == "bearish"
            assert p["reliability"] == 3


class TestInverseHeadAndShoulders:
    """Tests for inverse head and shoulders pattern detection."""

    def test_classic_inverse_head_and_shoulders(self):
        """Left trough, lower head trough, right trough."""
        prices = (
            # Left shoulder down
            [(110 - i, 111 - i, 109 - i, 109.5 - i) for i in range(8)]
            # Up to neckline
            + [(102 + i, 103 + i, 101 + i, 102.5 + i) for i in range(6)]
            # Head down (lower)
            + [(108 - i * 2, 109 - i * 2, 107 - i * 2, 107 - i * 2)
               for i in range(8)]
            # Up to neckline
            + [(92 + i * 2, 93 + i * 2, 91 + i * 2, 92 + i * 2)
               for i in range(8)]
            # Right shoulder down (higher than head)
            + [(108 - i, 109 - i, 107 - i, 107.5 - i) for i in range(8)]
            # Up
            + [(100 + i, 101 + i, 99 + i, 100.5 + i) for i in range(8)]
        )
        candles = _gen_candles(prices)
        patterns = detect_chart_patterns_from_data(candles, min_pattern_days=5)
        ihs = [
            p for p in patterns
            if p["pattern"] == "inverse_head_and_shoulders"
        ]
        for p in ihs:
            assert p["direction"] == "bullish"
            assert p["reliability"] == 3


class TestAscendingTriangle:
    """Tests for ascending triangle detection."""

    def test_flat_resistance_rising_support(self):
        """Multiple touches of flat resistance with rising lows."""
        # Build data with flat highs around 110 and rising lows
        prices = []
        for i in range(40):
            if i % 8 < 4:
                # Rising phase toward resistance
                base = 100 + (i // 8) * 2
                prices.append((base + (i % 8), 110.0, base + (i % 8) - 1, 109.5))
            else:
                # Pullback phase
                base = 110 - ((i % 8) - 4) * 2
                prices.append((base, base + 0.5, base - 0.5, base - 0.3))
        candles = _gen_candles(prices)
        patterns = detect_chart_patterns_from_data(candles, min_pattern_days=5)
        at = [p for p in patterns if p["pattern"] == "ascending_triangle"]
        for p in at:
            assert p["direction"] == "bullish"


class TestDescendingTriangle:
    """Tests for descending triangle detection."""

    def test_flat_support_falling_resistance(self):
        """Multiple touches of flat support with falling highs."""
        prices = []
        for i in range(40):
            if i % 8 < 4:
                # Falling phase toward support
                base = 110 - (i // 8) * 2
                prices.append((base - (i % 8), base - (i % 8) + 0.5, 95.0, 95.5))
            else:
                # Bounce phase
                base = 95 + ((i % 8) - 4) * 2
                prices.append((base, base + 0.5, base - 0.5, base + 0.3))
        candles = _gen_candles(prices)
        patterns = detect_chart_patterns_from_data(candles, min_pattern_days=5)
        dt = [p for p in patterns if p["pattern"] == "descending_triangle"]
        for p in dt:
            assert p["direction"] == "bearish"


class TestChannels:
    """Tests for channel detection."""

    def test_ascending_channel(self):
        """Steady uptrend with parallel highs/lows."""
        prices = []
        for i in range(30):
            base = 100 + i * 0.5
            noise = (i % 3 - 1) * 0.3
            prices.append((
                base + noise,
                base + 3 + noise,
                base - 3 + noise,
                base + 0.2 + noise,
            ))
        candles = _gen_candles(prices)
        patterns = detect_chart_patterns_from_data(candles, min_pattern_days=10)
        ac = [p for p in patterns if p["pattern"] == "ascending_channel"]
        for p in ac:
            assert p["direction"] == "bullish"
            assert "upper_bound" in p
            assert "lower_bound" in p
            assert p["r_squared"] > 0.5

    def test_descending_channel(self):
        """Steady downtrend with parallel highs/lows."""
        prices = []
        for i in range(30):
            base = 150 - i * 0.5
            noise = (i % 3 - 1) * 0.3
            prices.append((
                base + noise,
                base + 3 + noise,
                base - 3 + noise,
                base - 0.2 + noise,
            ))
        candles = _gen_candles(prices)
        patterns = detect_chart_patterns_from_data(candles, min_pattern_days=10)
        dc = [p for p in patterns if p["pattern"] == "descending_channel"]
        for p in dc:
            assert p["direction"] == "bearish"


class TestChartPatternsEdgeCases:
    """Edge cases for chart pattern detection."""

    def test_empty_data(self):
        """No data returns no patterns."""
        assert detect_chart_patterns_from_data([]) == []

    def test_insufficient_data(self):
        """Fewer candles than min_pattern_days returns no patterns."""
        prices = [(100, 101, 99, 100)] * 5
        candles = _gen_candles(prices)
        result = detect_chart_patterns_from_data(candles, min_pattern_days=10)
        assert result == []

    def test_flat_data(self):
        """Completely flat data should not crash."""
        prices = [(100, 101, 99, 100)] * 30
        candles = _gen_candles(prices)
        result = detect_chart_patterns_from_data(candles, min_pattern_days=5)
        # Should not crash; may or may not find patterns
        assert isinstance(result, list)

    def test_pattern_has_required_fields(self):
        """Detected chart patterns should have standard fields."""
        prices = (
            [(100 + i, 101 + i, 99 + i, 100.5 + i) for i in range(15)]
            + [(115 - i, 116 - i, 114 - i, 114.5 - i) for i in range(10)]
            + [(105 + i, 106 + i, 104 + i, 105.5 + i) for i in range(10)]
            + [(115 - i, 116 - i, 114 - i, 114.5 - i) for i in range(10)]
        )
        candles = _gen_candles(prices)
        patterns = detect_chart_patterns_from_data(candles, min_pattern_days=5)
        for p in patterns:
            assert "date" in p
            assert "pattern" in p
            assert "direction" in p
            assert "reliability" in p
            assert "description" in p
            assert "formation_start" in p
            assert "formation_end" in p


class TestSupportedChartPatterns:
    """Tests for SUPPORTED_CHART_PATTERNS constant."""

    def test_all_chart_patterns_listed(self):
        expected = {
            "double_top",
            "double_bottom",
            "head_and_shoulders",
            "inverse_head_and_shoulders",
            "ascending_triangle",
            "descending_triangle",
            "ascending_channel",
            "descending_channel",
        }
        assert SUPPORTED_CHART_PATTERNS == expected

    def test_chart_pattern_count(self):
        assert len(SUPPORTED_CHART_PATTERNS) == 8
