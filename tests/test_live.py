"""Tests for sawa.live module."""

from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest

from sawa.live import get_live_price, get_live_prices_batch


def _make_bar(date_str: str, close: float, open_: float = 100.0) -> dict:
    """Create a mock OHLCV bar."""
    ts = int(datetime.strptime(date_str, "%Y-%m-%d").timestamp() * 1000)
    return {
        "t": ts,
        "o": open_,
        "h": close + 1,
        "l": close - 1,
        "c": close,
        "v": 1000000,
    }


FIVE_BARS = [
    _make_bar("2026-02-20", 100.0),
    _make_bar("2026-02-21", 102.0),
    _make_bar("2026-02-23", 105.0),
    _make_bar("2026-02-24", 103.0),
    _make_bar("2026-02-25", 106.0),
]


class TestGetLivePrice:
    """Tests for get_live_price."""

    @pytest.mark.asyncio
    async def test_change_percent_uses_previous_day(self) -> None:
        """change_percent should be calculated from previous day's close, not oldest bar."""
        with patch("sawa.live.AsyncPolygonClient") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            mock_client.get_aggregates = AsyncMock(return_value=list(FIVE_BARS))

            result = await get_live_price("AAPL", days=5, api_key="test-key")

        # Should be (106 - 103) / 103 * 100 = 2.91%, NOT (106 - 100) / 100 = 6.0%
        assert result["change_percent"] == pytest.approx(2.91, abs=0.01)
        assert result["current_price"] == 106.0

    @pytest.mark.asyncio
    async def test_change_percent_negative(self) -> None:
        """change_percent should be negative when price drops from previous day."""
        bars = [
            _make_bar("2026-02-24", 110.0),
            _make_bar("2026-02-25", 105.0),
        ]
        with patch("sawa.live.AsyncPolygonClient") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            mock_client.get_aggregates = AsyncMock(return_value=list(bars))

            result = await get_live_price("AAPL", days=7, api_key="test-key")

        # (105 - 110) / 110 * 100 = -4.55%
        assert result["change_percent"] == pytest.approx(-4.55, abs=0.01)

    @pytest.mark.asyncio
    async def test_single_bar_returns_zero_change(self) -> None:
        """With only one bar, change_percent should be 0."""
        bars = [_make_bar("2026-02-25", 150.0)]
        with patch("sawa.live.AsyncPolygonClient") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            mock_client.get_aggregates = AsyncMock(return_value=list(bars))

            result = await get_live_price("AAPL", days=1, api_key="test-key")

        assert result["change_percent"] == 0.0
        assert result["current_price"] == 150.0

    @pytest.mark.asyncio
    async def test_no_data_returns_error(self) -> None:
        """Empty results should return error dict."""
        with patch("sawa.live.AsyncPolygonClient") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            mock_client.get_aggregates = AsyncMock(return_value=[])

            result = await get_live_price("AAPL", days=7, api_key="test-key")

        assert result["error"] is not None
        assert result["current_price"] is None
        assert result["change_percent"] is None

    @pytest.mark.asyncio
    async def test_results_sorted_ascending(self) -> None:
        """History should be sorted ascending by date regardless of API order."""
        # Provide bars in descending order (as API returns them)
        bars = list(reversed(FIVE_BARS))
        with patch("sawa.live.AsyncPolygonClient") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            mock_client.get_aggregates = AsyncMock(return_value=list(bars))

            result = await get_live_price("AAPL", days=5, api_key="test-key")

        dates = [
            datetime.fromtimestamp(bar["t"] / 1000).date()
            for bar in result["history"]
        ]
        assert dates == sorted(dates)

    @pytest.mark.asyncio
    async def test_ticker_normalized_uppercase(self) -> None:
        """Ticker should be normalized to uppercase."""
        with patch("sawa.live.AsyncPolygonClient") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            mock_client.get_aggregates = AsyncMock(
                return_value=[_make_bar("2026-02-25", 100.0)]
            )

            result = await get_live_price("aapl", days=1, api_key="test-key")

        assert result["ticker"] == "AAPL"

    @pytest.mark.asyncio
    async def test_invalid_days_raises(self) -> None:
        """Days outside 1-30 should raise ValueError."""
        with pytest.raises(ValueError, match="days must be between"):
            await get_live_price("AAPL", days=0, api_key="test-key")

        with pytest.raises(ValueError, match="days must be between"):
            await get_live_price("AAPL", days=31, api_key="test-key")

    @pytest.mark.asyncio
    async def test_empty_ticker_raises(self) -> None:
        """Empty ticker should raise ValueError."""
        with pytest.raises(ValueError, match="ticker cannot be empty"):
            await get_live_price("", days=7, api_key="test-key")

    @pytest.mark.asyncio
    async def test_no_api_key_raises(self) -> None:
        """Missing API key should raise ValueError."""
        with patch("sawa.live.get_env", return_value=None):
            with pytest.raises(ValueError, match="POLYGON_API_KEY"):
                await get_live_price("AAPL", days=7)

    @pytest.mark.asyncio
    async def test_current_date_from_latest_bar(self) -> None:
        """current_date should reflect the most recent bar's date."""
        with patch("sawa.live.AsyncPolygonClient") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            mock_client.get_aggregates = AsyncMock(return_value=list(FIVE_BARS))

            result = await get_live_price("AAPL", days=5, api_key="test-key")

        assert result["current_date"] == "2026-02-25"


class TestGetLivePricesBatch:
    """Tests for get_live_prices_batch."""

    @pytest.mark.asyncio
    async def test_change_percent_uses_previous_day(self) -> None:
        """change_percent should use previous day's close for each ticker."""
        batch_results = {
            "AAPL": list(FIVE_BARS),
            "MSFT": [
                _make_bar("2026-02-24", 400.0),
                _make_bar("2026-02-25", 390.0),
            ],
        }
        with patch("sawa.live.AsyncPolygonClient") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            mock_client.get_aggregates_batch = AsyncMock(return_value=batch_results)

            result = await get_live_prices_batch(
                ["AAPL", "MSFT"], days=5, api_key="test-key"
            )

        # AAPL: (106 - 103) / 103 * 100 = 2.91%
        assert result["AAPL"]["change_percent"] == pytest.approx(2.91, abs=0.01)
        # MSFT: (390 - 400) / 400 * 100 = -2.5%
        assert result["MSFT"]["change_percent"] == pytest.approx(-2.5, abs=0.01)

    @pytest.mark.asyncio
    async def test_ticker_with_no_data(self) -> None:
        """Tickers with no data should return error entry."""
        batch_results = {"AAPL": list(FIVE_BARS), "FAKE": []}
        with patch("sawa.live.AsyncPolygonClient") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            mock_client.get_aggregates_batch = AsyncMock(return_value=batch_results)

            result = await get_live_prices_batch(
                ["AAPL", "FAKE"], days=5, api_key="test-key"
            )

        assert result["AAPL"]["error"] is None
        assert result["FAKE"]["error"] is not None
        assert result["FAKE"]["current_price"] is None

    @pytest.mark.asyncio
    async def test_single_bar_per_ticker(self) -> None:
        """Single bar should give 0% change."""
        batch_results = {"AAPL": [_make_bar("2026-02-25", 200.0)]}
        with patch("sawa.live.AsyncPolygonClient") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            mock_client.get_aggregates_batch = AsyncMock(return_value=batch_results)

            result = await get_live_prices_batch(
                ["AAPL"], days=1, api_key="test-key"
            )

        assert result["AAPL"]["change_percent"] == 0.0

    @pytest.mark.asyncio
    async def test_no_api_key_raises(self) -> None:
        """Missing API key should raise ValueError."""
        with patch("sawa.live.get_env", return_value=None):
            with pytest.raises(ValueError, match="POLYGON_API_KEY"):
                await get_live_prices_batch(["AAPL"], days=7)

    @pytest.mark.asyncio
    async def test_results_sorted_ascending(self) -> None:
        """History bars for each ticker should be sorted ascending."""
        batch_results = {"AAPL": list(reversed(FIVE_BARS))}
        with patch("sawa.live.AsyncPolygonClient") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            mock_client.get_aggregates_batch = AsyncMock(return_value=batch_results)

            result = await get_live_prices_batch(
                ["AAPL"], days=5, api_key="test-key"
            )

        dates = [
            datetime.fromtimestamp(bar["t"] / 1000).date()
            for bar in result["AAPL"]["history"]
        ]
        assert dates == sorted(dates)
