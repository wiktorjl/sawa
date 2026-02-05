"""Technical indicators domain model.

Technical indicators calculated daily for each stock using ta-lib.
All 20 indicators stored in a single dataclass for screening efficiency.
"""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class TechnicalIndicators:
    """Technical indicators for a ticker on a date.

    All 25 core indicators calculated daily from OHLCV data using ta-lib.
    Optional types used - indicators are NULL when insufficient historical data
    (e.g., SMA-200 requires 200 days of history).

    Attributes:
        ticker: Stock symbol (normalized to uppercase)
        date: Trading date

        # Trend (13 indicators)
        sma_5: 5-day Simple Moving Average
        sma_10: 10-day Simple Moving Average
        sma_20: 20-day Simple Moving Average
        sma_50: 50-day Simple Moving Average
        sma_100: 100-day Simple Moving Average
        sma_150: 150-day Simple Moving Average (6-month trend)
        sma_200: 200-day Simple Moving Average (long-term trend)
        ema_12: 12-day Exponential Moving Average
        ema_26: 26-day Exponential Moving Average
        ema_50: 50-day Exponential Moving Average
        ema_100: 100-day Exponential Moving Average
        ema_200: 200-day Exponential Moving Average (long-term trend)
        vwap: Volume Weighted Average Price (cumulative)

        # Momentum (5 indicators)
        rsi_14: 14-day Relative Strength Index (0-100)
        rsi_21: 21-day Relative Strength Index (0-100)
        macd_line: MACD Line (12-26 EMA difference)
        macd_signal: MACD Signal Line (9-day EMA of MACD)
        macd_histogram: MACD Histogram (MACD minus Signal)

        # Volatility (4 indicators)
        bb_upper: Bollinger Band Upper (20-day SMA + 2 std)
        bb_middle: Bollinger Band Middle (20-day SMA)
        bb_lower: Bollinger Band Lower (20-day SMA - 2 std)
        atr_14: 14-day Average True Range

        # Volume (3 indicators)
        obv: On Balance Volume (cumulative, can be negative)
        volume_sma_20: 20-day Volume SMA
        volume_ratio: Volume Ratio (today volume / 20-day avg)
    """

    ticker: str
    date: date

    # Trend (13 indicators)
    sma_5: Decimal | None = None
    sma_10: Decimal | None = None
    sma_20: Decimal | None = None
    sma_50: Decimal | None = None
    sma_100: Decimal | None = None
    sma_150: Decimal | None = None
    sma_200: Decimal | None = None
    ema_12: Decimal | None = None
    ema_26: Decimal | None = None
    ema_50: Decimal | None = None
    ema_100: Decimal | None = None
    ema_200: Decimal | None = None
    vwap: Decimal | None = None

    # Momentum (5 indicators)
    rsi_14: Decimal | None = None
    rsi_21: Decimal | None = None
    macd_line: Decimal | None = None
    macd_signal: Decimal | None = None
    macd_histogram: Decimal | None = None

    # Volatility (4 indicators)
    bb_upper: Decimal | None = None
    bb_middle: Decimal | None = None
    bb_lower: Decimal | None = None
    atr_14: Decimal | None = None

    # Volume (3 indicators)
    obv: int | None = None
    volume_sma_20: int | None = None
    volume_ratio: Decimal | None = None

    def __post_init__(self) -> None:
        """Normalize ticker to uppercase."""
        object.__setattr__(self, "ticker", self.ticker.upper())

    @classmethod
    def column_names(cls) -> list[str]:
        """Return list of indicator column names for database operations."""
        return [
            "ticker",
            "date",
            # Trend
            "sma_5",
            "sma_10",
            "sma_20",
            "sma_50",
            "sma_100",
            "sma_150",
            "sma_200",
            "ema_12",
            "ema_26",
            "ema_50",
            "ema_100",
            "ema_200",
            "vwap",
            # Momentum
            "rsi_14",
            "rsi_21",
            "macd_line",
            "macd_signal",
            "macd_histogram",
            # Volatility
            "bb_upper",
            "bb_middle",
            "bb_lower",
            "atr_14",
            # Volume
            "obv",
            "volume_sma_20",
            "volume_ratio",
        ]

    def to_tuple(self) -> tuple:
        """Convert to tuple for database insert."""
        return (
            self.ticker,
            self.date,
            # Trend
            self.sma_5,
            self.sma_10,
            self.sma_20,
            self.sma_50,
            self.sma_100,
            self.sma_150,
            self.sma_200,
            self.ema_12,
            self.ema_26,
            self.ema_50,
            self.ema_100,
            self.ema_200,
            self.vwap,
            # Momentum
            self.rsi_14,
            self.rsi_21,
            self.macd_line,
            self.macd_signal,
            self.macd_histogram,
            # Volatility
            self.bb_upper,
            self.bb_middle,
            self.bb_lower,
            self.atr_14,
            # Volume
            self.obv,
            self.volume_sma_20,
            self.volume_ratio,
        )
