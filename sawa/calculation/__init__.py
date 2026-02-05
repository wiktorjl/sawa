"""Technical indicator calculation package.

Provides calculation engines for computing technical analysis indicators
from OHLCV price data using ta-lib.
"""

from sawa.calculation.ta_engine import (
    calculate_indicators_for_ticker,
    validate_indicator,
)

__all__ = [
    "calculate_indicators_for_ticker",
    "validate_indicator",
]
