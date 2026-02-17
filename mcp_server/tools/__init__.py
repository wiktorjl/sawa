"""MCP server tools package."""

from .companies import (
    get_company_details,
    list_companies,
    search_companies,
)
from .corporate_actions import (
    get_dividend_yield_leaders,
    get_dividends,
    get_earnings_calendar,
    get_earnings_history,
    get_ex_dividend_calendar,
    get_recent_splits,
    get_stock_splits,
)
from .economy import get_economy_dashboard, get_economy_data
from .fundamentals import get_fundamentals
from .indices import (
    check_index_membership,
    get_index_constituents,
    get_index_with_prices,
    list_indices,
)
from .market_data import (
    get_financial_ratios,
    get_latest_price,
    get_latest_technical_indicators,
    get_live_price_async,
    get_live_prices_batch_async,
    get_stock_prices,
    get_technical_indicators,
    list_technical_indicators,
    screen_by_technical_indicators,
)
from .momentum import get_momentum_indicators, get_squeeze_indicators
from .multi_timeframe import (
    calculate_relative_strength,
    get_multi_timeframe_alignment,
    get_weekly_monthly_candles,
)
from .movers import get_market_breadth, get_top_movers, get_volume_leaders
from .news import get_recent_news_sentiment
from .patterns import detect_candlestick_patterns, detect_chart_patterns
from .scanner import scan_ytd_performance
from .schema import describe_database, describe_table
from .screener import get_52week_extremes, get_daily_range_leaders, screen_stocks
from .sectors import get_sector_performance, list_sectors
from .support_resistance import calculate_support_resistance_levels
from .volume_analysis import (
    detect_volume_anomalies,
    get_advanced_volume_indicators,
    get_volume_profile,
)

__all__ = [
    # Companies
    "list_companies",
    "get_company_details",
    "search_companies",
    # Economy
    "get_economy_data",
    "get_economy_dashboard",
    # Fundamentals
    "get_fundamentals",
    # Market data
    "get_stock_prices",
    "get_financial_ratios",
    "get_latest_price",
    "get_live_price_async",
    "get_live_prices_batch_async",
    # Technical indicators
    "get_technical_indicators",
    "get_latest_technical_indicators",
    "screen_by_technical_indicators",
    "list_technical_indicators",
    # Scanner
    "scan_ytd_performance",
    # Schema discovery
    "describe_database",
    "describe_table",
    # Sectors
    "list_sectors",
    "get_sector_performance",
    # Market movers
    "get_top_movers",
    "get_volume_leaders",
    "get_market_breadth",
    # Screener
    "screen_stocks",
    "get_52week_extremes",
    "get_daily_range_leaders",
    # Indices
    "list_indices",
    "get_index_constituents",
    "check_index_membership",
    "get_index_with_prices",
    # Corporate actions
    "get_stock_splits",
    "get_dividends",
    "get_ex_dividend_calendar",
    "get_recent_splits",
    "get_dividend_yield_leaders",
    "get_earnings_calendar",
    "get_earnings_history",
    # News sentiment
    "get_recent_news_sentiment",
    # Patterns
    "detect_candlestick_patterns",
    "detect_chart_patterns",
    # Momentum / Squeeze
    "get_squeeze_indicators",
    "get_momentum_indicators",
    # Support & Resistance
    "calculate_support_resistance_levels",
    # Volume analysis
    "get_volume_profile",
    "detect_volume_anomalies",
    "get_advanced_volume_indicators",
    # Multi-timeframe analysis
    "get_weekly_monthly_candles",
    "get_multi_timeframe_alignment",
    "calculate_relative_strength",
]
