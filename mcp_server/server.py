#!/usr/bin/env python3
"""
Stock Data MCP Server

An MCP server providing read-only access to stock market data in PostgreSQL.
Includes colorful Unicode charts for data visualization.
"""

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

# Load environment variables from .env file in project root
# Find .env relative to this file's location (mcp_server/server.py -> ../.env)
_project_root = Path(__file__).parent.parent
_env_file = _project_root / ".env"
load_dotenv(_env_file)

from .charts.config import ChartDetail, get_chart_config  # noqa: E402
from .charts.core.layout import get_layout  # noqa: E402
from .charts.core.modal import check_width_and_warn  # noqa: E402
from .charts.renderers import (  # noqa: E402
    render_economy_chart,
    render_economy_dashboard,
    render_fundamentals_chart,
    render_price_chart,
    render_ratios_chart,
)
from .charts.themes import get_theme  # noqa: E402
from .database import execute_query  # noqa: E402
from .tools.companies import (  # noqa: E402
    get_company_details,
    list_companies,
    search_companies,
)
from .tools.corporate_actions import (  # noqa: E402
    get_dividend_yield_leaders,
    get_dividends,
    get_earnings_calendar,
    get_earnings_history,
    get_ex_dividend_calendar,
    get_recent_splits,
    get_stock_splits,
)
from .tools.economy import (  # noqa: E402
    get_economy_dashboard,
    get_economy_data,
)
from .tools.fundamentals import get_fundamentals  # noqa: E402
from .tools.indices import (  # noqa: E402
    check_index_membership,
    get_index_constituents,
    get_index_with_prices,
    list_indices,
)
from .tools.market_data import (  # noqa: E402
    get_data_status,
    get_financial_ratios,
    get_intraday_bars,
    get_latest_price,
    get_latest_technical_indicators,
    get_live_price_async,
    get_live_prices_batch_async,
    get_stock_prices,
    get_technical_indicators,
    list_technical_indicators,
    screen_by_technical_indicators,
)
from .tools.momentum import get_momentum_indicators, get_squeeze_indicators  # noqa: E402
from .tools.movers import get_market_breadth, get_top_movers, get_volume_leaders  # noqa: E402
from .tools.multi_timeframe import (  # noqa: E402
    calculate_relative_strength,
    get_multi_timeframe_alignment,
    get_weekly_monthly_candles,
)
from .tools.news import get_recent_news_sentiment  # noqa: E402
from .tools.patterns import detect_candlestick_patterns, detect_chart_patterns  # noqa: E402
from .tools.scanner import scan_ytd_performance  # noqa: E402
from .tools.schema import describe_database, describe_table  # noqa: E402
from .tools.screener import (  # noqa: E402
    detect_crossovers,
    get_52week_extremes,
    get_daily_range_leaders,
    get_ytd_returns,
    screen_stocks,
)
from .tools.sectors import get_sector_performance, list_sectors  # noqa: E402
from .tools.support_resistance import calculate_support_resistance_levels  # noqa: E402
from .tools.volume_analysis import (  # noqa: E402
    detect_volume_anomalies,
    get_advanced_volume_indicators,
    get_volume_profile,
)
from .validation import validate_tool_arguments  # noqa: E402

# Setup logging
log_level = os.environ.get("MCP_LOG_LEVEL", "info").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

# Create MCP server
app = Server("stock-data-server")


@app.list_tools()
async def list_tools() -> list[Tool]:
    """List all available tools."""
    return [
        Tool(
            name="list_companies",
            description="List active companies with optional filtering by sector",
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results (default: 100, max: 1000)",
                        "default": 100,
                        "minimum": 1,
                        "maximum": 1000,
                    },
                    "offset": {
                        "type": "integer",
                        "description": "Number of results to skip",
                        "default": 0,
                        "minimum": 0,
                    },
                    "sector": {
                        "type": "string",
                        "description": "Filter by sector/SIC description (partial match)",
                    },
                    "index": {
                        "type": "string",
                        "description": "Filter by index membership (sp500, nasdaq5000)",
                        "enum": ["sp500", "nasdaq5000"],
                    },
                },
            },
        ),
        Tool(
            name="get_company_details",
            description="Get detailed company information including latest price and metrics",
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol (e.g., AAPL)",
                    },
                },
                "required": ["ticker"],
            },
        ),
        Tool(
            name="search_companies",
            description="Search companies by name, ticker, or sector",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search term",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum results (default: 20, max: 100)",
                        "default": 20,
                        "minimum": 1,
                        "maximum": 100,
                    },
                    "index": {
                        "type": "string",
                        "description": "Filter by index membership (sp500, nasdaq5000)",
                        "enum": ["sp500", "nasdaq5000"],
                    },
                },
                "required": ["query"],
            },
        ),
        Tool(
            name="get_live_price",
            description="Get live stock price from Polygon API (real-time, not from database)",
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol (e.g., AAPL, MSFT)",
                    },
                    "days": {
                        "type": "integer",
                        "description": "Number of days of history to include (default: 7)",
                        "default": 7,
                        "minimum": 1,
                        "maximum": 30,
                    },
                },
                "required": ["ticker"],
            },
        ),
        Tool(
            name="get_live_prices_batch",
            description="Get live stock prices for multiple tickers from Polygon API (real-time batch query)",  # noqa: E501
            inputSchema={
                "type": "object",
                "properties": {
                    "tickers": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of stock ticker symbols (e.g., ['AAPL', 'MSFT', 'GOOGL'])",  # noqa: E501
                    },
                    "days": {
                        "type": "integer",
                        "description": "Number of days of history per ticker (default: 7)",
                        "default": 7,
                        "minimum": 1,
                        "maximum": 30,
                    },
                },
                "required": ["tickers"],
            },
        ),
        Tool(
            name="get_latest_price",
            description="Get the most recent closing price from the database (fast, always has latest data)",  # noqa: E501
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol (e.g., AAPL, MSFT)",
                    },
                    "use_live": {
                        "type": "boolean",
                        "description": "Include today's intraday data if available (default: true)",
                        "default": True,
                    },
                },
                "required": ["ticker"],
            },
        ),
        Tool(
            name="get_stock_prices",
            description="Get daily OHLCV prices for a ticker with visual chart",
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol",
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Start date in YYYY-MM-DD format",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date in YYYY-MM-DD format (defaults to today)",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum rows (default: 252, max: 1000)",
                        "default": 252,
                        "minimum": 1,
                        "maximum": 1000,
                    },
                    "use_live": {
                        "type": "boolean",
                        "description": "Include today's intraday data if available (default: true)",
                        "default": True,
                    },
                    "chart_detail": {
                        "type": "string",
                        "description": "Chart detail level",
                        "enum": ["compact", "normal", "detailed"],
                    },
                },
                "required": ["ticker", "start_date"],
            },
        ),
        Tool(
            name="get_financial_ratios",
            description="Get time-series financial ratios (P/E, ROE, debt/equity, etc.) with visual chart",  # noqa: E501
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol",
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Start date in YYYY-MM-DD format",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date (defaults to today)",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum rows (default: 100, max: 1000)",
                        "default": 100,
                        "minimum": 1,
                        "maximum": 1000,
                    },
                    "chart_detail": {
                        "type": "string",
                        "description": "Chart detail level",
                        "enum": ["compact", "normal", "detailed"],
                    },
                },
                "required": ["ticker", "start_date"],
            },
        ),
        Tool(
            name="get_fundamentals",
            description="Get latest balance sheet, cash flow, and income statement data with visual charts",  # noqa: E501
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol",
                    },
                    "timeframe": {
                        "type": "string",
                        "description": "quarterly or annual (default: quarterly)",
                        "enum": ["quarterly", "annual"],
                        "default": "quarterly",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of periods (default: 4, max: 20)",
                        "default": 4,
                        "minimum": 1,
                        "maximum": 20,
                    },
                    "chart_detail": {
                        "type": "string",
                        "description": "Chart detail level",
                        "enum": ["compact", "normal", "detailed"],
                    },
                },
                "required": ["ticker"],
            },
        ),
        Tool(
            name="get_technical_indicators",
            description="Get technical indicators (SMA, RSI, MACD, Bollinger Bands, etc.) for a ticker",  # noqa: E501
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol",
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Start date in YYYY-MM-DD format",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date in YYYY-MM-DD format (defaults to today)",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum rows (default: 252, max: 1000)",
                        "default": 252,
                        "minimum": 1,
                        "maximum": 1000,
                    },
                },
                "required": ["ticker", "start_date"],
            },
        ),
        Tool(
            name="get_latest_technical_indicators",
            description="Get the most recent technical indicators for a ticker",
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol",
                    },
                },
                "required": ["ticker"],
            },
        ),
        Tool(
            name="get_intraday_bars",
            description="Get intraday 5-minute bars for a ticker (15-min delayed)",
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol (e.g., AAPL)",
                    },
                    "tickers": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Multiple ticker symbols (e.g., ['SPY', 'QQQ', 'DIA']). Use instead of ticker for multi-stock queries.",  # noqa: E501
                    },
                    "date": {
                        "type": "string",
                        "description": "Date in YYYY-MM-DD format (default: today)",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum bars to return (default: 100, max: 500)",
                        "default": 100,
                        "minimum": 1,
                        "maximum": 500,
                    },
                    "aggregate": {
                        "type": "boolean",
                        "description": "Return daily OHLCV summary instead of individual bars",  # noqa: E501
                        "default": False,
                    },
                },
            },
        ),
        Tool(
            name="screen_technical_indicators",
            description="Screen stocks by technical indicator values (e.g., RSI < 30)",
            inputSchema={
                "type": "object",
                "properties": {
                    "rsi_14_max": {
                        "type": "number",
                        "description": "Maximum RSI-14 value (e.g., 30 for oversold)",
                    },
                    "rsi_14_min": {
                        "type": "number",
                        "description": "Minimum RSI-14 value (e.g., 70 for overbought)",
                    },
                    "volume_ratio_min": {
                        "type": "number",
                        "description": "Minimum volume ratio (today vs 20-day avg)",
                    },
                    "macd_histogram_min": {
                        "type": "number",
                        "description": "Minimum MACD histogram (positive = bullish)",
                    },
                    "macd_histogram_max": {
                        "type": "number",
                        "description": "Maximum MACD histogram (negative = bearish)",
                    },
                    "target_date": {
                        "type": "string",
                        "description": "Date to screen (defaults to most recent)",
                    },
                    "index": {
                        "type": "string",
                        "description": "Filter by index membership (sp500, nasdaq5000)",
                        "enum": ["sp500", "nasdaq5000"],
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum results (default: 100, max: 500)",
                        "default": 100,
                        "minimum": 1,
                        "maximum": 500,
                    },
                },
            },
        ),
        Tool(
            name="get_economy_data",
            description="Get economic indicators for a date range with visual charts",
            inputSchema={
                "type": "object",
                "properties": {
                    "indicator_type": {
                        "type": "string",
                        "description": "Type of economic indicator",
                        "enum": [
                            "treasury_yields",
                            "inflation",
                            "inflation_expectations",
                            "labor_market",
                        ],
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Start date in YYYY-MM-DD format",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date (defaults to today)",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum rows (default: 100, max: 1000)",
                        "default": 100,
                        "minimum": 1,
                        "maximum": 1000,
                    },
                    "chart_detail": {
                        "type": "string",
                        "description": "Chart detail level",
                        "enum": ["compact", "normal", "detailed"],
                    },
                },
                "required": ["indicator_type", "start_date"],
            },
        ),
        Tool(
            name="get_economy_dashboard",
            description="Get a visual summary of recent economic indicators",
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Number of recent data points (default: 10, max: 100)",
                        "default": 10,
                        "minimum": 1,
                        "maximum": 100,
                    },
                    "chart_detail": {
                        "type": "string",
                        "description": "Chart detail level",
                        "enum": ["compact", "normal", "detailed"],
                    },
                },
            },
        ),
        Tool(
            name="scan_ytd_performance",
            description="Scan market indices (S&P 500, NASDAQ-100, or both) for YTD performance analysis with sector grouping",  # noqa: E501
            inputSchema={
                "type": "object",
                "properties": {
                    "start_date": {
                        "type": "string",
                        "description": "Start date YYYY-MM-DD (default: Jan 1 current year)",
                    },
                    "large_cap_threshold": {
                        "type": "number",
                        "description": "Market cap threshold in billions (default: 100)",
                        "default": 100,
                    },
                    "top_n": {
                        "type": "integer",
                        "description": "Number of top winners/losers to show (default: 10)",
                        "default": 10,
                        "minimum": 5,
                        "maximum": 50,
                    },
                    "index": {
                        "type": "string",
                        "description": "Index to scan: sp500, nasdaq5000, or both (default: sp500)",
                        "enum": ["sp500", "nasdaq5000", "both"],
                        "default": "sp500",
                    },
                },
            },
        ),
        Tool(
            name="execute_query",
            description=(
                "Execute a custom read-only SQL query (SELECT only). "
                "Use describe_database and describe_table tools first to discover schema.\n\n"
                "TABLES:\n"
                "  companies(ticker PK, name, sic_code, sic_description, market_cap, active, ...)\n"
                "  stock_prices(ticker, date PK, open, high, low, close, volume)\n"
                "  stock_prices_intraday(ticker, timestamp PK, open, high, low, close, volume)\n"
                "  financial_ratios(ticker, date PK, price_to_earnings, debt_to_equity, "
                "return_on_equity, dividend_yield, ...)\n"
                "  balance_sheets(ticker, period_end, timeframe PK, total_assets, "
                "total_liabilities, total_equity, ...)\n"
                "  cash_flows(ticker, period_end, timeframe PK, net_cash_from_operating_activities, ...)\n"
                "  income_statements(ticker, period_end, timeframe PK, revenue, operating_income, "
                "diluted_earnings_per_share, ...)\n"
                "  technical_indicators(ticker, date PK, sma_50, sma_150, sma_200, rsi_14, "
                "macd_line, macd_histogram, bb_upper, bb_lower, atr_14, volume_ratio, ...)\n"
                "  indices(id PK, code, name) - codes: 'sp500', 'nasdaq5000'\n"
                "  index_constituents(index_id, ticker PK) - JOIN with indices on id\n"
                "  sic_gics_mapping(sic_code PK, gics_sector, gics_industry) - "
                "JOIN with companies on sic_code\n"
                "  treasury_yields(date PK, yield_1_month, yield_2_year, yield_10_year, yield_30_year, ...)\n"
                "  inflation(date PK, cpi, cpi_year_over_year, pce, ...)\n"
                "  inflation_expectations(date PK, market_5_year, market_10_year, ...)\n"
                "  labor_market(date PK, unemployment_rate, job_openings, ...)\n"
                "  stock_splits(ticker, execution_date, split_from, split_to)\n"
                "  dividends(ticker, ex_dividend_date, cash_amount, frequency, ...)\n"
                "  earnings(ticker, report_date, eps_estimate, eps_actual, revenue_actual, "
                "surprise_pct, timing)\n"
                "  news_articles(id PK, title, published_utc, ...)\n"
                "  news_article_tickers(article_id, ticker)\n"
                "  news_sentiment(article_id, ticker, sentiment)\n\n"
                "VIEWS:\n"
                "  stock_prices_live - union of historical EOD + today's intraday data\n"
                "  v_company_summary - companies with latest price and ratios\n"
                "  v_company_with_indices - companies with index membership (in_sp500, in_nasdaq5000)\n"
                "  v_latest_fundamentals - latest quarterly fundamentals per company\n"
                "  v_economy_dashboard - combined economy indicators\n"
                "  v_sector_summary - sector aggregates by SIC code\n\n"
                "COMMON MISTAKES (these DO NOT exist):\n"
                "  daily_prices -> use stock_prices\n"
                "  price_metrics -> no such table, compute from stock_prices\n"
                "  intraday_bars -> use stock_prices_intraday\n"
                "  index_members -> use index_constituents JOIN indices\n"
                "  market_indexes -> use indices\n"
                "  companies.sp500 -> use index_constituents JOIN indices WHERE code='sp500'\n"
                "  companies.gics_sector -> use sic_gics_mapping JOIN on sic_code"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "SQL SELECT statement to execute",
                    },
                },
                "required": ["sql"],
            },
        ),
        # Schema discovery tools
        Tool(
            name="describe_database",
            description="List all tables with column counts, row counts, and descriptions",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="describe_table",
            description="Get detailed table info: columns, types, samples, foreign keys, indexes",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to describe",
                    },
                },
                "required": ["table_name"],
            },
        ),
        # Sector tools
        Tool(
            name="list_sectors",
            description="List all sectors/industries with stock counts",
            inputSchema={
                "type": "object",
                "properties": {
                    "taxonomy": {
                        "type": "string",
                        "description": "Classification system: 'sic' (SEC) or 'gics' (S&P)",
                        "enum": ["sic", "gics"],
                        "default": "gics",
                    },
                    "index": {
                        "type": "string",
                        "description": "Filter by index membership (sp500, nasdaq5000)",
                        "enum": ["sp500", "nasdaq5000"],
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum results (default: 100)",
                        "default": 100,
                        "minimum": 1,
                        "maximum": 500,
                    },
                },
            },
        ),
        Tool(
            name="get_sector_performance",
            description="Get sector performance across multiple time periods (1d, 1w, 1m, YTD)",
            inputSchema={
                "type": "object",
                "properties": {
                    "taxonomy": {
                        "type": "string",
                        "description": "Classification system: 'sic' or 'gics' (default: gics)",
                        "enum": ["sic", "gics"],
                        "default": "gics",
                    },
                    "index": {
                        "type": "string",
                        "description": "Filter by index membership (sp500, nasdaq5000)",
                        "enum": ["sp500", "nasdaq5000"],
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum sectors (default: 50)",
                        "default": 50,
                        "minimum": 1,
                        "maximum": 100,
                    },
                },
            },
        ),
        # Market movers tools
        Tool(
            name="get_top_movers",
            description="Get top gaining or losing stocks",
            inputSchema={
                "type": "object",
                "properties": {
                    "direction": {
                        "type": "string",
                        "description": "Direction: 'gainers', 'losers', or 'both'",
                        "enum": ["gainers", "losers", "both"],
                        "default": "both",
                    },
                    "period": {
                        "type": "string",
                        "description": "Time period: '1d', '1w', '1m', or 'ytd'",
                        "enum": ["1d", "1w", "1m", "ytd"],
                        "default": "1d",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of results per direction (default: 20)",
                        "default": 20,
                        "minimum": 1,
                        "maximum": 100,
                    },
                    "sector": {
                        "type": "string",
                        "description": "Optional sector filter (partial match)",
                    },
                    "index": {
                        "type": "string",
                        "description": "Filter by index membership (sp500, nasdaq5000)",
                        "enum": ["sp500", "nasdaq5000"],
                    },
                    "min_price": {
                        "type": "number",
                        "description": "Minimum stock price filter",
                    },
                    "min_volume": {
                        "type": "integer",
                        "description": "Minimum volume filter",
                    },
                },
            },
        ),
        Tool(
            name="get_volume_leaders",
            description="Get stocks with highest trading volume",
            inputSchema={
                "type": "object",
                "properties": {
                    "metric": {
                        "type": "string",
                        "description": "Metric: 'volume', 'dollar_volume', or 'volume_ratio'",
                        "enum": ["volume", "dollar_volume", "volume_ratio"],
                        "default": "dollar_volume",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of results (default: 20)",
                        "default": 20,
                        "minimum": 1,
                        "maximum": 100,
                    },
                    "sector": {
                        "type": "string",
                        "description": "Optional sector filter",
                    },
                    "index": {
                        "type": "string",
                        "description": "Filter by index membership (sp500, nasdaq5000)",
                        "enum": ["sp500", "nasdaq5000"],
                    },
                    "min_price": {
                        "type": "number",
                        "description": "Minimum stock price filter",
                    },
                },
            },
        ),
        Tool(
            name="get_market_breadth",
            description="Get market breadth: advancers, decliners, unchanged, A/D ratio",
            inputSchema={
                "type": "object",
                "properties": {
                    "date": {
                        "type": "string",
                        "description": "Date YYYY-MM-DD (default: latest trading day)",
                    },
                    "index": {
                        "type": "string",
                        "description": "Filter by index: sp500, nasdaq5000, or all",
                        "enum": ["sp500", "nasdaq5000", "all"],
                        "default": "all",
                    },
                },
            },
        ),
        Tool(
            name="list_technical_indicators",
            description="List available technical indicators with descriptions",
            inputSchema={
                "type": "object",
                "properties": {
                    "category": {
                        "type": "string",
                        "description": "Category: 'trend', 'momentum', 'volatility', 'volume'",
                        "enum": ["trend", "momentum", "volatility", "volume"],
                    },
                },
            },
        ),
        # Flexible screener
        Tool(
            name="screen_stocks",
            description="Multi-criteria screener with price, volume, and technical filters",
            inputSchema={
                "type": "object",
                "properties": {
                    "filters": {
                        "type": "object",
                        "description": "Filters: {name: [min, max]} (null = unbounded)",
                        "properties": {
                            "price": {
                                "type": "array",
                                "items": {"type": ["number", "null"]},
                                "minItems": 2,
                                "maxItems": 2,
                                "description": "Price range [min, max]",
                            },
                            "price_change_1d": {
                                "type": "array",
                                "items": {"type": ["number", "null"]},
                                "minItems": 2,
                                "maxItems": 2,
                                "description": "1-day change % [min, max]",
                            },
                            "price_change_1w": {
                                "type": "array",
                                "items": {"type": ["number", "null"]},
                                "minItems": 2,
                                "maxItems": 2,
                                "description": "1-week change % [min, max]",
                            },
                            "price_change_1m": {
                                "type": "array",
                                "items": {"type": ["number", "null"]},
                                "minItems": 2,
                                "maxItems": 2,
                                "description": "1-month change % [min, max]",
                            },
                            "price_change_ytd": {
                                "type": "array",
                                "items": {"type": ["number", "null"]},
                                "minItems": 2,
                                "maxItems": 2,
                                "description": "YTD change % [min, max]",
                            },
                            "market_cap": {
                                "type": "array",
                                "items": {"type": ["number", "null"]},
                                "minItems": 2,
                                "maxItems": 2,
                                "description": "Market cap [min, max]",
                            },
                            "volume": {
                                "type": "array",
                                "items": {"type": ["number", "null"]},
                                "minItems": 2,
                                "maxItems": 2,
                                "description": "Volume [min, max]",
                            },
                            "volume_ratio": {
                                "type": "array",
                                "items": {"type": ["number", "null"]},
                                "minItems": 2,
                                "maxItems": 2,
                                "description": "Volume ratio vs 20-day avg [min, max]",
                            },
                            "rsi_14": {
                                "type": "array",
                                "items": {"type": ["number", "null"]},
                                "minItems": 2,
                                "maxItems": 2,
                                "description": "RSI-14 [min, max], e.g. [null, 30] for oversold",
                            },
                            "rsi_21": {
                                "type": "array",
                                "items": {"type": ["number", "null"]},
                                "minItems": 2,
                                "maxItems": 2,
                                "description": "RSI-21 [min, max]",
                            },
                            "sma_50_distance_pct": {
                                "type": "array",
                                "items": {"type": ["number", "null"]},
                                "minItems": 2,
                                "maxItems": 2,
                                "description": "% distance from SMA-50 [min, max]",
                            },
                            "sma_150_distance_pct": {
                                "type": "array",
                                "items": {"type": ["number", "null"]},
                                "minItems": 2,
                                "maxItems": 2,
                                "description": "% distance from SMA-150 [min, max]",
                            },
                            "sma_200_distance_pct": {
                                "type": "array",
                                "items": {"type": ["number", "null"]},
                                "minItems": 2,
                                "maxItems": 2,
                                "description": "% distance from SMA-200 [min, max]",
                            },
                            "macd_histogram": {
                                "type": "array",
                                "items": {"type": ["number", "null"]},
                                "minItems": 2,
                                "maxItems": 2,
                                "description": "MACD histogram [min, max]",
                            },
                            "pe_ratio": {
                                "type": "array",
                                "items": {"type": ["number", "null"]},
                                "minItems": 2,
                                "maxItems": 2,
                                "description": "P/E ratio [min, max]",
                            },
                            "dividend_yield": {
                                "type": "array",
                                "items": {"type": ["number", "null"]},
                                "minItems": 2,
                                "maxItems": 2,
                                "description": "Dividend yield % [min, max]",
                            },
                            "roe": {
                                "type": "array",
                                "items": {"type": ["number", "null"]},
                                "minItems": 2,
                                "maxItems": 2,
                                "description": "Return on equity % [min, max]",
                            },
                            "debt_to_equity": {
                                "type": "array",
                                "items": {"type": ["number", "null"]},
                                "minItems": 2,
                                "maxItems": 2,
                                "description": "Debt to equity ratio [min, max]",
                            },
                        },
                        "additionalProperties": {
                            "type": "array",
                            "items": {"type": ["number", "null"]},
                            "minItems": 2,
                            "maxItems": 2,
                        },
                    },
                    "sector": {
                        "type": "string",
                        "description": "Optional sector filter (partial match)",
                    },
                    "sector_exclude": {
                        "type": "string",
                        "description": "Optional sector to exclude (partial match)",
                    },
                    "index": {
                        "type": "string",
                        "description": "Filter by index membership (sp500, nasdaq5000)",
                        "enum": ["sp500", "nasdaq5000"],
                    },
                    "taxonomy": {
                        "type": "string",
                        "description": "Sector taxonomy: 'sic' or 'gics'",
                        "enum": ["sic", "gics"],
                        "default": "gics",
                    },
                    "sort_by": {
                        "type": "string",
                        "description": "Column to sort by",
                        "enum": [
                            "market_cap",
                            "price",
                            "volume",
                            "change_1d",
                            "change_1w",
                            "rsi_14",
                            "pe_ratio",
                            "dividend_yield",
                        ],
                        "default": "market_cap",
                    },
                    "sort_order": {
                        "type": "string",
                        "description": "Sort direction",
                        "enum": ["asc", "desc"],
                        "default": "desc",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum results (default: 50, max: 500)",
                        "default": 50,
                        "minimum": 1,
                        "maximum": 500,
                    },
                },
            },
        ),
        # 52-week high/low screener
        Tool(
            name="get_52week_extremes",
            description="Find stocks at or near 52-week highs or lows",
            inputSchema={
                "type": "object",
                "properties": {
                    "extreme": {
                        "type": "string",
                        "description": "Which extreme: 'highs', 'lows', or 'both'",
                        "enum": ["highs", "lows", "both"],
                        "default": "both",
                    },
                    "threshold_pct": {
                        "type": "number",
                        "description": "% threshold from extreme (default: 2 = within 2%)",
                        "default": 2.0,
                    },
                    "index": {
                        "type": "string",
                        "description": "Filter by index: sp500, nasdaq5000, or all",
                        "enum": ["sp500", "nasdaq5000", "all"],
                        "default": "all",
                    },
                    "min_volume": {
                        "type": "integer",
                        "description": "Minimum volume filter",
                    },
                    "since_date": {
                        "type": "string",
                        "description": "Only stocks with new 52w extremes since this date",
                    },
                    "include_fundamentals": {
                        "type": "boolean",
                        "description": "Include PE, dividend yield, ROE, debt/equity",
                        "default": False,
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum results (default: 50, max: 200)",
                        "default": 50,
                        "minimum": 1,
                        "maximum": 200,
                    },
                },
            },
        ),
        # Daily range (intraday volatility) screener
        Tool(
            name="get_daily_range_leaders",
            description="Find stocks with high intraday volatility (daily range %)",
            inputSchema={
                "type": "object",
                "properties": {
                    "min_range_pct": {
                        "type": "number",
                        "description": "Minimum daily range % (default: 3%)",
                        "default": 3.0,
                    },
                    "max_range_pct": {
                        "type": "number",
                        "description": "Maximum daily range % (optional)",
                    },
                    "sector": {
                        "type": "string",
                        "description": "Optional sector filter",
                    },
                    "index": {
                        "type": "string",
                        "description": "Filter by index membership (sp500, nasdaq5000)",
                        "enum": ["sp500", "nasdaq5000"],
                    },
                    "min_price": {
                        "type": "number",
                        "description": "Minimum stock price filter",
                    },
                    "min_volume": {
                        "type": "integer",
                        "description": "Minimum volume filter",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum results (default: 50, max: 200)",
                        "default": 50,
                        "minimum": 1,
                        "maximum": 200,
                    },
                },
            },
        ),
        # YTD returns for arbitrary ticker lists
        Tool(
            name="get_ytd_returns",
            description="Get YTD percentage returns for a list of tickers (database-based)",
            inputSchema={
                "type": "object",
                "properties": {
                    "tickers": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of ticker symbols (e.g., ['AAPL', 'MSFT'])",
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Start date YYYY-MM-DD (default: Jan 1 current year)",
                    },
                },
                "required": ["tickers"],
            },
        ),
        # SMA crossover detection
        Tool(
            name="detect_crossovers",
            description="Detect stocks that recently crossed above or below a moving average (SMA crossover scanner)",  # noqa: E501
            inputSchema={
                "type": "object",
                "properties": {
                    "sma_period": {
                        "type": "integer",
                        "description": "SMA period to check (50, 100, 150, or 200). Default: 150",
                        "enum": [50, 100, 150, 200],
                        "default": 150,
                    },
                    "direction": {
                        "type": "string",
                        "description": "'above' (bullish) or 'below' (bearish) crossover",
                        "enum": ["above", "below"],
                        "default": "above",
                    },
                    "lookback_days": {
                        "type": "integer",
                        "description": "Number of recent trading days to check (default: 5)",
                        "default": 5,
                        "minimum": 1,
                        "maximum": 30,
                    },
                    "min_volume_ratio": {
                        "type": "number",
                        "description": "Min volume ratio on crossover day (e.g., 1.5)",
                    },
                    "index": {
                        "type": "string",
                        "description": "Filter by index membership (sp500, nasdaq5000)",
                        "enum": ["sp500", "nasdaq5000"],
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum results (default: 50, max: 200)",
                        "default": 50,
                        "minimum": 1,
                        "maximum": 200,
                    },
                },
            },
        ),
        # Index tools
        Tool(
            name="list_indices",
            description="List all market indices (S&P 500, NASDAQ-100) with constituent counts",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="get_index_constituents",
            description="Get all constituent stocks of a market index",
            inputSchema={
                "type": "object",
                "properties": {
                    "code": {
                        "type": "string",
                        "description": "Index code (e.g., 'sp500', 'nasdaq5000')",
                    },
                },
                "required": ["code"],
            },
        ),
        Tool(
            name="check_index_membership",
            description="Check which market indices a stock belongs to",
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol (e.g., AAPL)",
                    },
                },
                "required": ["ticker"],
            },
        ),
        Tool(
            name="get_index_with_prices",
            description="Get index constituents with latest price data, sorted by market cap",
            inputSchema={
                "type": "object",
                "properties": {
                    "code": {
                        "type": "string",
                        "description": "Index code (e.g., 'sp500', 'nasdaq5000')",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum constituents to return (default: 50, max: 500)",
                        "default": 50,
                        "minimum": 1,
                        "maximum": 500,
                    },
                },
                "required": ["code"],
            },
        ),
        # Corporate actions tools
        Tool(
            name="get_stock_splits",
            description="Get stock split history",
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Filter by ticker symbol (optional)",
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Start date YYYY-MM-DD (optional)",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date YYYY-MM-DD (optional)",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum results (default: 100)",
                        "default": 100,
                    },
                },
            },
        ),
        Tool(
            name="get_dividends",
            description="Get dividend history or upcoming dividends",
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Filter by ticker symbol (optional)",
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Start date YYYY-MM-DD (optional)",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date YYYY-MM-DD (optional)",
                    },
                    "upcoming_only": {
                        "type": "boolean",
                        "description": "Only return future dividends",
                        "default": False,
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum results (default: 100)",
                        "default": 100,
                    },
                },
            },
        ),
        Tool(
            name="get_ex_dividend_calendar",
            description="Get ex-dividend calendar for a date range",
            inputSchema={
                "type": "object",
                "properties": {
                    "start_date": {
                        "type": "string",
                        "description": "Start date YYYY-MM-DD",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date YYYY-MM-DD",
                    },
                    "index": {
                        "type": "string",
                        "description": "Filter by index: sp500, nasdaq5000, or all",
                        "enum": ["sp500", "nasdaq5000", "all"],
                        "default": "all",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum results (default: 200)",
                        "default": 200,
                    },
                },
                "required": ["start_date", "end_date"],
            },
        ),
        Tool(
            name="get_recent_splits",
            description="Get recent stock splits",
            inputSchema={
                "type": "object",
                "properties": {
                    "days": {
                        "type": "integer",
                        "description": "Days to look back (default: 30)",
                        "default": 30,
                    },
                    "index": {
                        "type": "string",
                        "description": "Filter by index: sp500, nasdaq5000, or all",
                        "enum": ["sp500", "nasdaq5000", "all"],
                        "default": "all",
                    },
                },
            },
        ),
        Tool(
            name="get_dividend_yield_leaders",
            description="Get stocks with highest dividend yields",
            inputSchema={
                "type": "object",
                "properties": {
                    "index": {
                        "type": "string",
                        "description": "Filter by index: sp500, nasdaq5000, or all",
                        "enum": ["sp500", "nasdaq5000", "all"],
                        "default": "all",
                    },
                    "min_yield": {
                        "type": "number",
                        "description": "Minimum dividend yield % (default: 2.0)",
                        "default": 2.0,
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum results (default: 50)",
                        "default": 50,
                    },
                },
            },
        ),
        Tool(
            name="get_earnings_calendar",
            description="Get earnings calendar for a date range",
            inputSchema={
                "type": "object",
                "properties": {
                    "start_date": {
                        "type": "string",
                        "description": "Start date YYYY-MM-DD",
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date YYYY-MM-DD",
                    },
                    "index": {
                        "type": "string",
                        "description": "Filter by index: sp500, nasdaq5000, or all",
                        "enum": ["sp500", "nasdaq5000", "all"],
                        "default": "all",
                    },
                    "timing": {
                        "type": "string",
                        "description": "Filter by timing: BMO, AMC, or all",
                        "enum": ["BMO", "AMC", "all"],
                        "default": "all",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum results (default: 200)",
                        "default": 200,
                    },
                },
                "required": ["start_date", "end_date"],
            },
        ),
        Tool(
            name="get_earnings_history",
            description="Get historical earnings for a ticker",
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of quarters (default: 12)",
                        "default": 12,
                    },
                },
                "required": ["ticker"],
            },
        ),
        # Data status tool
        Tool(
            name="get_data_status",
            description="Check latest stock price data in the database"
            " (daily, intraday, and live tables)",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        # News sentiment tools
        Tool(
            name="get_recent_news_sentiment",
            description="Get recent news articles with sentiment analysis for a ticker",
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol (e.g., AAPL)",
                    },
                    "days_back": {
                        "type": "integer",
                        "description": "Number of days to look back (default: 14, max: 90)",
                        "default": 14,
                        "minimum": 1,
                        "maximum": 90,
                    },
                    "max_articles": {
                        "type": "integer",
                        "description": "Maximum articles to return (default: 10, max: 50)",
                        "default": 10,
                        "minimum": 1,
                        "maximum": 50,
                    },
                },
                "required": ["ticker"],
            },
        ),
        # Support & Resistance tools
        Tool(
            name="calculate_support_resistance_levels",
            description="Calculate support and resistance levels using pivot points, price clustering, or volume analysis",
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol (e.g., AAPL)",
                    },
                    "lookback_days": {
                        "type": "integer",
                        "description": "Number of days to analyze (default: 90, min: 5, max: 500)",
                        "default": 90,
                        "minimum": 5,
                        "maximum": 500,
                    },
                    "max_levels": {
                        "type": "integer",
                        "description": "Maximum number of levels to return (default: 5, max: 20)",
                        "default": 5,
                        "minimum": 1,
                        "maximum": 20,
                    },
                    "method": {
                        "type": "string",
                        "description": "Detection method: pivot (pivot points), cluster (price clustering), volume (volume profile)",
                        "enum": ["pivot", "cluster", "volume"],
                        "default": "cluster",
                    },
                },
                "required": ["ticker"],
            },
        ),
        # Pattern detection tools
        Tool(
            name="detect_candlestick_patterns",
            description="Detect candlestick patterns (hammer, engulfing, doji, stars, soldiers, etc.)",
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol (e.g., AAPL)",
                    },
                    "days": {
                        "type": "integer",
                        "description": "Number of days to analyze (default: 30, max: 252)",
                        "default": 30,
                        "minimum": 1,
                        "maximum": 252,
                    },
                    "patterns_to_detect": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional list of specific patterns to detect (default: all patterns)",
                    },
                },
                "required": ["ticker"],
            },
        ),
        Tool(
            name="detect_chart_patterns",
            description="Detect chart patterns (cup & handle, head & shoulders, triangles, channels, etc.)",
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol (e.g., AAPL)",
                    },
                    "lookback_days": {
                        "type": "integer",
                        "description": "Number of days to analyze (default: 60, max: 252)",
                        "default": 60,
                        "minimum": 20,
                        "maximum": 252,
                    },
                    "min_pattern_days": {
                        "type": "integer",
                        "description": "Minimum days for pattern formation (default: 10)",
                        "default": 10,
                        "minimum": 5,
                    },
                },
                "required": ["ticker"],
            },
        ),
        # Momentum & Squeeze tools
        Tool(
            name="get_squeeze_indicators",
            description="Get TTM Squeeze indicators (Bollinger Bands, Keltner Channels, momentum histogram, squeeze status)",
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol (e.g., AAPL)",
                    },
                    "lookback_days": {
                        "type": "integer",
                        "description": "Number of days to analyze (default: 60, max: 252)",
                        "default": 60,
                        "minimum": 1,
                        "maximum": 252,
                    },
                },
                "required": ["ticker"],
            },
        ),
        Tool(
            name="get_momentum_indicators",
            description="Get advanced momentum indicators (ADX, DMI, Stochastic, Williams %R, ROC)",
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol (e.g., AAPL)",
                    },
                    "lookback_days": {
                        "type": "integer",
                        "description": "Number of days to analyze (default: 60, max: 252)",
                        "default": 60,
                        "minimum": 1,
                        "maximum": 252,
                    },
                },
                "required": ["ticker"],
            },
        ),
        # Volume analysis tools
        Tool(
            name="get_volume_profile",
            description="Get volume distribution by price level (POC, value area, volume by price bins)",
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol (e.g., AAPL)",
                    },
                    "lookback_days": {
                        "type": "integer",
                        "description": "Number of days to analyze (default: 30, max: 252)",
                        "default": 30,
                        "minimum": 1,
                        "maximum": 252,
                    },
                    "price_bins": {
                        "type": "integer",
                        "description": "Number of price bins (default: 20, max: 100)",
                        "default": 20,
                        "minimum": 5,
                        "maximum": 100,
                    },
                },
                "required": ["ticker"],
            },
        ),
        Tool(
            name="detect_volume_anomalies",
            description="Detect unusual volume patterns (spikes, drops, price-volume divergences)",
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol (e.g., AAPL)",
                    },
                    "lookback_days": {
                        "type": "integer",
                        "description": "Number of days to analyze (default: 90, max: 252)",
                        "default": 90,
                        "minimum": 1,
                        "maximum": 252,
                    },
                    "threshold_multiplier": {
                        "type": "number",
                        "description": "Volume spike threshold multiplier (default: 2.0)",
                        "default": 2.0,
                        "minimum": 1.0,
                    },
                },
                "required": ["ticker"],
            },
        ),
        Tool(
            name="get_advanced_volume_indicators",
            description="Get advanced volume indicators (OBV, A/D line, CMF, VWAP)",
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol (e.g., AAPL)",
                    },
                    "lookback_days": {
                        "type": "integer",
                        "description": "Number of days to analyze (default: 60, max: 252)",
                        "default": 60,
                        "minimum": 1,
                        "maximum": 252,
                    },
                },
                "required": ["ticker"],
            },
        ),
        # Multi-timeframe analysis tools
        Tool(
            name="get_weekly_monthly_candles",
            description="Get weekly or monthly aggregated OHLCV candles from daily data",
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol (e.g., AAPL)",
                    },
                    "timeframe": {
                        "type": "string",
                        "description": "Timeframe: weekly or monthly",
                        "enum": ["weekly", "monthly"],
                    },
                    "periods": {
                        "type": "integer",
                        "description": "Number of periods (default: 52 for weekly, 12 for monthly)",
                        "minimum": 1,
                        "maximum": 260,
                    },
                },
                "required": ["ticker", "timeframe"],
            },
        ),
        Tool(
            name="get_multi_timeframe_alignment",
            description="Check indicator alignment across multiple timeframes (daily, weekly, monthly)",
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol (e.g., AAPL)",
                    },
                    "indicators": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Indicators to check (sma, rsi, macd)",
                    },
                    "timeframes": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Timeframes to analyze (daily, weekly, monthly)",
                    },
                },
                "required": ["ticker"],
            },
        ),
        Tool(
            name="calculate_relative_strength",
            description="Calculate relative strength vs benchmark (RS line, trend, beta, outperformance)",
            inputSchema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol (e.g., AAPL)",
                    },
                    "benchmark": {
                        "type": "string",
                        "description": "Benchmark ticker (default: SPY)",
                        "default": "SPY",
                    },
                    "lookback_days": {
                        "type": "integer",
                        "description": "Number of days to analyze (default: 90, max: 500)",
                        "default": 90,
                        "minimum": 20,
                        "maximum": 500,
                    },
                },
                "required": ["ticker"],
            },
        ),
    ]


@app.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    """Handle tool calls."""
    logger.info(f"Calling function: {name}")
    if arguments:
        logger.info(f"  Arguments: {arguments}")

    try:
        # Validate common arguments (tickers, dates, limits, etc.)
        arguments = validate_tool_arguments(name, arguments)

        # Get chart configuration
        config = get_chart_config()

        # Override detail level if specified in arguments
        if chart_detail := arguments.get("chart_detail"):
            try:
                config.detail = ChartDetail(chart_detail.lower())
            except ValueError:
                pass

        layout = get_layout(config)
        theme = get_theme(config.theme)

        # Check terminal width and get warning if needed
        width_warning = check_width_and_warn(
            layout.width,
            config.get_min_width(),
            theme,
        )

        chart: str | None = None
        result: Any = None

        if name == "list_companies":
            logger.info("  Executing: list_companies")
            result = list_companies(
                limit=arguments.get("limit", 100),
                offset=arguments.get("offset", 0),
                sector=arguments.get("sector"),
                index=arguments.get("index"),
            )
        elif name == "get_company_details":
            logger.info("  Executing: get_company_details")
            result = get_company_details(arguments["ticker"])
            if result is None:
                return [TextContent(type="text", text=f"Company {arguments['ticker']} not found")]
        elif name == "search_companies":
            logger.info("  Executing: search_companies")
            result = search_companies(
                query=arguments["query"],
                limit=arguments.get("limit", 20),
                index=arguments.get("index"),
            )
        elif name == "get_live_price":
            logger.info("  Executing: get_live_price")
            result = await get_live_price_async(
                ticker=arguments["ticker"],
                days=arguments.get("days", 7),
            )
        elif name == "get_live_prices_batch":
            logger.info("  Executing: get_live_prices_batch")
            result = await get_live_prices_batch_async(
                tickers=arguments["tickers"],
                days=arguments.get("days", 7),
            )
        elif name == "get_latest_price":
            logger.info("  Executing: get_latest_price")
            result = get_latest_price(
                ticker=arguments["ticker"], use_live=arguments.get("use_live", True)
            )
            if result is None:
                return [
                    TextContent(type="text", text=f"No price data found for {arguments['ticker']}")
                ]
        elif name == "get_stock_prices":
            logger.info("  Executing: get_stock_prices")
            result = get_stock_prices(
                ticker=arguments["ticker"],
                start_date=arguments["start_date"],
                end_date=arguments.get("end_date"),
                limit=arguments.get("limit", 252),
                use_live=arguments.get("use_live", True),
            )
            chart = render_price_chart(result, arguments["ticker"], layout, theme)
        elif name == "get_financial_ratios":
            logger.info("  Executing: get_financial_ratios")
            result = get_financial_ratios(
                ticker=arguments["ticker"],
                start_date=arguments["start_date"],
                end_date=arguments.get("end_date"),
                limit=arguments.get("limit", 100),
            )
            chart = render_ratios_chart(result, arguments["ticker"], layout, theme)
        elif name == "get_fundamentals":
            logger.info("  Executing: get_fundamentals")
            result = get_fundamentals(
                ticker=arguments["ticker"],
                timeframe=arguments.get("timeframe", "quarterly"),
                limit=arguments.get("limit", 4),
            )
            chart = render_fundamentals_chart(result, arguments["ticker"], layout, theme)
        elif name == "get_technical_indicators":
            logger.info("  Executing: get_technical_indicators")
            result = get_technical_indicators(
                ticker=arguments["ticker"],
                start_date=arguments["start_date"],
                end_date=arguments.get("end_date"),
                limit=arguments.get("limit", 252),
            )
        elif name == "get_latest_technical_indicators":
            logger.info("  Executing: get_latest_technical_indicators")
            result = get_latest_technical_indicators(ticker=arguments["ticker"])
            if result is None:
                return [
                    TextContent(
                        type="text",
                        text=f"No technical indicators found for {arguments['ticker']}",
                    )
                ]
        elif name == "get_intraday_bars":
            logger.info("  Executing: get_intraday_bars")
            result = get_intraday_bars(
                ticker=arguments.get("ticker"),
                tickers=arguments.get("tickers"),
                date=arguments.get("date"),
                limit=arguments.get("limit", 100),
                aggregate=arguments.get("aggregate", False),
            )
            if not result:
                ticker_desc = arguments.get("ticker") or arguments.get("tickers", "")
                return [
                    TextContent(
                        type="text",
                        text=f"No intraday data found for {ticker_desc}",
                    )
                ]
        elif name == "screen_technical_indicators":
            logger.info("  Executing: screen_technical_indicators")
            # Build filters dict from individual arguments
            filters: dict[str, tuple[float | None, float | None]] = {}
            if "rsi_14_min" in arguments or "rsi_14_max" in arguments:
                filters["rsi_14"] = (
                    arguments.get("rsi_14_min"),
                    arguments.get("rsi_14_max"),
                )
            if "volume_ratio_min" in arguments:
                filters["volume_ratio"] = (arguments.get("volume_ratio_min"), None)
            if "macd_histogram_min" in arguments or "macd_histogram_max" in arguments:
                filters["macd_histogram"] = (
                    arguments.get("macd_histogram_min"),
                    arguments.get("macd_histogram_max"),
                )
            result = screen_by_technical_indicators(
                filters=filters,
                target_date=arguments.get("target_date"),
                index=arguments.get("index"),
                limit=arguments.get("limit", 100),
            )
        elif name == "get_economy_data":
            logger.info("  Executing: get_economy_data")
            result = get_economy_data(
                indicator_type=arguments["indicator_type"],
                start_date=arguments["start_date"],
                end_date=arguments.get("end_date"),
                limit=arguments.get("limit", 100),
            )
            chart = render_economy_chart(result, arguments["indicator_type"], layout, theme)
        elif name == "get_economy_dashboard":
            logger.info("  Executing: get_economy_dashboard")
            result = get_economy_dashboard(limit=arguments.get("limit", 10))
            chart = render_economy_dashboard(result, layout, theme)
        elif name == "scan_ytd_performance":
            logger.info("  Executing: scan_ytd_performance")
            result = scan_ytd_performance(
                start_date=arguments.get("start_date"),
                large_cap_threshold=arguments.get("large_cap_threshold", 100.0),
                top_n=arguments.get("top_n", 10),
                index=arguments.get("index", "sp500"),
            )
            return [
                TextContent(
                    type="text",
                    text=f"{result['summary']}\n\n{result['table_a']}\n\n{result['table_b']}",
                )
            ]
        elif name == "execute_query":
            logger.info("  Executing: execute_query")
            from .database import log_execute_query

            sql_query = arguments["sql"]
            log_execute_query(sql_query, arguments.get("params"))
            result = execute_query(sql_query)
        # Schema discovery tools
        elif name == "describe_database":
            logger.info("  Executing: describe_database")
            result = describe_database()
        elif name == "describe_table":
            logger.info("  Executing: describe_table")
            result = describe_table(table_name=arguments["table_name"])
        # Sector tools
        elif name == "list_sectors":
            logger.info("  Executing: list_sectors")
            result = list_sectors(
                taxonomy=arguments.get("taxonomy", "gics"),
                index=arguments.get("index"),
                limit=arguments.get("limit", 100),
            )
        elif name == "get_sector_performance":
            logger.info("  Executing: get_sector_performance")
            result = get_sector_performance(
                taxonomy=arguments.get("taxonomy", "gics"),
                index=arguments.get("index"),
                limit=arguments.get("limit", 50),
            )
        # Market movers tools
        elif name == "get_top_movers":
            logger.info("  Executing: get_top_movers")
            result = get_top_movers(
                direction=arguments.get("direction", "both"),
                period=arguments.get("period", "1d"),
                limit=arguments.get("limit", 20),
                sector=arguments.get("sector"),
                index=arguments.get("index"),
                min_price=arguments.get("min_price"),
                min_volume=arguments.get("min_volume"),
            )
        elif name == "get_volume_leaders":
            logger.info("  Executing: get_volume_leaders")
            result = get_volume_leaders(
                metric=arguments.get("metric", "dollar_volume"),
                limit=arguments.get("limit", 20),
                sector=arguments.get("sector"),
                index=arguments.get("index"),
                min_price=arguments.get("min_price"),
            )
        elif name == "get_market_breadth":
            logger.info("  Executing: get_market_breadth")
            result = get_market_breadth(
                date=arguments.get("date"),
                index=arguments.get("index", "all"),
            )
        elif name == "list_technical_indicators":
            logger.info("  Executing: list_technical_indicators")
            result = list_technical_indicators(
                category=arguments.get("category"),
            )
        # Flexible screener
        elif name == "screen_stocks":
            logger.info("  Executing: screen_stocks")
            result = screen_stocks(
                filters=arguments.get("filters", {}),
                sector=arguments.get("sector"),
                sector_exclude=arguments.get("sector_exclude"),
                index=arguments.get("index"),
                taxonomy=arguments.get("taxonomy", "gics"),
                sort_by=arguments.get("sort_by", "market_cap"),
                sort_order=arguments.get("sort_order", "desc"),
                limit=arguments.get("limit", 50),
            )
        # YTD returns for ticker list
        elif name == "get_ytd_returns":
            logger.info("  Executing: get_ytd_returns")
            result = get_ytd_returns(
                tickers=arguments["tickers"],
                start_date=arguments.get("start_date"),
            )
        # SMA crossover detection
        elif name == "detect_crossovers":
            logger.info("  Executing: detect_crossovers")
            result = detect_crossovers(
                sma_period=arguments.get("sma_period", 150),
                direction=arguments.get("direction", "above"),
                lookback_days=arguments.get("lookback_days", 5),
                min_volume_ratio=arguments.get("min_volume_ratio"),
                index=arguments.get("index"),
                limit=arguments.get("limit", 50),
            )
        # 52-week extremes screener
        elif name == "get_52week_extremes":
            logger.info("  Executing: get_52week_extremes")
            result = get_52week_extremes(
                extreme=arguments.get("extreme", "both"),
                threshold_pct=arguments.get("threshold_pct", 2.0),
                index=arguments.get("index", "all"),
                min_volume=arguments.get("min_volume"),
                since_date=arguments.get("since_date"),
                include_fundamentals=arguments.get("include_fundamentals", False),
                limit=arguments.get("limit", 50),
            )
        # Daily range screener
        elif name == "get_daily_range_leaders":
            logger.info("  Executing: get_daily_range_leaders")
            result = get_daily_range_leaders(
                min_range_pct=arguments.get("min_range_pct", 3.0),
                max_range_pct=arguments.get("max_range_pct"),
                sector=arguments.get("sector"),
                index=arguments.get("index"),
                min_price=arguments.get("min_price"),
                min_volume=arguments.get("min_volume"),
                limit=arguments.get("limit", 50),
            )
        # Index tools
        elif name == "list_indices":
            logger.info("  Executing: list_indices")
            result = list_indices()
        elif name == "get_index_constituents":
            logger.info("  Executing: get_index_constituents")
            result = get_index_constituents(code=arguments["code"])
        elif name == "check_index_membership":
            logger.info("  Executing: check_index_membership")
            result = check_index_membership(ticker=arguments["ticker"])
        elif name == "get_index_with_prices":
            logger.info("  Executing: get_index_with_prices")
            result = get_index_with_prices(
                code=arguments["code"],
                limit=arguments.get("limit", 50),
            )
        # Corporate actions tools
        elif name == "get_stock_splits":
            logger.info("  Executing: get_stock_splits")
            result = get_stock_splits(
                ticker=arguments.get("ticker"),
                start_date=arguments.get("start_date"),
                end_date=arguments.get("end_date"),
                limit=arguments.get("limit", 100),
            )
        elif name == "get_dividends":
            logger.info("  Executing: get_dividends")
            result = get_dividends(
                ticker=arguments.get("ticker"),
                start_date=arguments.get("start_date"),
                end_date=arguments.get("end_date"),
                upcoming_only=arguments.get("upcoming_only", False),
                limit=arguments.get("limit", 100),
            )
        elif name == "get_ex_dividend_calendar":
            logger.info("  Executing: get_ex_dividend_calendar")
            result = get_ex_dividend_calendar(
                start_date=arguments["start_date"],
                end_date=arguments["end_date"],
                index=arguments.get("index", "all"),
                limit=arguments.get("limit", 200),
            )
        elif name == "get_recent_splits":
            logger.info("  Executing: get_recent_splits")
            result = get_recent_splits(
                days=arguments.get("days", 30),
                index=arguments.get("index", "all"),
            )
        elif name == "get_dividend_yield_leaders":
            logger.info("  Executing: get_dividend_yield_leaders")
            result = get_dividend_yield_leaders(
                index=arguments.get("index", "all"),
                min_yield=arguments.get("min_yield", 2.0),
                limit=arguments.get("limit", 50),
            )
        elif name == "get_earnings_calendar":
            logger.info("  Executing: get_earnings_calendar")
            result = get_earnings_calendar(
                start_date=arguments["start_date"],
                end_date=arguments["end_date"],
                index=arguments.get("index", "all"),
                timing=arguments.get("timing", "all"),
                limit=arguments.get("limit", 200),
            )
        elif name == "get_earnings_history":
            logger.info("  Executing: get_earnings_history")
            result = get_earnings_history(
                ticker=arguments["ticker"],
                limit=arguments.get("limit", 12),
            )
        elif name == "get_data_status":
            logger.info("  Executing: get_data_status")
            result = get_data_status()
        elif name == "get_recent_news_sentiment":
            logger.info("  Executing: get_recent_news_sentiment")
            result = get_recent_news_sentiment(
                ticker=arguments["ticker"],
                days_back=arguments.get("days_back", 14),
                max_articles=arguments.get("max_articles", 10),
            )
        elif name == "calculate_support_resistance_levels":
            logger.info("  Executing: calculate_support_resistance_levels")
            result = calculate_support_resistance_levels(
                ticker=arguments["ticker"],
                lookback_days=arguments.get("lookback_days", 90),
                max_levels=arguments.get("max_levels", 5),
                method=arguments.get("method", "cluster"),
            )
        elif name == "detect_candlestick_patterns":
            logger.info("  Executing: detect_candlestick_patterns")
            result = detect_candlestick_patterns(
                ticker=arguments["ticker"],
                days=arguments.get("days", 30),
                patterns_to_detect=arguments.get("patterns_to_detect"),
            )
        elif name == "detect_chart_patterns":
            logger.info("  Executing: detect_chart_patterns")
            result = detect_chart_patterns(
                ticker=arguments["ticker"],
                lookback_days=arguments.get("lookback_days", 60),
                min_pattern_days=arguments.get("min_pattern_days", 10),
            )
        elif name == "get_squeeze_indicators":
            logger.info("  Executing: get_squeeze_indicators")
            result = get_squeeze_indicators(
                ticker=arguments["ticker"],
                lookback_days=arguments.get("lookback_days", 60),
            )
        elif name == "get_momentum_indicators":
            logger.info("  Executing: get_momentum_indicators")
            result = get_momentum_indicators(
                ticker=arguments["ticker"],
                lookback_days=arguments.get("lookback_days", 60),
            )
        elif name == "get_volume_profile":
            logger.info("  Executing: get_volume_profile")
            result = get_volume_profile(
                ticker=arguments["ticker"],
                lookback_days=arguments.get("lookback_days", 30),
                price_bins=arguments.get("price_bins", 20),
            )
        elif name == "detect_volume_anomalies":
            logger.info("  Executing: detect_volume_anomalies")
            result = detect_volume_anomalies(
                ticker=arguments["ticker"],
                lookback_days=arguments.get("lookback_days", 90),
                threshold_multiplier=arguments.get("threshold_multiplier", 2.0),
            )
        elif name == "get_advanced_volume_indicators":
            logger.info("  Executing: get_advanced_volume_indicators")
            result = get_advanced_volume_indicators(
                ticker=arguments["ticker"],
                lookback_days=arguments.get("lookback_days", 60),
            )
        elif name == "get_weekly_monthly_candles":
            logger.info("  Executing: get_weekly_monthly_candles")
            result = get_weekly_monthly_candles(
                ticker=arguments["ticker"],
                timeframe=arguments["timeframe"],
                periods=arguments.get("periods"),
            )
        elif name == "get_multi_timeframe_alignment":
            logger.info("  Executing: get_multi_timeframe_alignment")
            result = get_multi_timeframe_alignment(
                ticker=arguments["ticker"],
                indicators=arguments.get("indicators"),
                timeframes=arguments.get("timeframes"),
            )
        elif name == "calculate_relative_strength":
            logger.info("  Executing: calculate_relative_strength")
            result = calculate_relative_strength(
                ticker=arguments["ticker"],
                benchmark=arguments.get("benchmark", "SPY"),
                lookback_days=arguments.get("lookback_days", 90),
            )
        else:
            raise ValueError(f"Unknown tool: {name}")

        # Build response with chart and data
        parts = []

        # Add width warning if needed
        if width_warning:
            parts.append(width_warning)
            parts.append("")

        # Add chart if available
        if chart:
            parts.append(chart)
            parts.append("")
            parts.append("\u2500" * 40 + " DATA " + "\u2500" * 40)
            parts.append("")

        # Add JSON data
        parts.append(json.dumps(result, indent=2, default=str))

        return [TextContent(type="text", text="\n".join(parts))]

    except Exception as e:
        logger.error(f"Error executing tool {name}: {e}", exc_info=True)
        return [TextContent(type="text", text=f"Error: {str(e)}")]
    except BaseException as e:
        logger.error(f"Unexpected error in tool {name}: {e}", exc_info=True)
        return [TextContent(type="text", text=f"Server error: {str(e)}")]


async def main():
    """Main entry point."""
    logger.info("Starting Stock Data MCP Server")
    logger.info("Press Ctrl-C to exit gracefully")

    # Verify database connection
    try:
        from .database import get_database_url

        get_database_url()
        logger.info("Database configuration verified")
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)

    # Run server with stdio transport
    try:
        async with stdio_server() as (read_stream, write_stream):
            await app.run(
                read_stream,
                write_stream,
                app.create_initialization_options(),
            )
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Server error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    import asyncio

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting...")
        sys.exit(0)
