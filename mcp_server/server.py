#!/usr/bin/env python3
"""
Stock Data MCP Server

An MCP server providing read-only access to stock market data in PostgreSQL.
Includes colorful Unicode charts for data visualization.
"""

import asyncio
import json
import logging
import os
import sys
import time
from functools import partial
from typing import Any

from jsonschema import Draft202012Validator
from mcp import MCPError
from mcp.server import Server, ServerRequestContext
from mcp.server.stdio import stdio_server
from mcp.types import (
    INVALID_PARAMS,
    CallToolRequestParams,
    CallToolResult,
    ContentBlock,
    ListToolsResult,
    PaginatedRequestParams,
    TextContent,
    Tool,
    ToolAnnotations,
)
from sawa.utils.logging import (  # noqa: E402
    RedactingFilter,
    RedactingFormatter,
    install_redaction_filters,
)
from sawa.utils.security import redact_sensitive_text  # noqa: E402

from . import database as database_runtime  # noqa: E402
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
from .monitoring import configure_file_logging, record_call_outcome  # noqa: E402
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
    get_market_internals,
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
from .tools.patterns import (  # noqa: E402
    SUPPORTED_PATTERNS,
    detect_candlestick_patterns,
    detect_chart_patterns,
)
from .tools.scanner import scan_ytd_performance_async  # noqa: E402
from .tools.schema import describe_database, describe_table  # noqa: E402
from .tools.screener import (  # noqa: E402
    FILTER_SPECS,
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
from .utils.json_values import normalize_json_value  # noqa: E402
from .validation import validate_tool_arguments  # noqa: E402

# Setup logging. stderr handler is kept so MCP clients (Claude Desktop / Code)
# still see live diagnostics; configure_file_logging attaches a daily-rotated
# file handler under ~/.sawa/logs/mcp.log so the audit trail survives the pipe.
log_level = os.environ.get("MCP_LOG_LEVEL", "info").upper()
_stderr_handler = logging.StreamHandler(sys.stderr)
_stderr_handler.addFilter(RedactingFilter())
_stderr_handler.setFormatter(
    RedactingFormatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )
)
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    handlers=[_stderr_handler],
)
install_redaction_filters()
logger = logging.getLogger(__name__)
_mcp_log_file = configure_file_logging(logger)


async def _run_sync(func, *args, **kwargs):
    """Run a sync function in a thread executor to avoid blocking the event loop."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(func, *args, **kwargs))


def _get_chart_runtime(arguments: dict[str, Any]) -> tuple[Any, Any, str | None]:
    """Load chart-only configuration without coupling it to data tools."""
    config = get_chart_config()
    if chart_detail := arguments.get("chart_detail"):
        config.detail = ChartDetail(chart_detail.lower())
    layout = get_layout(config)
    theme = get_theme(config.theme, colors_enabled=config.colors_enabled)
    width_warning = check_width_and_warn(
        layout.width,
        config.get_min_width(),
        theme,
    )
    return layout, theme, width_warning


_RESPONSE_SCHEMA_VERSION = "sawa.mcp.tool_response.v1"
_INDEX_CODE_PATTERN = r"^[a-z][a-z0-9_]{0,31}$"
_INDEX_CODE_EXAMPLES = "sp500, nasdaq_listed, us_active, nasdaq100, dow30, russell1000, mag7"
_SERVER_INSTRUCTIONS = (
    "PRICE DATA TOOL SELECTION:\n"
    "- Current session / today's price action / intraday -> get_intraday_bars\n"
    "- Historical daily OHLCV with chart -> get_stock_prices\n"
    "- Quick latest closing price -> get_latest_price\n"
    "- Real-time quote from API -> get_live_price\n"
    "During market hours (Mon-Fri 9:30AM-4PM ET), prefer get_intraday_bars "
    "for any question about today's prices.\n\n"
    "TOOL RESPONSE FORMAT:\n"
    "Successful tools return one JSON object with data, chart, warnings, and metadata. "
    "Read structuredContent for machine processing; content contains the same JSON "
    "for compatibility."
)
_TOOL_OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "data": {},
        "chart": {"type": ["string", "null"]},
        "warnings": {"type": "array", "items": {"type": "string"}},
        "metadata": {
            "type": "object",
            "properties": {
                "tool": {"type": "string"},
                "schema_version": {"type": "string"},
                "duration_ms": {"type": "number"},
                "source": {"type": "string", "maxLength": 64},
            },
            "required": ["tool", "schema_version"],
            "additionalProperties": False,
        },
    },
    "required": ["data", "chart", "warnings", "metadata"],
    "additionalProperties": False,
}
_OPEN_WORLD_TOOLS = {"get_live_price", "get_live_prices_batch"}
_TICKER_INPUT_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9.\-]{0,9}$"
_DATE_INPUT_PATTERN = r"^\d{4}-\d{2}-\d{2}$"
_MAX_FREE_TEXT_INPUT_LENGTH = 256
_MAX_TABLE_NAME_INPUT_LENGTH = 63
_SCANNER_INDEX_CODES = [
    "sp500",
    "nasdaq_listed",
    "us_active",
    "both",
]


def _bound_input_strings(value: Any) -> Any:
    """Return a schema copy with a finite bound on every input string."""
    if isinstance(value, dict):
        bounded = {key: _bound_input_strings(item) for key, item in value.items()}
        if bounded.get("type") == "string":
            bounded.setdefault("minLength", 1)
            bounded.setdefault("maxLength", _MAX_FREE_TEXT_INPUT_LENGTH)
        return bounded
    if isinstance(value, list):
        return [_bound_input_strings(item) for item in value]
    return value


def _index_schema_description(description: str, suffix: str = "") -> str:
    return (
        f"{description} ({_INDEX_CODE_EXAMPLES}{suffix}). "
        "Use list_indices for the current database-backed codes."
    )


def _index_filter_schema(
    *,
    description: str = "Filter by index membership",
    allow_all: bool = False,
    allow_both: bool = False,
    default: str | None = None,
) -> dict[str, Any]:
    """Schema for database-backed index filters without freezing valid codes."""
    suffix = ""
    if allow_all:
        suffix = ", or all"
    elif allow_both:
        suffix = ", or both"

    schema: dict[str, Any] = {
        "type": "string",
        "description": _index_schema_description(description, suffix),
        "pattern": _INDEX_CODE_PATTERN,
        "not": {
            "enum": [
                "nasdaq5000",
                *([] if allow_all else ["all"]),
                *([] if allow_both else ["both"]),
            ]
        },
    }
    if default is not None:
        schema["default"] = default
    return schema


def _index_code_schema() -> dict[str, Any]:
    """Schema for a single database-backed index code."""
    return {
        "type": "string",
        "description": _index_schema_description("Index code"),
        "pattern": _INDEX_CODE_PATTERN,
        "not": {"enum": ["all", "both", "nasdaq5000"]},
    }


def _tool_response(
    name: str,
    result: Any,
    *,
    chart: str | None = None,
    warnings: list[str] | None = None,
    duration_ms: float | None = None,
    metadata: dict[str, Any] | None = None,
) -> list[TextContent]:
    """Build the stable JSON response envelope returned by every successful tool."""
    response_metadata: dict[str, Any] = {
        "tool": name,
        "schema_version": _RESPONSE_SCHEMA_VERSION,
    }
    if duration_ms is not None:
        response_metadata["duration_ms"] = round(duration_ms, 2)
    if metadata:
        response_metadata.update(metadata)

    payload = normalize_json_value(
        {
            "data": result,
            "chart": chart,
            "warnings": warnings or [],
            "metadata": response_metadata,
        }
    )
    rendered = json.dumps(payload, indent=2, ensure_ascii=False, allow_nan=False)
    rendered_bytes = len(rendered.encode("utf-8"))
    if rendered_bytes > database_runtime.MAX_RESULT_BYTES:
        raise ValueError(
            "Tool response exceeds maximum serialized size of "
            f"{database_runtime.MAX_RESULT_BYTES} bytes"
        )
    return [
        TextContent(
            type="text",
            text=rendered,
        )
    ]


def _success_response(
    name: str,
    result: Any,
    started: float,
    *,
    chart: str | None = None,
    warnings: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> list[TextContent]:
    """Return a successful JSON envelope; the protocol boundary records it."""
    duration_ms = (time.monotonic() - started) * 1000
    return _tool_response(
        name,
        result,
        chart=chart,
        warnings=warnings,
        duration_ms=duration_ms,
        metadata=metadata,
    )


async def list_tools() -> list[Tool]:
    """List all available tools."""
    tools = [
        Tool(
            name="list_companies",
            description="List active companies with optional filtering by sector",
            input_schema={
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
                        "description": "Number of results to skip (max: 10000)",
                        "default": 0,
                        "minimum": 0,
                        "maximum": 10000,
                    },
                    "sector": {
                        "type": "string",
                        "description": "Filter by sector/SIC description (partial match)",
                    },
                    "index": _index_filter_schema(),
                },
            },
        ),
        Tool(
            name="get_company_details",
            description="Get detailed company information including latest price and metrics",
            input_schema={
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
            input_schema={
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
                    "index": _index_filter_schema(),
                },
                "required": ["query"],
            },
        ),
        Tool(
            name="get_live_price",
            description="Get live stock price from Polygon API (real-time, not from database). For intraday OHLCV bars and session summaries, use get_intraday_bars.",  # noqa: E501
            input_schema={
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
            input_schema={
                "type": "object",
                "properties": {
                    "tickers": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": 10,
                            "pattern": r"^[A-Za-z0-9][A-Za-z0-9.\-]{0,9}$",
                        },
                        "minItems": 1,
                        "maxItems": 50,
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
            description="Get the most recent closing price from the database (fast). Returns yesterday's close or today's if market has closed. For current-session intraday prices during market hours, use get_intraday_bars.",  # noqa: E501
            input_schema={
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
            description="Get historical daily OHLCV prices with visual chart. Best for multi-day/week/month/year ranges. For current trading session data during market hours, use get_intraday_bars instead.",  # noqa: E501
            input_schema={
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
            input_schema={
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
            input_schema={
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
            input_schema={
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
            input_schema={
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
            description="Get today's stored intraday price bars (15-min delayed). PREFERRED tool for any current-session or today's price queries during market hours. Supports multiple tickers, selectable stored bar sizes, and daily OHLCV aggregation via aggregate=true.",  # noqa: E501
            input_schema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol (e.g., AAPL)",
                    },
                    "tickers": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": 10,
                            "pattern": r"^[A-Za-z0-9][A-Za-z0-9.\-]{0,9}$",
                        },
                        "minItems": 1,
                        "maxItems": 20,
                        "description": "Multiple ticker symbols (e.g., ['SPY', 'QQQ', 'DIA']). Use instead of ticker for multi-stock queries.",  # noqa: E501
                    },
                    "date": {
                        "type": "string",
                        "description": "Date in YYYY-MM-DD format (default: today)",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Most-recent bars per ticker (default: 100, max: 500)",
                        "default": 100,
                        "minimum": 1,
                        "maximum": 500,
                    },
                    "bar_size_minutes": {
                        "type": "integer",
                        "description": "Stored bar interval to query",
                        "enum": [1, 5, 15, 30, 60],
                        "default": 5,
                    },
                    "aggregate": {
                        "type": "boolean",
                        "description": "Return daily OHLCV summary instead of individual bars",  # noqa: E501
                        "default": False,
                    },
                },
                "oneOf": [
                    {"required": ["ticker"], "not": {"required": ["tickers"]}},
                    {"required": ["tickers"], "not": {"required": ["ticker"]}},
                ],
            },
        ),
        Tool(
            name="screen_technical_indicators",
            description="Screen stocks by technical indicator values (e.g., RSI < 30)",
            input_schema={
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
                    "index": _index_filter_schema(),
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
            input_schema={
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
                            "market_internals",
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
            input_schema={
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
            name="get_market_internals",
            description=(
                "Get daily market internals from FRED: CBOE VIX (vix), "
                "3-month VIX (vix3m), and US high-yield credit spread (hy_spread). "
                "Includes derived metrics: term_structure (vix3m/vix), 20-day "
                "SMA/stddev of VIX, and 252-day percentile ranks for VIX and "
                "HY spread. One row per trading day."
            ),
            input_schema={
                "type": "object",
                "properties": {
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
                },
                "required": ["start_date"],
            },
        ),
        Tool(
            name="scan_ytd_performance",
            description=(
                "Scan stored prices and index membership for performance analysis "
                "with sector grouping"
            ),
            input_schema={
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
                        "minimum": 0,
                        "maximum": 1000000,
                    },
                    "top_n": {
                        "type": "integer",
                        "description": "Number of top winners to show (default: 10)",
                        "default": 10,
                        "minimum": 1,
                        "maximum": 50,
                    },
                    "bottom_n": {
                        "type": "integer",
                        "description": "Number of bottom performers to show (default: 10)",
                        "default": 10,
                        "minimum": 1,
                        "maximum": 50,
                    },
                    "index": {
                        "type": "string",
                        "description": "Supported stored constituent universe",
                        "enum": _SCANNER_INDEX_CODES,
                        "default": "sp500",
                    },
                },
            },
        ),
        # Schema discovery tools
        Tool(
            name="describe_database",
            description="List all tables with column counts, row counts, and descriptions",
            input_schema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="describe_table",
            description="Get detailed table info: columns, types, samples, foreign keys, indexes",
            input_schema={
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
            input_schema={
                "type": "object",
                "properties": {
                    "taxonomy": {
                        "type": "string",
                        "description": "Classification system: 'sic' (SEC) or 'gics' (S&P)",
                        "enum": ["sic", "gics"],
                        "default": "gics",
                    },
                    "index": _index_filter_schema(),
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
            input_schema={
                "type": "object",
                "properties": {
                    "taxonomy": {
                        "type": "string",
                        "description": "Classification system: 'sic' or 'gics' (default: gics)",
                        "enum": ["sic", "gics"],
                        "default": "gics",
                    },
                    "index": _index_filter_schema(),
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
            input_schema={
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
                    "index": _index_filter_schema(),
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
            input_schema={
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
                    "index": _index_filter_schema(),
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
            input_schema={
                "type": "object",
                "properties": {
                    "date": {
                        "type": "string",
                        "description": "Date YYYY-MM-DD (default: latest trading day)",
                    },
                    "index": _index_filter_schema(allow_all=True, default="all"),
                },
            },
        ),
        Tool(
            name="list_technical_indicators",
            description="List available technical indicators with descriptions",
            input_schema={
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
            input_schema={
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
                    "index": _index_filter_schema(),
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
            input_schema={
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
                    "index": _index_filter_schema(allow_all=True, default="all"),
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
            input_schema={
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
                    "index": _index_filter_schema(),
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
            input_schema={
                "type": "object",
                "properties": {
                    "tickers": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": 10,
                            "pattern": _TICKER_INPUT_PATTERN,
                        },
                        "minItems": 1,
                        "maxItems": 50,
                        "uniqueItems": True,
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
            input_schema={
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
                    "index": _index_filter_schema(),
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
            description="List all market indices with constituent counts",
            input_schema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="get_index_constituents",
            description="Get all constituent stocks of a market index",
            input_schema={
                "type": "object",
                "properties": {
                    "code": _index_code_schema(),
                },
                "required": ["code"],
            },
        ),
        Tool(
            name="check_index_membership",
            description="Check which market indices a stock belongs to",
            input_schema={
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
            input_schema={
                "type": "object",
                "properties": {
                    "code": _index_code_schema(),
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
            input_schema={
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
                        "description": "Maximum results (default: 100, max: 500)",
                        "default": 100,
                        "minimum": 1,
                        "maximum": 500,
                    },
                },
            },
        ),
        Tool(
            name="get_dividends",
            description="Get dividend history or upcoming dividends",
            input_schema={
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
                        "description": "Maximum results (default: 100, max: 500)",
                        "default": 100,
                        "minimum": 1,
                        "maximum": 500,
                    },
                },
            },
        ),
        Tool(
            name="get_ex_dividend_calendar",
            description="Get ex-dividend calendar for a date range",
            input_schema={
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
                    "index": _index_filter_schema(allow_all=True, default="all"),
                    "limit": {
                        "type": "integer",
                        "description": "Maximum results (default: 200, max: 500)",
                        "default": 200,
                        "minimum": 1,
                        "maximum": 500,
                    },
                },
                "required": ["start_date", "end_date"],
            },
        ),
        Tool(
            name="get_recent_splits",
            description="Get recent stock splits",
            input_schema={
                "type": "object",
                "properties": {
                    "days": {
                        "type": "integer",
                        "description": "Days to look back (default: 30, max: 30)",
                        "default": 30,
                        "minimum": 1,
                        "maximum": 30,
                    },
                    "index": _index_filter_schema(allow_all=True, default="all"),
                },
            },
        ),
        Tool(
            name="get_dividend_yield_leaders",
            description="Get stocks with highest dividend yields",
            input_schema={
                "type": "object",
                "properties": {
                    "index": _index_filter_schema(allow_all=True, default="all"),
                    "min_yield": {
                        "type": "number",
                        "description": "Minimum dividend yield % (default: 2.0)",
                        "default": 2.0,
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
        Tool(
            name="get_earnings_calendar",
            description="Get earnings calendar for a date range",
            input_schema={
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
                    "index": _index_filter_schema(allow_all=True, default="all"),
                    "timing": {
                        "type": "string",
                        "description": "Filter by timing: BMO, AMC, or all",
                        "enum": ["BMO", "AMC", "all"],
                        "default": "all",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum results (default: 200, max: 500)",
                        "default": 200,
                        "minimum": 1,
                        "maximum": 500,
                    },
                },
                "required": ["start_date", "end_date"],
            },
        ),
        Tool(
            name="get_earnings_history",
            description="Get historical earnings for a ticker",
            input_schema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of quarters (default: 12, max: 40)",
                        "default": 12,
                        "minimum": 1,
                        "maximum": 40,
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
            input_schema={
                "type": "object",
                "properties": {},
            },
        ),
        # News sentiment tools
        Tool(
            name="get_recent_news_sentiment",
            description="Get recent news articles with sentiment analysis for a ticker",
            input_schema={
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
            description=(
                "Calculate support and resistance levels using "
                "pivot points, price clustering, or volume analysis"
            ),
            input_schema={
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
                        "description": (
                            "Detection method: pivot (pivot points), "
                            "cluster (price clustering), volume (volume profile)"
                        ),
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
            description=(
                "Detect candlestick patterns "
                "(hammer, engulfing, doji, stars, soldiers, etc.)"
            ),
            input_schema={
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
                        "items": {
                            "type": "string",
                            "enum": sorted(SUPPORTED_PATTERNS),
                        },
                        "minItems": 1,
                        "maxItems": len(SUPPORTED_PATTERNS),
                        "uniqueItems": True,
                        "description": (
                            "Optional list of specific patterns to detect "
                            "(default: all patterns)"
                        ),
                    },
                },
                "required": ["ticker"],
            },
        ),
        Tool(
            name="detect_chart_patterns",
            description=(
                "Detect chart patterns "
                "(cup & handle, head & shoulders, triangles, channels, etc.)"
            ),
            input_schema={
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
                        "maximum": 252,
                    },
                },
                "required": ["ticker"],
            },
        ),
        # Momentum & Squeeze tools
        Tool(
            name="get_squeeze_indicators",
            description=(
                "Get TTM Squeeze indicators (Bollinger Bands, Keltner Channels, "
                "momentum histogram, squeeze status)"
            ),
            input_schema={
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
                },
                "required": ["ticker"],
            },
        ),
        Tool(
            name="get_momentum_indicators",
            description="Get advanced momentum indicators (ADX, DMI, Stochastic, Williams %R, ROC)",
            input_schema={
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
            description=(
                "Get volume distribution by price level "
                "(POC, value area, volume by price bins)"
            ),
            input_schema={
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
                        "description": "Number of price bins (default: 20, max: 50)",
                        "default": 20,
                        "minimum": 5,
                        "maximum": 50,
                    },
                },
                "required": ["ticker"],
            },
        ),
        Tool(
            name="detect_volume_anomalies",
            description="Detect unusual volume patterns (spikes, drops, price-volume divergences)",
            input_schema={
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
                        "minimum": 20,
                        "maximum": 252,
                    },
                    "threshold_multiplier": {
                        "type": "number",
                        "description": "Volume spike threshold multiplier (default: 2.0)",
                        "default": 2.0,
                        "minimum": 1.1,
                    },
                },
                "required": ["ticker"],
            },
        ),
        Tool(
            name="get_advanced_volume_indicators",
            description="Get advanced volume indicators (OBV, A/D line, CMF, VWAP)",
            input_schema={
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
                        "minimum": 5,
                        "maximum": 252,
                    },
                },
                "required": ["ticker"],
            },
        ),
        # Multi-timeframe analysis tools
        Tool(
            name="get_weekly_monthly_candles",
            description=(
                "Get weekly or monthly aggregated OHLCV candles from daily data. The "
                "newest candle may be the still-forming current period — check the "
                "is_partial flag (true = period in progress, not yet complete)."
            ),
            input_schema={
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
                        "description": (
                            "Number of periods (default: 52 weekly/12 monthly; "
                            "max: 260 weekly/120 monthly)"
                        ),
                        "minimum": 1,
                        "maximum": 260,
                    },
                },
                "required": ["ticker", "timeframe"],
                "allOf": [
                    {
                        "if": {
                            "properties": {"timeframe": {"const": "monthly"}},
                            "required": ["timeframe"],
                        },
                        "then": {
                            "properties": {"periods": {"maximum": 120}}
                        },
                    }
                ],
            },
        ),
        Tool(
            name="get_multi_timeframe_alignment",
            description=(
                "Check indicator alignment across multiple timeframes "
                "(daily, weekly, monthly)"
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": "string",
                        "description": "Stock ticker symbol (e.g., AAPL)",
                    },
                    "indicators": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": ["sma", "sma_trend", "rsi", "macd"],
                        },
                        "minItems": 1,
                        "maxItems": 3,
                        "uniqueItems": True,
                        "description": (
                            "Indicators to check (sma or sma_trend, rsi, macd)"
                        ),
                    },
                    "timeframes": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": ["daily", "weekly", "monthly"],
                        },
                        "minItems": 1,
                        "maxItems": 3,
                        "uniqueItems": True,
                        "description": "Timeframes to analyze (daily, weekly, monthly)",
                    },
                },
                "required": ["ticker"],
            },
        ),
        Tool(
            name="calculate_relative_strength",
            description=(
                "Calculate relative strength vs benchmark "
                "(RS line, trend, beta, outperformance)"
            ),
            input_schema={
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
    # Arbitrary PostgreSQL SELECT statements can invoke volatile user-defined
    # or extension functions with side effects. A keyword blocklist cannot
    # make that surface safe, so raw SQL is never exposed as an MCP tool.
    listed_tools: list[Tool] = []
    for tool in tools:
        input_schema = _bound_input_strings(
            {**tool.input_schema, "additionalProperties": False}
        )
        root_properties = dict(input_schema.get("properties", {}))
        for ticker_field in ("ticker", "benchmark"):
            if ticker_field not in root_properties:
                continue
            ticker_schema = dict(root_properties[ticker_field])
            ticker_schema.update(
                {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 10,
                    "pattern": _TICKER_INPUT_PATTERN,
                }
            )
            root_properties[ticker_field] = ticker_schema
            input_schema["properties"] = root_properties
        for date_field in (
            "date",
            "start_date",
            "end_date",
            "target_date",
            "since_date",
        ):
            if date_field in root_properties:
                date_schema = dict(root_properties[date_field])
                date_schema.update(
                    {
                        "type": "string",
                        "minLength": 10,
                        "maxLength": 10,
                        "pattern": _DATE_INPUT_PATTERN,
                    }
                )
                root_properties[date_field] = date_schema
                input_schema["properties"] = root_properties
        if "table_name" in root_properties:
            table_schema = dict(root_properties["table_name"])
            table_schema["maxLength"] = _MAX_TABLE_NAME_INPUT_LENGTH
            root_properties["table_name"] = table_schema
            input_schema["properties"] = root_properties
        if tool.name == "screen_stocks":
            filters_schema = dict(root_properties["filters"])
            described_filters = dict(filters_schema["properties"])
            generic_range_schema = {
                "type": "array",
                "items": {"type": ["number", "null"]},
                "minItems": 2,
                "maxItems": 2,
            }
            filters_schema["properties"] = {
                name: described_filters.get(name, generic_range_schema)
                for name in sorted(FILTER_SPECS)
            }
            filters_schema["additionalProperties"] = False
            root_properties["filters"] = filters_schema
            input_schema["properties"] = root_properties

        listed_tools.append(
            tool.model_copy(
                update={
                    "annotations": ToolAnnotations(
                        read_only_hint=True,
                        destructive_hint=False,
                        open_world_hint=tool.name in _OPEN_WORLD_TOOLS,
                    ),
                    "input_schema": input_schema,
                    "output_schema": _TOOL_OUTPUT_SCHEMA,
                }
            )
        )
    return listed_tools


async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    """Handle tool calls."""
    logger.info("Calling function: %s", name)

    started = time.monotonic()
    try:
        # Validate common arguments (tickers, dates, limits, etc.)
        arguments = validate_tool_arguments(name, arguments)
        if arguments:
            # Values can be large or private even for read-only tools. Schema
            # and domain validation happen first; logs retain only bounded
            # diagnostic shape instead of copying user payloads.
            logger.info("  Validated argument fields: %d", len(arguments))

        chart: str | None = None
        width_warning: str | None = None
        warnings: list[str] = []
        result: Any = None

        if name == "list_companies":
            logger.info("  Executing: list_companies")
            result = await _run_sync(
                list_companies,
                limit=arguments.get("limit", 100),
                offset=arguments.get("offset", 0),
                sector=arguments.get("sector"),
                index=arguments.get("index"),
            )
        elif name == "get_company_details":
            logger.info("  Executing: get_company_details")
            result = await _run_sync(get_company_details, arguments["ticker"])
            if result is None:
                warnings.append(f"Company {arguments['ticker']} not found")
                return _success_response(name, None, started, warnings=warnings)
        elif name == "search_companies":
            logger.info("  Executing: search_companies")
            result = await _run_sync(
                search_companies,
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
            provider_failed_tickers = [
                ticker
                for ticker, value in result.items()
                if isinstance(value, dict)
                and value.get("error_type") == "provider_error"
            ]
            no_data_tickers = [
                ticker
                for ticker, value in result.items()
                if isinstance(value, dict) and value.get("error_type") == "no_data"
            ]
            if result and len(provider_failed_tickers) == len(result):
                raise RuntimeError(
                    "Live-price provider failed for every requested ticker"
                )
            if provider_failed_tickers:
                warnings.append(
                    f"Live-price provider failed for {len(provider_failed_tickers)}/"
                    f"{len(result)} tickers: {', '.join(provider_failed_tickers)}"
                )
            if no_data_tickers:
                warnings.append(
                    f"No live-price data for {len(no_data_tickers)}/"
                    f"{len(result)} tickers: {', '.join(no_data_tickers)}"
                )
        elif name == "get_latest_price":
            logger.info("  Executing: get_latest_price")
            result = await _run_sync(
                get_latest_price,
                ticker=arguments["ticker"], use_live=arguments.get("use_live", True),
            )
            if result is None:
                warnings.append(f"No price data found for {arguments['ticker']}")
                return _success_response(name, None, started, warnings=warnings)
        elif name == "get_stock_prices":
            logger.info("  Executing: get_stock_prices")
            result = await _run_sync(
                get_stock_prices,
                ticker=arguments["ticker"],
                start_date=arguments["start_date"],
                end_date=arguments.get("end_date"),
                limit=arguments.get("limit", 252),
                use_live=arguments.get("use_live", True),
            )
            try:
                layout, theme, width_warning = _get_chart_runtime(arguments)
                chart = render_price_chart(result, arguments["ticker"], layout, theme)
            except Exception as render_err:  # noqa: BLE001 - never fail the tool on a chart-render error
                logger.warning(
                    "  Chart render failed for %s: %s: %s",
                    name,
                    type(render_err).__name__,
                    _truncate_utf8(redact_sensitive_text(render_err), 500),
                )
        elif name == "get_financial_ratios":
            logger.info("  Executing: get_financial_ratios")
            result = await _run_sync(
                get_financial_ratios,
                ticker=arguments["ticker"],
                start_date=arguments["start_date"],
                end_date=arguments.get("end_date"),
                limit=arguments.get("limit", 100),
            )
            try:
                layout, theme, width_warning = _get_chart_runtime(arguments)
                chart = render_ratios_chart(result, arguments["ticker"], layout, theme)
            except Exception as render_err:  # noqa: BLE001 - never fail the tool on a chart-render error
                logger.warning(
                    "  Chart render failed for %s: %s: %s",
                    name,
                    type(render_err).__name__,
                    _truncate_utf8(redact_sensitive_text(render_err), 500),
                )
        elif name == "get_fundamentals":
            logger.info("  Executing: get_fundamentals")
            result = await _run_sync(
                get_fundamentals,
                ticker=arguments["ticker"],
                timeframe=arguments.get("timeframe", "quarterly"),
                limit=arguments.get("limit", 4),
            )
            try:
                layout, theme, width_warning = _get_chart_runtime(arguments)
                chart = render_fundamentals_chart(result, arguments["ticker"], layout, theme)
            except Exception as render_err:  # noqa: BLE001 - never fail the tool on a chart-render error
                logger.warning(
                    "  Chart render failed for %s: %s: %s",
                    name,
                    type(render_err).__name__,
                    _truncate_utf8(redact_sensitive_text(render_err), 500),
                )
        elif name == "get_technical_indicators":
            logger.info("  Executing: get_technical_indicators")
            result = await _run_sync(
                get_technical_indicators,
                ticker=arguments["ticker"],
                start_date=arguments["start_date"],
                end_date=arguments.get("end_date"),
                limit=arguments.get("limit", 252),
            )
        elif name == "get_latest_technical_indicators":
            logger.info("  Executing: get_latest_technical_indicators")
            result = await _run_sync(get_latest_technical_indicators, ticker=arguments["ticker"])
            if result is None:
                warnings.append(f"No technical indicators found for {arguments['ticker']}")
                return _success_response(name, None, started, warnings=warnings)
        elif name == "get_intraday_bars":
            logger.info("  Executing: get_intraday_bars")
            result = await _run_sync(
                get_intraday_bars,
                ticker=arguments.get("ticker"),
                tickers=arguments.get("tickers"),
                date=arguments.get("date"),
                limit=arguments.get("limit", 100),
                aggregate=arguments.get("aggregate", False),
                bar_size_minutes=arguments.get("bar_size_minutes", 5),
            )
            if not result:
                ticker_desc = arguments.get("ticker") or arguments.get("tickers", "")
                warnings.append(f"No intraday data found for {ticker_desc}")
                return _success_response(name, [], started, warnings=warnings)
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
            result = await _run_sync(
                screen_by_technical_indicators,
                filters=filters,
                target_date=arguments.get("target_date"),
                index=arguments.get("index"),
                limit=arguments.get("limit", 100),
            )
        elif name == "get_economy_data":
            logger.info("  Executing: get_economy_data")
            result = await _run_sync(
                get_economy_data,
                indicator_type=arguments["indicator_type"],
                start_date=arguments["start_date"],
                end_date=arguments.get("end_date"),
                limit=arguments.get("limit", 100),
            )
            try:
                layout, theme, width_warning = _get_chart_runtime(arguments)
                chart = render_economy_chart(result, arguments["indicator_type"], layout, theme)
            except Exception as render_err:  # noqa: BLE001 - never fail the tool on a chart-render error
                logger.warning(
                    "  Chart render failed for %s: %s: %s",
                    name,
                    type(render_err).__name__,
                    _truncate_utf8(redact_sensitive_text(render_err), 500),
                )
        elif name == "get_economy_dashboard":
            logger.info("  Executing: get_economy_dashboard")
            result = await _run_sync(get_economy_dashboard, limit=arguments.get("limit", 10))
            try:
                layout, theme, width_warning = _get_chart_runtime(arguments)
                chart = render_economy_dashboard(result, layout, theme)
            except Exception as render_err:  # noqa: BLE001 - never fail the tool on a chart-render error
                logger.warning(
                    "  Chart render failed for %s: %s: %s",
                    name,
                    type(render_err).__name__,
                    _truncate_utf8(redact_sensitive_text(render_err), 500),
                )
        elif name == "get_market_internals":
            logger.info("  Executing: get_market_internals")
            result = await _run_sync(
                get_market_internals,
                start_date=arguments["start_date"],
                end_date=arguments.get("end_date"),
                limit=arguments.get("limit", 100),
            )
        elif name == "scan_ytd_performance":
            logger.info("  Executing: scan_ytd_performance")
            result = await scan_ytd_performance_async(
                start_date=arguments.get("start_date"),
                large_cap_threshold=arguments.get("large_cap_threshold", 100.0),
                top_n=arguments.get("top_n", 10),
                bottom_n=arguments.get("bottom_n", 10),
                index=arguments.get("index", "sp500"),
            )
            if isinstance(result, dict) and result.get("error"):
                raise RuntimeError(str(result["error"]))
            return _success_response(
                name,
                result,
                started,
                warnings=warnings,
                metadata={"source": "database"},
            )
        # Schema discovery tools
        elif name == "describe_database":
            logger.info("  Executing: describe_database")
            result = await _run_sync(describe_database)
        elif name == "describe_table":
            logger.info("  Executing: describe_table")
            result = await _run_sync(describe_table, table_name=arguments["table_name"])
        # Sector tools
        elif name == "list_sectors":
            logger.info("  Executing: list_sectors")
            result = await _run_sync(
                list_sectors,
                taxonomy=arguments.get("taxonomy", "gics"),
                index=arguments.get("index"),
                limit=arguments.get("limit", 100),
            )
        elif name == "get_sector_performance":
            logger.info("  Executing: get_sector_performance")
            result = await _run_sync(
                get_sector_performance,
                taxonomy=arguments.get("taxonomy", "gics"),
                index=arguments.get("index"),
                limit=arguments.get("limit", 50),
            )
        # Market movers tools
        elif name == "get_top_movers":
            logger.info("  Executing: get_top_movers")
            result = await _run_sync(
                get_top_movers,
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
            result = await _run_sync(
                get_volume_leaders,
                metric=arguments.get("metric", "dollar_volume"),
                limit=arguments.get("limit", 20),
                sector=arguments.get("sector"),
                index=arguments.get("index"),
                min_price=arguments.get("min_price"),
            )
        elif name == "get_market_breadth":
            logger.info("  Executing: get_market_breadth")
            result = await _run_sync(
                get_market_breadth,
                date=arguments.get("date"),
                index=arguments.get("index", "all"),
            )
        elif name == "list_technical_indicators":
            logger.info("  Executing: list_technical_indicators")
            result = await _run_sync(
                list_technical_indicators,
                category=arguments.get("category"),
            )
        # Flexible screener
        elif name == "screen_stocks":
            logger.info("  Executing: screen_stocks")
            result = await _run_sync(
                screen_stocks,
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
            result = await _run_sync(
                get_ytd_returns,
                tickers=arguments["tickers"],
                start_date=arguments.get("start_date"),
            )
        # SMA crossover detection
        elif name == "detect_crossovers":
            logger.info("  Executing: detect_crossovers")
            result = await _run_sync(
                detect_crossovers,
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
            result = await _run_sync(
                get_52week_extremes,
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
            result = await _run_sync(
                get_daily_range_leaders,
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
            result = await _run_sync(list_indices)
        elif name == "get_index_constituents":
            logger.info("  Executing: get_index_constituents")
            result = await _run_sync(get_index_constituents, code=arguments["code"])
        elif name == "check_index_membership":
            logger.info("  Executing: check_index_membership")
            result = await _run_sync(check_index_membership, ticker=arguments["ticker"])
        elif name == "get_index_with_prices":
            logger.info("  Executing: get_index_with_prices")
            result = await _run_sync(
                get_index_with_prices,
                code=arguments["code"],
                limit=arguments.get("limit", 50),
            )
        # Corporate actions tools
        elif name == "get_stock_splits":
            logger.info("  Executing: get_stock_splits")
            result = await _run_sync(
                get_stock_splits,
                ticker=arguments.get("ticker"),
                start_date=arguments.get("start_date"),
                end_date=arguments.get("end_date"),
                limit=arguments.get("limit", 100),
            )
        elif name == "get_dividends":
            logger.info("  Executing: get_dividends")
            result = await _run_sync(
                get_dividends,
                ticker=arguments.get("ticker"),
                start_date=arguments.get("start_date"),
                end_date=arguments.get("end_date"),
                upcoming_only=arguments.get("upcoming_only", False),
                limit=arguments.get("limit", 100),
            )
        elif name == "get_ex_dividend_calendar":
            logger.info("  Executing: get_ex_dividend_calendar")
            result = await _run_sync(
                get_ex_dividend_calendar,
                start_date=arguments["start_date"],
                end_date=arguments["end_date"],
                index=arguments.get("index", "all"),
                limit=arguments.get("limit", 200),
            )
        elif name == "get_recent_splits":
            logger.info("  Executing: get_recent_splits")
            result = await _run_sync(
                get_recent_splits,
                days=arguments.get("days", 30),
                index=arguments.get("index", "all"),
            )
        elif name == "get_dividend_yield_leaders":
            logger.info("  Executing: get_dividend_yield_leaders")
            result = await _run_sync(
                get_dividend_yield_leaders,
                index=arguments.get("index", "all"),
                min_yield=arguments.get("min_yield", 2.0),
                limit=arguments.get("limit", 50),
            )
        elif name == "get_earnings_calendar":
            logger.info("  Executing: get_earnings_calendar")
            result = await _run_sync(
                get_earnings_calendar,
                start_date=arguments["start_date"],
                end_date=arguments["end_date"],
                index=arguments.get("index", "all"),
                timing=arguments.get("timing", "all"),
                limit=arguments.get("limit", 200),
            )
        elif name == "get_earnings_history":
            logger.info("  Executing: get_earnings_history")
            result = await _run_sync(
                get_earnings_history,
                ticker=arguments["ticker"],
                limit=arguments.get("limit", 12),
            )
        elif name == "get_data_status":
            logger.info("  Executing: get_data_status")
            result = await _run_sync(get_data_status)
        elif name == "get_recent_news_sentiment":
            logger.info("  Executing: get_recent_news_sentiment")
            result = await _run_sync(
                get_recent_news_sentiment,
                ticker=arguments["ticker"],
                days_back=arguments.get("days_back", 14),
                max_articles=arguments.get("max_articles", 10),
            )
        elif name == "calculate_support_resistance_levels":
            logger.info("  Executing: calculate_support_resistance_levels")
            result = await _run_sync(
                calculate_support_resistance_levels,
                ticker=arguments["ticker"],
                lookback_days=arguments.get("lookback_days", 90),
                max_levels=arguments.get("max_levels", 5),
                method=arguments.get("method", "cluster"),
            )
        elif name == "detect_candlestick_patterns":
            logger.info("  Executing: detect_candlestick_patterns")
            result = await _run_sync(
                detect_candlestick_patterns,
                ticker=arguments["ticker"],
                days=arguments.get("days", 30),
                patterns_to_detect=arguments.get("patterns_to_detect"),
            )
        elif name == "detect_chart_patterns":
            logger.info("  Executing: detect_chart_patterns")
            result = await _run_sync(
                detect_chart_patterns,
                ticker=arguments["ticker"],
                lookback_days=arguments.get("lookback_days", 60),
                min_pattern_days=arguments.get("min_pattern_days", 10),
            )
        elif name == "get_squeeze_indicators":
            logger.info("  Executing: get_squeeze_indicators")
            result = await _run_sync(
                get_squeeze_indicators,
                ticker=arguments["ticker"],
                lookback_days=arguments.get("lookback_days", 60),
            )
        elif name == "get_momentum_indicators":
            logger.info("  Executing: get_momentum_indicators")
            result = await _run_sync(
                get_momentum_indicators,
                ticker=arguments["ticker"],
                lookback_days=arguments.get("lookback_days", 60),
            )
        elif name == "get_volume_profile":
            logger.info("  Executing: get_volume_profile")
            result = await _run_sync(
                get_volume_profile,
                ticker=arguments["ticker"],
                lookback_days=arguments.get("lookback_days", 30),
                price_bins=arguments.get("price_bins", 20),
            )
        elif name == "detect_volume_anomalies":
            logger.info("  Executing: detect_volume_anomalies")
            result = await _run_sync(
                detect_volume_anomalies,
                ticker=arguments["ticker"],
                lookback_days=arguments.get("lookback_days", 90),
                threshold_multiplier=arguments.get("threshold_multiplier", 2.0),
            )
        elif name == "get_advanced_volume_indicators":
            logger.info("  Executing: get_advanced_volume_indicators")
            result = await _run_sync(
                get_advanced_volume_indicators,
                ticker=arguments["ticker"],
                lookback_days=arguments.get("lookback_days", 60),
            )
        elif name == "get_weekly_monthly_candles":
            logger.info("  Executing: get_weekly_monthly_candles")
            result = await _run_sync(
                get_weekly_monthly_candles,
                ticker=arguments["ticker"],
                timeframe=arguments["timeframe"],
                periods=arguments.get("periods"),
            )
        elif name == "get_multi_timeframe_alignment":
            logger.info("  Executing: get_multi_timeframe_alignment")
            result = await _run_sync(
                get_multi_timeframe_alignment,
                ticker=arguments["ticker"],
                indicators=arguments.get("indicators"),
                timeframes=arguments.get("timeframes"),
            )
        elif name == "calculate_relative_strength":
            logger.info("  Executing: calculate_relative_strength")
            result = await _run_sync(
                calculate_relative_strength,
                ticker=arguments["ticker"],
                benchmark=arguments.get("benchmark", "SPY"),
                lookback_days=arguments.get("lookback_days", 90),
            )
        else:
            raise ValueError(f"Unknown tool: {name}")

        if width_warning:
            warnings.append(width_warning)

        # Add market-hours hint for EOD tools
        if name in ("get_latest_price", "get_stock_prices"):
            hint = _market_hours_hint()
            if hint:
                warnings.append(hint)

        return _success_response(name, result, started, chart=chart, warnings=warnings)

    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.error(
            "Error executing tool %s: %s: %s",
            _truncate_utf8(name, 64),
            type(e).__name__,
            _truncate_utf8(redact_sensitive_text(e), 2048),
        )
        raise


def _market_hours_hint() -> str | None:
    """Return a hint if market is open and an EOD tool was used."""
    try:
        from sawa.utils.market_hours import is_market_open

        if is_market_open():
            return (
                "[Note: US market is currently open. "
                "For current-session intraday data, use get_intraday_bars.]"
            )
    except ImportError:
        return None
    return None


def _structured_payload(content: list[TextContent]) -> dict[str, Any]:
    """Recover the compatibility JSON envelope as v2 structured content."""
    if len(content) != 1:
        raise RuntimeError("Tool returned an invalid content envelope")
    payload: object = json.loads(content[0].text)
    if not isinstance(payload, dict):
        raise RuntimeError("Tool returned a non-object content envelope")
    return {str(key): value for key, value in payload.items()}


def _truncate_utf8(text: str, max_bytes: int, *, suffix: str = "…") -> str:
    """Truncate text without splitting a UTF-8 code point."""
    if max_bytes <= 0:
        return ""
    if len(text.encode("utf-8")) <= max_bytes:
        return text
    suffix_bytes = len(suffix.encode("utf-8"))
    if suffix_bytes > max_bytes:
        return ""
    low, high = 0, len(text)
    while low < high:
        middle = (low + high + 1) // 2
        if len(text[:middle].encode("utf-8")) + suffix_bytes <= max_bytes:
            low = middle
        else:
            high = middle - 1
    return text[:low] + suffix


def _bounded_error_payload(
    name: str,
    error: BaseException,
    duration_ms: float,
) -> tuple[dict[str, Any], str]:
    """Build a redacted error envelope within the configured response cap."""
    maximum = max(2, database_runtime.MAX_RESULT_BYTES)
    safe_tool = _truncate_utf8(str(name), 64)
    safe_type = _truncate_utf8(type(error).__name__, 64)
    safe_message = redact_sensitive_text(error)

    def build(message: str, *, include_duration: bool) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "tool": safe_tool,
            "schema_version": _RESPONSE_SCHEMA_VERSION,
        }
        if include_duration:
            metadata["duration_ms"] = round(duration_ms, 2)
        return {
            "error": {"type": safe_type, "message": message},
            "metadata": metadata,
        }

    def render(payload: dict[str, Any]) -> str:
        return json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )

    # Keep the complete diagnostic whenever it fits, dropping optional timing
    # metadata before shortening the useful error message.
    for include_duration in (True, False):
        payload = build(safe_message, include_duration=include_duration)
        rendered = render(payload)
        if len(rendered.encode("utf-8")) <= maximum:
            return payload, rendered

    def build_without_duration(message: str) -> dict[str, Any]:
        return build(message, include_duration=False)

    empty_payload = build_without_duration("")
    if len(render(empty_payload).encode("utf-8")) <= maximum:
        low, high = 0, len(safe_message)
        best_payload = empty_payload
        best_rendered = render(empty_payload)
        while low <= high:
            middle = (low + high) // 2
            candidate_message = safe_message
            if middle < len(safe_message):
                candidate_message = safe_message[:middle] + "…"
            candidate = build_without_duration(candidate_message)
            candidate_rendered = render(candidate)
            if len(candidate_rendered.encode("utf-8")) <= maximum:
                best_payload = candidate
                best_rendered = candidate_rendered
                low = middle + 1
            else:
                high = middle - 1
        return best_payload, best_rendered

    # Extremely small operator-configured caps cannot hold standard metadata.
    # Errors use compatibility TextContent only, so a minimal JSON fallback is
    # valid even though successful structured output has a closed schema.
    fallbacks: list[dict[str, Any]] = [
        {"error": {"type": safe_type, "message": ""}},
        {"error": {"message": ""}},
        {"error": ""},
        {},
    ]
    for fallback_payload in fallbacks:
        rendered = render(fallback_payload)
        if len(rendered.encode("utf-8")) <= maximum:
            return fallback_payload, rendered
    return {}, "{}"


def _record_call_outcome_safely(
    name: str,
    *,
    success: bool,
    duration_ms: float,
    error: BaseException | None = None,
) -> None:
    """Keep best-effort monitoring from changing a protocol result."""
    try:
        bounded_error: BaseException | None = None
        error_type: str | None = None
        if error is not None:
            error_type = type(error).__name__
            bounded_error = RuntimeError(
                _truncate_utf8(redact_sensitive_text(error), 2048)
            )
        record_call_outcome(
            name,
            success=success,
            duration_ms=duration_ms,
            logger=logger,
            # Pass a bounded, redacted exception plus explicit source type so
            # monitoring retains diagnostic fidelity without holding an
            # arbitrarily large or sensitive provider exception.
            error=bounded_error,
            error_type=error_type,
        )
    except Exception as monitoring_error:  # noqa: BLE001 - monitoring is best-effort
        logger.warning(
            "MCP outcome monitoring failed for %s: %s: %s",
            name,
            type(monitoring_error).__name__,
            redact_sensitive_text(monitoring_error),
        )


async def _validate_protocol_arguments(name: str, arguments: dict[str, Any]) -> None:
    """Validate v2 low-level calls against the schema advertised for the tool."""
    tool = next((item for item in await list_tools() if item.name == name), None)
    if tool is None:
        # Tool discovery failures are JSON-RPC invalid-params errors, not
        # execution results. Do not echo or monitor the attacker-controlled name.
        raise MCPError(code=INVALID_PARAMS, message="Unknown tool")
    errors = sorted(
        Draft202012Validator(tool.input_schema).iter_errors(arguments),
        key=lambda error: list(error.absolute_path),
    )
    if errors:
        error = errors[0]
        location = ".".join(str(part) for part in error.absolute_path) or "arguments"
        # Do not echo the rejected value: protocol arguments may contain data
        # that should not be copied into logs or tool-error responses.
        raise ValueError(
            f"Invalid arguments for {name} at {location}: {error.validator} constraint failed"
        )


async def _handle_list_tools(
    _ctx: ServerRequestContext,
    _params: PaginatedRequestParams | None,
) -> ListToolsResult:
    """MCP v2 low-level list handler."""
    return ListToolsResult(tools=await list_tools())


async def _handle_call_tool(
    _ctx: ServerRequestContext,
    params: CallToolRequestParams,
) -> CallToolResult:
    """MCP v2 low-level call handler with explicit success/error results."""
    name = params.name
    arguments = params.arguments or {}
    started = time.monotonic()
    try:
        await _validate_protocol_arguments(name, arguments)
        content = await call_tool(name, arguments)
        protocol_content: list[ContentBlock] = list(content)
        response = CallToolResult(
            content=protocol_content,
            structured_content=_structured_payload(content),
            is_error=False,
        )
    except asyncio.CancelledError:
        raise
    except MCPError:
        raise
    except Exception as error:  # noqa: BLE001 - converted to an MCP tool error
        safe_name = _truncate_utf8(name, 64)
        _payload, rendered = _bounded_error_payload(
            safe_name,
            error,
            (time.monotonic() - started) * 1000,
        )
        response = CallToolResult(
            content=[
                TextContent(
                    type="text",
                    text=rendered,
                )
            ],
            is_error=True,
        )
        await _run_sync(
            _record_call_outcome_safely,
            safe_name,
            success=False,
            duration_ms=(time.monotonic() - started) * 1000,
            error=error,
        )
        return response

    await _run_sync(
        _record_call_outcome_safely,
        name,
        success=True,
        duration_ms=(time.monotonic() - started) * 1000,
    )
    return response


app = Server(
    "stock-data-server",
    version="0.3.0",
    instructions=_SERVER_INSTRUCTIONS,
    on_list_tools=_handle_list_tools,
    on_call_tool=_handle_call_tool,
)


async def main():
    """Main entry point."""
    logger.info("Starting Stock Data MCP Server")
    if _mcp_log_file:
        logger.info("File log: %s", _mcp_log_file)
    try:
        from sawa.mcp_query_insights import load_cached_query_warning

        if warning := load_cached_query_warning():
            logger.warning(warning)
    except Exception as e:
        logger.debug(
            "Could not load cached MCP query insights: %s: %s",
            type(e).__name__,
            _truncate_utf8(redact_sensitive_text(e), 500),
        )
    logger.info("Press Ctrl-C to exit gracefully")

    # Verify database connection
    try:
        from .database import get_database_url

        get_database_url()
        logger.info("Database configuration verified")
    except ValueError as e:
        logger.error(
            "Configuration error: %s: %s",
            type(e).__name__,
            _truncate_utf8(redact_sensitive_text(e), 500),
        )
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
        logger.error(
            "Server error: %s: %s",
            type(e).__name__,
            _truncate_utf8(redact_sensitive_text(e), 500),
        )
        sys.exit(1)


if __name__ == "__main__":
    import asyncio

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Exiting...")
        sys.exit(0)
