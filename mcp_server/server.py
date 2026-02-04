#!/usr/bin/env python3
"""
Stock Data MCP Server

An MCP server providing read-only access to S&P 500 stock data in PostgreSQL.
Includes colorful Unicode charts for data visualization.
"""

import json
import logging
import os
import sys
from typing import Any

from dotenv import load_dotenv
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

# Load environment variables from .env file
load_dotenv()

from .charts.config import ChartDetail, get_chart_config
from .charts.core.layout import get_layout
from .charts.core.modal import check_width_and_warn
from .charts.renderers import (
    render_economy_chart,
    render_economy_dashboard,
    render_fundamentals_chart,
    render_price_chart,
    render_ratios_chart,
)
from .charts.themes import get_theme
from .database import execute_query
from .services import use_service_layer
from .tools.companies import (
    get_company_details,
    get_company_details_async,
    list_companies,
    search_companies,
    search_companies_async,
)
from .tools.economy import get_economy_dashboard, get_economy_data, get_economy_data_async
from .tools.fundamentals import get_fundamentals, get_fundamentals_async
from .tools.market_data import (
    get_financial_ratios,
    get_financial_ratios_async,
    get_live_price,
    get_stock_prices,
    get_stock_prices_async,
)
from .tools.scanner import scan_ytd_performance

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
            description="Get time-series financial ratios (P/E, ROE, debt/equity, etc.) with visual chart",
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
            description="Get latest balance sheet, cash flow, and income statement data with visual charts",
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
            description="Scan market indices (S&P 500, NASDAQ-100, or both) for YTD performance analysis with sector grouping",
            inputSchema={
                "type": "object",
                "properties": {
                    "start_date": {
                        "type": "string",
                        "description": "Start date in YYYY-MM-DD format (default: Jan 1 current year)",
                    },
                    "large_cap_threshold": {
                        "type": "number",
                        "description": "Market cap threshold in billions for Table B (default: 100)",
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
                        "description": "Index to scan: sp500, nasdaq100, or both (default: sp500)",
                        "enum": ["sp500", "nasdaq100", "both"],
                        "default": "sp500",
                    },
                },
            },
        ),
        Tool(
            name="execute_query",
            description="Execute a custom read-only SQL query (SELECT only)",
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
    ]


@app.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    """Handle tool calls."""
    logger.debug(f"Tool called: {name} with arguments: {arguments}")

    try:
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

        chart = None
        result = None

        # Check if we should use the service layer
        use_services = use_service_layer()

        if name == "list_companies":
            # list_companies not available in service layer (no async version)
            result = list_companies(
                limit=arguments.get("limit", 100),
                offset=arguments.get("offset", 0),
                sector=arguments.get("sector"),
            )
        elif name == "get_company_details":
            if use_services:
                result = await get_company_details_async(arguments["ticker"])
            else:
                result = get_company_details(arguments["ticker"])
            if result is None:
                return [TextContent(type="text", text=f"Company {arguments['ticker']} not found")]
        elif name == "search_companies":
            if use_services:
                result = await search_companies_async(
                    query=arguments["query"],
                    limit=arguments.get("limit", 20),
                )
            else:
                result = search_companies(
                    query=arguments["query"],
                    limit=arguments.get("limit", 20),
                )
        elif name == "get_live_price":
            # Live price lookup from Polygon API (always synchronous, no chart)
            result = get_live_price(
                ticker=arguments["ticker"],
                days=arguments.get("days", 7),
            )
        elif name == "get_stock_prices":
            if use_services:
                result = await get_stock_prices_async(
                    ticker=arguments["ticker"],
                    start_date=arguments["start_date"],
                    end_date=arguments.get("end_date"),
                    limit=arguments.get("limit", 252),
                )
            else:
                result = get_stock_prices(
                    ticker=arguments["ticker"],
                    start_date=arguments["start_date"],
                    end_date=arguments.get("end_date"),
                    limit=arguments.get("limit", 252),
                )
            # Render price chart
            chart = render_price_chart(result, arguments["ticker"], layout, theme)
        elif name == "get_financial_ratios":
            if use_services:
                result = await get_financial_ratios_async(
                    ticker=arguments["ticker"],
                    start_date=arguments["start_date"],
                    end_date=arguments.get("end_date"),
                    limit=arguments.get("limit", 100),
                )
            else:
                result = get_financial_ratios(
                    ticker=arguments["ticker"],
                    start_date=arguments["start_date"],
                    end_date=arguments.get("end_date"),
                    limit=arguments.get("limit", 100),
                )
            # Render ratios chart
            chart = render_ratios_chart(result, arguments["ticker"], layout, theme)
        elif name == "get_fundamentals":
            if use_services:
                result = await get_fundamentals_async(
                    ticker=arguments["ticker"],
                    timeframe=arguments.get("timeframe", "quarterly"),
                    limit=arguments.get("limit", 4),
                )
            else:
                result = get_fundamentals(
                    ticker=arguments["ticker"],
                    timeframe=arguments.get("timeframe", "quarterly"),
                    limit=arguments.get("limit", 4),
                )
            # Render fundamentals chart
            chart = render_fundamentals_chart(result, arguments["ticker"], layout, theme)
        elif name == "get_economy_data":
            if use_services:
                result = await get_economy_data_async(
                    indicator_type=arguments["indicator_type"],
                    start_date=arguments["start_date"],
                    end_date=arguments.get("end_date"),
                    limit=arguments.get("limit", 100),
                )
            else:
                result = get_economy_data(
                    indicator_type=arguments["indicator_type"],
                    start_date=arguments["start_date"],
                    end_date=arguments.get("end_date"),
                    limit=arguments.get("limit", 100),
                )
            # Render economy chart
            chart = render_economy_chart(result, arguments["indicator_type"], layout, theme)
        elif name == "get_economy_dashboard":
            # get_economy_dashboard not available in service layer (no async version)
            result = get_economy_dashboard(limit=arguments.get("limit", 10))
            # Render economy dashboard
            chart = render_economy_dashboard(result, layout, theme)
        elif name == "scan_ytd_performance":
            # YTD performance scanner (always synchronous, returns formatted tables)
            result = scan_ytd_performance(
                start_date=arguments.get("start_date"),
                large_cap_threshold=arguments.get("large_cap_threshold", 100.0),
                top_n=arguments.get("top_n", 10),
                index=arguments.get("index", "sp500"),
            )
            # Return formatted output directly (don't convert to JSON)
            return [
                TextContent(
                    type="text",
                    text=f"{result['summary']}\n\n{result['table_a']}\n\n{result['table_b']}",
                )
            ]
        elif name == "execute_query":
            result = execute_query(arguments["sql"])
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
        logger.error(f"Error executing tool {name}: {e}")
        return [TextContent(type="text", text=f"Error: {str(e)}")]


async def main():
    """Main entry point."""
    logger.info("Starting Stock Data MCP Server")

    # Verify database connection
    try:
        from .database import get_database_url

        get_database_url()
        logger.info("Database configuration verified")
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)

    # Run server with stdio transport
    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options(),
        )


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
