#!/usr/bin/env python3
"""
Stock Data MCP Server

An MCP server providing read-only access to S&P 500 stock data in PostgreSQL.
"""

import logging
import os
import sys
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from database import execute_query
from tools.companies import get_company_details, list_companies, search_companies
from tools.economy import get_economy_dashboard, get_economy_data
from tools.fundamentals import get_fundamentals
from tools.market_data import get_financial_ratios, get_stock_prices

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
            name="get_stock_prices",
            description="Get daily OHLCV prices for a ticker",
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
            name="get_financial_ratios",
            description="Get time-series financial ratios (P/E, ROE, debt/equity, etc.)",
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
                },
                "required": ["ticker", "start_date"],
            },
        ),
        Tool(
            name="get_fundamentals",
            description="Get latest balance sheet, cash flow, and income statement data",
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
                },
                "required": ["ticker"],
            },
        ),
        Tool(
            name="get_economy_data",
            description="Get economic indicators for a date range",
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
                },
                "required": ["indicator_type", "start_date"],
            },
        ),
        Tool(
            name="get_economy_dashboard",
            description="Get a summary of recent economic indicators",
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
        if name == "list_companies":
            result = list_companies(
                limit=arguments.get("limit", 100),
                offset=arguments.get("offset", 0),
                sector=arguments.get("sector"),
            )
        elif name == "get_company_details":
            result = get_company_details(arguments["ticker"])
            if result is None:
                return [TextContent(type="text", text=f"Company {arguments['ticker']} not found")]
        elif name == "search_companies":
            result = search_companies(
                query=arguments["query"],
                limit=arguments.get("limit", 20),
            )
        elif name == "get_stock_prices":
            result = get_stock_prices(
                ticker=arguments["ticker"],
                start_date=arguments["start_date"],
                end_date=arguments.get("end_date"),
                limit=arguments.get("limit", 252),
            )
        elif name == "get_financial_ratios":
            result = get_financial_ratios(
                ticker=arguments["ticker"],
                start_date=arguments["start_date"],
                end_date=arguments.get("end_date"),
                limit=arguments.get("limit", 100),
            )
        elif name == "get_fundamentals":
            result = get_fundamentals(
                ticker=arguments["ticker"],
                timeframe=arguments.get("timeframe", "quarterly"),
                limit=arguments.get("limit", 4),
            )
        elif name == "get_economy_data":
            result = get_economy_data(
                indicator_type=arguments["indicator_type"],
                start_date=arguments["start_date"],
                end_date=arguments.get("end_date"),
                limit=arguments.get("limit", 100),
            )
        elif name == "get_economy_dashboard":
            result = get_economy_dashboard(limit=arguments.get("limit", 10))
        elif name == "execute_query":
            result = execute_query(arguments["sql"])
        else:
            raise ValueError(f"Unknown tool: {name}")

        # Convert result to JSON string
        import json

        return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

    except Exception as e:
        logger.error(f"Error executing tool {name}: {e}")
        return [TextContent(type="text", text=f"Error: {str(e)}")]


async def main():
    """Main entry point."""
    logger.info("Starting Stock Data MCP Server")

    # Verify database connection
    try:
        from database import get_database_url

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
