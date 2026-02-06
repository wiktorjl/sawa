"""
Live data functions for real-time market data.

This module provides high-level async functions for fetching live price data
from Polygon.io REST API. Data is fetched directly from the API and not from
the local database, ensuring real-time accuracy.

Usage:
    from sawa.live import get_live_price, get_live_prices_batch

    # Single ticker
    data = await get_live_price("AAPL", days=7)

    # Multiple tickers
    data = await get_live_prices_batch(["AAPL", "MSFT", "GOOGL"], days=7)
"""

import logging
from datetime import date, datetime, timedelta
from typing import Any

from sawa.api.async_client import AsyncPolygonClient
from sawa.utils.config import get_env


async def get_live_price(
    ticker: str,
    days: int = 7,
    api_key: str | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """
    Get live price with recent history from Polygon API.

    Args:
        ticker: Stock ticker symbol (e.g., "AAPL")
        days: Number of days of history to include (1-30)
        api_key: Polygon API key (defaults to POLYGON_API_KEY env var)
        logger: Logger instance (creates default if None)

    Returns:
        Dict with:
            - ticker: str
            - current_price: float (most recent close)
            - current_date: str (ISO format)
            - history: list of OHLCV dicts
            - change_percent: float (change over period)
            - error: str (if any error occurred)

    Raises:
        ValueError: If days out of range or ticker invalid
    """
    if not (1 <= days <= 30):
        raise ValueError(f"days must be between 1 and 30, got {days}")

    if not ticker or not ticker.strip():
        raise ValueError("ticker cannot be empty")

    ticker = ticker.upper().strip()
    api_key = api_key or get_env("POLYGON_API_KEY")
    if not api_key:
        raise ValueError(
            "POLYGON_API_KEY environment variable is not set. "
            "Set it before running: export POLYGON_API_KEY=your_api_key"
        )
    logger = logger or logging.getLogger(__name__)

    # Calculate date range
    end_date = date.today()
    start_date = end_date - timedelta(days=days * 2)  # Extra buffer for weekends

    # Fetch data
    client = AsyncPolygonClient(api_key, logger)
    results = await client.get_aggregates(
        ticker=ticker,
        start_date=start_date,
        end_date=end_date,
        sort="desc",
        limit=days,
    )

    if not results:
        return {
            "ticker": ticker,
            "error": f"No data found for {ticker}",
            "current_price": None,
            "current_date": None,
            "history": [],
            "change_percent": None,
        }

    # Sort ascending for history
    results.sort(key=lambda x: x["t"])

    # Calculate change
    first_close = results[0]["c"]
    last_close = results[-1]["c"]
    change_percent = ((last_close - first_close) / first_close) * 100

    return {
        "ticker": ticker,
        "current_price": last_close,
        "current_date": datetime.fromtimestamp(results[-1]["t"] / 1000).date().isoformat(),
        "history": results,
        "change_percent": round(change_percent, 2),
        "error": None,
    }


async def get_live_prices_batch(
    tickers: list[str],
    days: int = 7,
    concurrency: int = 10,
    api_key: str | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, dict[str, Any]]:
    """
    Get live prices for multiple tickers concurrently.

    Args:
        tickers: List of ticker symbols
        days: Number of days of history per ticker
        concurrency: Max concurrent API requests
        api_key: Polygon API key (defaults to env var)
        logger: Logger instance

    Returns:
        Dict mapping ticker -> result dict (same format as get_live_price)
    """
    resolved_api_key = api_key or get_env("POLYGON_API_KEY")
    if not resolved_api_key:
        raise ValueError("POLYGON_API_KEY not set")
    logger = logger or logging.getLogger(__name__)

    # Calculate date range
    end_date = date.today()
    start_date = end_date - timedelta(days=days * 2)

    # Fetch batch
    client = AsyncPolygonClient(resolved_api_key, logger)
    batch_results = await client.get_aggregates_batch(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
        sort="desc",
        limit=days,
        concurrency=concurrency,
    )

    # Process results
    output: dict[str, dict[str, Any]] = {}
    for ticker, results in batch_results.items():
        if not results:
            output[ticker] = {
                "ticker": ticker,
                "error": f"No data found for {ticker}",
                "current_price": None,
                "current_date": None,
                "history": [],
                "change_percent": None,
            }
            continue

        # Sort ascending
        results.sort(key=lambda x: x["t"])

        # Calculate change
        first_close = results[0]["c"]
        last_close = results[-1]["c"]
        change_percent = ((last_close - first_close) / first_close) * 100

        output[ticker] = {
            "ticker": ticker,
            "current_price": last_close,
            "current_date": datetime.fromtimestamp(results[-1]["t"] / 1000).date().isoformat(),
            "history": results,
            "change_percent": round(change_percent, 2),
            "error": None,
        }

    return output
