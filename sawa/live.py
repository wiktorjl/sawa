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
from datetime import date, timedelta
from typing import Any

from sawa.api.async_client import AsyncPolygonClient
from sawa.utils.config import get_env
from sawa.utils.dates import timestamp_to_date
from sawa.utils.security import redact_sensitive_text


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
            "error_type": "no_data",
            "current_price": None,
            "current_date": None,
            "history": [],
            "change_percent": None,
        }

    # Sort ascending for history
    results.sort(key=lambda x: x["t"])

    # Calculate daily change (from previous close to current). Guard against a
    # missing/zero previous close so we never divide by zero.
    last_close = results[-1]["c"]
    prev_close = results[-2]["c"] if len(results) >= 2 else last_close
    if not prev_close:
        change_percent = None
    else:
        change_percent = round(((last_close - prev_close) / prev_close) * 100, 2)

    return {
        "ticker": ticker,
        "current_price": last_close,
        "current_date": timestamp_to_date(results[-1]["t"]).isoformat(),
        "history": results,
        "change_percent": change_percent,
        "error": None,
        "error_type": None,
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
    if not (1 <= days <= 30):
        raise ValueError(f"days must be between 1 and 30, got {days}")
    if concurrency < 1:
        raise ValueError("concurrency must be at least 1")
    if not tickers:
        raise ValueError("tickers cannot be empty")

    normalized_tickers: list[str] = []
    seen: set[str] = set()
    for ticker in tickers:
        normalized = ticker.upper().strip()
        if not normalized:
            raise ValueError("ticker cannot be empty")
        if normalized not in seen:
            normalized_tickers.append(normalized)
            seen.add(normalized)

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
        tickers=normalized_tickers,
        start_date=start_date,
        end_date=end_date,
        sort="desc",
        limit=days,
        concurrency=concurrency,
    )

    # Process results
    output: dict[str, dict[str, Any]] = {}
    batch_failures: dict[str, str] = getattr(batch_results, "failures", {})
    for ticker in normalized_tickers:
        if ticker in batch_results:
            continue
        failure = batch_failures.get(ticker)
        if failure:
            message = f"Provider request failed for {ticker}: {failure}"
        else:
            message = f"No batch result returned for {ticker}"
        output[ticker] = {
            "ticker": ticker,
            "error": redact_sensitive_text(message),
            "error_type": "provider_error",
            "current_price": None,
            "current_date": None,
            "history": [],
            "change_percent": None,
        }

    for ticker, results in batch_results.items():
        if not results:
            output[ticker] = {
                "ticker": ticker,
                "error": f"No data found for {ticker}",
                "error_type": "no_data",
                "current_price": None,
                "current_date": None,
                "history": [],
                "change_percent": None,
            }
            continue

        # Sort ascending
        results.sort(key=lambda x: x["t"])

        # Calculate daily change (from previous close to current). Guard against
        # a missing/zero previous close so we never divide by zero.
        last_close = results[-1]["c"]
        prev_close = results[-2]["c"] if len(results) >= 2 else last_close
        if not prev_close:
            change_percent = None
        else:
            change_percent = round(((last_close - prev_close) / prev_close) * 100, 2)

        output[ticker] = {
            "ticker": ticker,
            "current_price": last_close,
            "current_date": timestamp_to_date(results[-1]["t"]).isoformat(),
            "history": results,
            "change_percent": change_percent,
            "error": None,
            "error_type": None,
        }

    return output
