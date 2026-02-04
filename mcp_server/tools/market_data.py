"""Market data MCP tools (prices and financial ratios)."""

import logging
import os
from datetime import date, datetime, timedelta
from typing import Any

from ..database import execute_query

logger = logging.getLogger(__name__)


# --- Async service-based implementations ---


async def get_stock_prices_async(
    ticker: str,
    start_date: str,
    end_date: str | None = None,
    limit: int = 252,
) -> list[dict[str, Any]]:
    """Get stock prices via service layer (async)."""
    from ..services import get_stock_service

    service = get_stock_service()
    return await service.get_prices(ticker, start_date, end_date, limit)


async def get_financial_ratios_async(
    ticker: str,
    start_date: str,
    end_date: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Get financial ratios via service layer (async)."""
    from ..services import get_stock_service

    service = get_stock_service()
    return await service.get_financial_ratios(ticker, start_date, end_date, limit)


async def get_latest_price_async(ticker: str) -> dict[str, Any] | None:
    """Get latest price via service layer (async)."""
    from ..services import get_stock_service

    service = get_stock_service()
    return await service.get_latest_price(ticker)


# --- Sync SQL-based implementations (original) ---


def get_stock_prices(
    ticker: str,
    start_date: str,
    end_date: str | None = None,
    limit: int = 252,
) -> list[dict[str, Any]]:
    """
    Get daily OHLCV prices for a ticker.

    Args:
        ticker: Stock ticker symbol (e.g., "AAPL")
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format (defaults to today)
        limit: Maximum rows to return (default: 252, max: 1000)

    Returns:
        List of price records with date, open, high, low, close, volume
    """
    limit = min(limit, 1000)

    if end_date is None:
        end_date = date.today().isoformat()

    sql = """
        SELECT 
            date,
            open,
            high,
            low,
            close,
            volume
        FROM stock_prices
        WHERE ticker = %(ticker)s
            AND date >= %(start_date)s
            AND date <= %(end_date)s
        ORDER BY date ASC
        LIMIT %(limit)s
    """

    params = {
        "ticker": ticker.upper(),
        "start_date": start_date,
        "end_date": end_date,
        "limit": limit,
    }

    return execute_query(sql, params)


def get_financial_ratios(
    ticker: str,
    start_date: str,
    end_date: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """
    Get time-series financial ratios.

    Args:
        ticker: Stock ticker symbol
        start_date: Start date in YYYY-MM-DD format
        end_date: End date (defaults to today)
        limit: Maximum rows (default: 100, max: 1000)

    Returns:
        List of ratio records including P/E, ROE, debt/equity, etc.
    """
    limit = min(limit, 1000)

    if end_date is None:
        end_date = date.today().isoformat()

    sql = """
        SELECT 
            date,
            price,
            price_to_earnings as pe_ratio,
            price_to_book as pb_ratio,
            price_to_sales as ps_ratio,
            price_to_cash_flow as pcf_ratio,
            price_to_free_cash_flow as pfcf_ratio,
            debt_to_equity,
            return_on_equity as roe,
            return_on_assets as roa,
            dividend_yield,
            earnings_per_share as eps,
            market_cap,
            enterprise_value as ev,
            ev_to_ebitda,
            ev_to_sales,
            free_cash_flow as fcf,
            average_volume
        FROM financial_ratios
        WHERE ticker = %(ticker)s
            AND date >= %(start_date)s
            AND date <= %(end_date)s
        ORDER BY date ASC
        LIMIT %(limit)s
    """

    params = {
        "ticker": ticker.upper(),
        "start_date": start_date,
        "end_date": end_date,
        "limit": limit,
    }

    return execute_query(sql, params)


def get_latest_price(ticker: str) -> dict[str, Any] | None:
    """
    Get the most recent stock price for a ticker from database.

    Args:
        ticker: Stock ticker symbol

    Returns:
        Latest price record or None
    """
    sql = """
        SELECT 
            date,
            open,
            high,
            low,
            close,
            volume
        FROM stock_prices
        WHERE ticker = %(ticker)s
        ORDER BY date DESC
        LIMIT 1
    """

    results = execute_query(sql, {"ticker": ticker.upper()})
    return results[0] if results else None


def get_live_price(ticker: str, days: int = 7) -> dict[str, Any]:
    """
    Get live stock price from Polygon API.

    Fetches the most recent price data directly from Polygon API,
    not from the database. Returns recent price history.

    Args:
        ticker: Stock ticker symbol
        days: Number of days of history to fetch (default: 7)

    Returns:
        Dictionary with latest price info and recent history
    """
    from sawa.api.client import PolygonClient
    from sawa.utils.config import get_env

    api_key = get_env("POLYGON_API_KEY")
    if not api_key:
        raise ValueError("POLYGON_API_KEY not configured")

    client = PolygonClient(api_key, logger)

    # Calculate date range
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    try:
        # Fetch recent prices
        data = client.get(
            "aggregates",
            path_params={"ticker": ticker.upper(), "start": start_date, "end": end_date},
            params={"adjusted": "true", "sort": "desc", "limit": days},
        )

        results = data.get("results", [])

        if not results:
            raise ValueError(f"No price data found for {ticker}")

        # Format response
        latest = results[0]
        latest_date = datetime.fromtimestamp(latest["t"] / 1000)

        # Calculate price change
        prev_close = results[1]["c"] if len(results) > 1 else latest["c"]
        change = latest["c"] - prev_close
        change_pct = (change / prev_close * 100) if prev_close else 0

        return {
            "ticker": ticker.upper(),
            "latest_price": latest["c"],
            "latest_date": latest_date.strftime("%Y-%m-%d %H:%M:%S"),
            "open": latest["o"],
            "high": latest["h"],
            "low": latest["l"],
            "close": latest["c"],
            "volume": latest["v"],
            "change": round(change, 2),
            "change_percent": round(change_pct, 2),
            "previous_close": prev_close,
            "history": [
                {
                    "date": datetime.fromtimestamp(bar["t"] / 1000).strftime("%Y-%m-%d"),
                    "open": bar["o"],
                    "high": bar["h"],
                    "low": bar["l"],
                    "close": bar["c"],
                    "volume": bar["v"],
                }
                for bar in results
            ],
            "source": "polygon_api",
            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    except Exception as e:
        logger.error(f"Error fetching live price for {ticker}: {e}")
        raise
