"""Market data MCP tools (prices and financial ratios)."""

import logging
from datetime import date
from typing import Any

from ..database import execute_query

logger = logging.getLogger(__name__)


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
    Get the most recent stock price for a ticker.

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
