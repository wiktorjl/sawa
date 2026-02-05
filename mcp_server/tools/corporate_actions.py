"""Corporate actions MCP tools (splits, dividends, earnings)."""

import logging
from typing import Any, Literal

from ..database import execute_query

logger = logging.getLogger(__name__)


def get_stock_splits(
    ticker: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """
    Get stock split history.

    Args:
        ticker: Filter by ticker symbol (optional)
        start_date: Start date YYYY-MM-DD (optional)
        end_date: End date YYYY-MM-DD (optional)
        limit: Maximum results (default: 100, max: 500)

    Returns:
        List of splits with ticker, execution_date, split_from, split_to, ratio
    """
    limit = min(limit, 500)

    filters = []
    params: dict[str, Any] = {"limit": limit}

    if ticker:
        filters.append("s.ticker = %(ticker)s")
        params["ticker"] = ticker.upper()
    if start_date:
        filters.append("s.execution_date >= %(start_date)s")
        params["start_date"] = start_date
    if end_date:
        filters.append("s.execution_date <= %(end_date)s")
        params["end_date"] = end_date

    where_clause = " AND ".join(filters) if filters else "1=1"

    sql = f"""
        SELECT
            s.ticker,
            c.name,
            s.execution_date,
            s.split_from,
            s.split_to,
            s.split_to || ':' || s.split_from as ratio
        FROM stock_splits s
        JOIN companies c ON s.ticker = c.ticker
        WHERE {where_clause}
        ORDER BY s.execution_date DESC
        LIMIT %(limit)s
    """

    return execute_query(sql, params)


def get_dividends(
    ticker: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    upcoming_only: bool = False,
    dividend_type: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """
    Get dividend history or upcoming dividends.

    Args:
        ticker: Filter by ticker symbol (optional)
        start_date: Start date YYYY-MM-DD for ex_dividend_date (optional)
        end_date: End date YYYY-MM-DD for ex_dividend_date (optional)
        upcoming_only: If True, only return future dividends
        dividend_type: Filter by type (CD=cash, SC=special, etc.)
        limit: Maximum results (default: 100, max: 500)

    Returns:
        List of dividends with dates, amounts, and frequencies
    """
    limit = min(limit, 500)

    filters = []
    params: dict[str, Any] = {"limit": limit}

    if ticker:
        filters.append("d.ticker = %(ticker)s")
        params["ticker"] = ticker.upper()
    if start_date:
        filters.append("d.ex_dividend_date >= %(start_date)s")
        params["start_date"] = start_date
    if end_date:
        filters.append("d.ex_dividend_date <= %(end_date)s")
        params["end_date"] = end_date
    if upcoming_only:
        filters.append("d.ex_dividend_date >= CURRENT_DATE")
    if dividend_type:
        filters.append("d.dividend_type = %(dividend_type)s")
        params["dividend_type"] = dividend_type

    where_clause = " AND ".join(filters) if filters else "1=1"

    sql = f"""
        SELECT
            d.ticker,
            c.name,
            d.ex_dividend_date,
            d.record_date,
            d.pay_date,
            d.cash_amount,
            d.dividend_type,
            d.frequency,
            CASE d.frequency
                WHEN 1 THEN 'Annual'
                WHEN 2 THEN 'Semi-Annual'
                WHEN 4 THEN 'Quarterly'
                WHEN 12 THEN 'Monthly'
                WHEN 0 THEN 'One-Time'
                ELSE 'Unknown'
            END as frequency_label
        FROM dividends d
        JOIN companies c ON d.ticker = c.ticker
        WHERE {where_clause}
        ORDER BY d.ex_dividend_date DESC
        LIMIT %(limit)s
    """

    return execute_query(sql, params)


def get_ex_dividend_calendar(
    start_date: str,
    end_date: str,
    index: Literal["sp500", "nasdaq100", "all"] = "all",
    limit: int = 200,
) -> list[dict[str, Any]]:
    """
    Get ex-dividend calendar for a date range.

    Args:
        start_date: Start date YYYY-MM-DD
        end_date: End date YYYY-MM-DD
        index: Filter by index membership (default: all)
        limit: Maximum results (default: 200, max: 500)

    Returns:
        List of dividends grouped by ex_dividend_date
    """
    limit = min(limit, 500)

    # Index filter
    index_filter = ""
    if index == "sp500":
        index_filter = """AND d.ticker IN (
            SELECT ic.ticker FROM index_constituents ic
            JOIN indices i ON ic.index_id = i.id
            WHERE i.code = 'sp500'
        )"""
    elif index == "nasdaq100":
        index_filter = """AND d.ticker IN (
            SELECT ic.ticker FROM index_constituents ic
            JOIN indices i ON ic.index_id = i.id
            WHERE i.code = 'nasdaq100'
        )"""

    sql = f"""
        SELECT
            d.ex_dividend_date,
            d.ticker,
            c.name,
            d.cash_amount,
            d.pay_date,
            d.dividend_type
        FROM dividends d
        JOIN companies c ON d.ticker = c.ticker
        WHERE d.ex_dividend_date >= %(start_date)s
          AND d.ex_dividend_date <= %(end_date)s
          {index_filter}
        ORDER BY d.ex_dividend_date, d.ticker
        LIMIT %(limit)s
    """

    return execute_query(sql, {"start_date": start_date, "end_date": end_date, "limit": limit})


def get_recent_splits(
    days: int = 30,
    index: Literal["sp500", "nasdaq100", "all"] = "all",
) -> list[dict[str, Any]]:
    """
    Get recent stock splits.

    Args:
        days: Number of days to look back (default: 30)
        index: Filter by index membership (default: all)

    Returns:
        List of recent splits with company info
    """
    # Index filter
    index_filter = ""
    if index == "sp500":
        index_filter = """AND s.ticker IN (
            SELECT ic.ticker FROM index_constituents ic
            JOIN indices i ON ic.index_id = i.id
            WHERE i.code = 'sp500'
        )"""
    elif index == "nasdaq100":
        index_filter = """AND s.ticker IN (
            SELECT ic.ticker FROM index_constituents ic
            JOIN indices i ON ic.index_id = i.id
            WHERE i.code = 'nasdaq100'
        )"""

    sql = f"""
        SELECT
            s.ticker,
            c.name,
            s.execution_date,
            s.split_from,
            s.split_to,
            s.split_to || ':' || s.split_from as ratio
        FROM stock_splits s
        JOIN companies c ON s.ticker = c.ticker
        WHERE s.execution_date >= CURRENT_DATE - %(days)s
          {index_filter}
        ORDER BY s.execution_date DESC
    """

    return execute_query(sql, {"days": days})


def get_dividend_yield_leaders(
    index: Literal["sp500", "nasdaq100", "all"] = "all",
    min_yield: float = 2.0,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """
    Get stocks with highest dividend yields.

    Args:
        index: Filter by index membership (default: all)
        min_yield: Minimum dividend yield % (default: 2.0)
        limit: Maximum results (default: 50, max: 200)

    Returns:
        List of stocks with dividend_yield and recent dividend info
    """
    limit = min(limit, 200)

    # Index filter
    index_filter = ""
    if index == "sp500":
        index_filter = """AND c.ticker IN (
            SELECT ic.ticker FROM index_constituents ic
            JOIN indices i ON ic.index_id = i.id
            WHERE i.code = 'sp500'
        )"""
    elif index == "nasdaq100":
        index_filter = """AND c.ticker IN (
            SELECT ic.ticker FROM index_constituents ic
            JOIN indices i ON ic.index_id = i.id
            WHERE i.code = 'nasdaq100'
        )"""

    sql = f"""
        WITH latest_prices AS (
            SELECT DISTINCT ON (ticker)
                ticker,
                close as price
            FROM stock_prices
            ORDER BY ticker, date DESC
        ),
        latest_divs AS (
            SELECT DISTINCT ON (ticker)
                ticker,
                cash_amount,
                ex_dividend_date,
                frequency
            FROM dividends
            WHERE dividend_type = 'CD'
            ORDER BY ticker, ex_dividend_date DESC
        )
        SELECT
            c.ticker,
            c.name,
            c.sic_description as sector,
            lp.price,
            fr.dividend_yield,
            ld.cash_amount as last_dividend,
            ld.ex_dividend_date as last_ex_date,
            ld.frequency
        FROM companies c
        JOIN latest_prices lp ON c.ticker = lp.ticker
        LEFT JOIN financial_ratios fr ON c.ticker = fr.ticker
        LEFT JOIN latest_divs ld ON c.ticker = ld.ticker
        WHERE c.active = true
          AND fr.dividend_yield >= %(min_yield)s
          {index_filter}
        ORDER BY fr.dividend_yield DESC NULLS LAST
        LIMIT %(limit)s
    """

    return execute_query(sql, {"min_yield": min_yield, "limit": limit})


def get_earnings_calendar(
    start_date: str,
    end_date: str,
    index: Literal["sp500", "nasdaq100", "all"] = "all",
    timing: Literal["BMO", "AMC", "all"] = "all",
    limit: int = 200,
) -> list[dict[str, Any]]:
    """
    Get earnings calendar for a date range.

    Args:
        start_date: Start date YYYY-MM-DD
        end_date: End date YYYY-MM-DD
        index: Filter by index membership (default: all)
        timing: Filter by timing (BMO=before market, AMC=after close, all)
        limit: Maximum results (default: 200, max: 500)

    Returns:
        List of earnings reports with dates and estimates
    """
    limit = min(limit, 500)

    filters = [
        "e.report_date >= %(start_date)s",
        "e.report_date <= %(end_date)s",
    ]
    params: dict[str, Any] = {"start_date": start_date, "end_date": end_date, "limit": limit}

    if timing != "all":
        filters.append("e.timing = %(timing)s")
        params["timing"] = timing

    # Index filter
    index_filter = ""
    if index == "sp500":
        index_filter = """AND e.ticker IN (
            SELECT ic.ticker FROM index_constituents ic
            JOIN indices i ON ic.index_id = i.id
            WHERE i.code = 'sp500'
        )"""
    elif index == "nasdaq100":
        index_filter = """AND e.ticker IN (
            SELECT ic.ticker FROM index_constituents ic
            JOIN indices i ON ic.index_id = i.id
            WHERE i.code = 'nasdaq100'
        )"""

    where_clause = " AND ".join(filters)

    sql = f"""
        SELECT
            e.report_date,
            e.ticker,
            c.name,
            e.timing,
            e.fiscal_quarter,
            e.fiscal_year,
            e.eps_estimate,
            e.eps_actual,
            e.revenue_estimate,
            e.revenue_actual
        FROM earnings e
        JOIN companies c ON e.ticker = c.ticker
        WHERE {where_clause}
          {index_filter}
        ORDER BY e.report_date, e.timing, e.ticker
        LIMIT %(limit)s
    """

    return execute_query(sql, params)


def get_earnings_history(
    ticker: str,
    limit: int = 12,
) -> list[dict[str, Any]]:
    """
    Get historical earnings for a ticker.

    Args:
        ticker: Stock ticker symbol
        limit: Number of quarters to return (default: 12 = 3 years)

    Returns:
        List of earnings reports with actuals and surprises
    """
    limit = min(limit, 40)

    sql = """
        SELECT
            e.ticker,
            e.report_date,
            e.fiscal_quarter,
            e.fiscal_year,
            e.timing,
            e.eps_estimate,
            e.eps_actual,
            CASE WHEN e.eps_estimate IS NOT NULL AND e.eps_actual IS NOT NULL
                 THEN e.eps_actual - e.eps_estimate
                 ELSE NULL END as eps_surprise,
            CASE WHEN e.eps_estimate IS NOT NULL AND e.eps_estimate != 0
                      AND e.eps_actual IS NOT NULL
                 THEN ROUND(
                     ((e.eps_actual - e.eps_estimate) / ABS(e.eps_estimate) * 100)::numeric, 2)
                 ELSE NULL END as eps_surprise_pct,
            e.revenue_estimate,
            e.revenue_actual
        FROM earnings e
        WHERE e.ticker = %(ticker)s
        ORDER BY e.fiscal_year DESC, e.fiscal_quarter DESC
        LIMIT %(limit)s
    """

    return execute_query(sql, {"ticker": ticker.upper(), "limit": limit})
