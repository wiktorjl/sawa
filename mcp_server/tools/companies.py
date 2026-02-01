"""Company-related MCP tools."""

import logging
from typing import Any

from ..database import execute_query

logger = logging.getLogger(__name__)


def list_companies(
    limit: int = 100,
    offset: int = 0,
    sector: str | None = None,
) -> list[dict[str, Any]]:
    """
    List companies with optional filtering.

    Args:
        limit: Maximum number of results (default: 100, max: 1000)
        offset: Number of results to skip
        sector: Filter by SIC description (partial match)

    Returns:
        List of company records
    """
    limit = min(limit, 1000)

    if sector:
        sql = """
            SELECT 
                ticker,
                name,
                market_cap,
                sic_description as sector,
                primary_exchange as exchange
            FROM companies
            WHERE active = TRUE
                AND sic_description ILIKE %(sector)s
            ORDER BY market_cap DESC NULLS LAST
            LIMIT %(limit)s OFFSET %(offset)s
        """
        params = {
            "sector": f"%{sector}%",
            "limit": limit,
            "offset": offset,
        }
    else:
        sql = """
            SELECT 
                ticker,
                name,
                market_cap,
                sic_description as sector,
                primary_exchange as exchange
            FROM companies
            WHERE active = TRUE
            ORDER BY market_cap DESC NULLS LAST
            LIMIT %(limit)s OFFSET %(offset)s
        """
        params = {"limit": limit, "offset": offset}

    return execute_query(sql, params)


def get_company_details(ticker: str) -> dict[str, Any] | None:
    """
    Get detailed company information including latest metrics.

    Args:
        ticker: Stock ticker symbol (e.g., "AAPL")

    Returns:
        Company details or None if not found
    """
    sql = """
        SELECT 
            c.ticker,
            c.name,
            c.description,
            c.market_cap,
            c.total_employees,
            c.sic_description as sector,
            c.primary_exchange as exchange,
            c.list_date,
            c.homepage_url,
            c.address_city,
            c.address_state,
            sp.close as latest_price,
            sp.date as price_date,
            fr.price_to_earnings as pe_ratio,
            fr.debt_to_equity,
            fr.return_on_equity as roe,
            fr.dividend_yield,
            fr.market_cap as latest_market_cap
        FROM companies c
        LEFT JOIN LATERAL (
            SELECT close, date 
            FROM stock_prices 
            WHERE ticker = c.ticker 
            ORDER BY date DESC 
            LIMIT 1
        ) sp ON true
        LEFT JOIN LATERAL (
            SELECT price_to_earnings, debt_to_equity, return_on_equity, 
                   dividend_yield, market_cap
            FROM financial_ratios
            WHERE ticker = c.ticker
            ORDER BY date DESC
            LIMIT 1
        ) fr ON true
        WHERE c.ticker = %(ticker)s
    """

    results = execute_query(sql, {"ticker": ticker.upper()})
    return results[0] if results else None


def search_companies(
    query: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """
    Search companies by name or ticker.

    Args:
        query: Search term
        limit: Maximum results (default: 20, max: 100)

    Returns:
        List of matching companies
    """
    limit = min(limit, 100)
    search_term = f"%{query}%"

    sql = """
        SELECT 
            ticker,
            name,
            market_cap,
            sic_description as sector
        FROM companies
        WHERE active = TRUE
            AND (
                ticker ILIKE %(query)s
                OR name ILIKE %(query)s
                OR sic_description ILIKE %(query)s
            )
        ORDER BY 
            CASE 
                WHEN ticker ILIKE %(exact)s THEN 1
                WHEN name ILIKE %(exact)s THEN 2
                WHEN ticker ILIKE %(query)s THEN 3
                ELSE 4
            END,
            market_cap DESC NULLS LAST
        LIMIT %(limit)s
    """

    params = {
        "query": search_term,
        "exact": query,
        "limit": limit,
    }

    return execute_query(sql, params)
