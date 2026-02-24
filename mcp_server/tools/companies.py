"""Company-related MCP tools."""

import logging
from typing import Any

from psycopg import sql

from ..database import execute_query

logger = logging.getLogger(__name__)


def list_companies(
    limit: int = 100,
    offset: int = 0,
    sector: str | None = None,
    index: str | None = None,
) -> list[dict[str, Any]]:
    """
    List companies with optional filtering.

    Args:
        limit: Maximum number of results (default: 100, max: 1000)
        offset: Number of results to skip
        sector: Filter by SIC description (partial match)
        index: Filter by index membership (sp500, nasdaq5000)

    Returns:
        List of company records with indices array
    """
    limit = min(limit, 1000)

    # Build WHERE clauses
    where_clauses = ["c.active = TRUE"]
    params: dict[str, Any] = {"limit": limit, "offset": offset}

    if sector:
        where_clauses.append("c.sic_description ILIKE %(sector)s")
        params["sector"] = f"%{sector}%"

    if index:
        where_clauses.append("""
            c.ticker IN (
                SELECT ic.ticker FROM index_constituents ic
                JOIN indices i ON ic.index_id = i.id
                WHERE i.code = %(index)s
            )
        """)
        params["index"] = index.lower()

    where_sql = sql.SQL(" AND ").join(sql.SQL(c) for c in where_clauses)

    query = sql.SQL("""
        SELECT
            c.ticker,
            c.name,
            c.market_cap,
            c.sic_description as sector,
            c.primary_exchange as exchange,
            ARRAY(
                SELECT i.code FROM index_constituents ic
                JOIN indices i ON ic.index_id = i.id
                WHERE ic.ticker = c.ticker
                ORDER BY i.code
            ) as indices
        FROM companies c
        WHERE {where_sql}
        ORDER BY c.market_cap DESC NULLS LAST
        LIMIT %(limit)s OFFSET %(offset)s
    """).format(where_sql=where_sql)

    return execute_query(query, params)


def get_company_details(ticker: str) -> dict[str, Any] | None:
    """
    Get detailed company information including latest metrics.

    Args:
        ticker: Stock ticker symbol (e.g., "AAPL")

    Returns:
        Company details with indices array, or None if not found
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
            fr.market_cap as latest_market_cap,
            ARRAY(
                SELECT i.code FROM index_constituents ic
                JOIN indices i ON ic.index_id = i.id
                WHERE ic.ticker = c.ticker
                ORDER BY i.code
            ) as indices
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
    index: str | None = None,
) -> list[dict[str, Any]]:
    """
    Search companies by name or ticker.

    Args:
        query: Search term
        limit: Maximum results (default: 20, max: 100)
        index: Filter by index membership (sp500, nasdaq5000)

    Returns:
        List of matching companies with indices array
    """
    limit = min(limit, 100)
    search_term = f"%{query}%"

    # Build WHERE clauses
    where_clauses = [
        "c.active = TRUE",
        "(c.ticker ILIKE %(query)s OR c.name ILIKE %(query)s OR c.sic_description ILIKE %(query)s)",
    ]
    params: dict[str, Any] = {
        "query": search_term,
        "exact": query,
        "limit": limit,
    }

    if index:
        where_clauses.append("""
            c.ticker IN (
                SELECT ic.ticker FROM index_constituents ic
                JOIN indices i ON ic.index_id = i.id
                WHERE i.code = %(index)s
            )
        """)
        params["index"] = index.lower()

    where_sql = sql.SQL(" AND ").join(sql.SQL(c) for c in where_clauses)

    search_query = sql.SQL("""
        SELECT
            c.ticker,
            c.name,
            c.market_cap,
            c.sic_description as sector,
            ARRAY(
                SELECT i.code FROM index_constituents ic
                JOIN indices i ON ic.index_id = i.id
                WHERE ic.ticker = c.ticker
                ORDER BY i.code
            ) as indices
        FROM companies c
        WHERE {where_sql}
        ORDER BY
            CASE
                WHEN c.ticker ILIKE %(exact)s THEN 1
                WHEN c.name ILIKE %(exact)s THEN 2
                WHEN c.ticker ILIKE %(query)s THEN 3
                ELSE 4
            END,
            c.market_cap DESC NULLS LAST
        LIMIT %(limit)s
    """).format(where_sql=where_sql)

    return execute_query(search_query, params)
