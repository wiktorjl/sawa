"""Market index-related MCP tools."""

import logging
from typing import Any

from ..database import execute_query

logger = logging.getLogger(__name__)


def list_indices() -> list[dict[str, Any]]:
    """
    List all available market indices with constituent counts.

    Returns:
        List of index records with:
        - code: Index code (e.g., 'sp500', 'nasdaq5000')
        - name: Full name (e.g., 'S&P 500', 'NASDAQ-100')
        - description: Index description
        - constituent_count: Number of stocks in the index
        - last_updated: When constituents were last refreshed
    """
    sql = """
        SELECT
            i.code,
            i.name,
            i.description,
            i.source_url,
            i.last_updated,
            COUNT(ic.ticker) as constituent_count
        FROM indices i
        LEFT JOIN index_constituents ic ON i.id = ic.index_id
        GROUP BY i.id, i.code, i.name, i.description, i.source_url, i.last_updated
        ORDER BY i.name
    """
    return execute_query(sql, {})


def get_index_constituents(code: str) -> dict[str, Any]:
    """
    Get all constituents of a market index.

    Args:
        code: Index code (e.g., 'sp500', 'nasdaq5000')

    Returns:
        Dict with:
        - code: Index code
        - name: Index name
        - description: Index description
        - constituent_count: Number of stocks
        - constituents: List of ticker symbols
    """
    # Get index info
    index_sql = """
        SELECT
            i.code,
            i.name,
            i.description,
            i.last_updated,
            COUNT(ic.ticker) as constituent_count
        FROM indices i
        LEFT JOIN index_constituents ic ON i.id = ic.index_id
        WHERE i.code = %(code)s
        GROUP BY i.id, i.code, i.name, i.description, i.last_updated
    """
    index_result = execute_query(index_sql, {"code": code.lower()})

    if not index_result:
        return {"error": f"Index not found: {code}"}

    index_info = index_result[0]

    # Get constituents
    constituents_sql = """
        SELECT ic.ticker
        FROM index_constituents ic
        JOIN indices i ON ic.index_id = i.id
        WHERE i.code = %(code)s
        ORDER BY ic.ticker
    """
    constituents = execute_query(constituents_sql, {"code": code.lower()})

    return {
        "code": index_info["code"],
        "name": index_info["name"],
        "description": index_info.get("description"),
        "last_updated": (
            index_info["last_updated"].isoformat() if index_info.get("last_updated") else None
        ),
        "constituent_count": index_info["constituent_count"],
        "constituents": [c["ticker"] for c in constituents],
    }


def check_index_membership(ticker: str) -> dict[str, Any]:
    """
    Check which market indices a stock belongs to.

    Args:
        ticker: Stock ticker symbol (e.g., 'AAPL')

    Returns:
        Dict with:
        - ticker: The queried ticker
        - indices: List of index codes the stock belongs to
        - index_details: List of dicts with index code and name
    """
    sql = """
        SELECT
            i.code,
            i.name
        FROM indices i
        JOIN index_constituents ic ON i.id = ic.index_id
        WHERE ic.ticker = %(ticker)s
        ORDER BY i.name
    """
    results = execute_query(sql, {"ticker": ticker.upper()})

    return {
        "ticker": ticker.upper(),
        "indices": [r["code"] for r in results],
        "index_details": results,
    }


def get_index_with_prices(
    code: str,
    limit: int = 50,
) -> dict[str, Any]:
    """
    Get index constituents with latest price data.

    Args:
        code: Index code (e.g., 'sp500', 'nasdaq5000')
        limit: Maximum number of constituents to return (default: 50)

    Returns:
        Dict with index info and constituents with price data
    """
    limit = min(limit, 500)

    sql = """
        WITH latest_prices AS (
            SELECT DISTINCT ON (ticker)
                ticker,
                date,
                close as price,
                volume
            FROM stock_prices
            ORDER BY ticker, date DESC
        )
        SELECT
            c.ticker,
            c.name,
            c.market_cap,
            c.sic_description as sector,
            lp.price,
            lp.volume,
            lp.date as price_date
        FROM index_constituents ic
        JOIN indices i ON ic.index_id = i.id
        JOIN companies c ON ic.ticker = c.ticker
        LEFT JOIN latest_prices lp ON c.ticker = lp.ticker
        WHERE i.code = %(code)s
        ORDER BY c.market_cap DESC NULLS LAST
        LIMIT %(limit)s
    """
    constituents = execute_query(sql, {"code": code.lower(), "limit": limit})

    # Get index info
    index_sql = """
        SELECT code, name, description, last_updated
        FROM indices
        WHERE code = %(code)s
    """
    index_result = execute_query(index_sql, {"code": code.lower()})

    if not index_result:
        return {"error": f"Index not found: {code}"}

    index_info = index_result[0]

    # Format dates
    for c in constituents:
        if c.get("price_date"):
            c["price_date"] = c["price_date"].isoformat()

    return {
        "index": {
            "code": index_info["code"],
            "name": index_info["name"],
            "description": index_info.get("description"),
            "last_updated": (
                index_info["last_updated"].isoformat() if index_info.get("last_updated") else None
            ),
        },
        "constituent_count": len(constituents),
        "constituents": constituents,
    }
