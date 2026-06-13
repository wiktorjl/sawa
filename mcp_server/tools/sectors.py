"""Sector and industry MCP tools.

Provides tools for sector/industry classification and performance analysis.
Supports both SIC (SEC) and GICS (S&P) taxonomies.
"""

import logging
from typing import Any, Literal

from psycopg import sql

from ..database import execute_query
from ._dates import get_price_date_refs
from ._index_filter import build_index_filter

logger = logging.getLogger(__name__)


def list_sectors(
    taxonomy: Literal["sic", "gics"] = "gics",
    index: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """
    List all sectors with stock counts.

    Args:
        taxonomy: Classification system - "gics" (S&P Global) or "sic" (SEC)
        index: Filter by index membership (sp500, nasdaq_listed)
        limit: Maximum results (default: 100)

    Returns:
        List of sector dicts with:
        - sector: Sector name
        - stock_count: Number of active stocks in sector
        - sample_tickers: Comma-separated list of example tickers
        For SIC: also includes sic_code
        For GICS: also includes industry breakdown
    """
    limit = min(limit, 500)

    # Build index filter (shared helper; lower-cased to match stored codes)
    params: dict[str, Any] = {"limit": limit}
    index_filter = build_index_filter(
        index.lower() if index else index, "c", params, param_name="index"
    )

    if taxonomy == "gics":
        query = sql.SQL("""
            SELECT
                get_gics_sector(c.ticker, c.sic_code, c.sic_description) as sector,
                COUNT(DISTINCT c.ticker) as stock_count,
                STRING_AGG(DISTINCT c.ticker, ', ' ORDER BY c.ticker)
                    FILTER (WHERE c.ticker IS NOT NULL) as sample_tickers
            FROM companies c
            WHERE c.active = true
            {index_filter}
            GROUP BY sector
            ORDER BY stock_count DESC
            LIMIT %(limit)s
        """).format(index_filter=index_filter)
    else:  # SIC
        query = sql.SQL("""
            SELECT
                c.sic_code,
                c.sic_description as sector,
                COUNT(DISTINCT c.ticker) as stock_count,
                STRING_AGG(DISTINCT c.ticker, ', ' ORDER BY c.ticker) as sample_tickers
            FROM companies c
            WHERE c.active = true
              AND c.sic_description IS NOT NULL
            {index_filter}
            GROUP BY c.sic_code, c.sic_description
            ORDER BY stock_count DESC, c.sic_description
            LIMIT %(limit)s
        """).format(index_filter=index_filter)

    return execute_query(query, params)


def get_sector_performance(
    taxonomy: Literal["sic", "gics"] = "gics",
    index: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """
    Get sector performance across multiple time periods.

    Args:
        taxonomy: Classification system - "sic" or "gics" (default: gics)
        index: Filter by index membership (sp500, nasdaq_listed)
        limit: Maximum sectors to return (default: 50)

    Returns:
        List of sector performance dicts with:
        - sector: Sector name
        - stock_count: Number of stocks in sector
        - return_1d: Average 1-day return (%)
        - return_1w: Average 1-week return (%)
        - return_1m: Average 1-month return (%)
        - return_ytd: Average year-to-date return (%)
        - best_ticker: Best performing stock in sector (1d)
        - best_return_1d: Best stock's 1-day return
        - worst_ticker: Worst performing stock in sector (1d)
        - worst_return_1d: Worst stock's 1-day return
    """
    limit = min(limit, 100)

    # Build sector grouping based on taxonomy (controlled SQL expressions)
    if taxonomy == "gics":
        sector_expr = sql.SQL("get_gics_sector(c.ticker, c.sic_code, c.sic_description)")
        join_clause = sql.SQL("")
    else:
        sector_expr = sql.SQL("c.sic_description")
        join_clause = sql.SQL("")

    # Reference dates are computed up front and passed as bind parameters so
    # the planner can push them into stock_prices_live's UNION ALL arms;
    # join keys sourced from a date_refs CTE force full view materializations.
    date_refs = get_price_date_refs()
    if date_refs["latest"] is None:
        return []

    # Build index filter (shared helper; lower-cased to match stored codes)
    params: dict[str, Any] = {"limit": limit, **date_refs}
    index_filter = build_index_filter(
        index.lower() if index else index, "c", params, param_name="index"
    )

    query = sql.SQL("""
        WITH stock_returns AS (
            SELECT
                c.ticker,
                c.name,
                {sector_expr} as sector,
                p_now.close as price,
                CASE WHEN p_prev.close > 0
                     THEN (p_now.close - p_prev.close) / p_prev.close * 100
                     ELSE NULL END as return_1d,
                CASE WHEN p_week.close > 0
                     THEN (p_now.close - p_week.close) / p_week.close * 100
                     ELSE NULL END as return_1w,
                CASE WHEN p_month.close > 0
                     THEN (p_now.close - p_month.close) / p_month.close * 100
                     ELSE NULL END as return_1m,
                CASE WHEN p_ytd.close > 0
                     THEN (p_now.close - p_ytd.close) / p_ytd.close * 100
                     ELSE NULL END as return_ytd
            FROM companies c
            {join_clause}
            JOIN stock_prices_live p_now ON c.ticker = p_now.ticker AND p_now.date = %(latest)s
            LEFT JOIN stock_prices_live p_prev
                ON c.ticker = p_prev.ticker AND p_prev.date = %(prev_day)s
            LEFT JOIN stock_prices_live p_week
                ON c.ticker = p_week.ticker AND p_week.date = %(week_ago)s
            LEFT JOIN stock_prices_live p_month
                ON c.ticker = p_month.ticker AND p_month.date = %(month_ago)s
            LEFT JOIN stock_prices_live p_ytd
                ON c.ticker = p_ytd.ticker AND p_ytd.date = %(ytd_start)s
            WHERE c.active = true
              AND {sector_expr_null} IS NOT NULL
            {index_filter}
        ),
        sector_stats AS (
            SELECT
                sector,
                COUNT(*) as stock_count,
                ROUND(AVG(return_1d)::numeric, 2) as return_1d,
                ROUND(AVG(return_1w)::numeric, 2) as return_1w,
                ROUND(AVG(return_1m)::numeric, 2) as return_1m,
                ROUND(AVG(return_ytd)::numeric, 2) as return_ytd
            FROM stock_returns
            GROUP BY sector
        ),
        best_performers AS (
            SELECT DISTINCT ON (sector)
                sector,
                ticker as best_ticker,
                ROUND(return_1d::numeric, 2) as best_return_1d
            FROM stock_returns
            WHERE return_1d IS NOT NULL
            ORDER BY sector, return_1d DESC
        ),
        worst_performers AS (
            SELECT DISTINCT ON (sector)
                sector,
                ticker as worst_ticker,
                ROUND(return_1d::numeric, 2) as worst_return_1d
            FROM stock_returns
            WHERE return_1d IS NOT NULL
            ORDER BY sector, return_1d ASC
        )
        SELECT
            s.*,
            b.best_ticker,
            b.best_return_1d,
            w.worst_ticker,
            w.worst_return_1d
        FROM sector_stats s
        LEFT JOIN best_performers b ON s.sector = b.sector
        LEFT JOIN worst_performers w ON s.sector = w.sector
        ORDER BY s.return_1d DESC NULLS LAST
        LIMIT %(limit)s
    """).format(
        sector_expr=sector_expr,
        sector_expr_null=sector_expr,
        join_clause=join_clause,
        index_filter=index_filter,
    )

    return execute_query(query, params)
