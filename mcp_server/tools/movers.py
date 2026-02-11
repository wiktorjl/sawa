"""Market movers MCP tools.

Provides tools for finding top gainers, losers, and volume leaders.
"""

import logging
from typing import Any, Literal

from psycopg import sql

from ..database import execute_query

logger = logging.getLogger(__name__)


def get_top_movers(
    direction: Literal["gainers", "losers", "both"] = "both",
    period: Literal["1d", "1w", "1m", "ytd"] = "1d",
    limit: int = 20,
    sector: str | None = None,
    index: str | None = None,
    min_price: float | None = None,
    min_volume: int | None = None,
) -> list[dict[str, Any]]:
    """
    Get top gaining or losing stocks.

    Args:
        direction: "gainers", "losers", or "both"
        period: Time period - "1d", "1w", "1m", or "ytd"
        limit: Number of results per direction (default: 20, max: 100)
        sector: Optional sector filter (partial match on SIC description or GICS sector)
        index: Filter by index membership (sp500, nasdaq100)
        min_price: Minimum stock price filter
        min_volume: Minimum volume filter

    Returns:
        List of stock dicts with:
        - ticker: Stock symbol
        - name: Company name
        - sector: SIC description
        - indices: List of index memberships
        - price: Current price
        - change_pct: Percentage change for the period
        - volume: Trading volume
        - market_cap: Market capitalization
    """
    limit = min(limit, 100)

    # Build period comparison (controlled SQL expressions)
    period_map = {
        "1d": sql.SQL("MAX(date) FILTER (WHERE date < (SELECT MAX(date) FROM stock_prices_live))"),
        "1w": sql.SQL("MAX(date) FILTER (WHERE date <= CURRENT_DATE - INTERVAL '7 days')"),
        "1m": sql.SQL("MAX(date) FILTER (WHERE date <= CURRENT_DATE - INTERVAL '30 days')"),
        "ytd": sql.SQL("MIN(date) FILTER (WHERE date >= DATE_TRUNC('year', CURRENT_DATE))"),
    }
    period_expr = period_map.get(period, period_map["1d"])

    # Build sector filter
    sector_filter = sql.SQL("")
    params: dict[str, Any] = {"limit": limit}
    if sector:
        sector_filter = sql.SQL("""
            AND get_gics_sector(c.ticker, c.sic_code, c.sic_description) ILIKE %(sector)s
        """)
        params["sector"] = f"%{sector}%"

    # Build index filter
    index_filter = sql.SQL("")
    if index:
        index_filter = sql.SQL("""AND c.ticker IN (
            SELECT ic.ticker FROM index_constituents ic
            JOIN indices i ON ic.index_id = i.id
            WHERE i.code = %(index)s
        )""")
        params["index"] = index.lower()

    # Build price/volume filters
    price_parts: list[sql.Composable] = []
    if min_price is not None:
        price_parts.append(sql.SQL(" AND p_now.close >= %(min_price)s"))
        params["min_price"] = min_price
    if min_volume is not None:
        price_parts.append(sql.SQL(" AND p_now.volume >= %(min_volume)s"))
        params["min_volume"] = min_volume
    price_filter = sql.SQL("").join(price_parts)

    # Determine sort order (controlled SQL literals)
    if direction == "gainers":
        order_clause = sql.SQL("change_pct DESC NULLS LAST")
    elif direction == "losers":
        order_clause = sql.SQL("change_pct ASC NULLS LAST")
    else:
        order_clause = sql.SQL("")

    if direction == "both":
        query = sql.SQL("""
            WITH date_refs AS (
                SELECT
                    MAX(date) as latest,
                    {period_expr} as compare_date
                FROM stock_prices_live
            ),
            changes AS (
                SELECT
                    c.ticker,
                    c.name,
                    c.sic_description as sector,
                    c.market_cap,
                    p_now.close as price,
                    p_now.volume,
                    CASE WHEN p_prev.close > 0
                         THEN ROUND(((p_now.close - p_prev.close) / p_prev.close * 100)::numeric, 2)
                         ELSE NULL END as change_pct,
                    ARRAY(
                        SELECT i.code FROM index_constituents ic
                        JOIN indices i ON ic.index_id = i.id
                        WHERE ic.ticker = c.ticker
                        ORDER BY i.code
                    ) as indices
                FROM companies c
                CROSS JOIN date_refs dr
                JOIN stock_prices_live p_now
                    ON c.ticker = p_now.ticker AND p_now.date = dr.latest
                LEFT JOIN stock_prices_live p_prev
                    ON c.ticker = p_prev.ticker AND p_prev.date = dr.compare_date
                WHERE c.active = true
                {sector_filter}
                {index_filter}
                {price_filter}
            ),
            gainers AS (
                SELECT *, 'gainer' as direction
                FROM changes
                WHERE change_pct IS NOT NULL
                ORDER BY change_pct DESC
                LIMIT %(limit)s
            ),
            losers AS (
                SELECT *, 'loser' as direction
                FROM changes
                WHERE change_pct IS NOT NULL
                ORDER BY change_pct ASC
                LIMIT %(limit)s
            ),
            combined AS (
                SELECT * FROM gainers
                UNION ALL
                SELECT * FROM losers
            )
            SELECT * FROM combined
            ORDER BY direction,
                     CASE WHEN direction = 'gainer' THEN -change_pct ELSE change_pct END
        """).format(
            period_expr=period_expr,
            sector_filter=sector_filter,
            index_filter=index_filter,
            price_filter=price_filter,
        )
    else:
        query = sql.SQL("""
            WITH date_refs AS (
                SELECT
                    MAX(date) as latest,
                    {period_expr} as compare_date
                FROM stock_prices_live
            )
            SELECT
                c.ticker,
                c.name,
                c.sic_description as sector,
                c.market_cap,
                p_now.close as price,
                p_now.volume,
                CASE WHEN p_prev.close > 0
                     THEN ROUND(((p_now.close - p_prev.close) / p_prev.close * 100)::numeric, 2)
                     ELSE NULL END as change_pct,
                ARRAY(
                    SELECT i.code FROM index_constituents ic
                    JOIN indices i ON ic.index_id = i.id
                    WHERE ic.ticker = c.ticker
                    ORDER BY i.code
                ) as indices
            FROM companies c
            CROSS JOIN date_refs dr
            JOIN stock_prices p_now
                ON c.ticker = p_now.ticker AND p_now.date = dr.latest
            LEFT JOIN stock_prices p_prev
                ON c.ticker = p_prev.ticker AND p_prev.date = dr.compare_date
            WHERE c.active = true
              AND p_prev.close IS NOT NULL
            {sector_filter}
            {index_filter}
            {price_filter}
            ORDER BY {order_clause}
            LIMIT %(limit)s
        """).format(
            period_expr=period_expr,
            sector_filter=sector_filter,
            index_filter=index_filter,
            price_filter=price_filter,
            order_clause=order_clause,
        )

    return execute_query(query, params)


def get_volume_leaders(
    metric: Literal["volume", "dollar_volume", "volume_ratio"] = "dollar_volume",
    limit: int = 20,
    sector: str | None = None,
    index: str | None = None,
    min_price: float | None = None,
) -> list[dict[str, Any]]:
    """
    Get stocks with highest trading volume.

    Args:
        metric: Volume metric to sort by:
            - "volume": Raw share volume
            - "dollar_volume": Volume * price (default)
            - "volume_ratio": Today's volume vs 20-day average
        limit: Number of results (default: 20, max: 100)
        sector: Optional sector filter (partial match)
        index: Filter by index membership (sp500, nasdaq100)
        min_price: Minimum stock price filter

    Returns:
        List of stock dicts with:
        - ticker: Stock symbol
        - name: Company name
        - sector: SIC description
        - indices: List of index memberships
        - price: Current price
        - volume: Trading volume
        - dollar_volume: Volume * price
        - volume_ratio: Volume vs 20-day average (if available)
        - change_1d: 1-day price change %
    """
    limit = min(limit, 100)

    # Build sector filter
    sector_filter = sql.SQL("")
    params: dict[str, Any] = {"limit": limit}
    if sector:
        sector_filter = sql.SQL("""
            AND get_gics_sector(c.ticker, c.sic_code, c.sic_description) ILIKE %(sector)s
        """)
        params["sector"] = f"%{sector}%"

    # Build index filter
    index_filter = sql.SQL("")
    if index:
        index_filter = sql.SQL("""AND c.ticker IN (
            SELECT ic.ticker FROM index_constituents ic
            JOIN indices i ON ic.index_id = i.id
            WHERE i.code = %(index)s
        )""")
        params["index"] = index.lower()

    price_filter = sql.SQL("")
    if min_price is not None:
        price_filter = sql.SQL(" AND p.close >= %(min_price)s")
        params["min_price"] = min_price

    # Determine sort column (controlled SQL expressions)
    sort_map = {
        "volume": sql.SQL("p.volume"),
        "dollar_volume": sql.SQL("p.volume * p.close"),
        "volume_ratio": sql.SQL("COALESCE(ti.volume_ratio, 0)"),
    }
    sort_expr = sort_map.get(metric, sort_map["dollar_volume"])

    query = sql.SQL("""
        WITH latest_date AS (
            SELECT MAX(date) as dt FROM stock_prices
        ),
        prev_date AS (
            SELECT MAX(date) as dt
            FROM stock_prices
            WHERE date < (SELECT dt FROM latest_date)
        )
        SELECT
            c.ticker,
            c.name,
            c.sic_description as sector,
            c.market_cap,
            p.close as price,
            p.volume,
            ROUND((p.volume * p.close)::numeric, 0) as dollar_volume,
            ROUND(ti.volume_ratio::numeric, 2) as volume_ratio,
            CASE WHEN p_prev.close > 0
                 THEN ROUND(((p.close - p_prev.close) / p_prev.close * 100)::numeric, 2)
                 ELSE NULL END as change_1d,
            ARRAY(
                SELECT i.code FROM index_constituents ic
                JOIN indices i ON ic.index_id = i.id
                WHERE ic.ticker = c.ticker
                ORDER BY i.code
            ) as indices
        FROM companies c
        CROSS JOIN latest_date ld
        CROSS JOIN prev_date pd
        JOIN stock_prices_live p ON c.ticker = p.ticker AND p.date = ld.dt
        LEFT JOIN stock_prices_live p_prev ON c.ticker = p_prev.ticker AND p_prev.date = pd.dt
        LEFT JOIN technical_indicators ti ON c.ticker = ti.ticker AND ti.date = ld.dt
        WHERE c.active = true
          AND p.volume > 0
        {sector_filter}
        {index_filter}
        {price_filter}
        ORDER BY {sort_expr} DESC NULLS LAST
        LIMIT %(limit)s
    """).format(
        sector_filter=sector_filter,
        index_filter=index_filter,
        price_filter=price_filter,
        sort_expr=sort_expr,
    )

    return execute_query(query, params)


def get_market_breadth(
    date: str | None = None,
    index: Literal["sp500", "nasdaq100", "all"] = "all",
) -> dict[str, Any]:
    """
    Get market breadth statistics (advancers vs decliners, MA breadth).

    Args:
        date: Date in YYYY-MM-DD format (default: latest trading day)
        index: Filter by index membership (default: all active stocks)

    Returns:
        Dict with:
        - advancers, decliners, unchanged counts
        - ad_ratio (advance/decline ratio)
        - above_50dma, above_200dma counts (stocks above moving averages)
        - pct_above_50dma, pct_above_200dma (percentage)
    """
    # Build index filter (uses index_constituents junction table)
    index_filter = sql.SQL("")
    if index == "sp500":
        index_filter = sql.SQL("""AND c.ticker IN (
            SELECT ic.ticker FROM index_constituents ic
            JOIN indices i ON ic.index_id = i.id
            WHERE i.code = 'sp500'
        )""")
    elif index == "nasdaq100":
        index_filter = sql.SQL("""AND c.ticker IN (
            SELECT ic.ticker FROM index_constituents ic
            JOIN indices i ON ic.index_id = i.id
            WHERE i.code = 'nasdaq100'
        )""")

    params: dict[str, Any] = {}

    if date:
        date_cte = sql.SQL("""
            WITH date_refs AS (
                SELECT
                    %(target_date)s::date as latest,
                    (SELECT MAX(date) FROM stock_prices
                     WHERE date < %(target_date)s::date) as previous
            )""")
        params["target_date"] = date
    else:
        date_cte = sql.SQL("""
            WITH latest_dates AS (
                SELECT DISTINCT date FROM stock_prices_live ORDER BY date DESC LIMIT 2
            ),
            date_refs AS (
                SELECT MAX(date) as latest, MIN(date) as previous FROM latest_dates
            )""")

    query = sql.SQL("""
        {date_cte},
        stock_data AS (
            SELECT
                c.ticker,
                p_now.close,
                p_prev.close as prev_close,
                ti.sma_50,
                ti.sma_200,
                CASE
                    WHEN p_now.close > p_prev.close THEN 'advancer'
                    WHEN p_now.close < p_prev.close THEN 'decliner'
                    ELSE 'unchanged'
                END as direction,
                CASE WHEN p_now.close > ti.sma_50 THEN 1 ELSE 0 END as above_50,
                CASE WHEN p_now.close > ti.sma_200 THEN 1 ELSE 0 END as above_200
            FROM companies c
            CROSS JOIN date_refs dr
            JOIN stock_prices p_now
                ON c.ticker = p_now.ticker AND p_now.date = dr.latest
            JOIN stock_prices p_prev
                ON c.ticker = p_prev.ticker AND p_prev.date = dr.previous
            LEFT JOIN technical_indicators ti
                ON c.ticker = ti.ticker AND ti.date = dr.latest
            WHERE c.active = true
            {index_filter}
        )
        SELECT
            (SELECT latest FROM date_refs) as date,
            COUNT(*) FILTER (WHERE direction = 'advancer') as advancers,
            COUNT(*) FILTER (WHERE direction = 'decliner') as decliners,
            COUNT(*) FILTER (WHERE direction = 'unchanged') as unchanged,
            COUNT(*) as total,
            CASE
                WHEN COUNT(*) FILTER (WHERE direction = 'decliner') > 0
                THEN ROUND(
                    (COUNT(*) FILTER (WHERE direction = 'advancer')::numeric /
                     COUNT(*) FILTER (WHERE direction = 'decliner'))::numeric, 2)
                ELSE NULL
            END as ad_ratio,
            SUM(above_50) as above_50dma,
            SUM(above_200) as above_200dma,
            ROUND((SUM(above_50)::numeric / NULLIF(COUNT(*), 0) * 100)::numeric, 1)
                as pct_above_50dma,
            ROUND((SUM(above_200)::numeric / NULLIF(COUNT(*), 0) * 100)::numeric, 1)
                as pct_above_200dma
        FROM stock_data
    """).format(
        date_cte=date_cte,
        index_filter=index_filter,
    )

    results = execute_query(query, params)
    if results:
        return results[0]
    return {
        "date": date,
        "advancers": 0,
        "decliners": 0,
        "unchanged": 0,
        "total": 0,
        "ad_ratio": None,
        "above_50dma": 0,
        "above_200dma": 0,
        "pct_above_50dma": 0,
        "pct_above_200dma": 0,
    }
