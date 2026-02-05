"""Market movers MCP tools.

Provides tools for finding top gainers, losers, and volume leaders.
"""

import logging
from typing import Any, Literal

from ..database import execute_query

logger = logging.getLogger(__name__)


def get_top_movers(
    direction: Literal["gainers", "losers", "both"] = "both",
    period: Literal["1d", "1w", "1m", "ytd"] = "1d",
    limit: int = 20,
    sector: str | None = None,
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
        min_price: Minimum stock price filter
        min_volume: Minimum volume filter

    Returns:
        List of stock dicts with:
        - ticker: Stock symbol
        - name: Company name
        - sector: SIC description
        - price: Current price
        - change_pct: Percentage change for the period
        - volume: Trading volume
        - market_cap: Market capitalization
    """
    limit = min(limit, 100)

    # Build period comparison
    period_map = {
        "1d": "MAX(date) FILTER (WHERE date < (SELECT MAX(date) FROM stock_prices))",
        "1w": "MAX(date) FILTER (WHERE date <= CURRENT_DATE - INTERVAL '7 days')",
        "1m": "MAX(date) FILTER (WHERE date <= CURRENT_DATE - INTERVAL '30 days')",
        "ytd": "MIN(date) FILTER (WHERE date >= DATE_TRUNC('year', CURRENT_DATE))",
    }
    period_expr = period_map.get(period, period_map["1d"])

    # Build sector filter
    sector_filter = ""
    params: dict[str, Any] = {"limit": limit}
    if sector:
        sector_filter = """
            AND (c.sic_description ILIKE %(sector)s
                 OR EXISTS (
                     SELECT 1 FROM sic_gics_mapping m
                     WHERE m.sic_code = c.sic_code
                       AND m.gics_sector ILIKE %(sector)s
                 ))
        """
        params["sector"] = f"%{sector}%"

    # Build price/volume filters
    price_filter = ""
    if min_price is not None:
        price_filter += " AND p_now.close >= %(min_price)s"
        params["min_price"] = min_price
    if min_volume is not None:
        price_filter += " AND p_now.volume >= %(min_volume)s"
        params["min_volume"] = min_volume

    # Determine sort order
    if direction == "gainers":
        order_clause = "change_pct DESC NULLS LAST"
    elif direction == "losers":
        order_clause = "change_pct ASC NULLS LAST"
    else:
        # For "both", we'll use a UNION approach
        pass

    if direction == "both":
        sql = f"""
            WITH date_refs AS (
                SELECT
                    MAX(date) as latest,
                    {period_expr} as compare_date
                FROM stock_prices
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
                         ELSE NULL END as change_pct
                FROM companies c
                CROSS JOIN date_refs dr
                JOIN stock_prices p_now
                    ON c.ticker = p_now.ticker AND p_now.date = dr.latest
                LEFT JOIN stock_prices p_prev
                    ON c.ticker = p_prev.ticker AND p_prev.date = dr.compare_date
                WHERE c.active = true
                {sector_filter}
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
            )
            SELECT * FROM gainers
            UNION ALL
            SELECT * FROM losers
            ORDER BY direction,
                     CASE WHEN direction = 'gainer' THEN -change_pct ELSE change_pct END
        """
    else:
        sql = f"""
            WITH date_refs AS (
                SELECT
                    MAX(date) as latest,
                    {period_expr} as compare_date
                FROM stock_prices
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
                     ELSE NULL END as change_pct
            FROM companies c
            CROSS JOIN date_refs dr
            JOIN stock_prices p_now
                ON c.ticker = p_now.ticker AND p_now.date = dr.latest
            LEFT JOIN stock_prices p_prev
                ON c.ticker = p_prev.ticker AND p_prev.date = dr.compare_date
            WHERE c.active = true
              AND p_prev.close IS NOT NULL
            {sector_filter}
            {price_filter}
            ORDER BY {order_clause}
            LIMIT %(limit)s
        """

    return execute_query(sql, params)


def get_volume_leaders(
    metric: Literal["volume", "dollar_volume", "volume_ratio"] = "dollar_volume",
    limit: int = 20,
    sector: str | None = None,
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
        min_price: Minimum stock price filter

    Returns:
        List of stock dicts with:
        - ticker: Stock symbol
        - name: Company name
        - sector: SIC description
        - price: Current price
        - volume: Trading volume
        - dollar_volume: Volume * price
        - volume_ratio: Volume vs 20-day average (if available)
        - change_1d: 1-day price change %
    """
    limit = min(limit, 100)

    # Build sector filter
    sector_filter = ""
    params: dict[str, Any] = {"limit": limit}
    if sector:
        sector_filter = """
            AND (c.sic_description ILIKE %(sector)s
                 OR EXISTS (
                     SELECT 1 FROM sic_gics_mapping m
                     WHERE m.sic_code = c.sic_code
                       AND m.gics_sector ILIKE %(sector)s
                 ))
        """
        params["sector"] = f"%{sector}%"

    price_filter = ""
    if min_price is not None:
        price_filter = " AND p.close >= %(min_price)s"
        params["min_price"] = min_price

    # Determine sort column
    sort_map = {
        "volume": "p.volume",
        "dollar_volume": "p.volume * p.close",
        "volume_ratio": "COALESCE(ti.volume_ratio, 0)",
    }
    sort_expr = sort_map.get(metric, sort_map["dollar_volume"])

    sql = f"""
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
                 ELSE NULL END as change_1d
        FROM companies c
        CROSS JOIN latest_date ld
        CROSS JOIN prev_date pd
        JOIN stock_prices p ON c.ticker = p.ticker AND p.date = ld.dt
        LEFT JOIN stock_prices p_prev ON c.ticker = p_prev.ticker AND p_prev.date = pd.dt
        LEFT JOIN technical_indicators ti ON c.ticker = ti.ticker AND ti.date = ld.dt
        WHERE c.active = true
          AND p.volume > 0
        {sector_filter}
        {price_filter}
        ORDER BY {sort_expr} DESC NULLS LAST
        LIMIT %(limit)s
    """

    return execute_query(sql, params)
