"""Market movers MCP tools.

Provides tools for finding top gainers, losers, and volume leaders.
"""

import logging
from typing import Any, Literal

from psycopg import sql

from ..database import execute_query
from ._dates import get_eod_date_refs, get_price_date_refs
from ._index_filter import build_index_filter

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
        index: Filter by index membership (sp500, nasdaq_listed)
        min_price: Minimum stock price filter
        min_volume: Minimum volume filter

    Returns:
        List of stock dicts with:
        - ticker: Stock symbol
        - name: Company name
        - sector: GICS sector (same expression used by the sector filter)
        - direction: "gainer" or "loser"
        - indices: List of index memberships
        - price: Current price
        - change_pct: Percentage change for the period
        - volume: Trading volume
        - market_cap: Market capitalization
    """
    limit = min(limit, 100)

    # Reference dates are computed up front and passed as bind parameters so
    # the planner can push them into stock_prices_live's UNION ALL arms.
    date_refs = get_price_date_refs()
    if date_refs["latest"] is None:
        return []

    period_map = {
        "1d": "prev_day",
        "1w": "week_ago",
        "1m": "month_ago",
        "ytd": "ytd_start",
    }
    compare_date = date_refs[period_map.get(period, "prev_day")]

    # Build sector filter
    sector_filter = sql.SQL("")
    params: dict[str, Any] = {
        "limit": limit,
        "latest": date_refs["latest"],
        "compare_date": compare_date,
    }
    if sector:
        sector_filter = sql.SQL("""
            AND get_gics_sector(c.ticker, c.sic_code, c.sic_description) ILIKE %(sector)s
        """)
        params["sector"] = f"%{sector}%"

    # Build index filter (shared helper; lower-cased to match stored codes)
    index_filter = build_index_filter(
        index.lower() if index else index, "c", params, param_name="index"
    )

    # Build price/volume filters
    price_parts: list[sql.Composable] = []
    if min_price is not None:
        price_parts.append(sql.SQL(" AND p_now.close >= %(min_price)s"))
        params["min_price"] = min_price
    if min_volume is not None:
        price_parts.append(sql.SQL(" AND p_now.volume >= %(min_volume)s"))
        params["min_volume"] = min_volume
    price_filter = sql.SQL("").join(price_parts)

    # Determine sort order and the single-direction label (controlled SQL
    # literals). Single-direction results carry the same 'direction' column as
    # 'both' so every row shape is consistent.
    if direction == "gainers":
        order_clause = sql.SQL("change_pct DESC NULLS LAST")
        direction_label = sql.SQL("'gainer'")
    elif direction == "losers":
        order_clause = sql.SQL("change_pct ASC NULLS LAST")
        direction_label = sql.SQL("'loser'")
    else:
        order_clause = sql.SQL("")
        direction_label = sql.SQL("")

    if direction == "both":
        query = sql.SQL("""
            WITH changes AS (
                SELECT
                    c.ticker,
                    c.name,
                    get_gics_sector(c.ticker, c.sic_code, c.sic_description) as sector,
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
                JOIN stock_prices_live p_now
                    ON c.ticker = p_now.ticker AND p_now.date = %(latest)s
                LEFT JOIN stock_prices_live p_prev
                    ON c.ticker = p_prev.ticker AND p_prev.date = %(compare_date)s
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
            sector_filter=sector_filter,
            index_filter=index_filter,
            price_filter=price_filter,
        )
    else:
        query = sql.SQL("""
            SELECT
                c.ticker,
                c.name,
                get_gics_sector(c.ticker, c.sic_code, c.sic_description) as sector,
                c.market_cap,
                p_now.close as price,
                p_now.volume,
                CASE WHEN p_prev.close > 0
                     THEN ROUND(((p_now.close - p_prev.close) / p_prev.close * 100)::numeric, 2)
                     ELSE NULL END as change_pct,
                {direction_label} as direction,
                ARRAY(
                    SELECT i.code FROM index_constituents ic
                    JOIN indices i ON ic.index_id = i.id
                    WHERE ic.ticker = c.ticker
                    ORDER BY i.code
                ) as indices
            FROM companies c
            JOIN stock_prices_live p_now
                ON c.ticker = p_now.ticker AND p_now.date = %(latest)s
            LEFT JOIN stock_prices_live p_prev
                ON c.ticker = p_prev.ticker AND p_prev.date = %(compare_date)s
            WHERE c.active = true
              AND p_prev.close IS NOT NULL
            {sector_filter}
            {index_filter}
            {price_filter}
            ORDER BY {order_clause}
            LIMIT %(limit)s
        """).format(
            sector_filter=sector_filter,
            index_filter=index_filter,
            price_filter=price_filter,
            order_clause=order_clause,
            direction_label=direction_label,
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
        index: Filter by index membership (sp500, nasdaq_listed)
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

    # Completed-session dates, passed as bind parameters (see _dates module).
    # Volume metrics compare against technical_indicators, which only exists
    # for completed sessions, so this tool stays on EOD dates.
    eod_refs = get_eod_date_refs()
    if eod_refs["latest"] is None:
        return []

    # Build sector filter
    sector_filter = sql.SQL("")
    params: dict[str, Any] = {
        "limit": limit,
        "latest": eod_refs["latest"],
        "prev_day": eod_refs["prev_day"],
    }
    if sector:
        sector_filter = sql.SQL("""
            AND get_gics_sector(c.ticker, c.sic_code, c.sic_description) ILIKE %(sector)s
        """)
        params["sector"] = f"%{sector}%"

    # Build index filter (shared helper; lower-cased to match stored codes)
    index_filter = build_index_filter(
        index.lower() if index else index, "c", params, param_name="index"
    )

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
        JOIN stock_prices_live p ON c.ticker = p.ticker AND p.date = %(latest)s
        LEFT JOIN stock_prices_live p_prev
            ON c.ticker = p_prev.ticker AND p_prev.date = %(prev_day)s
        LEFT JOIN technical_indicators ti ON c.ticker = ti.ticker AND ti.date = %(latest)s
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
    index: Literal["sp500", "nasdaq_listed", "us_active", "nasdaq100", "dow30", "russell1000", "mag7", "all"] = "all",
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
    params: dict[str, Any] = {}
    index_filter = build_index_filter(index, "c", params)

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
        # Breadth joins stock_prices (advance/decline needs completed
        # sessions), so reference the latest EOD dates rather than the
        # live view, whose latest date has no EOD rows during market hours.
        eod_refs = get_eod_date_refs()
        date_cte = sql.SQL("""
            WITH date_refs AS (
                SELECT %(latest)s::date as latest, %(previous)s::date as previous
            )""")
        params["latest"] = eod_refs["latest"]
        params["previous"] = eod_refs["prev_day"]

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
