"""Flexible stock screener MCP tool.

Provides multi-criteria stock screening with support for price, volume,
fundamental, and technical indicator filters.
"""

import logging
from typing import Any, Literal

from ..database import execute_query

logger = logging.getLogger(__name__)

# Valid filter keys for the screener
VALID_FILTERS = {
    # Price filters
    "price",
    "price_change_1d",
    "price_change_1w",
    "price_change_1m",
    "price_change_ytd",
    # Volume filters
    "volume",
    "dollar_volume",
    "volume_ratio",
    # Fundamental filters
    "market_cap",
    # Technical indicator filters - SMAs
    "sma_5",
    "sma_10",
    "sma_20",
    "sma_50",
    "sma_100",
    "sma_150",
    "sma_200",
    # Technical indicator filters - EMAs
    "ema_12",
    "ema_26",
    "ema_50",
    "ema_100",
    "ema_200",
    # Technical indicator filters - distance from SMA (%)
    "sma_20_distance_pct",
    "sma_50_distance_pct",
    "sma_100_distance_pct",
    "sma_150_distance_pct",
    "sma_200_distance_pct",
    # Technical indicator filters - momentum
    "rsi_14",
    "rsi_21",
    "macd_line",
    "macd_signal",
    "macd_histogram",
    # Technical indicator filters - volatility
    "bb_upper",
    "bb_middle",
    "bb_lower",
    "atr_14",
    # Technical indicator filters - volume
    "obv",
    "volume_sma_20",
}


def screen_stocks(
    filters: dict[str, list[float | None]],
    sector: str | None = None,
    taxonomy: Literal["sic", "gics"] = "gics",
    sort_by: str = "market_cap",
    sort_order: Literal["asc", "desc"] = "desc",
    limit: int = 50,
) -> list[dict[str, Any]]:
    """
    Flexible multi-criteria stock screener.

    Args:
        filters: Dict mapping filter name to [min, max] bounds.
                 Use null for unbounded side.
                 Example: {"rsi_14": [null, 30], "volume_ratio": [1.5, null]}

                 Available filters:
                 - Price: price, price_change_1d/1w/1m/ytd
                 - Volume: volume, dollar_volume, volume_ratio
                 - Fundamental: market_cap
                 - SMA values: sma_5/10/20/50/100/150/200
                 - SMA distance %: sma_20/50/100/150/200_distance_pct
                 - EMA values: ema_12/26/50/100/200
                 - Momentum: rsi_14, rsi_21, macd_line/signal/histogram
                 - Volatility: bb_upper/middle/lower, atr_14

        sector: Optional sector filter (partial match on SIC or GICS)
        taxonomy: Sector taxonomy - "sic" or "gics" (default: gics)
        sort_by: Column to sort by (default: market_cap)
        sort_order: Sort direction - "asc" or "desc" (default: desc)
        limit: Maximum results (default: 50, max: 500)

    Returns:
        List of matching stocks with:
        - ticker, name, sector, price, market_cap
        - change_1d, change_1w, change_1m, change_ytd
        - volume, dollar_volume, volume_ratio
        - Key technical indicators based on filters used
    """
    limit = min(limit, 500)

    # Build the base query with CTEs
    params: dict[str, Any] = {"limit": limit}

    # Sector expression based on taxonomy
    if taxonomy == "gics":
        sector_expr = "COALESCE(m.gics_sector, c.sic_description)"
        sector_join = "LEFT JOIN sic_gics_mapping m ON c.sic_code = m.sic_code"
    else:
        sector_expr = "c.sic_description"
        sector_join = ""

    # Sector filter
    sector_filter = ""
    if sector:
        sector_filter = f"AND {sector_expr} ILIKE %(sector)s"
        params["sector"] = f"%{sector}%"

    # Build filter conditions
    where_conditions = []

    # Track which computed columns we need

    filter_idx = 0
    for filter_name, bounds in filters.items():
        if filter_name not in VALID_FILTERS:
            logger.warning(f"Unknown filter: {filter_name}, skipping")
            continue

        if not isinstance(bounds, list) or len(bounds) != 2:
            logger.warning(f"Invalid bounds for {filter_name}: {bounds}, skipping")
            continue

        min_val, max_val = bounds

        # Determine the column/expression for this filter
        col_expr = _get_filter_expression(filter_name)

        # Track dependencies
        if filter_name.startswith("price_change"):
            pass
        if filter_name.startswith(
            ("sma_", "ema_", "rsi_", "macd_", "bb_", "atr_", "obv", "volume_ratio", "volume_sma")
        ):
            pass
        if "_distance_pct" in filter_name:
            pass

        # Build condition
        if min_val is not None and max_val is not None:
            where_conditions.append(
                f"{col_expr} BETWEEN %(f{filter_idx}_min)s AND %(f{filter_idx}_max)s"
            )
            params[f"f{filter_idx}_min"] = min_val
            params[f"f{filter_idx}_max"] = max_val
        elif min_val is not None:
            where_conditions.append(f"{col_expr} >= %(f{filter_idx}_min)s")
            params[f"f{filter_idx}_min"] = min_val
        elif max_val is not None:
            where_conditions.append(f"{col_expr} <= %(f{filter_idx}_max)s")
            params[f"f{filter_idx}_max"] = max_val

        filter_idx += 1

    # Build the complete query
    where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"

    # Validate sort_by
    valid_sort_columns = {
        "market_cap",
        "price",
        "volume",
        "dollar_volume",
        "volume_ratio",
        "change_1d",
        "change_1w",
        "change_1m",
        "change_ytd",
        "rsi_14",
        "ticker",
        "name",
    }
    if sort_by not in valid_sort_columns:
        sort_by = "market_cap"

    sort_dir = "DESC" if sort_order == "desc" else "ASC"

    sql = f"""
        WITH date_refs AS (
            SELECT
                MAX(date) as latest,
                MAX(date) FILTER (WHERE date < (SELECT MAX(date) FROM stock_prices)) as prev_day,
                MAX(date) FILTER (WHERE date <= CURRENT_DATE - INTERVAL '7 days') as week_ago,
                MAX(date) FILTER (WHERE date <= CURRENT_DATE - INTERVAL '30 days') as month_ago,
                MIN(date) FILTER (WHERE date >= DATE_TRUNC('year', CURRENT_DATE)) as ytd_start
            FROM stock_prices
        ),
        base_data AS (
            SELECT
                c.ticker,
                c.name,
                {sector_expr} as sector,
                c.market_cap,
                p.close as price,
                p.volume,
                ROUND((p.volume * p.close)::numeric, 0) as dollar_volume,
                -- Price changes
                CASE WHEN p_prev.close > 0
                     THEN ROUND(((p.close - p_prev.close) / p_prev.close * 100)::numeric, 2)
                     ELSE NULL END as change_1d,
                CASE WHEN p_week.close > 0
                     THEN ROUND(((p.close - p_week.close) / p_week.close * 100)::numeric, 2)
                     ELSE NULL END as change_1w,
                CASE WHEN p_month.close > 0
                     THEN ROUND(((p.close - p_month.close) / p_month.close * 100)::numeric, 2)
                     ELSE NULL END as change_1m,
                CASE WHEN p_ytd.close > 0
                     THEN ROUND(((p.close - p_ytd.close) / p_ytd.close * 100)::numeric, 2)
                     ELSE NULL END as change_ytd,
                -- Technical indicators
                ti.rsi_14,
                ti.rsi_21,
                ti.sma_20,
                ti.sma_50,
                ti.sma_100,
                ti.sma_150,
                ti.sma_200,
                ti.ema_50,
                ti.ema_200,
                ti.macd_line,
                ti.macd_signal,
                ti.macd_histogram,
                ti.bb_upper,
                ti.bb_lower,
                ti.atr_14,
                ti.volume_ratio,
                -- SMA distance percentages
                CASE WHEN ti.sma_20 > 0
                     THEN ROUND(((p.close - ti.sma_20) / ti.sma_20 * 100)::numeric, 2)
                     ELSE NULL END as sma_20_distance_pct,
                CASE WHEN ti.sma_50 > 0
                     THEN ROUND(((p.close - ti.sma_50) / ti.sma_50 * 100)::numeric, 2)
                     ELSE NULL END as sma_50_distance_pct,
                CASE WHEN ti.sma_100 > 0
                     THEN ROUND(((p.close - ti.sma_100) / ti.sma_100 * 100)::numeric, 2)
                     ELSE NULL END as sma_100_distance_pct,
                CASE WHEN ti.sma_150 > 0
                     THEN ROUND(((p.close - ti.sma_150) / ti.sma_150 * 100)::numeric, 2)
                     ELSE NULL END as sma_150_distance_pct,
                CASE WHEN ti.sma_200 > 0
                     THEN ROUND(((p.close - ti.sma_200) / ti.sma_200 * 100)::numeric, 2)
                     ELSE NULL END as sma_200_distance_pct
            FROM companies c
            {sector_join}
            CROSS JOIN date_refs dr
            JOIN stock_prices p ON c.ticker = p.ticker AND p.date = dr.latest
            LEFT JOIN stock_prices p_prev
                ON c.ticker = p_prev.ticker AND p_prev.date = dr.prev_day
            LEFT JOIN stock_prices p_week
                ON c.ticker = p_week.ticker AND p_week.date = dr.week_ago
            LEFT JOIN stock_prices p_month
                ON c.ticker = p_month.ticker AND p_month.date = dr.month_ago
            LEFT JOIN stock_prices p_ytd
                ON c.ticker = p_ytd.ticker AND p_ytd.date = dr.ytd_start
            LEFT JOIN technical_indicators ti ON c.ticker = ti.ticker AND ti.date = dr.latest
            WHERE c.active = true
            {sector_filter}
        )
        SELECT
            ticker,
            name,
            sector,
            market_cap,
            price,
            volume,
            dollar_volume,
            change_1d,
            change_1w,
            change_1m,
            change_ytd,
            rsi_14,
            volume_ratio,
            sma_50_distance_pct,
            sma_200_distance_pct
        FROM base_data
        WHERE {where_clause}
        ORDER BY {sort_by} {sort_dir} NULLS LAST
        LIMIT %(limit)s
    """

    return execute_query(sql, params)


def _get_filter_expression(filter_name: str) -> str:
    """Map filter name to SQL column/expression."""
    # Direct column mappings
    direct_mappings = {
        "price": "price",
        "market_cap": "market_cap",
        "volume": "volume",
        "dollar_volume": "dollar_volume",
        "price_change_1d": "change_1d",
        "price_change_1w": "change_1w",
        "price_change_1m": "change_1m",
        "price_change_ytd": "change_ytd",
        "volume_ratio": "volume_ratio",
        # Technical indicators
        "rsi_14": "rsi_14",
        "rsi_21": "rsi_21",
        "sma_20": "sma_20",
        "sma_50": "sma_50",
        "sma_100": "sma_100",
        "sma_150": "sma_150",
        "sma_200": "sma_200",
        "ema_50": "ema_50",
        "ema_200": "ema_200",
        "macd_line": "macd_line",
        "macd_signal": "macd_signal",
        "macd_histogram": "macd_histogram",
        "bb_upper": "bb_upper",
        "bb_lower": "bb_lower",
        "atr_14": "atr_14",
        # SMA distance percentages
        "sma_20_distance_pct": "sma_20_distance_pct",
        "sma_50_distance_pct": "sma_50_distance_pct",
        "sma_100_distance_pct": "sma_100_distance_pct",
        "sma_150_distance_pct": "sma_150_distance_pct",
        "sma_200_distance_pct": "sma_200_distance_pct",
    }

    return direct_mappings.get(filter_name, filter_name)
