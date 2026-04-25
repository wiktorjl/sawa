"""Flexible stock screener MCP tool.

Provides multi-criteria stock screening with support for price, volume,
fundamental, and technical indicator filters.
"""

import logging
from dataclasses import dataclass
from typing import Any, Literal

from psycopg import sql

from ..database import execute_query

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FilterSpec:
    """SQL expression and output alias for a supported screener filter."""

    expression: str
    alias: str


FILTER_SPECS = {
    # Price filters
    "price": FilterSpec("p.close", "price"),
    "price_change_1d": FilterSpec(
        """
        CASE WHEN p_prev.close > 0
             THEN ROUND(((p.close - p_prev.close) / p_prev.close * 100)::numeric, 2)
             ELSE NULL END
        """,
        "change_1d",
    ),
    "price_change_1w": FilterSpec(
        """
        CASE WHEN p_week.close > 0
             THEN ROUND(((p.close - p_week.close) / p_week.close * 100)::numeric, 2)
             ELSE NULL END
        """,
        "change_1w",
    ),
    "price_change_1m": FilterSpec(
        """
        CASE WHEN p_month.close > 0
             THEN ROUND(((p.close - p_month.close) / p_month.close * 100)::numeric, 2)
             ELSE NULL END
        """,
        "change_1m",
    ),
    "price_change_ytd": FilterSpec(
        """
        CASE WHEN p_ytd.close > 0
             THEN ROUND(((p.close - p_ytd.close) / p_ytd.close * 100)::numeric, 2)
             ELSE NULL END
        """,
        "change_ytd",
    ),
    "daily_range_pct": FilterSpec(
        "ROUND(((p.high - p.low) / NULLIF(p.close, 0) * 100)::numeric, 2)",
        "daily_range_pct",
    ),
    "high_52w_pct": FilterSpec(
        "ROUND(((p.close - e.high_52w) / NULLIF(e.high_52w, 0) * 100)::numeric, 2)",
        "high_52w_pct",
    ),
    "low_52w_pct": FilterSpec(
        "ROUND(((p.close - e.low_52w) / NULLIF(e.low_52w, 0) * 100)::numeric, 2)",
        "low_52w_pct",
    ),
    # Volume filters
    "volume": FilterSpec("p.volume", "volume"),
    "dollar_volume": FilterSpec("ROUND((p.volume * p.close)::numeric, 0)", "dollar_volume"),
    "volume_ratio": FilterSpec("ti.volume_ratio", "volume_ratio"),
    # Fundamental filters
    "market_cap": FilterSpec("c.market_cap", "market_cap"),
    "pe_ratio": FilterSpec("fr.price_to_earnings", "pe_ratio"),
    "dividend_yield": FilterSpec("fr.dividend_yield", "dividend_yield"),
    "roe": FilterSpec("fr.return_on_equity", "roe"),
    "debt_to_equity": FilterSpec("fr.debt_to_equity", "debt_to_equity"),
    # Technical indicator filters - SMAs
    "sma_5": FilterSpec("ti.sma_5", "sma_5"),
    "sma_10": FilterSpec("ti.sma_10", "sma_10"),
    "sma_20": FilterSpec("ti.sma_20", "sma_20"),
    "sma_50": FilterSpec("ti.sma_50", "sma_50"),
    "sma_100": FilterSpec("ti.sma_100", "sma_100"),
    "sma_150": FilterSpec("ti.sma_150", "sma_150"),
    "sma_200": FilterSpec("ti.sma_200", "sma_200"),
    # Technical indicator filters - EMAs
    "ema_12": FilterSpec("ti.ema_12", "ema_12"),
    "ema_26": FilterSpec("ti.ema_26", "ema_26"),
    "ema_50": FilterSpec("ti.ema_50", "ema_50"),
    "ema_100": FilterSpec("ti.ema_100", "ema_100"),
    "ema_200": FilterSpec("ti.ema_200", "ema_200"),
    # Technical indicator filters - distance from SMA (%)
    "sma_20_distance_pct": FilterSpec(
        """
        CASE WHEN ti.sma_20 > 0
             THEN ROUND(((p.close - ti.sma_20) / ti.sma_20 * 100)::numeric, 2)
             ELSE NULL END
        """,
        "sma_20_distance_pct",
    ),
    "sma_50_distance_pct": FilterSpec(
        """
        CASE WHEN ti.sma_50 > 0
             THEN ROUND(((p.close - ti.sma_50) / ti.sma_50 * 100)::numeric, 2)
             ELSE NULL END
        """,
        "sma_50_distance_pct",
    ),
    "sma_100_distance_pct": FilterSpec(
        """
        CASE WHEN ti.sma_100 > 0
             THEN ROUND(((p.close - ti.sma_100) / ti.sma_100 * 100)::numeric, 2)
             ELSE NULL END
        """,
        "sma_100_distance_pct",
    ),
    "sma_150_distance_pct": FilterSpec(
        """
        CASE WHEN ti.sma_150 > 0
             THEN ROUND(((p.close - ti.sma_150) / ti.sma_150 * 100)::numeric, 2)
             ELSE NULL END
        """,
        "sma_150_distance_pct",
    ),
    "sma_200_distance_pct": FilterSpec(
        """
        CASE WHEN ti.sma_200 > 0
             THEN ROUND(((p.close - ti.sma_200) / ti.sma_200 * 100)::numeric, 2)
             ELSE NULL END
        """,
        "sma_200_distance_pct",
    ),
    # Technical indicator filters - momentum
    "rsi_14": FilterSpec("ti.rsi_14", "rsi_14"),
    "rsi_21": FilterSpec("ti.rsi_21", "rsi_21"),
    "macd_line": FilterSpec("ti.macd_line", "macd_line"),
    "macd_signal": FilterSpec("ti.macd_signal", "macd_signal"),
    "macd_histogram": FilterSpec("ti.macd_histogram", "macd_histogram"),
    # Technical indicator filters - volatility
    "bb_upper": FilterSpec("ti.bb_upper", "bb_upper"),
    "bb_middle": FilterSpec("ti.bb_middle", "bb_middle"),
    "bb_lower": FilterSpec("ti.bb_lower", "bb_lower"),
    "atr_14": FilterSpec("ti.atr_14", "atr_14"),
    # Technical indicator filters - volume
    "obv": FilterSpec("ti.obv", "obv"),
    "volume_sma_20": FilterSpec("ti.volume_sma_20", "volume_sma_20"),
}

VALID_FILTERS = set(FILTER_SPECS)

DEFAULT_OUTPUT_FILTERS = (
    "price",
    "market_cap",
    "price_change_1d",
    "price_change_1w",
    "price_change_1m",
    "price_change_ytd",
    "volume",
    "dollar_volume",
    "volume_ratio",
    "rsi_14",
    "sma_50_distance_pct",
    "sma_150_distance_pct",
    "sma_200_distance_pct",
    "pe_ratio",
    "dividend_yield",
    "roe",
    "debt_to_equity",
)


def _build_filter_selects(filter_specs: list[FilterSpec] | None = None) -> sql.Composable:
    """Build base_data SELECT expressions for every supported filter."""
    specs = filter_specs or list(FILTER_SPECS.values())
    return sql.SQL(",\n                ").join(
        sql.SQL("{} AS {}").format(
            sql.SQL(spec.expression.strip()),
            sql.Identifier(spec.alias),
        )
        for spec in specs
    )


def _build_output_selects(filter_specs: list[FilterSpec] | None = None) -> sql.Composable:
    """Build final output columns from filter aliases, preserving registry order."""
    specs = filter_specs or list(FILTER_SPECS.values())
    seen: set[str] = set()
    columns: list[sql.Composable] = []
    for spec in specs:
        if spec.alias in seen:
            continue
        seen.add(spec.alias)
        columns.append(sql.Identifier(spec.alias))
    return sql.SQL(",\n            ").join(columns)


def _get_sort_alias(sort_by: str) -> str:
    """Resolve accepted sort names to base_data output aliases."""
    sort_columns = {
        "ticker": "ticker",
        "name": "name",
        "sector": "sector",
    }
    for filter_name, spec in FILTER_SPECS.items():
        sort_columns[filter_name] = spec.alias
        sort_columns[spec.alias] = spec.alias
    return sort_columns.get(sort_by, "market_cap")


def _get_required_filter_specs(
    filters: dict[str, list[float | None]],
    sort_by: str,
) -> list[FilterSpec]:
    """Resolve the filter expressions needed by output, predicates, and sorting."""
    required_aliases = {FILTER_SPECS[name].alias for name in DEFAULT_OUTPUT_FILTERS}

    for filter_name in filters:
        spec = FILTER_SPECS.get(filter_name)
        if spec is not None:
            required_aliases.add(spec.alias)

    sort_alias = _get_sort_alias(sort_by)
    if sort_alias not in {"ticker", "name", "sector"}:
        required_aliases.add(sort_alias)

    seen: set[str] = set()
    required_specs: list[FilterSpec] = []
    for spec in FILTER_SPECS.values():
        if spec.alias not in required_aliases or spec.alias in seen:
            continue
        seen.add(spec.alias)
        required_specs.append(spec)

    return required_specs


def _has_expression_reference(filter_specs: list[FilterSpec], token: str) -> bool:
    """Return whether any selected expression references a table alias."""
    return any(token in spec.expression for spec in filter_specs)


def screen_stocks(
    filters: dict[str, list[float | None]],
    sector: str | None = None,
    sector_exclude: str | None = None,
    index: str | None = None,
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
                 - Fundamental: market_cap, pe_ratio, dividend_yield, roe, debt_to_equity
                 - SMA values: sma_5/10/20/50/100/150/200
                 - SMA distance %: sma_20/50/100/150/200_distance_pct
                 - EMA values: ema_12/26/50/100/200
                 - Momentum: rsi_14, rsi_21, macd_line/signal/histogram
                 - Volatility: bb_upper/middle/lower, atr_14

        sector: Optional sector filter (partial match on SIC or GICS)
        sector_exclude: Optional sector to exclude (partial match)
        index: Filter by index membership (sp500, nasdaq5000)
        taxonomy: Sector taxonomy - "sic" or "gics" (default: gics)
        sort_by: Column to sort by (default: market_cap)
        sort_order: Sort direction - "asc" or "desc" (default: desc)
        limit: Maximum results (default: 50, max: 500)

    Returns:
        List of matching stocks with:
        - ticker, name, sector, indices, price, market_cap
        - change_1d, change_1w, change_1m, change_ytd
        - volume, dollar_volume, volume_ratio
        - Key technical indicators based on filters used
    """
    limit = min(limit, 500)

    # Build the base query with CTEs
    params: dict[str, Any] = {"limit": limit}

    # Sector expression based on taxonomy (controlled SQL expressions)
    if taxonomy == "gics":
        sector_expr = "get_gics_sector(c.ticker, c.sic_code, c.sic_description)"
    else:
        sector_expr = "c.sic_description"
    sector_join = ""

    # Sector filter
    sector_filter = ""
    if sector:
        sector_filter = "AND " + sector_expr + " ILIKE %(sector)s"
        params["sector"] = f"%{sector}%"

    # Sector exclude filter
    sector_exclude_filter = ""
    if sector_exclude:
        sector_exclude_filter = "AND " + sector_expr + " NOT ILIKE %(sector_exclude)s"
        params["sector_exclude"] = f"%{sector_exclude}%"

    # Index filter
    index_filter = ""
    if index:
        index_filter = """AND c.ticker IN (
            SELECT ic.ticker FROM index_constituents ic
            JOIN indices i ON ic.index_id = i.id
            WHERE i.code = %(index)s
        )"""
        params["index"] = index.lower()

    # Build filter conditions as Composable objects
    where_conditions: list[sql.Composable] = []

    filter_idx = 0
    for filter_name, bounds in filters.items():
        filter_spec = FILTER_SPECS.get(filter_name)
        if filter_spec is None:
            logger.warning(f"Unknown filter: {filter_name}, skipping")
            continue

        if not isinstance(bounds, list) or len(bounds) != 2:
            logger.warning(f"Invalid bounds for {filter_name}: {bounds}, skipping")
            continue

        min_val, max_val = bounds

        col_ident = sql.Identifier(filter_spec.alias)

        # Build condition using safe sql composition
        if min_val is not None and max_val is not None:
            where_conditions.append(
                sql.SQL("{} BETWEEN {} AND {}").format(
                    col_ident,
                    sql.Placeholder(f"f{filter_idx}_min"),
                    sql.Placeholder(f"f{filter_idx}_max"),
                )
            )
            params[f"f{filter_idx}_min"] = min_val
            params[f"f{filter_idx}_max"] = max_val
        elif min_val is not None:
            where_conditions.append(
                sql.SQL("{} >= {}").format(
                    col_ident, sql.Placeholder(f"f{filter_idx}_min")
                )
            )
            params[f"f{filter_idx}_min"] = min_val
        elif max_val is not None:
            where_conditions.append(
                sql.SQL("{} <= {}").format(
                    col_ident, sql.Placeholder(f"f{filter_idx}_max")
                )
            )
            params[f"f{filter_idx}_max"] = max_val

        filter_idx += 1

    # Build the complete query
    where_clause = (
        sql.SQL(" AND ").join(where_conditions) if where_conditions else sql.SQL("1=1")
    )

    sort_alias = _get_sort_alias(sort_by)
    sort_dir = sql.SQL("DESC") if sort_order == "desc" else sql.SQL("ASC")
    required_specs = _get_required_filter_specs(filters, sort_by)
    filter_selects = _build_filter_selects(required_specs)
    output_selects = _build_output_selects(required_specs)

    technical_join = sql.SQL("")
    if _has_expression_reference(required_specs, "ti."):
        technical_join = sql.SQL(
            "LEFT JOIN technical_indicators ti ON c.ticker = ti.ticker AND ti.date = dr.latest_ta"
        )

    fundamentals_cte = sql.SQL("")
    fundamentals_join = sql.SQL("")
    if _has_expression_reference(required_specs, "fr."):
        fundamentals_cte = sql.SQL("""
        latest_ratios AS (
            SELECT DISTINCT ON (ticker)
                ticker,
                price_to_earnings,
                dividend_yield,
                return_on_equity,
                debt_to_equity
                FROM financial_ratios
            ORDER BY ticker, date DESC
        ),
        """)
        fundamentals_join = sql.SQL("LEFT JOIN latest_ratios fr ON fr.ticker = c.ticker")

    extremes_join = sql.SQL("")
    if _has_expression_reference(required_specs, "e."):
        extremes_join = sql.SQL(
            "LEFT JOIN mv_52week_extremes e ON c.ticker = e.ticker AND e.date = dr.latest_52w"
        )

    query = sql.SQL("""
        WITH date_refs AS (
            SELECT
                MAX(date) as latest,
                (SELECT MAX(date) FROM technical_indicators) as latest_ta,
                (SELECT MAX(date) FROM mv_52week_extremes) as latest_52w,
                MAX(date) FILTER (
                    WHERE date < (SELECT MAX(date) FROM stock_prices_live)
                ) as prev_day,
                MAX(date) FILTER (
                    WHERE date <= CURRENT_DATE - INTERVAL '7 days'
                ) as week_ago,
                MAX(date) FILTER (
                    WHERE date <= CURRENT_DATE - INTERVAL '30 days'
                ) as month_ago,
                MIN(date) FILTER (
                    WHERE date >= DATE_TRUNC('year', CURRENT_DATE)
                ) as ytd_start
                FROM stock_prices_live
        ),
        {fundamentals_cte}
        base_data AS (
            SELECT
                c.ticker,
                c.name,
                {sector_expr} as sector,
                {filter_selects},
                -- Index membership
                ARRAY(
                    SELECT i.code FROM index_constituents ic
                    JOIN indices i ON ic.index_id = i.id
                    WHERE ic.ticker = c.ticker
                    ORDER BY i.code
                ) as indices
            FROM companies c
            {sector_join}
            CROSS JOIN date_refs dr
            JOIN stock_prices_live p ON c.ticker = p.ticker AND p.date = dr.latest
            LEFT JOIN stock_prices_live p_prev
                ON c.ticker = p_prev.ticker AND p_prev.date = dr.prev_day
            LEFT JOIN stock_prices_live p_week
                ON c.ticker = p_week.ticker AND p_week.date = dr.week_ago
            LEFT JOIN stock_prices_live p_month
                ON c.ticker = p_month.ticker AND p_month.date = dr.month_ago
            LEFT JOIN stock_prices_live p_ytd
                ON c.ticker = p_ytd.ticker AND p_ytd.date = dr.ytd_start
            {technical_join}
            {fundamentals_join}
            {extremes_join}
            WHERE c.active = true
            {sector_filter}
            {sector_exclude_filter}
            {index_filter}
        )
        SELECT
            ticker,
            name,
            sector,
            indices,
            {output_selects}
        FROM base_data
        WHERE {where_clause}
        ORDER BY {sort_by} {sort_dir} NULLS LAST
        LIMIT %(limit)s
    """).format(
        sector_expr=sql.SQL(sector_expr),
        sector_join=sql.SQL(sector_join),
        sector_filter=sql.SQL(sector_filter),
        sector_exclude_filter=sql.SQL(sector_exclude_filter),
        index_filter=sql.SQL(index_filter),
        filter_selects=filter_selects,
        output_selects=output_selects,
        technical_join=technical_join,
        fundamentals_cte=fundamentals_cte,
        fundamentals_join=fundamentals_join,
        extremes_join=extremes_join,
        where_clause=where_clause,
        sort_by=sql.Identifier(sort_alias),
        sort_dir=sort_dir,
    )

    return execute_query(query, params)


def _get_filter_expression(filter_name: str) -> str:
    """Map an external filter name to its base_data alias."""
    spec = FILTER_SPECS.get(filter_name)
    return spec.alias if spec else filter_name


def get_ytd_returns(
    tickers: list[str],
    start_date: str | None = None,
) -> list[dict[str, Any]]:
    """
    Get YTD percentage returns for an arbitrary list of tickers.

    Args:
        tickers: List of stock ticker symbols (e.g., ["AAPL", "MSFT", "GOOGL"])
        start_date: Start date YYYY-MM-DD (defaults to Jan 1 of current year)

    Returns:
        List of dicts with ticker, name, sector, start_price, current_price, ytd_pct, market_cap
    """
    if not tickers:
        return []

    # Normalize tickers
    tickers = [t.upper() for t in tickers[:100]]  # Cap at 100

    query = """
        WITH ytd_start AS (
            SELECT MIN(date) as dt
            FROM stock_prices_live
            WHERE date >= COALESCE(%(start_date)s::date, DATE_TRUNC('year', CURRENT_DATE))
        ),
        latest AS (
            SELECT MAX(date) as dt FROM stock_prices_live
        )
        SELECT
            c.ticker,
            c.name,
            get_gics_sector(c.ticker, c.sic_code, c.sic_description) as sector,
            c.market_cap,
            p_start.close as start_price,
            p_now.close as current_price,
            CASE WHEN p_start.close > 0
                 THEN ROUND(((p_now.close - p_start.close) / p_start.close * 100)::numeric, 2)
                 ELSE NULL END as ytd_pct
        FROM companies c
        CROSS JOIN ytd_start ys
        CROSS JOIN latest lt
        JOIN stock_prices_live p_start ON c.ticker = p_start.ticker AND p_start.date = ys.dt
        JOIN stock_prices_live p_now ON c.ticker = p_now.ticker AND p_now.date = lt.dt
        WHERE c.ticker = ANY(%(tickers)s)
        ORDER BY ytd_pct DESC NULLS LAST
    """

    return execute_query(query, {"tickers": tickers, "start_date": start_date})


def detect_crossovers(
    sma_period: int = 150,
    direction: Literal["above", "below"] = "above",
    lookback_days: int = 5,
    min_volume_ratio: float | None = None,
    index: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """
    Detect stocks that recently crossed above or below a moving average.

    Args:
        sma_period: SMA period to check crossover against (50, 150, or 200)
        direction: "above" (bullish) or "below" (bearish) crossover
        lookback_days: How many recent days to check for crossover (default: 5)
        min_volume_ratio: Minimum volume ratio on crossover day for confirmation
        index: Filter by index membership (sp500, nasdaq5000)
        limit: Maximum results (default: 50, max: 200)

    Returns:
        List of stocks with crossover details:
        - ticker, name, sector, price, sma_value, crossover_date
        - volume_ratio, change_since_crossover
    """
    limit = min(limit, 200)

    # Validate sma_period
    valid_periods = {50, 100, 150, 200}
    if sma_period not in valid_periods:
        sma_period = 150

    sma_col = f"sma_{sma_period}"

    # Build index filter
    index_filter = sql.SQL("")
    params: dict[str, Any] = {"lookback_days": lookback_days, "limit": limit}

    if index:
        index_filter = sql.SQL("""AND c.ticker IN (
            SELECT ic.ticker FROM index_constituents ic
            JOIN indices i ON ic.index_id = i.id
            WHERE i.code = %(index)s
        )""")
        params["index"] = index.lower()

    # Volume filter
    volume_filter = sql.SQL("")
    if min_volume_ratio is not None:
        volume_filter = sql.SQL("AND cross.volume_ratio >= %(min_volume_ratio)s")
        params["min_volume_ratio"] = min_volume_ratio

    # Direction condition
    if direction == "above":
        cross_condition = sql.SQL(
            "prev_close < prev_sma AND sp.close >= ti.{sma_col}"
        ).format(sma_col=sql.Identifier(sma_col))
    else:
        cross_condition = sql.SQL(
            "prev_close >= prev_sma AND sp.close < ti.{sma_col}"
        ).format(sma_col=sql.Identifier(sma_col))

    query = sql.SQL("""
        WITH recent_dates AS (
            SELECT DISTINCT date
            FROM stock_prices_live
            ORDER BY date DESC
            LIMIT %(lookback_days)s + 1
        ),
        latest AS (
            SELECT MAX(date) as dt FROM recent_dates
        ),
        crossover_data AS (
            SELECT
                sp.ticker,
                sp.date,
                sp.close,
                ti.{sma_col} as sma_value,
                LAG(sp.close) OVER (PARTITION BY sp.ticker ORDER BY sp.date) as prev_close,
                LAG(ti.{sma_col}) OVER (PARTITION BY sp.ticker ORDER BY sp.date) as prev_sma,
                ti.volume_ratio
            FROM stock_prices_live sp
            JOIN technical_indicators ti ON sp.ticker = ti.ticker AND sp.date = ti.date
            WHERE sp.date IN (SELECT date FROM recent_dates)
              AND ti.{sma_col} IS NOT NULL
              AND ti.{sma_col} > 0
        ),
        crossovers AS (
            SELECT
                ticker,
                date as crossover_date,
                close as crossover_price,
                sma_value,
                volume_ratio,
                prev_close,
                prev_sma,
                ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) as rn
            FROM crossover_data
            WHERE prev_close IS NOT NULL
              AND prev_sma IS NOT NULL
              AND {cross_condition}
        )
        SELECT
            cross.ticker,
            c.name,
            get_gics_sector(c.ticker, c.sic_code, c.sic_description) as sector,
            c.market_cap,
            p_now.close as price,
            cross.sma_value,
            cross.crossover_date,
            cross.crossover_price,
            cross.volume_ratio,
            CASE WHEN cross.crossover_price > 0
                 THEN ROUND(((p_now.close - cross.crossover_price)
                       / cross.crossover_price * 100)::numeric, 2)
                 ELSE NULL END as change_since_crossover
        FROM crossovers cross
        JOIN companies c ON cross.ticker = c.ticker
        CROSS JOIN latest lt
        JOIN stock_prices_live p_now ON cross.ticker = p_now.ticker AND p_now.date = lt.dt
        WHERE cross.rn = 1
          AND c.active = true
          {index_filter}
          {volume_filter}
        ORDER BY cross.crossover_date DESC, c.market_cap DESC
        LIMIT %(limit)s
    """).format(
        sma_col=sql.Identifier(sma_col),
        cross_condition=cross_condition,
        index_filter=index_filter,
        volume_filter=volume_filter,
    )

    return execute_query(query, params)


def get_52week_extremes(
    extreme: Literal["highs", "lows", "both"] = "both",
    threshold_pct: float = 2.0,
    index: Literal["sp500", "nasdaq5000", "all"] = "all",
    min_volume: int | None = None,
    since_date: str | None = None,
    include_fundamentals: bool = False,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """
    Find stocks at or near 52-week highs or lows.

    Args:
        extreme: Which extreme to find - "highs", "lows", or "both"
        threshold_pct: How close to extreme (default 2% = within 2% of high/low)
        index: Filter by index membership (requires sp500/nasdaq5000 columns)
        min_volume: Minimum volume filter
        since_date: Only return stocks that hit new 52w extreme on or after this date
        include_fundamentals: Include PE, dividend yield, ROE, and net margin
        limit: Maximum results (default: 50, max: 200)

    Returns:
        List of stocks with:
        - ticker, name, sector, price, volume
        - high_52w, low_52w, high_52w_pct, low_52w_pct
        - change_1d, extreme_type ("high" or "low")
        - pe_ratio, dividend_yield, roe, net_margin (if include_fundamentals)
    """
    limit = min(limit, 200)

    # Index filter (uses index_constituents junction table)
    index_filter = sql.SQL("")
    params: dict[str, Any] = {}
    if index == "sp500":
        index_filter = sql.SQL("""AND c.ticker IN (
            SELECT ic.ticker FROM index_constituents ic
            JOIN indices i ON ic.index_id = i.id
            WHERE i.code = 'sp500'
        )""")
    elif index == "nasdaq5000":
        index_filter = sql.SQL("""AND c.ticker IN (
            SELECT ic.ticker FROM index_constituents ic
            JOIN indices i ON ic.index_id = i.id
            WHERE i.code = 'nasdaq5000'
        )""")

    # Volume filter (parameterized)
    volume_filter = sql.SQL("")
    if min_volume:
        volume_filter = sql.SQL("AND p.volume >= %(min_volume)s")
        params["min_volume"] = int(min_volume)

    # Since date filter: only stocks that hit new extremes in the window
    since_date_filter = sql.SQL("")
    if since_date:
        since_date_filter = sql.SQL("""AND c.ticker IN (
            SELECT sp2.ticker FROM stock_prices sp2
            JOIN mv_52week_extremes e2 ON sp2.ticker = e2.ticker AND sp2.date = e2.date
            WHERE sp2.date >= %(since_date)s
              AND (sp2.high >= e2.high_52w OR sp2.low <= e2.low_52w)
        )""")
        params["since_date"] = since_date

    # Build extreme filter
    if extreme == "highs":
        extreme_filter = sql.SQL("AND high_52w_pct >= %(neg_threshold)s")
    elif extreme == "lows":
        extreme_filter = sql.SQL("AND low_52w_pct <= %(threshold)s")
    else:  # both
        extreme_filter = sql.SQL("""
            AND (high_52w_pct >= %(neg_threshold)s OR low_52w_pct <= %(threshold)s)
        """)

    # Fundamentals JOIN and columns
    if include_fundamentals:
        fundamentals_cte = sql.SQL("""
        latest_ratios AS (
            SELECT DISTINCT ON (ticker)
                ticker,
                price_to_earnings,
                dividend_yield,
                return_on_equity,
                debt_to_equity
            FROM financial_ratios
            ORDER BY ticker, date DESC
        ),
        """)
        fundamentals_join = sql.SQL("LEFT JOIN latest_ratios fr ON fr.ticker = c.ticker")
        fundamentals_cols = sql.SQL(""",
                fr.price_to_earnings as pe_ratio,
                fr.dividend_yield,
                fr.return_on_equity as roe,
                fr.debt_to_equity""")
        fundamentals_select = sql.SQL(""",
            pe_ratio,
            dividend_yield,
            roe,
            debt_to_equity""")
    else:
        fundamentals_cte = sql.SQL("")
        fundamentals_join = sql.SQL("")
        fundamentals_cols = sql.SQL("")
        fundamentals_select = sql.SQL("")

    query = sql.SQL("""
        WITH date_refs AS (
            SELECT
                (SELECT MAX(date) FROM stock_prices_live) as latest_live,
                (SELECT MAX(date) FROM mv_52week_extremes) as latest_52w
        ),
        prev_date AS (
            SELECT MAX(date) as dt FROM stock_prices_live
            WHERE date < (SELECT latest_live FROM date_refs)
        ),
        {fundamentals_cte}
        stock_data AS (
            SELECT
                c.ticker,
                c.name,
                get_gics_sector(c.ticker, c.sic_code, c.sic_description) as sector,
                c.market_cap,
                p.close as price,
                p.high,
                p.low,
                p.volume,
                p_prev.close as prev_close,
                e.high_52w,
                e.low_52w,
                ROUND(((p.close - e.high_52w) / NULLIF(e.high_52w, 0) * 100)::numeric, 2)
                    as high_52w_pct,
                ROUND(((p.close - e.low_52w) / NULLIF(e.low_52w, 0) * 100)::numeric, 2)
                    as low_52w_pct,
                ROUND(((p.high - p.low) / NULLIF(p.close, 0) * 100)::numeric, 2)
                    as daily_range_pct,
                CASE WHEN p_prev.close > 0
                     THEN ROUND(((p.close - p_prev.close) / p_prev.close * 100)::numeric, 2)
                     ELSE NULL END as change_1d
                {fundamentals_cols}
            FROM companies c
            CROSS JOIN date_refs dr
            CROSS JOIN prev_date pd
            JOIN stock_prices_live p ON c.ticker = p.ticker AND p.date = dr.latest_live
            LEFT JOIN stock_prices_live p_prev
                ON c.ticker = p_prev.ticker AND p_prev.date = pd.dt
            JOIN mv_52week_extremes e ON c.ticker = e.ticker AND e.date = dr.latest_52w
            {fundamentals_join}
            WHERE c.active = true
            {index_filter}
            {volume_filter}
            {since_date_filter}
        )
        SELECT
            ticker,
            name,
            sector,
            market_cap,
            price,
            volume,
            high_52w,
            low_52w,
            high_52w_pct,
            low_52w_pct,
            daily_range_pct,
            change_1d,
            CASE
                WHEN high_52w_pct >= %(neg_threshold)s THEN 'high'
                WHEN low_52w_pct <= %(threshold)s THEN 'low'
                ELSE NULL
            END as extreme_type
            {fundamentals_select}
        FROM stock_data
        WHERE 1=1
        {extreme_filter}
        ORDER BY
            CASE
                WHEN high_52w_pct >= %(neg_threshold)s THEN high_52w_pct
                ELSE NULL
            END DESC NULLS LAST,
            CASE
                WHEN low_52w_pct <= %(threshold)s THEN low_52w_pct
                ELSE NULL
            END ASC NULLS LAST
        LIMIT %(limit)s
    """).format(
        fundamentals_cols=fundamentals_cols,
        fundamentals_cte=fundamentals_cte,
        fundamentals_join=fundamentals_join,
        fundamentals_select=fundamentals_select,
        index_filter=index_filter,
        volume_filter=volume_filter,
        since_date_filter=since_date_filter,
        extreme_filter=extreme_filter,
    )

    params["threshold"] = threshold_pct
    params["neg_threshold"] = -threshold_pct
    params["limit"] = limit

    return execute_query(query, params)


def get_daily_range_leaders(
    min_range_pct: float = 3.0,
    max_range_pct: float | None = None,
    sector: str | None = None,
    index: str | None = None,
    min_price: float | None = None,
    min_volume: int | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """
    Find stocks with high intraday volatility (daily range).

    Daily range = (high - low) / close * 100

    Args:
        min_range_pct: Minimum daily range % (default: 3%)
        max_range_pct: Maximum daily range % (optional)
        sector: Optional sector filter
        index: Filter by index membership (sp500, nasdaq5000)
        min_price: Minimum stock price filter
        min_volume: Minimum volume filter
        limit: Maximum results (default: 50, max: 200)

    Returns:
        List of stocks with:
        - ticker, name, sector, indices, price, volume
        - high, low, daily_range_pct, change_1d
    """
    limit = min(limit, 200)

    # Build filters
    filters = []
    params: dict[str, Any] = {"limit": limit, "min_range": min_range_pct}

    if max_range_pct:
        filters.append("daily_range_pct <= %(max_range)s")
        params["max_range"] = max_range_pct

    if sector:
        filters.append("sector ILIKE %(sector)s")
        params["sector"] = f"%{sector}%"

    if min_price:
        filters.append("price >= %(min_price)s")
        params["min_price"] = min_price

    if min_volume:
        filters.append("volume >= %(min_volume)s")
        params["min_volume"] = min_volume

    where_clause = sql.SQL(" AND ").join(
        sql.SQL(f) for f in filters
    ) if filters else sql.SQL("1=1")

    # Index filter (applied in the CTE)
    index_filter = sql.SQL("")
    if index:
        index_filter = sql.SQL("""AND c.ticker IN (
            SELECT ic.ticker FROM index_constituents ic
            JOIN indices i ON ic.index_id = i.id
            WHERE i.code = %(index)s
        )""")
        params["index"] = index.lower()

    query = sql.SQL("""
        WITH latest_date AS (
            SELECT MAX(date) as dt FROM stock_prices
        ),
        prev_date AS (
            SELECT MAX(date) as dt FROM stock_prices
            WHERE date < (SELECT dt FROM latest_date)
        ),
        stock_data AS (
            SELECT
                c.ticker,
                c.name,
                c.sic_description as sector,
                c.market_cap,
                p.open,
                p.high,
                p.low,
                p.close as price,
                p.volume,
                ROUND(((p.high - p.low) / NULLIF(p.close, 0) * 100)::numeric, 2)
                    as daily_range_pct,
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
            WHERE c.active = true
            {index_filter}
        )
        SELECT
            ticker, name, sector, indices, market_cap, open, high, low, price,
            volume, daily_range_pct, change_1d
        FROM stock_data
        WHERE daily_range_pct >= %(min_range)s
          AND {where_clause}
        ORDER BY daily_range_pct DESC
        LIMIT %(limit)s
    """).format(
        index_filter=index_filter,
        where_clause=where_clause,
    )

    return execute_query(query, params)
