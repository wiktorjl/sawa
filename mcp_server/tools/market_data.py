"""Market data MCP tools (prices, financial ratios, and technical indicators)."""

import logging
from datetime import date, datetime
from typing import Any

from ..database import execute_query

logger = logging.getLogger(__name__)


# --- Async service-based implementations ---


async def get_stock_prices_async(
    ticker: str,
    start_date: str,
    end_date: str | None = None,
    limit: int = 252,
) -> list[dict[str, Any]]:
    """Get stock prices via service layer (async)."""
    from ..services import get_stock_service

    service = get_stock_service()
    return await service.get_prices(ticker, start_date, end_date, limit)


async def get_financial_ratios_async(
    ticker: str,
    start_date: str,
    end_date: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Get financial ratios via service layer (async)."""
    from ..services import get_stock_service

    service = get_stock_service()
    return await service.get_financial_ratios(ticker, start_date, end_date, limit)


async def get_latest_price_async(ticker: str) -> dict[str, Any] | None:
    """Get latest price via service layer (async)."""
    from ..services import get_stock_service

    service = get_stock_service()
    return await service.get_latest_price(ticker)


# --- Sync SQL-based implementations (original) ---


def get_stock_prices(
    ticker: str,
    start_date: str,
    end_date: str | None = None,
    limit: int = 252,
    use_live: bool = True,
) -> list[dict[str, Any]]:
    """
    Get daily OHLCV prices for a ticker.

    Args:
        ticker: Stock ticker symbol (e.g., "AAPL")
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format (defaults to today)
        limit: Maximum rows to return (default: 252, max: 1000)
        use_live: If True, use stock_prices_live view (includes intraday for today).
                  If False, use stock_prices table (historical EOD only).

    Returns:
        List of price records with date, open, high, low, close, volume
    """
    limit = min(limit, 1000)

    if end_date is None:
        end_date = date.today().isoformat()

    table_name = "stock_prices_live" if use_live else "stock_prices"

    sql = f"""
        SELECT
            date,
            open,
            high,
            low,
            close,
            volume
        FROM {table_name}
        WHERE ticker = %(ticker)s
            AND date >= %(start_date)s
            AND date <= %(end_date)s
        ORDER BY date ASC
        LIMIT %(limit)s
    """

    params = {
        "ticker": ticker.upper(),
        "start_date": start_date,
        "end_date": end_date,
        "limit": limit,
    }

    return execute_query(sql, params)


def get_financial_ratios(
    ticker: str,
    start_date: str,
    end_date: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """
    Get time-series financial ratios.

    Args:
        ticker: Stock ticker symbol
        start_date: Start date in YYYY-MM-DD format
        end_date: End date (defaults to today)
        limit: Maximum rows (default: 100, max: 1000)

    Returns:
        List of ratio records including P/E, ROE, debt/equity, etc.
    """
    limit = min(limit, 1000)

    if end_date is None:
        end_date = date.today().isoformat()

    sql = """
        SELECT
            date,
            price,
            price_to_earnings as pe_ratio,
            price_to_book as pb_ratio,
            price_to_sales as ps_ratio,
            price_to_cash_flow as pcf_ratio,
            price_to_free_cash_flow as pfcf_ratio,
            debt_to_equity,
            return_on_equity as roe,
            return_on_assets as roa,
            dividend_yield,
            earnings_per_share as eps,
            market_cap,
            enterprise_value as ev,
            ev_to_ebitda,
            ev_to_sales,
            free_cash_flow as fcf,
            average_volume
        FROM financial_ratios
        WHERE ticker = %(ticker)s
            AND date >= %(start_date)s
            AND date <= %(end_date)s
        ORDER BY date ASC
        LIMIT %(limit)s
    """

    params = {
        "ticker": ticker.upper(),
        "start_date": start_date,
        "end_date": end_date,
        "limit": limit,
    }

    return execute_query(sql, params)


def get_latest_price(ticker: str, use_live: bool = True) -> dict[str, Any] | None:
    """
    Get the most recent stock price for a ticker from database.

    Args:
        ticker: Stock ticker symbol
        use_live: If True, includes today's intraday data if available.
                  If False, returns only historical EOD data.

    Returns:
        Latest price record or None
    """
    table_name = "stock_prices_live" if use_live else "stock_prices"

    sql = f"""
        SELECT
            date,
            open,
            high,
            low,
            close,
            volume
        FROM {table_name}
        WHERE ticker = %(ticker)s
        ORDER BY date DESC
        LIMIT 1
    """

    results = execute_query(sql, {"ticker": ticker.upper()})
    return results[0] if results else None


async def get_live_price_async(ticker: str, days: int = 7) -> dict[str, Any]:
    """Get live stock price from Polygon API (async wrapper for sawa).

    Args:
        ticker: Stock ticker symbol
        days: Number of days of history to fetch (default: 7)

    Returns:
        Dictionary with latest price info and recent history
    """
    from sawa import get_live_price as sawa_get_live_price

    try:
        result = await sawa_get_live_price(ticker=ticker, days=days)

        if result.get("error"):
            raise ValueError(result["error"])

        # Format for MCP display (adapt sawa output to match existing format)
        history = result.get("history", [])
        latest = history[-1] if history else {}

        return {
            "ticker": result["ticker"],
            "latest_price": result["current_price"],
            "latest_date": result["current_date"],
            "open": latest.get("o"),
            "high": latest.get("h"),
            "low": latest.get("l"),
            "close": latest.get("c", result["current_price"]),
            "volume": latest.get("v"),
            "change_percent": result["change_percent"],
            "history": [
                {
                    "date": datetime.fromtimestamp(bar["t"] / 1000).strftime("%Y-%m-%d"),
                    "open": bar["o"],
                    "high": bar["h"],
                    "low": bar["l"],
                    "close": bar["c"],
                    "volume": bar["v"],
                }
                for bar in history
            ],
            "source": "polygon_api",
            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    except Exception as e:
        logger.error(f"Live price error: {e}")
        raise


async def get_live_prices_batch_async(
    tickers: list[str],
    days: int = 7,
) -> dict[str, dict[str, Any]]:
    """Get live stock prices for multiple tickers from Polygon API.

    Args:
        tickers: List of stock ticker symbols (e.g., ["AAPL", "MSFT", "GOOGL"])
        days: Number of days of history to fetch per ticker (default: 7)

    Returns:
        Dictionary mapping ticker -> price info with history
    """
    from sawa import get_live_prices_batch as sawa_get_live_prices_batch

    try:
        results = await sawa_get_live_prices_batch(tickers=tickers, days=days)
        fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Format each result for MCP display
        output: dict[str, dict[str, Any]] = {}
        for ticker, result in results.items():
            if result.get("error"):
                output[ticker] = {
                    "ticker": ticker,
                    "error": result["error"],
                    "source": "polygon_api",
                    "fetched_at": fetched_at,
                }
                continue

            history = result.get("history", [])
            latest = history[-1] if history else {}

            output[ticker] = {
                "ticker": result["ticker"],
                "latest_price": result["current_price"],
                "latest_date": result["current_date"],
                "open": latest.get("o"),
                "high": latest.get("h"),
                "low": latest.get("l"),
                "close": latest.get("c", result["current_price"]),
                "volume": latest.get("v"),
                "change_percent": result["change_percent"],
                "history": [
                    {
                        "date": datetime.fromtimestamp(bar["t"] / 1000).strftime("%Y-%m-%d"),
                        "open": bar["o"],
                        "high": bar["h"],
                        "low": bar["l"],
                        "close": bar["c"],
                        "volume": bar["v"],
                    }
                    for bar in history
                ],
                "source": "polygon_api",
                "fetched_at": fetched_at,
            }

        return output
    except Exception as e:
        logger.error(f"Live prices batch error: {e}")
        raise


# --- Technical Indicators ---


def get_technical_indicators(
    ticker: str,
    start_date: str,
    end_date: str | None = None,
    limit: int = 252,
) -> list[dict[str, Any]]:
    """
    Get technical indicators for a ticker.

    Args:
        ticker: Stock ticker symbol (e.g., "AAPL")
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format (defaults to today)
        limit: Maximum rows to return (default: 252, max: 1000)

    Returns:
        List of technical indicator records with all 20 indicators
    """
    limit = min(limit, 1000)

    if end_date is None:
        end_date = date.today().isoformat()

    sql = """
        SELECT
            date,
            -- Trend
            sma_5, sma_10, sma_20, sma_50,
            ema_12, ema_26, ema_50, vwap,
            -- Momentum
            rsi_14, rsi_21,
            macd_line, macd_signal, macd_histogram,
            -- Volatility
            bb_upper, bb_middle, bb_lower, atr_14,
            -- Volume
            obv, volume_sma_20, volume_ratio
        FROM technical_indicators
        WHERE ticker = %(ticker)s
            AND date >= %(start_date)s
            AND date <= %(end_date)s
        ORDER BY date ASC
        LIMIT %(limit)s
    """

    params = {
        "ticker": ticker.upper(),
        "start_date": start_date,
        "end_date": end_date,
        "limit": limit,
    }

    return execute_query(sql, params)


def get_latest_technical_indicators(ticker: str) -> dict[str, Any] | None:
    """
    Get the most recent technical indicators for a ticker.

    Args:
        ticker: Stock ticker symbol

    Returns:
        Latest technical indicators record or None
    """
    sql = """
        SELECT
            date,
            -- Trend
            sma_5, sma_10, sma_20, sma_50,
            ema_12, ema_26, ema_50, vwap,
            -- Momentum
            rsi_14, rsi_21,
            macd_line, macd_signal, macd_histogram,
            -- Volatility
            bb_upper, bb_middle, bb_lower, atr_14,
            -- Volume
            obv, volume_sma_20, volume_ratio
        FROM technical_indicators
        WHERE ticker = %(ticker)s
        ORDER BY date DESC
        LIMIT 1
    """

    results = execute_query(sql, {"ticker": ticker.upper()})
    return results[0] if results else None


def screen_by_technical_indicators(
    filters: dict[str, tuple[float | None, float | None]],
    target_date: str | None = None,
    index: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """
    Screen stocks by technical indicator values.

    Args:
        filters: Dict mapping indicator name to (min, max) tuple.
                 Use None for unbounded side.
                 Example: {"rsi_14": (None, 30), "volume_ratio": (1.5, None)}
        target_date: Date to screen (defaults to most recent)
        index: Filter by index membership (sp500, nasdaq100)
        limit: Maximum results (default: 100, max: 500)

    Returns:
        List of tickers matching all filters with their indicator values and indices
    """
    limit = min(limit, 500)

    # Valid indicator columns
    valid_indicators = {
        "sma_5",
        "sma_10",
        "sma_20",
        "sma_50",
        "sma_100",
        "sma_150",
        "sma_200",
        "ema_12",
        "ema_26",
        "ema_50",
        "ema_100",
        "ema_200",
        "vwap",
        "rsi_14",
        "rsi_21",
        "macd_line",
        "macd_signal",
        "macd_histogram",
        "bb_upper",
        "bb_middle",
        "bb_lower",
        "atr_14",
        "obv",
        "volume_sma_20",
        "volume_ratio",
    }

    # Build WHERE conditions
    conditions = []
    params: dict[str, Any] = {"limit": limit}

    if target_date:
        conditions.append("ti.date = %(target_date)s")
        params["target_date"] = target_date
    else:
        # Use most recent date
        conditions.append("ti.date = (SELECT MAX(date) FROM technical_indicators)")

    # Index filter
    if index:
        conditions.append("""ti.ticker IN (
            SELECT ic.ticker FROM index_constituents ic
            JOIN indices i ON ic.index_id = i.id
            WHERE i.code = %(index)s
        )""")
        params["index"] = index.lower()

    # Add filter conditions
    for i, (indicator, (min_val, max_val)) in enumerate(filters.items()):
        if indicator not in valid_indicators:
            continue

        if min_val is not None and max_val is not None:
            conditions.append(f"ti.{indicator} BETWEEN %(min_{i})s AND %(max_{i})s")
            params[f"min_{i}"] = min_val
            params[f"max_{i}"] = max_val
        elif min_val is not None:
            conditions.append(f"ti.{indicator} >= %(min_{i})s")
            params[f"min_{i}"] = min_val
        elif max_val is not None:
            conditions.append(f"ti.{indicator} <= %(max_{i})s")
            params[f"max_{i}"] = max_val

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    sql = f"""
        SELECT
            ti.ticker,
            ti.date,
            ti.rsi_14, ti.rsi_21,
            ti.macd_line, ti.macd_histogram,
            ti.sma_20, ti.sma_50,
            ti.bb_upper, ti.bb_lower,
            ti.atr_14,
            ti.volume_ratio,
            ARRAY(
                SELECT i.code FROM index_constituents ic
                JOIN indices i ON ic.index_id = i.id
                WHERE ic.ticker = ti.ticker
                ORDER BY i.code
            ) as indices
        FROM technical_indicators ti
        WHERE {where_clause}
        ORDER BY ti.ticker
        LIMIT %(limit)s
    """

    return execute_query(sql, params)


def list_technical_indicators(
    category: str | None = None,
) -> list[dict[str, Any]]:
    """
    List available technical indicators with descriptions.

    Args:
        category: Optional filter by category:
            - "trend": Moving averages (SMA, EMA, VWAP)
            - "momentum": RSI, MACD
            - "volatility": Bollinger Bands, ATR
            - "volume": OBV, volume ratios

    Returns:
        List of indicator info dicts with:
        - name: Indicator name (e.g., "sma_50", "rsi_14")
        - display_name: Human-readable name
        - category: Category (trend/momentum/volatility/volume)
        - description: What the indicator measures
        - min_periods: Days of data required to calculate
        - is_bounded: Whether indicator has fixed range (e.g., RSI 0-100)
        - bounds: [min, max] if bounded, null otherwise
        - unit: Value unit (dollars/percent/ratio/count)
    """
    if category:
        sql = """
            SELECT
                indicator_name as name,
                display_name,
                category,
                description,
                min_periods_required as min_periods,
                is_bounded,
                CASE WHEN is_bounded
                     THEN jsonb_build_array(validation_min, validation_max)
                     ELSE NULL END as bounds,
                unit
            FROM technical_indicator_metadata
            WHERE category = %(category)s
            ORDER BY sort_order
        """
        return execute_query(sql, {"category": category})
    else:
        sql = """
            SELECT
                indicator_name as name,
                display_name,
                category,
                description,
                min_periods_required as min_periods,
                is_bounded,
                CASE WHEN is_bounded
                     THEN jsonb_build_array(validation_min, validation_max)
                     ELSE NULL END as bounds,
                unit
            FROM technical_indicator_metadata
            ORDER BY sort_order
        """
        return execute_query(sql)


def get_intraday_bars(
    ticker: str,
    date: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """
    Get intraday 5-minute bars for a ticker.

    Args:
        ticker: Stock ticker symbol (e.g., AAPL)
        date: Date in YYYY-MM-DD format (defaults to today)
        limit: Maximum bars to return (default: 100, max: 500)

    Returns:
        List of intraday bars with timestamp, OHLCV
    """
    from datetime import datetime

    if date is None:
        date = datetime.now().date().isoformat()

    limit = min(limit, 500)

    sql = """
        SELECT
            timestamp,
            open,
            high,
            low,
            close,
            volume
        FROM stock_prices_intraday
        WHERE ticker = %(ticker)s
          AND timestamp::date = %(date)s
        ORDER BY timestamp ASC
        LIMIT %(limit)s
    """

    params = {
        "ticker": ticker.upper(),
        "date": date,
        "limit": limit,
    }

    return execute_query(sql, params)
