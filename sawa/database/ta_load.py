"""Database operations for technical indicators.

Handles bulk insert/upsert and queries for technical_indicators table.
"""

import logging
from datetime import date
from typing import Any

from psycopg import sql

from sawa.domain.technical_indicators import TechnicalIndicators
from sawa.utils.constants import DEFAULT_BATCH_SIZE

logger = logging.getLogger(__name__)


def load_technical_indicators(
    conn,
    indicators: list[TechnicalIndicators],
    log: logging.Logger | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    """Bulk insert technical indicators with upsert.

    Args:
        conn: Database connection
        indicators: List of TechnicalIndicators to insert
        log: Logger instance
        batch_size: Number of rows per batch

    Returns:
        Number of rows inserted/updated
    """
    log = log or logger

    if not indicators:
        return 0

    columns = TechnicalIndicators.column_names()
    cols_sql = sql.SQL(", ").join(map(sql.Identifier, columns))
    placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(columns))

    # Non-PK columns for UPDATE
    update_cols = [c for c in columns if c not in ("ticker", "date")]
    set_sql = sql.SQL(", ").join(
        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
        for c in update_cols
    )

    query = sql.SQL(
        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (ticker, date) DO UPDATE SET {}"
    ).format(
        sql.Identifier("technical_indicators"),
        cols_sql,
        placeholders,
        set_sql,
    )

    inserted = 0
    errors = 0

    with conn.cursor() as cur:
        for i in range(0, len(indicators), batch_size):
            batch = indicators[i : i + batch_size]
            for ind in batch:
                try:
                    cur.execute(query, ind.to_tuple())
                    inserted += 1
                except Exception as e:
                    conn.rollback()
                    errors += 1
                    if errors <= 3:
                        log.warning(f"Insert failed for {ind.ticker}/{ind.date}: {e}")
                    elif errors == 4:
                        log.warning("(suppressing further errors...)")
            conn.commit()

            if (i + batch_size) % 10000 == 0:
                log.info(f"  Progress: {min(i + batch_size, len(indicators))}/{len(indicators)}")

    if errors > 0:
        log.warning(f"  {errors} rows failed to insert")

    return inserted


def get_last_ta_date(conn, ticker: str) -> date | None:
    """Get most recent technical indicator calculation date for ticker.

    Args:
        conn: Database connection
        ticker: Stock symbol

    Returns:
        Most recent date, or None if no data
    """
    query = "SELECT MAX(date) FROM technical_indicators WHERE ticker = %s"
    with conn.cursor() as cur:
        cur.execute(query, (ticker.upper(),))
        result = cur.fetchone()
        if result and result[0]:
            return result[0]
    return None


def get_tickers_with_prices(conn) -> list[str]:
    """Get all tickers that have price data.

    Args:
        conn: Database connection

    Returns:
        List of ticker symbols
    """
    query = "SELECT DISTINCT ticker FROM stock_prices ORDER BY ticker"
    with conn.cursor() as cur:
        cur.execute(query)
        return [row[0] for row in cur.fetchall()]


def get_tickers_needing_ta(conn) -> list[str]:
    """Get tickers with prices but incomplete technical indicators.

    Returns tickers where price data exists for dates not in technical_indicators.

    Args:
        conn: Database connection

    Returns:
        List of ticker symbols needing TA calculation
    """
    query = """
        SELECT DISTINCT sp.ticker
        FROM stock_prices sp
        LEFT JOIN technical_indicators ti
            ON sp.ticker = ti.ticker AND sp.date = ti.date
        WHERE ti.ticker IS NULL
        ORDER BY sp.ticker
    """
    with conn.cursor() as cur:
        cur.execute(query)
        return [row[0] for row in cur.fetchall()]


def get_prices_for_ticker(
    conn,
    ticker: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[dict[str, Any]]:
    """Fetch OHLCV prices for a ticker.

    Args:
        conn: Database connection
        ticker: Stock symbol
        start_date: Optional start date filter
        end_date: Optional end date filter

    Returns:
        List of price dicts sorted by date ascending
    """
    conditions = ["ticker = %s"]
    params: list[Any] = [ticker.upper()]

    if start_date:
        conditions.append("date >= %s")
        params.append(start_date)
    if end_date:
        conditions.append("date <= %s")
        params.append(end_date)

    where_clause = " AND ".join(conditions)
    query = f"""
        SELECT date, open, high, low, close, volume
        FROM stock_prices
        WHERE {where_clause}
        ORDER BY date ASC
    """

    with conn.cursor() as cur:
        cur.execute(query, params)
        return [
            {
                "date": row[0],
                "open": row[1],
                "high": row[2],
                "low": row[3],
                "close": row[4],
                "volume": row[5],
            }
            for row in cur.fetchall()
        ]


def get_ta_count(conn, ticker: str | None = None) -> int:
    """Get count of technical indicator records.

    Args:
        conn: Database connection
        ticker: Optional ticker filter

    Returns:
        Count of records
    """
    if ticker:
        query = "SELECT COUNT(*) FROM technical_indicators WHERE ticker = %s"
        params: tuple = (ticker.upper(),)
    else:
        query = "SELECT COUNT(*) FROM technical_indicators"
        params = ()

    with conn.cursor() as cur:
        cur.execute(query, params)
        result = cur.fetchone()
        return result[0] if result else 0


def delete_ta_for_ticker(conn, ticker: str) -> int:
    """Delete all technical indicators for a ticker.

    Args:
        conn: Database connection
        ticker: Stock symbol

    Returns:
        Number of rows deleted
    """
    query = "DELETE FROM technical_indicators WHERE ticker = %s"
    with conn.cursor() as cur:
        cur.execute(query, (ticker.upper(),))
        deleted = cur.rowcount
        conn.commit()
    return deleted
