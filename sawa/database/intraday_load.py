"""Database operations for intraday price data."""

import logging
from typing import Any

import psycopg
from psycopg import sql


def load_intraday_bars(
    conn: psycopg.Connection,
    bars: list[dict[str, Any]],
    logger: logging.Logger,
) -> int:
    """
    Batch insert intraday bars with upsert.

    Args:
        conn: psycopg connection
        bars: List of dicts with keys: ticker, timestamp, open, high, low, close, volume
        logger: Logger instance

    Returns:
        Number of bars inserted/updated
    """
    if not bars:
        return 0

    query = sql.SQL("""
        INSERT INTO stock_prices_intraday 
            (ticker, timestamp, open, high, low, close, volume, bar_size_minutes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, timestamp) 
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume
    """)

    inserted = 0
    with conn.cursor() as cur:
        for bar in bars:
            cur.execute(
                query,
                (
                    bar["ticker"],
                    bar["timestamp"],
                    bar["open"],
                    bar["high"],
                    bar["low"],
                    bar["close"],
                    bar["volume"],
                    bar.get("bar_size_minutes", 5),
                ),
            )
            inserted += 1
        conn.commit()

    logger.debug(f"Inserted {inserted} intraday bars")
    return inserted


def cleanup_old_intraday_data(
    conn: psycopg.Connection,
    days: int,
    logger: logging.Logger,
) -> int:
    """
    Delete intraday data older than specified days.

    Args:
        conn: psycopg connection
        days: Delete data older than this many days
        logger: Logger instance

    Returns:
        Number of records deleted
    """
    query = sql.SQL("""
        DELETE FROM stock_prices_intraday 
        WHERE timestamp < CURRENT_TIMESTAMP - INTERVAL '%s days'
    """)

    with conn.cursor() as cur:
        cur.execute(query, (days,))
        deleted = cur.rowcount
        conn.commit()

    logger.info(f"Cleaned up {deleted} old intraday records (>{days} days)")
    return deleted


def cleanup_today_intraday_data(
    conn: psycopg.Connection,
    logger: logging.Logger,
) -> int:
    """
    Delete today's intraday data (called after EOD arrives).

    Args:
        conn: psycopg connection
        logger: Logger instance

    Returns:
        Number of records deleted
    """
    query = sql.SQL("""
        DELETE FROM stock_prices_intraday 
        WHERE timestamp::date = CURRENT_DATE
    """)

    with conn.cursor() as cur:
        cur.execute(query)
        deleted = cur.rowcount
        conn.commit()

    logger.info(f"Cleaned up {deleted} intraday records for today (EOD arrived)")
    return deleted
