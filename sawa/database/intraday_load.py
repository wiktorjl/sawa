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

    # Each window is normally flushed once, fully aggregated, by the websocket
    # client's per-ticker event-time watermark. A second write for the same
    # (ticker, timestamp) can still occur if Polygon replays a window after a
    # reconnect (the aggregator persists across reconnects, so a popped window
    # gets re-created from the replayed minutes). On conflict we therefore MERGE
    # rather than blind-overwrite, so a duplicate same-window bar cannot corrupt
    # the OHLCV: keep the earliest open already recorded, take the true high/low
    # extremes across both writes, advance the close to the latest write, and
    # keep the larger volume (a full replay carries the full volume; a partial
    # straggler carries less and must not reduce the count). An identical
    # re-stream is an exact no-op. We deliberately do NOT SUM volume: a replay's
    # minutes overlap the original window, so summing would double-count.
    query = sql.SQL("""
        INSERT INTO stock_prices_intraday
            (ticker, timestamp, open, high, low, close, volume, bar_size_minutes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, timestamp)
        DO UPDATE SET
            open = stock_prices_intraday.open,
            high = GREATEST(stock_prices_intraday.high, EXCLUDED.high),
            low = LEAST(stock_prices_intraday.low, EXCLUDED.low),
            close = EXCLUDED.close,
            volume = GREATEST(stock_prices_intraday.volume, EXCLUDED.volume)
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
        WHERE timestamp < CURRENT_TIMESTAMP - (%s * INTERVAL '1 day')
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
        WHERE (timestamp AT TIME ZONE 'America/New_York')::date =
            (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')::date
    """)

    with conn.cursor() as cur:
        cur.execute(query)
        deleted = cur.rowcount
        conn.commit()

    logger.info(f"Cleaned up {deleted} intraday records for today (EOD arrived)")
    return deleted
