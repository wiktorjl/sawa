"""Database operations for intraday price data."""

import logging
import math
import re
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any

import psycopg
from psycopg import sql

TICKER_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,9}$")
MAX_PRICE_EXCLUSIVE = Decimal("1000000000000")
PRICE_SCALE = Decimal("0.00000001")
MAX_VOLUME = 9_223_372_036_854_775_807


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

    # Polygon rebroadcasts a full recalculated minute after late trades. The
    # websocket client retains per-minute state, recomputes the N-minute window,
    # and supplies a minute bitmap. A same/superset-lineage revision is
    # authoritative even when high or volume decreases. A partial/incomparable
    # replay after process restart must not overwrite the stored snapshot.
    allowed_bar_sizes = {1, 5, 15, 30, 60}
    for bar in bars:
        ticker = bar.get("ticker")
        if not isinstance(ticker, str) or not TICKER_PATTERN.fullmatch(ticker):
            raise ValueError(f"invalid ticker: {ticker!r}")
        timestamp = bar.get("timestamp")
        if (
            not isinstance(timestamp, datetime)
            or timestamp.tzinfo is None
            or timestamp.utcoffset() is None
        ):
            raise ValueError("timestamp must be a timezone-aware datetime")

        prices: dict[str, Decimal] = {}
        for field in ("open", "high", "low", "close"):
            value = bar.get(field)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float, Decimal))
                or (isinstance(value, float) and not math.isfinite(value))
                or (isinstance(value, Decimal) and not value.is_finite())
            ):
                raise ValueError(f"{field} must be a finite number")
            decimal_value = Decimal(str(value))
            if not 0 < decimal_value < MAX_PRICE_EXCLUSIVE:
                raise ValueError(f"{field} is outside NUMERIC(20,8) bounds")
            try:
                rounded_value = decimal_value.quantize(
                    PRICE_SCALE, rounding=ROUND_HALF_UP
                )
            except InvalidOperation as exc:
                raise ValueError(
                    f"{field} is outside NUMERIC(20,8) bounds"
                ) from exc
            if not 0 < rounded_value < MAX_PRICE_EXCLUSIVE:
                raise ValueError(f"{field} is outside NUMERIC(20,8) bounds")
            prices[field] = rounded_value
        if prices["high"] < max(prices["open"], prices["low"], prices["close"]):
            raise ValueError("high is inconsistent with OHLC values")
        if prices["low"] > min(prices["open"], prices["high"], prices["close"]):
            raise ValueError("low is inconsistent with OHLC values")

        volume = bar.get("volume")
        if (
            isinstance(volume, bool)
            or not isinstance(volume, int)
            or not 0 <= volume <= MAX_VOLUME
        ):
            raise ValueError("volume must fit a non-negative PostgreSQL BIGINT")

        bar_size = bar.get("bar_size_minutes", 5)
        if isinstance(bar_size, bool) or bar_size not in allowed_bar_sizes:
            raise ValueError(
                f"bar_size_minutes must be one of {sorted(allowed_bar_sizes)}, "
                f"got {bar_size!r}"
            )
        source_minute_count = bar.get("source_minute_count", bar_size)
        if (
            isinstance(source_minute_count, bool)
            or not isinstance(source_minute_count, int)
            or not 1 <= source_minute_count <= bar_size
        ):
            raise ValueError(
                "source_minute_count must be an integer between 1 and "
                f"bar_size_minutes, got {source_minute_count!r}"
            )
        source_minute_mask = bar.get("source_minute_mask", (1 << bar_size) - 1)
        if (
            isinstance(source_minute_mask, bool)
            or not isinstance(source_minute_mask, int)
            or source_minute_mask <= 0
            or source_minute_mask >= 1 << bar_size
            or source_minute_mask.bit_count() != source_minute_count
        ):
            raise ValueError(
                "source_minute_mask must identify exactly source_minute_count "
                "minutes within the bar interval"
            )

    query = sql.SQL("""
        INSERT INTO public.stock_prices_intraday AS existing
            (ticker, timestamp, open, high, low, close, volume,
             bar_size_minutes, source_minute_count, source_minute_mask)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, timestamp, bar_size_minutes)
        DO UPDATE SET
            open = CASE
                WHEN (EXCLUDED.source_minute_mask |
                      existing.source_minute_mask) =
                     EXCLUDED.source_minute_mask
                THEN EXCLUDED.open ELSE existing.open
            END,
            high = CASE
                WHEN (EXCLUDED.source_minute_mask |
                      existing.source_minute_mask) =
                     EXCLUDED.source_minute_mask
                THEN EXCLUDED.high ELSE existing.high
            END,
            low = CASE
                WHEN (EXCLUDED.source_minute_mask |
                      existing.source_minute_mask) =
                     EXCLUDED.source_minute_mask
                THEN EXCLUDED.low ELSE existing.low
            END,
            close = CASE
                WHEN (EXCLUDED.source_minute_mask |
                      existing.source_minute_mask) =
                     EXCLUDED.source_minute_mask
                THEN EXCLUDED.close ELSE existing.close
            END,
            volume = CASE
                WHEN (EXCLUDED.source_minute_mask |
                      existing.source_minute_mask) =
                     EXCLUDED.source_minute_mask
                THEN EXCLUDED.volume ELSE existing.volume
            END,
            source_minute_count = CASE
                WHEN (EXCLUDED.source_minute_mask |
                      existing.source_minute_mask) =
                     EXCLUDED.source_minute_mask
                THEN EXCLUDED.source_minute_count
                ELSE existing.source_minute_count
            END,
            source_minute_mask = CASE
                WHEN (EXCLUDED.source_minute_mask |
                      existing.source_minute_mask) =
                     EXCLUDED.source_minute_mask
                THEN EXCLUDED.source_minute_mask
                ELSE existing.source_minute_mask
            END
    """)

    inserted = 0
    with conn.cursor() as cur:
        for bar in bars:
            bar_size = bar.get("bar_size_minutes", 5)
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
                    bar_size,
                    bar.get("source_minute_count", bar_size),
                    bar.get("source_minute_mask", (1 << bar_size) - 1),
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
    *,
    commit: bool = True,
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
        DELETE FROM public.stock_prices_intraday
        WHERE timestamp < CURRENT_TIMESTAMP - (%s * INTERVAL '1 day')
    """)

    with conn.cursor() as cur:
        cur.execute(query, (days,))
        deleted = cur.rowcount
        if commit:
            conn.commit()

    logger.info(f"Cleaned up {deleted} old intraday records (>{days} days)")
    return deleted


def cleanup_today_intraday_data(
    conn: psycopg.Connection,
    price_date: date,
    logger: logging.Logger,
    *,
    commit: bool = True,
) -> int:
    """Delete intraday fallbacks only for symbols whose EOD row arrived.

    Args:
        conn: psycopg connection
        price_date: Market date whose committed EOD rows replace intraday data
        logger: Logger instance

    Returns:
        Number of records deleted
    """
    query = sql.SQL("""
        DELETE FROM public.stock_prices_intraday AS spi
        WHERE (spi.timestamp AT TIME ZONE 'America/New_York')::date = %s
          AND EXISTS (
              SELECT 1
              FROM public.stock_prices AS sp
              WHERE sp.ticker = spi.ticker
                AND sp.date = %s
          )
    """)

    with conn.cursor() as cur:
        cur.execute(query, (price_date, price_date))
        deleted = cur.rowcount
        if commit:
            conn.commit()

    logger.info(
        "Cleaned up %d intraday records for %s where EOD arrived",
        deleted,
        price_date,
    )
    return deleted
