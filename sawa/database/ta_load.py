"""Database operations for technical indicators.

Handles bulk insert/upsert and queries for technical_indicators table.
"""

import logging
from datetime import date
from decimal import Decimal
from typing import Any, cast

import psycopg
from psycopg import sql

from sawa.domain.technical_indicators import CumulativeIndicatorSeed, TechnicalIndicators
from sawa.utils.constants import DEFAULT_BATCH_SIZE
from sawa.utils.security import redact_sensitive_text

logger = logging.getLogger(__name__)


class TechnicalIndicatorWriteError(RuntimeError):
    """Raised when a TA batch cannot be persisted in full."""


def load_technical_indicators(
    conn,
    indicators: list[TechnicalIndicators],
    log: logging.Logger | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    *,
    commit: bool = True,
) -> int:
    """Atomically insert or update a batch of technical indicators.

    Args:
        conn: Database connection
        indicators: List of TechnicalIndicators to insert
        log: Logger instance
        batch_size: Number of rows per batch
        commit: Commit the completed batch. Set this to ``False`` only when
            the caller owns the surrounding transaction (for example, an
            atomic delete-and-replace recompute).

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

    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    inserted = 0
    current: TechnicalIndicators | None = None
    try:
        with conn.cursor() as cur:
            for i in range(0, len(indicators), batch_size):
                batch = indicators[i : i + batch_size]
                for current in batch:
                    cur.execute(query, current.to_tuple())
                    inserted += 1

                if i + batch_size >= 10000 and (i + batch_size) % 10000 == 0:
                    log.info(
                        f"  Progress: {min(i + batch_size, len(indicators))}/"
                        f"{len(indicators)}"
                    )
    except psycopg.Error as exc:
        # A partially written indicator series is not useful: future daily
        # runs can advance past the missing date and the caller used to report
        # the ticker as successful. Roll back the whole caller transaction in
        # the ordinary path. Atomic replace callers leave rollback to their
        # surrounding connection context so the preceding DELETE is undone too.
        if commit:
            conn.rollback()
        target = (
            f"{current.ticker}/{current.date}" if current is not None else "unknown row"
        )
        # Carry the driver's reason into the message: callers only log the
        # exception text, so without it an overflow and a constraint violation
        # are indistinguishable in the daily log.
        raise TechnicalIndicatorWriteError(
            f"technical indicator write failed for {target}: "
            f"{type(exc).__name__}: {redact_sensitive_text(exc)}"
        ) from exc

    if commit:
        conn.commit()

    return inserted


def delete_technical_indicators_for_tickers(
    conn,
    tickers: list[str],
    log: logging.Logger | None = None,
    *,
    commit: bool = True,
) -> int:
    """Delete all technical_indicators rows for the given tickers.

    Used before a full-history TA recompute (e.g. after split adjustment
    rewrites historical OHLC) so stale indicator rows computed from the
    pre-adjustment prices cannot survive the (ticker, date) upsert — which only
    overwrites dates that the recompute re-emits.

    Returns the number of rows deleted.
    """
    log = log or logger
    if not tickers:
        return 0

    upper = [t.upper() for t in tickers]
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM technical_indicators WHERE ticker = ANY(%s)",
            (upper,),
        )
        deleted = int(cur.rowcount)
    if commit:
        conn.commit()
    action = "Deleted" if commit else "Staged deletion of"
    log.info(
        f"  {action} {deleted} stale technical_indicator rows for "
        f"{len(upper)} ticker(s)"
    )
    return deleted


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
            return cast(date, result[0])
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


def get_cumulative_indicator_seed(
    conn,
    ticker: str,
    before_date: date,
) -> CumulativeIndicatorSeed:
    """Return cumulative VWAP/OBV state strictly before ``before_date``.

    State is derived from authoritative price history rather than previously
    stored indicators, so future incremental rows self-heal even if an older
    release wrote window-relative OBV or VWAP values.
    """
    query = """
        WITH history AS (
            SELECT
                date,
                high,
                low,
                close,
                volume,
                LAG(close) OVER (ORDER BY date) AS previous_close
            FROM stock_prices
            WHERE ticker = %s
              AND date < %s
        )
        SELECT
            COALESCE(SUM(((high + low + close) / 3.0) * volume), 0),
            COALESCE(SUM(volume), 0),
            COALESCE(SUM(
                CASE
                    WHEN previous_close IS NULL THEN volume
                    WHEN close > previous_close THEN volume
                    WHEN close < previous_close THEN -volume
                    ELSE 0
                END
            ), 0),
            (ARRAY_AGG(close ORDER BY date DESC))[1]
        FROM history
    """
    with conn.cursor() as cur:
        cur.execute(query, (ticker.upper(), before_date))
        row = cur.fetchone()

    if not row:
        return CumulativeIndicatorSeed()

    return CumulativeIndicatorSeed(
        vwap_numerator=Decimal(row[0]),
        cumulative_volume=int(row[1]),
        obv=int(row[2]),
        previous_close=Decimal(row[3]) if row[3] is not None else None,
    )


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
