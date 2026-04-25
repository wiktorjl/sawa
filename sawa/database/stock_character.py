"""Database operations for the stock character classification system.

Handles insert/upsert and query operations for the stock_character_classification,
stock_character_baseline, stock_character_flags, and stock_character_scorecard tables.
"""

import logging
from datetime import date
from typing import Any

from psycopg import sql

from sawa.domain.stock_character import (
    CharacterBaseline,
    CharacterClassification,
    CharacterFlag,
    CharacterScorecard,
)
from sawa.utils.constants import DEFAULT_BATCH_SIZE

logger = logging.getLogger(__name__)


def load_classification(conn, classification: CharacterClassification, log=None) -> int:
    """Insert/upsert a single CharacterClassification into stock_character_classification.

    Args:
        conn: psycopg connection
        classification: CharacterClassification dataclass instance
        log: optional logger

    Returns:
        1 if inserted, 0 if failed
    """
    log = log or logger

    columns = CharacterClassification.column_names()
    cols_sql = sql.SQL(", ").join(map(sql.Identifier, columns))
    placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(columns))

    update_cols = [c for c in columns if c not in ("ticker", "run_date")]
    set_sql = sql.SQL(", ").join(
        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
        for c in update_cols
    )

    query = sql.SQL(
        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (ticker, run_date) DO UPDATE SET {}"
    ).format(
        sql.Identifier("stock_character_classification"),
        cols_sql,
        placeholders,
        set_sql,
    )

    try:
        with conn.cursor() as cur:
            cur.execute(query, classification.to_tuple())
        conn.commit()
        return 1
    except Exception as e:
        conn.rollback()
        log.warning(
            f"Insert failed for classification "
            f"{classification.ticker}/{classification.run_date}: {e}"
        )
        return 0


def load_baseline(conn, baseline: CharacterBaseline, log=None) -> int:
    """Insert/upsert a single CharacterBaseline into stock_character_baseline.

    hvn_levels and lvn_levels are PostgreSQL arrays. The to_tuple() method
    converts them to lists that psycopg handles natively.

    Args:
        conn: psycopg connection
        baseline: CharacterBaseline dataclass instance
        log: optional logger

    Returns:
        1 if inserted, 0 if failed
    """
    log = log or logger

    columns = CharacterBaseline.column_names()
    cols_sql = sql.SQL(", ").join(map(sql.Identifier, columns))
    placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(columns))

    update_cols = [c for c in columns if c not in ("ticker", "run_date")]
    set_sql = sql.SQL(", ").join(
        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
        for c in update_cols
    )

    query = sql.SQL(
        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (ticker, run_date) DO UPDATE SET {}"
    ).format(
        sql.Identifier("stock_character_baseline"),
        cols_sql,
        placeholders,
        set_sql,
    )

    try:
        with conn.cursor() as cur:
            cur.execute(query, baseline.to_tuple())
        conn.commit()
        return 1
    except Exception as e:
        conn.rollback()
        log.warning(
            f"Insert failed for baseline {baseline.ticker}/{baseline.run_date}: {e}"
        )
        return 0


def load_flags(
    conn,
    flags: list[CharacterFlag],
    log=None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    """Bulk insert CharacterFlag records into stock_character_flags.

    Uses batch processing with configurable batch_size.

    Args:
        conn: psycopg connection
        flags: list of CharacterFlag dataclass instances
        log: optional logger
        batch_size: number of rows per batch

    Returns:
        Number of rows inserted/updated
    """
    log = log or logger

    if not flags:
        return 0

    columns = CharacterFlag.column_names()
    cols_sql = sql.SQL(", ").join(map(sql.Identifier, columns))
    placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(columns))

    update_cols = [c for c in columns if c not in ("ticker", "run_date", "flag")]
    set_sql = sql.SQL(", ").join(
        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
        for c in update_cols
    )

    query = sql.SQL(
        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (ticker, run_date, flag) DO UPDATE SET {}"
    ).format(
        sql.Identifier("stock_character_flags"),
        cols_sql,
        placeholders,
        set_sql,
    )

    inserted = 0
    errors = 0

    with conn.cursor() as cur:
        for i in range(0, len(flags), batch_size):
            batch = flags[i : i + batch_size]
            for flag in batch:
                try:
                    cur.execute(query, flag.to_tuple())
                    inserted += 1
                except Exception as e:
                    conn.rollback()
                    errors += 1
                    if errors <= 3:
                        log.warning(
                            f"Insert failed for flag {flag.ticker}/{flag.run_date}/{flag.flag}: {e}"
                        )
                    elif errors == 4:
                        log.warning("(suppressing further errors...)")
            conn.commit()

            if (i + batch_size) % 10000 == 0:
                log.info(
                    f"  Progress: {min(i + batch_size, len(flags))}/{len(flags)}"
                )

    if errors > 0:
        log.warning(f"  {errors} rows failed to insert")

    return inserted


def load_scorecard(conn, scorecard: CharacterScorecard, log=None) -> int:
    """Insert/upsert a single CharacterScorecard into stock_character_scorecard.

    Args:
        conn: psycopg connection
        scorecard: CharacterScorecard dataclass instance
        log: optional logger

    Returns:
        1 if inserted, 0 if failed
    """
    log = log or logger

    columns = CharacterScorecard.column_names()
    cols_sql = sql.SQL(", ").join(map(sql.Identifier, columns))
    placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(columns))

    update_cols = [c for c in columns if c not in ("ticker", "run_date")]
    set_sql = sql.SQL(", ").join(
        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
        for c in update_cols
    )

    query = sql.SQL(
        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (ticker, run_date) DO UPDATE SET {}"
    ).format(
        sql.Identifier("stock_character_scorecard"),
        cols_sql,
        placeholders,
        set_sql,
    )

    try:
        with conn.cursor() as cur:
            cur.execute(query, scorecard.to_tuple())
        conn.commit()
        return 1
    except Exception as e:
        conn.rollback()
        log.warning(
            f"Insert failed for scorecard {scorecard.ticker}/{scorecard.run_date}: {e}"
        )
        return 0


def get_latest_classification(conn, ticker: str) -> CharacterClassification | None:
    """Get the most recent classification for a ticker.

    Args:
        conn: psycopg connection
        ticker: stock symbol

    Returns:
        CharacterClassification or None if no data
    """
    columns = CharacterClassification.column_names()
    cols_sql = ", ".join(columns)

    query = f"""
        SELECT {cols_sql}
        FROM stock_character_classification
        WHERE ticker = %s
        ORDER BY run_date DESC
        LIMIT 1
    """

    with conn.cursor() as cur:
        cur.execute(query, (ticker.upper(),))
        row = cur.fetchone()
        if row:
            return CharacterClassification(**dict(zip(columns, row)))
    return None


def get_latest_baseline(conn, ticker: str) -> CharacterBaseline | None:
    """Get the most recent baseline for a ticker.

    Args:
        conn: psycopg connection
        ticker: stock symbol

    Returns:
        CharacterBaseline or None if no data
    """
    columns = CharacterBaseline.column_names()
    cols_sql = ", ".join(columns)

    query = f"""
        SELECT {cols_sql}
        FROM stock_character_baseline
        WHERE ticker = %s
        ORDER BY run_date DESC
        LIMIT 1
    """

    with conn.cursor() as cur:
        cur.execute(query, (ticker.upper(),))
        row = cur.fetchone()
        if row:
            return CharacterBaseline(**dict(zip(columns, row)))
    return None


def get_latest_scorecard(conn, ticker: str) -> CharacterScorecard | None:
    """Get the most recent scorecard for a ticker.

    Args:
        conn: psycopg connection
        ticker: stock symbol

    Returns:
        CharacterScorecard or None if no data
    """
    columns = CharacterScorecard.column_names()
    cols_sql = ", ".join(columns)

    query = f"""
        SELECT {cols_sql}
        FROM stock_character_scorecard
        WHERE ticker = %s
        ORDER BY run_date DESC
        LIMIT 1
    """

    with conn.cursor() as cur:
        cur.execute(query, (ticker.upper(),))
        row = cur.fetchone()
        if row:
            return CharacterScorecard(**dict(zip(columns, row)))
    return None


def get_flags_for_ticker(
    conn, ticker: str, run_date: date | None = None
) -> list[CharacterFlag]:
    """Get all flags for a ticker on a given run_date (or latest).

    Args:
        conn: psycopg connection
        ticker: stock symbol
        run_date: optional date filter; if None, uses the most recent run_date

    Returns:
        List of CharacterFlag instances
    """
    columns = CharacterFlag.column_names()
    cols_sql = ", ".join(columns)

    if run_date is None:
        # Find the latest run_date for this ticker
        date_query = """
            SELECT MAX(run_date) FROM stock_character_flags WHERE ticker = %s
        """
        with conn.cursor() as cur:
            cur.execute(date_query, (ticker.upper(),))
            result = cur.fetchone()
            if not result or result[0] is None:
                return []
            run_date = result[0]

    query = f"""
        SELECT {cols_sql}
        FROM stock_character_flags
        WHERE ticker = %s AND run_date = %s
        ORDER BY flag
    """

    with conn.cursor() as cur:
        cur.execute(query, (ticker.upper(), run_date))
        return [
            CharacterFlag(**dict(zip(columns, row))) for row in cur.fetchall()
        ]


def get_ranked_alerts(
    conn, run_date: date | None = None, limit: int = 50
) -> list[dict]:
    """Get ranked alert list for a run date.

    Sorted by flag_count DESC, then confidence (HIGH before MEDIUM).

    Args:
        conn: psycopg connection
        run_date: optional date filter; if None, uses the most recent run_date
        limit: maximum number of results

    Returns:
        List of dicts with scorecard data
    """
    if run_date is None:
        date_query = "SELECT MAX(run_date) FROM stock_character_scorecard"
        with conn.cursor() as cur:
            cur.execute(date_query)
            result = cur.fetchone()
            if not result or result[0] is None:
                return []
            run_date = result[0]

    columns = CharacterScorecard.column_names()
    cols_sql = ", ".join(columns)

    query = f"""
        SELECT {cols_sql}
        FROM stock_character_scorecard
        WHERE run_date = %s
        ORDER BY
            flag_count DESC,
            CASE confidence WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
            ticker
        LIMIT %s
    """

    with conn.cursor() as cur:
        cur.execute(query, (run_date, limit))
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def get_prices_with_benchmarks(
    conn,
    ticker: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch OHLCV for a ticker plus benchmark tickers (SPY, GLD, TLT) in one call.

    Args:
        conn: psycopg connection
        ticker: stock symbol
        start_date: optional start date filter
        end_date: optional end date filter

    Returns:
        Dict mapping ticker symbol to list of price dicts:
        {
            ticker: [
                {"date": ..., "open": ..., "high": ..., "low": ..., "close": ..., "volume": ...},
                ...
            ],
            "SPY": [...],
            "GLD": [...],
            "TLT": [...],
        }
    """
    tickers = [ticker.upper(), "SPY", "GLD", "TLT"]

    conditions = ["ticker = ANY(%s)"]
    params: list[Any] = [tickers]

    if start_date:
        conditions.append("date >= %s")
        params.append(start_date)
    if end_date:
        conditions.append("date <= %s")
        params.append(end_date)

    where_clause = " AND ".join(conditions)
    query = f"""
        SELECT ticker, date, open, high, low, close, volume
        FROM stock_prices
        WHERE {where_clause}
        ORDER BY ticker, date ASC
    """

    result: dict[str, list[dict[str, Any]]] = {t: [] for t in tickers}

    with conn.cursor() as cur:
        cur.execute(query, params)
        for row in cur.fetchall():
            sym = row[0]
            if sym in result:
                result[sym].append(
                    {
                        "date": row[1],
                        "open": row[2],
                        "high": row[3],
                        "low": row[4],
                        "close": row[5],
                        "volume": row[6],
                    }
                )

    return result
