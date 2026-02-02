"""Database connection and query utilities for the TUI."""

import logging
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row

from sp500_tui.config import get_database_url

logger = logging.getLogger(__name__)


@contextmanager
def get_connection() -> Generator[psycopg.Connection, None, None]:
    """
    Get a database connection as a context manager.

    Yields:
        psycopg.Connection: Database connection configured for dict rows
    """
    conn = psycopg.connect(get_database_url(), row_factory=dict_row)
    try:
        yield conn
    finally:
        conn.close()


def execute_query(
    sql: str,
    params: dict[str, Any] | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """
    Execute a SELECT query and return results as list of dicts.

    Args:
        sql: SQL query string (can use %(name)s placeholders)
        params: Query parameters
        limit: Optional row limit to append

    Returns:
        List of row dictionaries
    """
    if limit is not None and "LIMIT" not in sql.upper():
        sql = f"{sql} LIMIT {limit}"

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]


def execute_write(
    sql: str,
    params: dict[str, Any] | None = None,
) -> int:
    """
    Execute an INSERT/UPDATE/DELETE query.

    Args:
        sql: SQL query string
        params: Query parameters

    Returns:
        Number of affected rows
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rowcount = cur.rowcount
            conn.commit()
            return rowcount


def execute_write_returning(
    sql: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """
    Execute an INSERT/UPDATE with RETURNING clause.

    Args:
        sql: SQL query string with RETURNING clause
        params: Query parameters

    Returns:
        Returned row as dict, or None if no row returned
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            result = cur.fetchone()
            conn.commit()
            return dict(result) if result else None


def init_schema(schema_dir: str = "sqlschema") -> None:
    """
    Initialize the TUI-specific schema (watchlists, glossary, users).

    Executes schema files: 08_watchlists.sql, 09_glossary.sql, 10_news.sql, 11_users.sql

    Args:
        schema_dir: Directory containing SQL schema files (default: sqlschema)
    """
    import os
    from pathlib import Path

    # Get project root (assuming this file is in tui/sp500_tui/)
    project_root = Path(__file__).parent.parent.parent
    schema_path = project_root / schema_dir

    # Schema files to execute in order
    schema_files = [
        "08_watchlists.sql",
        "09_glossary.sql",
        "10_news.sql",
        "11_users.sql",
    ]

    with get_connection() as conn:
        with conn.cursor() as cur:
            for schema_file in schema_files:
                file_path = schema_path / schema_file
                if file_path.exists():
                    logger.info(f"Executing schema file: {schema_file}")
                    with open(file_path) as f:
                        sql = f.read()
                        cur.execute(sql)
                else:
                    logger.warning(f"Schema file not found: {file_path}")
            conn.commit()

    logger.info("TUI schema initialized")


def init_schema_legacy() -> None:
    """
    Legacy init_schema with hardcoded SQL.

    Deprecated: Use init_schema() which reads from SQL files.
    """
    schema_sql = """
    -- Watchlists table
    CREATE TABLE IF NOT EXISTS watchlists (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50) NOT NULL UNIQUE,
        is_default BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Watchlist symbols junction table
    CREATE TABLE IF NOT EXISTS watchlist_symbols (
        watchlist_id INTEGER NOT NULL REFERENCES watchlists(id) ON DELETE CASCADE,
        ticker VARCHAR(10) NOT NULL REFERENCES companies(ticker) ON DELETE CASCADE,
        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        sort_order INTEGER DEFAULT 0,
        PRIMARY KEY (watchlist_id, ticker)
    );

    -- User settings table
    CREATE TABLE IF NOT EXISTS user_settings (
        key VARCHAR(50) PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Glossary tables
    CREATE TABLE IF NOT EXISTS glossary_terms (
        term VARCHAR(100) PRIMARY KEY,
        official_definition TEXT,
        plain_english TEXT,
        examples JSONB,
        related_terms JSONB,
        learn_more JSONB,
        custom_prompt TEXT,
        generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        model_used VARCHAR(50) DEFAULT 'glm-4.7'
    );

    CREATE TABLE IF NOT EXISTS glossary_term_list (
        term VARCHAR(100) PRIMARY KEY,
        category VARCHAR(50),
        source VARCHAR(20) DEFAULT 'curated'
    );

    -- Indexes
    CREATE INDEX IF NOT EXISTS idx_watchlist_symbols_ticker ON watchlist_symbols(ticker);
    CREATE INDEX IF NOT EXISTS idx_watchlists_default ON watchlists(is_default)
        WHERE is_default = TRUE;
    CREATE INDEX IF NOT EXISTS idx_glossary_term_list_category 
        ON glossary_term_list(category);
    """

    defaults_sql = """
    -- Insert default watchlist if not exists
    INSERT INTO watchlists (name, is_default)
    VALUES ('Default', TRUE)
    ON CONFLICT (name) DO NOTHING;

    -- Insert default symbols (only if they exist in companies table)
    INSERT INTO watchlist_symbols (watchlist_id, ticker, sort_order)
    SELECT w.id, c.ticker, t.sort_order
    FROM watchlists w
    CROSS JOIN (
        VALUES ('AAPL', 1), ('GOOGL', 2), ('AMZN', 3)
    ) AS t(ticker, sort_order)
    JOIN companies c ON c.ticker = t.ticker
    WHERE w.name = 'Default'
      AND w.is_default = TRUE
      AND NOT EXISTS (
          SELECT 1 FROM watchlist_symbols ws WHERE ws.watchlist_id = w.id
      )
    ON CONFLICT (watchlist_id, ticker) DO NOTHING;

    -- Insert default settings
    INSERT INTO user_settings (key, value) VALUES
        ('chart_period_days', '60'),
        ('auto_refresh', 'false'),
        ('refresh_interval_seconds', '60'),
        ('number_format', 'compact'),
        ('fundamentals_timeframe', 'quarterly'),
        ('table_rows', '25')
    ON CONFLICT (key) DO NOTHING;

    -- Insert curated glossary terms
    INSERT INTO glossary_term_list (term, category, source) VALUES
        ('P/E Ratio', 'Valuation', 'curated'),
        ('P/B Ratio', 'Valuation', 'curated'),
        ('P/S Ratio', 'Valuation', 'curated'),
        ('PEG Ratio', 'Valuation', 'curated'),
        ('EV/EBITDA', 'Valuation', 'curated'),
        ('EV/Sales', 'Valuation', 'curated'),
        ('Market Cap', 'Valuation', 'curated'),
        ('Enterprise Value', 'Valuation', 'curated'),
        ('Price Target', 'Valuation', 'curated'),
        ('Fair Value', 'Valuation', 'curated'),
        ('ROE', 'Profitability', 'curated'),
        ('ROA', 'Profitability', 'curated'),
        ('ROIC', 'Profitability', 'curated'),
        ('Gross Margin', 'Profitability', 'curated'),
        ('Operating Margin', 'Profitability', 'curated'),
        ('Net Margin', 'Profitability', 'curated'),
        ('EPS', 'Profitability', 'curated'),
        ('EBITDA', 'Profitability', 'curated'),
        ('Current Ratio', 'Liquidity', 'curated'),
        ('Quick Ratio', 'Liquidity', 'curated'),
        ('Cash Ratio', 'Liquidity', 'curated'),
        ('Working Capital', 'Liquidity', 'curated'),
        ('Debt/Equity', 'Leverage', 'curated'),
        ('Debt/Assets', 'Leverage', 'curated'),
        ('Interest Coverage', 'Leverage', 'curated'),
        ('Total Debt', 'Leverage', 'curated'),
        ('Leverage Ratio', 'Leverage', 'curated'),
        ('Free Cash Flow', 'Cash Flow', 'curated'),
        ('Operating Cash Flow', 'Cash Flow', 'curated'),
        ('CapEx', 'Cash Flow', 'curated'),
        ('FCF Yield', 'Cash Flow', 'curated'),
        ('Cash Conversion', 'Cash Flow', 'curated'),
        ('Dividend Yield', 'Dividends', 'curated'),
        ('Payout Ratio', 'Dividends', 'curated'),
        ('Dividend Growth', 'Dividends', 'curated'),
        ('Ex-Dividend Date', 'Dividends', 'curated'),
        ('Revenue Growth', 'Growth', 'curated'),
        ('Earnings Growth', 'Growth', 'curated'),
        ('YoY', 'Growth', 'curated'),
        ('QoQ', 'Growth', 'curated'),
        ('CAGR', 'Growth', 'curated'),
        ('Organic Growth', 'Growth', 'curated'),
        ('52-Week High', 'Trading', 'curated'),
        ('52-Week Low', 'Trading', 'curated'),
        ('Volume', 'Trading', 'curated'),
        ('Beta', 'Trading', 'curated'),
        ('Alpha', 'Trading', 'curated'),
        ('Volatility', 'Trading', 'curated'),
        ('Short Interest', 'Trading', 'curated'),
        ('Float', 'Trading', 'curated')
    ON CONFLICT (term) DO NOTHING;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
            cur.execute(defaults_sql)
            conn.commit()
    logger.info("Schema initialized (watchlists + glossary)")
