"""Watchlist data model and CRUD operations."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sp500_tui.database import execute_query, execute_write, execute_write_returning


@dataclass
class Watchlist:
    """Represents a watchlist."""

    id: int
    name: str
    is_default: bool
    created_at: datetime
    symbol_count: int = 0

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "Watchlist":
        """Create a Watchlist from a database row."""
        return cls(
            id=row["id"],
            name=row["name"],
            is_default=row.get("is_default", False),
            created_at=row.get("created_at", datetime.now()),
            symbol_count=row.get("symbol_count", 0),
        )


@dataclass
class WatchlistStock:
    """Represents a stock in a watchlist with current data."""

    ticker: str
    name: str
    price: float | None = None
    change: float | None = None
    change_pct: float | None = None
    volume: int | None = None
    market_cap: float | None = None
    pe_ratio: float | None = None
    high_52w: float | None = None
    low_52w: float | None = None
    sector: str | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "WatchlistStock":
        """Create a WatchlistStock from a database row."""
        return cls(
            ticker=row["ticker"],
            name=row.get("name", ""),
            price=row.get("price"),
            change=row.get("change"),
            change_pct=row.get("change_pct"),
            volume=row.get("volume"),
            market_cap=row.get("market_cap"),
            pe_ratio=row.get("pe_ratio"),
            high_52w=row.get("high_52w"),
            low_52w=row.get("low_52w"),
            sector=row.get("sector") or row.get("sic_description"),
        )


class WatchlistManager:
    """Manager for watchlist CRUD operations."""

    @staticmethod
    def get_all() -> list[Watchlist]:
        """Get all watchlists with symbol counts."""
        sql = """
            SELECT w.id, w.name, w.is_default, w.created_at,
                   COUNT(ws.ticker) as symbol_count
            FROM watchlists w
            LEFT JOIN watchlist_symbols ws ON w.id = ws.watchlist_id
            GROUP BY w.id, w.name, w.is_default, w.created_at
            ORDER BY w.is_default DESC, w.name ASC
        """
        rows = execute_query(sql)
        return [Watchlist.from_row(row) for row in rows]

    @staticmethod
    def get_by_id(watchlist_id: int) -> Watchlist | None:
        """Get a watchlist by ID."""
        sql = """
            SELECT w.id, w.name, w.is_default, w.created_at,
                   COUNT(ws.ticker) as symbol_count
            FROM watchlists w
            LEFT JOIN watchlist_symbols ws ON w.id = ws.watchlist_id
            WHERE w.id = %(id)s
            GROUP BY w.id, w.name, w.is_default, w.created_at
        """
        rows = execute_query(sql, {"id": watchlist_id})
        return Watchlist.from_row(rows[0]) if rows else None

    @staticmethod
    def get_default() -> Watchlist | None:
        """Get the default watchlist."""
        sql = """
            SELECT w.id, w.name, w.is_default, w.created_at,
                   COUNT(ws.ticker) as symbol_count
            FROM watchlists w
            LEFT JOIN watchlist_symbols ws ON w.id = ws.watchlist_id
            WHERE w.is_default = TRUE
            GROUP BY w.id, w.name, w.is_default, w.created_at
        """
        rows = execute_query(sql)
        return Watchlist.from_row(rows[0]) if rows else None

    @staticmethod
    def create(name: str, is_default: bool = False) -> Watchlist | None:
        """Create a new watchlist."""
        sql = """
            INSERT INTO watchlists (name, is_default)
            VALUES (%(name)s, %(is_default)s)
            RETURNING id, name, is_default, created_at
        """
        row = execute_write_returning(sql, {"name": name, "is_default": is_default})
        if row:
            row["symbol_count"] = 0
            return Watchlist.from_row(row)
        return None

    @staticmethod
    def rename(watchlist_id: int, new_name: str) -> bool:
        """Rename a watchlist."""
        sql = "UPDATE watchlists SET name = %(name)s WHERE id = %(id)s"
        return execute_write(sql, {"id": watchlist_id, "name": new_name}) > 0

    @staticmethod
    def delete(watchlist_id: int) -> bool:
        """Delete a watchlist (cascade deletes symbols)."""
        sql = "DELETE FROM watchlists WHERE id = %(id)s AND is_default = FALSE"
        return execute_write(sql, {"id": watchlist_id}) > 0

    @staticmethod
    def set_default(watchlist_id: int) -> bool:
        """Set a watchlist as the default (unsets other defaults)."""
        sql1 = "UPDATE watchlists SET is_default = FALSE WHERE is_default = TRUE"
        sql2 = "UPDATE watchlists SET is_default = TRUE WHERE id = %(id)s"
        execute_write(sql1)
        return execute_write(sql2, {"id": watchlist_id}) > 0

    @staticmethod
    def get_stocks(watchlist_id: int) -> list[WatchlistStock]:
        """
        Get all stocks in a watchlist with current price data.

        Returns stocks with latest price, change, volume, market cap, P/E ratio,
        and 52-week high/low.
        """
        sql = """
            WITH latest_prices AS (
                SELECT DISTINCT ON (ticker) ticker, date, close, volume
                FROM stock_prices
                ORDER BY ticker, date DESC
            ),
            prev_prices AS (
                SELECT DISTINCT ON (ticker) ticker, close as prev_close
                FROM stock_prices
                WHERE date < (SELECT MAX(date) FROM stock_prices)
                ORDER BY ticker, date DESC
            ),
            high_low_52w AS (
                SELECT ticker,
                       MAX(high) as high_52w,
                       MIN(low) as low_52w
                FROM stock_prices
                WHERE date >= CURRENT_DATE - INTERVAL '52 weeks'
                GROUP BY ticker
            ),
            latest_ratios AS (
                SELECT DISTINCT ON (ticker) ticker, price_to_earnings, market_cap
                FROM financial_ratios
                ORDER BY ticker, date DESC
            )
            SELECT
                ws.ticker,
                c.name,
                lp.close as price,
                lp.close - pp.prev_close as change,
                CASE
                    WHEN pp.prev_close > 0
                    THEN ((lp.close - pp.prev_close) / pp.prev_close) * 100
                    ELSE NULL
                END as change_pct,
                lp.volume,
                COALESCE(lr.market_cap, c.market_cap) as market_cap,
                lr.price_to_earnings as pe_ratio,
                hl.high_52w,
                hl.low_52w,
                c.sic_description as sector
            FROM watchlist_symbols ws
            JOIN companies c ON ws.ticker = c.ticker
            LEFT JOIN latest_prices lp ON ws.ticker = lp.ticker
            LEFT JOIN prev_prices pp ON ws.ticker = pp.ticker
            LEFT JOIN high_low_52w hl ON ws.ticker = hl.ticker
            LEFT JOIN latest_ratios lr ON ws.ticker = lr.ticker
            WHERE ws.watchlist_id = %(watchlist_id)s
            ORDER BY ws.sort_order, ws.ticker
        """
        rows = execute_query(sql, {"watchlist_id": watchlist_id})
        return [WatchlistStock.from_row(row) for row in rows]

    @staticmethod
    def add_symbol(watchlist_id: int, ticker: str) -> tuple[bool, str]:
        """
        Add a symbol to a watchlist.

        Returns:
            Tuple of (success, error_message). If success is True, error_message is empty.
        """
        # Validate ticker exists in companies table
        check_sql = "SELECT 1 FROM companies WHERE ticker = %(ticker)s"
        if not execute_query(check_sql, {"ticker": ticker}):
            return False, f"Ticker '{ticker}' not found"

        # Check if already in watchlist
        if WatchlistManager.has_symbol(watchlist_id, ticker):
            return False, f"'{ticker}' already in watchlist"

        # Get max sort_order first
        sql_order = """
            SELECT COALESCE(MAX(sort_order), 0) + 1 as next_order
            FROM watchlist_symbols
            WHERE watchlist_id = %(watchlist_id)s
        """
        result = execute_query(sql_order, {"watchlist_id": watchlist_id})
        next_order = result[0]["next_order"] if result else 1

        sql = """
            INSERT INTO watchlist_symbols (watchlist_id, ticker, sort_order)
            VALUES (%(watchlist_id)s, %(ticker)s, %(sort_order)s)
            ON CONFLICT (watchlist_id, ticker) DO NOTHING
        """
        success = (
            execute_write(
                sql,
                {"watchlist_id": watchlist_id, "ticker": ticker, "sort_order": next_order},
            )
            > 0
        )
        return success, "" if success else "Failed to add symbol"

    @staticmethod
    def remove_symbol(watchlist_id: int, ticker: str) -> bool:
        """Remove a symbol from a watchlist."""
        sql = """
            DELETE FROM watchlist_symbols
            WHERE watchlist_id = %(watchlist_id)s AND ticker = %(ticker)s
        """
        return execute_write(sql, {"watchlist_id": watchlist_id, "ticker": ticker}) > 0

    @staticmethod
    def has_symbol(watchlist_id: int, ticker: str) -> bool:
        """Check if a watchlist contains a symbol."""
        sql = """
            SELECT 1 FROM watchlist_symbols
            WHERE watchlist_id = %(watchlist_id)s AND ticker = %(ticker)s
        """
        return len(execute_query(sql, {"watchlist_id": watchlist_id, "ticker": ticker})) > 0

    @staticmethod
    def get_watchlists_for_symbol(ticker: str) -> list[Watchlist]:
        """Get all watchlists that contain a given symbol."""
        sql = """
            SELECT w.id, w.name, w.is_default, w.created_at,
                   COUNT(ws2.ticker) as symbol_count
            FROM watchlists w
            JOIN watchlist_symbols ws ON w.id = ws.watchlist_id AND ws.ticker = %(ticker)s
            LEFT JOIN watchlist_symbols ws2 ON w.id = ws2.watchlist_id
            GROUP BY w.id, w.name, w.is_default, w.created_at
            ORDER BY w.is_default DESC, w.name ASC
        """
        rows = execute_query(sql, {"ticker": ticker})
        return [Watchlist.from_row(row) for row in rows]
