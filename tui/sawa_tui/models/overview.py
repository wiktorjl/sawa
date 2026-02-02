"""Company overview data model and CRUD operations."""

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg.errors

from sawa_tui.database import execute_query, execute_write

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CompanyOverview:
    """AI-generated company overview."""

    ticker: str
    main_product: str
    revenue_model: str
    headwinds: list[str]
    tailwinds: list[str]
    sector_outlook: str
    competitive_position: str
    generated_at: datetime
    model_used: str
    custom_prompt: str | None = None
    is_user_override: bool = False

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "CompanyOverview":
        """Create from database row."""
        return cls(
            ticker=row["ticker"],
            main_product=row.get("main_product", ""),
            revenue_model=row.get("revenue_model", ""),
            headwinds=row.get("headwinds", []) or [],
            tailwinds=row.get("tailwinds", []) or [],
            sector_outlook=row.get("sector_outlook", ""),
            competitive_position=row.get("competitive_position", ""),
            generated_at=row.get("generated_at", datetime.now()),
            model_used=row.get("model_used", "unknown"),
            custom_prompt=row.get("custom_prompt"),
            is_user_override=row.get("user_id") is not None,
        )

    @classmethod
    def from_json(
        cls,
        ticker: str,
        data: dict[str, Any],
        model: str = "glm-4.7",
        custom_prompt: str | None = None,
    ) -> "CompanyOverview":
        """Create from parsed JSON API response."""
        return cls(
            ticker=ticker.upper(),
            main_product=data.get("main_product", ""),
            revenue_model=data.get("revenue_model", ""),
            headwinds=data.get("headwinds", []),
            tailwinds=data.get("tailwinds", []),
            sector_outlook=data.get("sector_outlook", ""),
            competitive_position=data.get("competitive_position", ""),
            generated_at=datetime.now(),
            model_used=model,
            custom_prompt=custom_prompt if custom_prompt else None,
        )


class OverviewManager:
    """Manager for company overview CRUD operations."""

    @staticmethod
    def get_cached(ticker: str, user_id: int | None = None) -> CompanyOverview | None:
        """
        Get cached overview for a ticker.

        Two-tier lookup:
        1. If user_id provided, check for user override first
        2. Fall back to shared overview (user_id IS NULL)

        Args:
            ticker: Stock ticker symbol
            user_id: Optional user ID for user-specific overrides

        Returns:
            CompanyOverview if found, None otherwise
        """
        ticker = ticker.upper()

        try:
            if user_id is not None:
                # Try user override first
                sql = """
                    SELECT ticker, main_product, revenue_model, headwinds, tailwinds,
                           sector_outlook, competitive_position, generated_at, model_used,
                           custom_prompt, user_id
                    FROM company_overviews
                    WHERE ticker = %(ticker)s AND user_id = %(user_id)s
                """
                rows = execute_query(sql, {"ticker": ticker, "user_id": user_id})
                if rows:
                    return CompanyOverview.from_row(rows[0])

            # Fall back to shared overview
            sql = """
                SELECT ticker, main_product, revenue_model, headwinds, tailwinds,
                       sector_outlook, competitive_position, generated_at, model_used,
                       custom_prompt, user_id
                FROM company_overviews
                WHERE ticker = %(ticker)s AND user_id IS NULL
            """
            rows = execute_query(sql, {"ticker": ticker})
            return CompanyOverview.from_row(rows[0]) if rows else None
        except psycopg.errors.UndefinedTable:
            # Table doesn't exist yet - return None (no cached overview)
            logger.debug("company_overviews table not found - run schema migration")
            return None

    @staticmethod
    def save(overview: CompanyOverview, user_id: int | None = None) -> bool:
        """
        Save an overview to the cache.

        If user_id is None, saves as shared overview.
        If user_id is provided, saves as user-specific override.

        Args:
            overview: The CompanyOverview to save
            user_id: Optional user ID for user-specific override

        Returns:
            True if saved successfully, False if table missing or error
        """
        ticker = overview.ticker.upper()

        try:
            if user_id is None:
                # Shared overview
                check_sql = "SELECT id FROM company_overviews WHERE ticker = %(ticker)s AND user_id IS NULL"
                existing = execute_query(check_sql, {"ticker": ticker})

                if existing:
                    sql = """
                        UPDATE company_overviews SET
                            main_product = %(main_product)s,
                            revenue_model = %(revenue_model)s,
                            headwinds = %(headwinds)s,
                            tailwinds = %(tailwinds)s,
                            sector_outlook = %(sector_outlook)s,
                            competitive_position = %(competitive_position)s,
                            custom_prompt = %(custom_prompt)s,
                            model_used = %(model_used)s,
                            generated_at = CURRENT_TIMESTAMP
                        WHERE ticker = %(ticker)s AND user_id IS NULL
                    """
                else:
                    sql = """
                        INSERT INTO company_overviews
                            (ticker, main_product, revenue_model, headwinds, tailwinds,
                             sector_outlook, competitive_position, custom_prompt, model_used, user_id)
                        VALUES
                            (%(ticker)s, %(main_product)s, %(revenue_model)s, %(headwinds)s,
                             %(tailwinds)s, %(sector_outlook)s, %(competitive_position)s,
                             %(custom_prompt)s, %(model_used)s, NULL)
                    """
            else:
                # User override
                check_sql = """
                    SELECT id FROM company_overviews
                    WHERE ticker = %(ticker)s AND user_id = %(user_id)s
                """
                existing = execute_query(check_sql, {"ticker": ticker, "user_id": user_id})

                if existing:
                    sql = """
                        UPDATE company_overviews SET
                            main_product = %(main_product)s,
                            revenue_model = %(revenue_model)s,
                            headwinds = %(headwinds)s,
                            tailwinds = %(tailwinds)s,
                            sector_outlook = %(sector_outlook)s,
                            competitive_position = %(competitive_position)s,
                            custom_prompt = %(custom_prompt)s,
                            model_used = %(model_used)s,
                            generated_at = CURRENT_TIMESTAMP
                        WHERE ticker = %(ticker)s AND user_id = %(user_id)s
                    """
                else:
                    sql = """
                        INSERT INTO company_overviews
                            (ticker, main_product, revenue_model, headwinds, tailwinds,
                             sector_outlook, competitive_position, custom_prompt, model_used, user_id)
                        VALUES
                            (%(ticker)s, %(main_product)s, %(revenue_model)s, %(headwinds)s,
                             %(tailwinds)s, %(sector_outlook)s, %(competitive_position)s,
                             %(custom_prompt)s, %(model_used)s, %(user_id)s)
                    """

            params = {
                "ticker": ticker,
                "main_product": overview.main_product,
                "revenue_model": overview.revenue_model,
                "headwinds": json.dumps(overview.headwinds),
                "tailwinds": json.dumps(overview.tailwinds),
                "sector_outlook": overview.sector_outlook,
                "competitive_position": overview.competitive_position,
                "custom_prompt": overview.custom_prompt,
                "model_used": overview.model_used,
            }
            if user_id is not None:
                params["user_id"] = user_id

            execute_write(sql, params)
            return True
        except psycopg.errors.UndefinedTable:
            logger.error("company_overviews table not found - run schema migration")
            return False
        except Exception as e:
            logger.error(f"Failed to save company overview: {e}")
            return False

    @staticmethod
    def delete_cached(ticker: str, user_id: int | None = None) -> bool:
        """
        Delete cached overview.

        If user_id is None, deletes shared overview.
        If user_id is provided, deletes only user override.

        Args:
            ticker: Stock ticker symbol
            user_id: Optional user ID for user-specific override

        Returns:
            True if deleted successfully
        """
        ticker = ticker.upper()

        if user_id is None:
            sql = "DELETE FROM company_overviews WHERE ticker = %(ticker)s AND user_id IS NULL"
            params = {"ticker": ticker}
        else:
            sql = "DELETE FROM company_overviews WHERE ticker = %(ticker)s AND user_id = %(user_id)s"
            params = {"ticker": ticker, "user_id": user_id}

        try:
            execute_write(sql, params)
            return True
        except Exception as e:
            logger.error(f"Failed to delete cached overview: {e}")
            return False

    @staticmethod
    def get_top_tickers_without_overview(limit: int = 100) -> list[str]:
        """
        Get top tickers by market cap that don't have shared cached overviews.

        Used by batch job to prioritize which tickers to process.

        Args:
            limit: Maximum number of tickers to return

        Returns:
            List of ticker symbols
        """
        sql = """
            SELECT c.ticker FROM companies c
            LEFT JOIN company_overviews co ON c.ticker = co.ticker AND co.user_id IS NULL
            WHERE co.ticker IS NULL AND c.active = true
            ORDER BY c.market_cap DESC NULLS LAST
            LIMIT %(limit)s
        """
        rows = execute_query(sql, {"limit": limit})
        return [row["ticker"] for row in rows]

    @staticmethod
    def has_user_override(ticker: str, user_id: int) -> bool:
        """
        Check if a user has a custom override for a ticker.

        Args:
            ticker: Stock ticker symbol
            user_id: User ID

        Returns:
            True if user has a custom override
        """
        ticker = ticker.upper()
        sql = "SELECT 1 FROM company_overviews WHERE ticker = %(ticker)s AND user_id = %(user_id)s"
        rows = execute_query(sql, {"ticker": ticker, "user_id": user_id})
        return len(rows) > 0

    @staticmethod
    def delete_user_override(ticker: str, user_id: int) -> bool:
        """
        Delete a user's custom override, reverting to shared overview.

        Args:
            ticker: Stock ticker symbol
            user_id: User ID

        Returns:
            True if deleted successfully
        """
        return OverviewManager.delete_cached(ticker, user_id=user_id)

    @staticmethod
    def get_overview_age_days(ticker: str, user_id: int | None = None) -> int | None:
        """
        Get the age of the cached overview in days.

        Args:
            ticker: Stock ticker symbol
            user_id: Optional user ID

        Returns:
            Number of days since generation, or None if not cached
        """
        ticker = ticker.upper()

        if user_id is not None:
            sql = """
                SELECT EXTRACT(DAY FROM (CURRENT_TIMESTAMP - generated_at))::int as age_days
                FROM company_overviews
                WHERE ticker = %(ticker)s AND user_id = %(user_id)s
            """
            rows = execute_query(sql, {"ticker": ticker, "user_id": user_id})
            if rows:
                return rows[0]["age_days"]

        sql = """
            SELECT EXTRACT(DAY FROM (CURRENT_TIMESTAMP - generated_at))::int as age_days
            FROM company_overviews
            WHERE ticker = %(ticker)s AND user_id IS NULL
        """
        rows = execute_query(sql, {"ticker": ticker})
        return rows[0]["age_days"] if rows else None
