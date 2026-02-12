"""Database repositories - read data from PostgreSQL.

This module provides repository implementations that read data from
the PostgreSQL database. These are the primary repositories used by
TUI and MCP server to access already-loaded data.

The database repositories are designed to be efficient for reading
and don't support writes (data is loaded via the CLI tools).

Usage:
    from sawa.repositories.database import DatabasePriceRepository

    repo = DatabasePriceRepository(database_url="postgresql://...")
    prices = await repo.get_prices("AAPL", start_date, end_date)
"""

import asyncio
from collections.abc import AsyncIterator
from datetime import date
from decimal import Decimal
from typing import Any, Literal

import psycopg
from psycopg.rows import dict_row

from sawa.domain.models import (
    BalanceSheet,
    CashFlow,
    CompanyInfo,
    FinancialRatio,
    IncomeStatement,
    InflationData,
    LaborMarketData,
    MarketIndex,
    NewsArticle,
    StockPrice,
    TreasuryYield,
)
from sawa.domain.technical_indicators import TechnicalIndicators
from sawa.repositories.base import (
    CompanyRepository,
    EconomyRepository,
    FundamentalRepository,
    IndexRepository,
    NewsRepository,
    RatiosRepository,
    StockPriceRepository,
    TechnicalIndicatorsRepository,
)


def _get_connection(database_url: str) -> psycopg.Connection:
    """Get database connection.

    Args:
        database_url: PostgreSQL connection URL

    Returns:
        psycopg connection object
    """
    return psycopg.connect(database_url)


def _to_decimal(value: Any) -> Decimal | None:
    """Convert value to Decimal, returning None for null values.

    Args:
        value: Value to convert (can be str, float, int, Decimal, or None)

    Returns:
        Decimal value or None
    """
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (ValueError, TypeError):
        return None


class DatabasePriceRepository(StockPriceRepository):
    """Read stock prices from PostgreSQL.

    This repository queries the stock_prices table which contains
    historical OHLCV data loaded by the coldstart process.
    """

    def __init__(self, database_url: str) -> None:
        """Initialize with database URL.

        Args:
            database_url: PostgreSQL connection URL
        """
        self.database_url = database_url

    @property
    def provider_name(self) -> str:
        """Return provider name."""
        return "database"

    @property
    def supports_historical_bulk(self) -> bool:
        """Database supports efficient bulk queries."""
        return True

    async def get_prices(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
    ) -> list[StockPrice]:
        """Query stock_prices table for a date range.

        Args:
            ticker: Stock symbol
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of StockPrice objects, sorted by date ascending
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_prices_sync, ticker, start_date, end_date)

    def _get_prices_sync(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
    ) -> list[StockPrice]:
        """Synchronous implementation of get_prices."""
        query = """
            SELECT ticker, date, open, high, low, close, volume
            FROM stock_prices
            WHERE ticker = %s AND date BETWEEN %s AND %s
            ORDER BY date
        """
        with _get_connection(self.database_url) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (ticker.upper(), start_date, end_date))
                rows = cur.fetchall()

        return [self._row_to_price(row) for row in rows]

    def _row_to_price(self, row: dict[str, Any]) -> StockPrice:
        """Convert database row to StockPrice domain model."""
        return StockPrice(
            ticker=row["ticker"],
            date=row["date"],
            open=Decimal(str(row["open"])),
            high=Decimal(str(row["high"])),
            low=Decimal(str(row["low"])),
            close=Decimal(str(row["close"])),
            volume=int(row["volume"]),
        )

    async def get_prices_stream(
        self,
        tickers: list[str],
        start_date: date,
        end_date: date,
    ) -> AsyncIterator[StockPrice]:
        """Stream prices for multiple tickers.

        For database, this just yields from get_prices since DB queries
        are already efficient.
        """
        for ticker in tickers:
            prices = await self.get_prices(ticker, start_date, end_date)
            for price in prices:
                yield price

    async def get_latest_price(self, ticker: str) -> StockPrice | None:
        """Get most recent price from database.

        Args:
            ticker: Stock symbol

        Returns:
            Most recent StockPrice, or None if not found
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_latest_sync, ticker)

    def _get_latest_sync(self, ticker: str) -> StockPrice | None:
        """Synchronous implementation of get_latest_price."""
        query = """
            SELECT ticker, date, open, high, low, close, volume
            FROM stock_prices
            WHERE ticker = %s
            ORDER BY date DESC
            LIMIT 1
        """
        with _get_connection(self.database_url) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (ticker.upper(),))
                row = cur.fetchone()

        return self._row_to_price(row) if row else None

    async def get_prices_for_date(
        self,
        tickers: list[str],
        target_date: date,
    ) -> list[StockPrice]:
        """Get prices for multiple tickers on a date.

        Args:
            tickers: List of stock symbols
            target_date: The date to fetch prices for

        Returns:
            List of StockPrice objects
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_for_date_sync, tickers, target_date)

    def _get_for_date_sync(
        self,
        tickers: list[str],
        target_date: date,
    ) -> list[StockPrice]:
        """Synchronous implementation of get_prices_for_date."""
        query = """
            SELECT ticker, date, open, high, low, close, volume
            FROM stock_prices
            WHERE ticker = ANY(%s) AND date = %s
        """
        with _get_connection(self.database_url) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, ([t.upper() for t in tickers], target_date))
                rows = cur.fetchall()

        return [self._row_to_price(row) for row in rows]


class DatabaseFundamentalRepository(FundamentalRepository):
    """Read fundamentals from PostgreSQL.

    This repository queries the income_statements, balance_sheets,
    and cash_flows tables.
    """

    def __init__(self, database_url: str) -> None:
        """Initialize with database URL."""
        self.database_url = database_url

    @property
    def provider_name(self) -> str:
        """Return provider name."""
        return "database"

    async def get_income_statements(
        self,
        ticker: str,
        timeframe: Literal["quarterly", "annual"],
        limit: int = 4,
    ) -> list[IncomeStatement]:
        """Get income statements from database.

        Args:
            ticker: Stock symbol
            timeframe: "quarterly" or "annual"
            limit: Maximum number of periods

        Returns:
            List of IncomeStatement objects, sorted by period_end descending
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_income_sync, ticker, timeframe, limit)

    def _get_income_sync(
        self,
        ticker: str,
        timeframe: Literal["quarterly", "annual"],
        limit: int,
    ) -> list[IncomeStatement]:
        """Synchronous implementation of get_income_statements."""
        query = """
            SELECT *
            FROM income_statements
            WHERE ticker = %s AND timeframe = %s
            ORDER BY period_end DESC
            LIMIT %s
        """
        with _get_connection(self.database_url) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (ticker.upper(), timeframe, limit))
                rows = cur.fetchall()

        return [self._row_to_income(row) for row in rows]

    def _row_to_income(self, row: dict[str, Any]) -> IncomeStatement:
        """Convert database row to IncomeStatement domain model."""
        return IncomeStatement(
            ticker=row["ticker"],
            period_end=row["period_end"],
            timeframe=row.get("timeframe", "quarterly"),
            fiscal_year=row.get("fiscal_year") or row["period_end"].year,
            fiscal_quarter=row.get("fiscal_quarter"),
            revenue=_to_decimal(row.get("revenue")),
            cost_of_revenue=_to_decimal(row.get("cost_of_revenue")),
            gross_profit=_to_decimal(row.get("gross_profit")),
            research_development=_to_decimal(row.get("research_development")),
            selling_general_administrative=_to_decimal(row.get("selling_general_administrative")),
            operating_income=_to_decimal(row.get("operating_income")),
            net_income=_to_decimal(row.get("net_income")),
            basic_eps=_to_decimal(row.get("basic_eps")),
            diluted_eps=_to_decimal(row.get("diluted_eps")),
        )

    async def get_balance_sheets(
        self,
        ticker: str,
        timeframe: Literal["quarterly", "annual"],
        limit: int = 4,
    ) -> list[BalanceSheet]:
        """Get balance sheets from database.

        Args:
            ticker: Stock symbol
            timeframe: "quarterly" or "annual"
            limit: Maximum number of periods

        Returns:
            List of BalanceSheet objects, sorted by period_end descending
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_balance_sync, ticker, timeframe, limit)

    def _get_balance_sync(
        self,
        ticker: str,
        timeframe: Literal["quarterly", "annual"],
        limit: int,
    ) -> list[BalanceSheet]:
        """Synchronous implementation of get_balance_sheets."""
        query = """
            SELECT *
            FROM balance_sheets
            WHERE ticker = %s AND timeframe = %s
            ORDER BY period_end DESC
            LIMIT %s
        """
        with _get_connection(self.database_url) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (ticker.upper(), timeframe, limit))
                rows = cur.fetchall()

        return [self._row_to_balance(row) for row in rows]

    def _row_to_balance(self, row: dict[str, Any]) -> BalanceSheet:
        """Convert database row to BalanceSheet domain model."""
        return BalanceSheet(
            ticker=row["ticker"],
            period_end=row["period_end"],
            timeframe=row.get("timeframe", "quarterly"),
            fiscal_year=row.get("fiscal_year"),
            fiscal_quarter=row.get("fiscal_quarter"),
            total_assets=_to_decimal(row.get("total_assets")),
            total_current_assets=_to_decimal(row.get("total_current_assets")),
            cash_and_equivalents=_to_decimal(row.get("cash_and_equivalents")),
            total_liabilities=_to_decimal(row.get("total_liabilities")),
            total_current_liabilities=_to_decimal(row.get("total_current_liabilities")),
            long_term_debt=_to_decimal(row.get("long_term_debt")),
            total_equity=_to_decimal(row.get("total_equity")),
            retained_earnings=_to_decimal(row.get("retained_earnings")),
        )

    async def get_cash_flows(
        self,
        ticker: str,
        timeframe: Literal["quarterly", "annual"],
        limit: int = 4,
    ) -> list[CashFlow]:
        """Get cash flow statements from database.

        Args:
            ticker: Stock symbol
            timeframe: "quarterly" or "annual"
            limit: Maximum number of periods

        Returns:
            List of CashFlow objects, sorted by period_end descending
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_cashflow_sync, ticker, timeframe, limit)

    def _get_cashflow_sync(
        self,
        ticker: str,
        timeframe: Literal["quarterly", "annual"],
        limit: int,
    ) -> list[CashFlow]:
        """Synchronous implementation of get_cash_flows."""
        query = """
            SELECT *
            FROM cash_flows
            WHERE ticker = %s AND timeframe = %s
            ORDER BY period_end DESC
            LIMIT %s
        """
        with _get_connection(self.database_url) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (ticker.upper(), timeframe, limit))
                rows = cur.fetchall()

        return [self._row_to_cashflow(row) for row in rows]

    def _row_to_cashflow(self, row: dict[str, Any]) -> CashFlow:
        """Convert database row to CashFlow domain model."""
        return CashFlow(
            ticker=row["ticker"],
            period_end=row["period_end"],
            timeframe=row.get("timeframe", "quarterly"),
            fiscal_year=row.get("fiscal_year"),
            fiscal_quarter=row.get("fiscal_quarter"),
            operating_cash_flow=_to_decimal(row.get("operating_cash_flow")),
            capital_expenditure=_to_decimal(row.get("capital_expenditure")),
            dividends_paid=_to_decimal(row.get("dividends_paid")),
            free_cash_flow=_to_decimal(row.get("free_cash_flow")),
        )


class DatabaseCompanyRepository(CompanyRepository):
    """Read company info from PostgreSQL.

    This repository queries the companies table which contains
    company metadata loaded during coldstart.
    """

    def __init__(self, database_url: str) -> None:
        """Initialize with database URL."""
        self.database_url = database_url

    @property
    def provider_name(self) -> str:
        """Return provider name."""
        return "database"

    async def get_company_info(self, ticker: str) -> CompanyInfo | None:
        """Get company overview.

        Args:
            ticker: Stock symbol

        Returns:
            CompanyInfo object, or None if not found
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_company_sync, ticker)

    def _get_company_sync(self, ticker: str) -> CompanyInfo | None:
        """Synchronous implementation of get_company_info."""
        query = "SELECT * FROM companies WHERE ticker = %s"
        with _get_connection(self.database_url) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (ticker.upper(),))
                row = cur.fetchone()

        if not row:
            return None

        return CompanyInfo(
            ticker=row["ticker"],
            name=row["name"],
            description=row.get("description"),
            sector=row.get("sector"),
            industry=row.get("industry"),
            market_cap=_to_decimal(row.get("market_cap")),
            employees=row.get("employees"),
            website=row.get("website"),
            ceo=row.get("ceo"),
            headquarters=row.get("headquarters"),
        )

    async def search_companies(
        self,
        query: str,
        limit: int = 20,
    ) -> list[CompanyInfo]:
        """Search companies by name or ticker.

        Args:
            query: Search query (matches ticker or name)
            limit: Maximum number of results

        Returns:
            List of matching CompanyInfo objects
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._search_sync, query, limit)

    def _search_sync(self, query: str, limit: int) -> list[CompanyInfo]:
        """Synchronous implementation of search_companies."""
        sql = """
            SELECT * FROM companies
            WHERE ticker ILIKE %s OR name ILIKE %s
            ORDER BY ticker
            LIMIT %s
        """
        pattern = f"%{query}%"
        with _get_connection(self.database_url) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(sql, (pattern, pattern, limit))
                rows = cur.fetchall()

        return [
            CompanyInfo(
                ticker=r["ticker"],
                name=r["name"],
                description=r.get("description"),
                sector=r.get("sector"),
                industry=r.get("industry"),
                market_cap=_to_decimal(r.get("market_cap")),
                employees=r.get("employees"),
                website=r.get("website"),
            )
            for r in rows
        ]


class DatabaseRatiosRepository(RatiosRepository):
    """Read financial ratios from PostgreSQL.

    This repository queries the financial_ratios table.
    """

    def __init__(self, database_url: str) -> None:
        """Initialize with database URL."""
        self.database_url = database_url

    @property
    def provider_name(self) -> str:
        """Return provider name."""
        return "database"

    async def get_ratios(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
    ) -> list[FinancialRatio]:
        """Get financial ratios for a ticker.

        Args:
            ticker: Stock symbol
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of FinancialRatio objects, sorted by date ascending
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_ratios_sync, ticker, start_date, end_date)

    def _get_ratios_sync(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
    ) -> list[FinancialRatio]:
        """Synchronous implementation of get_ratios."""
        query = """
            SELECT *
            FROM financial_ratios
            WHERE ticker = %s AND date BETWEEN %s AND %s
            ORDER BY date
        """
        with _get_connection(self.database_url) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (ticker.upper(), start_date, end_date))
                rows = cur.fetchall()

        return [self._row_to_ratio(row) for row in rows]

    def _row_to_ratio(self, row: dict[str, Any]) -> FinancialRatio:
        """Convert database row to FinancialRatio domain model."""
        return FinancialRatio(
            ticker=row["ticker"],
            date=row["date"],
            pe_ratio=_to_decimal(row.get("pe_ratio")),
            pb_ratio=_to_decimal(row.get("pb_ratio")),
            ps_ratio=_to_decimal(row.get("ps_ratio")),
            peg_ratio=_to_decimal(row.get("peg_ratio")),
            roe=_to_decimal(row.get("roe")),
            roa=_to_decimal(row.get("roa")),
            profit_margin=_to_decimal(row.get("profit_margin")),
            operating_margin=_to_decimal(row.get("operating_margin")),
            current_ratio=_to_decimal(row.get("current_ratio")),
            quick_ratio=_to_decimal(row.get("quick_ratio")),
            debt_to_equity=_to_decimal(row.get("debt_to_equity")),
            debt_to_assets=_to_decimal(row.get("debt_to_assets")),
            asset_turnover=_to_decimal(row.get("asset_turnover")),
            inventory_turnover=_to_decimal(row.get("inventory_turnover")),
        )

    async def get_latest_ratio(self, ticker: str) -> FinancialRatio | None:
        """Get most recent ratio for a ticker.

        Args:
            ticker: Stock symbol

        Returns:
            Most recent FinancialRatio, or None if not found
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_latest_sync, ticker)

    def _get_latest_sync(self, ticker: str) -> FinancialRatio | None:
        """Synchronous implementation of get_latest_ratio."""
        query = """
            SELECT *
            FROM financial_ratios
            WHERE ticker = %s
            ORDER BY date DESC
            LIMIT 1
        """
        with _get_connection(self.database_url) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (ticker.upper(),))
                row = cur.fetchone()

        return self._row_to_ratio(row) if row else None


class DatabaseEconomyRepository(EconomyRepository):
    """Read economy data from PostgreSQL.

    This repository queries the treasury_yields, inflation,
    and labor_market tables.
    """

    def __init__(self, database_url: str) -> None:
        """Initialize with database URL."""
        self.database_url = database_url

    @property
    def provider_name(self) -> str:
        """Return provider name."""
        return "database"

    async def get_treasury_yields(
        self,
        start_date: date,
        end_date: date,
    ) -> list[TreasuryYield]:
        """Get treasury yield data.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of TreasuryYield objects, sorted by date ascending
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_yields_sync, start_date, end_date)

    def _get_yields_sync(
        self,
        start_date: date,
        end_date: date,
    ) -> list[TreasuryYield]:
        """Synchronous implementation of get_treasury_yields."""
        query = """
            SELECT *
            FROM treasury_yields
            WHERE date BETWEEN %s AND %s
            ORDER BY date
        """
        with _get_connection(self.database_url) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (start_date, end_date))
                rows = cur.fetchall()

        return [self._row_to_yield(row) for row in rows]

    def _row_to_yield(self, row: dict[str, Any]) -> TreasuryYield:
        """Convert database row to TreasuryYield domain model."""
        return TreasuryYield(
            date=row["date"],
            yield_1mo=_to_decimal(row.get("yield_1mo")),
            yield_3mo=_to_decimal(row.get("yield_3mo")),
            yield_6mo=_to_decimal(row.get("yield_6mo")),
            yield_1yr=_to_decimal(row.get("yield_1yr")),
            yield_2yr=_to_decimal(row.get("yield_2yr")),
            yield_5yr=_to_decimal(row.get("yield_5yr")),
            yield_10yr=_to_decimal(row.get("yield_10yr")),
            yield_30yr=_to_decimal(row.get("yield_30yr")),
        )

    async def get_inflation(
        self,
        start_date: date,
        end_date: date,
        indicator: str | None = None,
    ) -> list[InflationData]:
        """Get inflation data.

        DEPRECATED: This method is not implemented. The inflation table uses
        a wide schema (separate columns per indicator) but this method expects
        a narrow schema (indicator column). Use direct SQL queries instead:

            SELECT date, cpi, cpi_core, cpi_year_over_year, pce, pce_core
            FROM inflation WHERE date BETWEEN %s AND %s

        See mcp_server/tools/economy.py for working examples.

        Raises:
            NotImplementedError: Always raised - method not compatible with schema
        """
        raise NotImplementedError(
            "get_inflation() is not implemented. The inflation table uses a wide "
            "schema with separate columns (cpi, pce, etc.) not a narrow schema "
            "with an 'indicator' column. Use direct SQL queries instead. "
            "See mcp_server/tools/economy.py for examples."
        )

    async def get_labor_market(
        self,
        start_date: date,
        end_date: date,
        indicator: str | None = None,
    ) -> list[LaborMarketData]:
        """Get labor market data.

        DEPRECATED: This method is not implemented. The labor_market table uses
        a wide schema (separate columns per indicator) but this method expects
        a narrow schema (indicator column). Use direct SQL queries instead:

            SELECT date, unemployment_rate, labor_force_participation_rate,
                   avg_hourly_earnings, job_openings
            FROM labor_market WHERE date BETWEEN %s AND %s

        See mcp_server/tools/economy.py for working examples.

        Raises:
            NotImplementedError: Always raised - method not compatible with schema
        """
        raise NotImplementedError(
            "get_labor_market() is not implemented. The labor_market table uses a wide "
            "schema with separate columns (unemployment_rate, job_openings, etc.) not a "
            "narrow schema with an 'indicator' column. Use direct SQL queries instead. "
            "See mcp_server/tools/economy.py for examples."
        )


class DatabaseNewsRepository(NewsRepository):
    """Read news and sentiment data from PostgreSQL.

    This repository queries the news_articles, news_article_tickers,
    and news_sentiment tables.
    """

    def __init__(self, database_url: str) -> None:
        """Initialize with database URL."""
        self.database_url = database_url

    @property
    def provider_name(self) -> str:
        """Return provider name."""
        return "database"

    async def get_news(
        self,
        ticker: str,
        limit: int = 20,
        days_back: int = 30,
    ) -> list[NewsArticle]:
        """Get news articles for a ticker with sentiment.

        Args:
            ticker: Stock symbol
            limit: Maximum number of articles to return
            days_back: Number of days to look back

        Returns:
            List of NewsArticle objects, sorted by date descending
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_news_sync, ticker, limit, days_back)

    def _get_news_sync(
        self,
        ticker: str,
        limit: int,
        days_back: int,
    ) -> list[NewsArticle]:
        """Synchronous implementation of get_news."""
        ticker = ticker.upper()
        query = """
            SELECT
                na.id,
                na.title,
                na.author,
                na.description,
                na.article_url,
                na.published_utc,
                na.publisher_name,
                ns.sentiment,
                ns.sentiment_reasoning
            FROM news_articles na
            JOIN news_article_tickers nat ON na.id = nat.article_id
            LEFT JOIN news_sentiment ns ON na.id = ns.article_id AND nat.ticker = ns.ticker
            WHERE nat.ticker = %s
              AND na.published_utc >= NOW() - INTERVAL '%s days'
            ORDER BY na.published_utc DESC
            LIMIT %s
        """
        with _get_connection(self.database_url) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (ticker, days_back, limit))
                rows = cur.fetchall()

        return [self._row_to_article(row) for row in rows]

    def _row_to_article(self, row: dict[str, Any]) -> NewsArticle:
        """Convert database row to NewsArticle domain model."""
        return NewsArticle(
            id=row["id"],
            title=row["title"],
            published_utc=row["published_utc"],
            author=row.get("author"),
            description=row.get("description"),
            article_url=row.get("article_url"),
            publisher_name=row.get("publisher_name"),
            sentiment=row.get("sentiment"),
            sentiment_reasoning=row.get("sentiment_reasoning"),
        )

    async def get_news_sentiment_summary(
        self,
        ticker: str,
        days: int = 30,
    ) -> dict[str, int]:
        """Get sentiment summary counts for a ticker.

        Args:
            ticker: Stock symbol
            days: Number of days to look back

        Returns:
            Dict mapping sentiment label to count (e.g., {"positive": 5, "negative": 2})
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_sentiment_summary_sync, ticker, days)

    def _get_sentiment_summary_sync(
        self,
        ticker: str,
        days: int,
    ) -> dict[str, int]:
        """Synchronous implementation of get_news_sentiment_summary."""
        ticker = ticker.upper()
        query = """
            SELECT
                ns.sentiment,
                COUNT(*) as count
            FROM news_sentiment ns
            JOIN news_articles na ON ns.article_id = na.id
            WHERE ns.ticker = %s
              AND na.published_utc >= NOW() - INTERVAL '%s days'
            GROUP BY ns.sentiment
        """
        with _get_connection(self.database_url) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (ticker, days))
                rows = cur.fetchall()

        return {row["sentiment"]: row["count"] for row in rows if row.get("sentiment")}

    def get_news_stream(
        self,
        tickers: list[str],
    ) -> AsyncIterator[NewsArticle]:
        """Stream news as it arrives (not implemented for database).

        Note: Streaming is not supported for database repository.
        Use Polygon API repository for real-time news streaming.

        Args:
            tickers: List of stock symbols to monitor

        Raises:
            NotImplementedError: Database doesn't support real-time streaming
        """
        raise NotImplementedError("News streaming not supported for database repository")


class DatabaseTechnicalIndicatorsRepository(TechnicalIndicatorsRepository):
    """Read technical indicators from PostgreSQL.

    This repository queries the technical_indicators table which contains
    calculated indicators (SMA, RSI, MACD, etc.) from OHLCV data.
    """

    def __init__(self, database_url: str) -> None:
        """Initialize with database URL."""
        self.database_url = database_url

    @property
    def provider_name(self) -> str:
        """Return provider name."""
        return "database"

    # Valid indicator columns for filtering
    VALID_INDICATORS = {
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

    async def get_indicators(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
    ) -> list[TechnicalIndicators]:
        """Get technical indicators for a ticker.

        Args:
            ticker: Stock symbol
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of TechnicalIndicators objects, sorted by date ascending
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self._get_indicators_sync, ticker, start_date, end_date
        )

    def _get_indicators_sync(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
    ) -> list[TechnicalIndicators]:
        """Synchronous implementation of get_indicators."""
        query = """
            SELECT *
            FROM technical_indicators
            WHERE ticker = %s AND date BETWEEN %s AND %s
            ORDER BY date
        """
        with _get_connection(self.database_url) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (ticker.upper(), start_date, end_date))
                rows = cur.fetchall()

        return [self._row_to_indicators(row) for row in rows]

    def _row_to_indicators(self, row: dict[str, Any]) -> TechnicalIndicators:
        """Convert database row to TechnicalIndicators domain model."""
        return TechnicalIndicators(
            ticker=row["ticker"],
            date=row["date"],
            # Trend - SMAs
            sma_5=_to_decimal(row.get("sma_5")),
            sma_10=_to_decimal(row.get("sma_10")),
            sma_20=_to_decimal(row.get("sma_20")),
            sma_50=_to_decimal(row.get("sma_50")),
            sma_100=_to_decimal(row.get("sma_100")),
            sma_150=_to_decimal(row.get("sma_150")),
            sma_200=_to_decimal(row.get("sma_200")),
            # Trend - EMAs
            ema_12=_to_decimal(row.get("ema_12")),
            ema_26=_to_decimal(row.get("ema_26")),
            ema_50=_to_decimal(row.get("ema_50")),
            ema_100=_to_decimal(row.get("ema_100")),
            ema_200=_to_decimal(row.get("ema_200")),
            vwap=_to_decimal(row.get("vwap")),
            # Momentum
            rsi_14=_to_decimal(row.get("rsi_14")),
            rsi_21=_to_decimal(row.get("rsi_21")),
            macd_line=_to_decimal(row.get("macd_line")),
            macd_signal=_to_decimal(row.get("macd_signal")),
            macd_histogram=_to_decimal(row.get("macd_histogram")),
            # Volatility
            bb_upper=_to_decimal(row.get("bb_upper")),
            bb_middle=_to_decimal(row.get("bb_middle")),
            bb_lower=_to_decimal(row.get("bb_lower")),
            atr_14=_to_decimal(row.get("atr_14")),
            # Volume
            obv=row.get("obv"),
            volume_sma_20=row.get("volume_sma_20"),
            volume_ratio=_to_decimal(row.get("volume_ratio")),
        )

    async def get_latest_indicators(self, ticker: str) -> TechnicalIndicators | None:
        """Get most recent technical indicators for a ticker.

        Args:
            ticker: Stock symbol

        Returns:
            Most recent TechnicalIndicators, or None if not found
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_latest_sync, ticker)

    def _get_latest_sync(self, ticker: str) -> TechnicalIndicators | None:
        """Synchronous implementation of get_latest_indicators."""
        query = """
            SELECT *
            FROM technical_indicators
            WHERE ticker = %s
            ORDER BY date DESC
            LIMIT 1
        """
        with _get_connection(self.database_url) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (ticker.upper(),))
                row = cur.fetchone()

        return self._row_to_indicators(row) if row else None

    async def screen_by_indicators(
        self,
        filters: dict[str, tuple[float | None, float | None]],
        target_date: date | None = None,
        index: str | None = None,
        limit: int = 100,
    ) -> list[TechnicalIndicators]:
        """Screen stocks by technical indicator values.

        Args:
            filters: Dict mapping indicator name to (min, max) tuple.
            target_date: Date to screen (defaults to most recent)
            index: Filter by index membership (sp500, nasdaq100)
            limit: Maximum number of results

        Returns:
            List of TechnicalIndicators matching all filters
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self._screen_sync, filters, target_date, index, limit
        )

    def _screen_sync(
        self,
        filters: dict[str, tuple[float | None, float | None]],
        target_date: date | None,
        index: str | None,
        limit: int,
    ) -> list[TechnicalIndicators]:
        """Synchronous implementation of screen_by_indicators."""
        limit = min(limit, 500)

        # Build WHERE conditions
        conditions = []
        params: list[Any] = []

        if target_date:
            conditions.append("date = %s")
            params.append(target_date)
        else:
            # Use most recent date
            conditions.append("date = (SELECT MAX(date) FROM technical_indicators)")

        # Index filter
        if index:
            conditions.append("""ticker IN (
                SELECT ic.ticker FROM index_constituents ic
                JOIN indices i ON ic.index_id = i.id
                WHERE i.code = %s
            )""")
            params.append(index.lower())

        # Add filter conditions
        for indicator, (min_val, max_val) in filters.items():
            if indicator not in self.VALID_INDICATORS:
                continue

            if min_val is not None and max_val is not None:
                conditions.append(f"{indicator} BETWEEN %s AND %s")
                params.extend([min_val, max_val])
            elif min_val is not None:
                conditions.append(f"{indicator} >= %s")
                params.append(min_val)
            elif max_val is not None:
                conditions.append(f"{indicator} <= %s")
                params.append(max_val)

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        params.append(limit)

        query = f"""
            SELECT *
            FROM technical_indicators
            WHERE {where_clause}
            ORDER BY ticker
            LIMIT %s
        """

        with _get_connection(self.database_url) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, params)
                rows = cur.fetchall()

        return [self._row_to_indicators(row) for row in rows]


class DatabaseIndexRepository(IndexRepository):
    """Read market index data from PostgreSQL.

    This repository queries the indices and index_constituents tables
    which track market index membership (S&P 500, NASDAQ-100, etc.).
    """

    def __init__(self, database_url: str) -> None:
        """Initialize with database URL."""
        self.database_url = database_url

    @property
    def provider_name(self) -> str:
        """Return provider name."""
        return "database"

    async def list_indices(self) -> list[MarketIndex]:
        """List all available market indices.

        Returns:
            List of MarketIndex objects with constituent counts
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._list_indices_sync)

    def _list_indices_sync(self) -> list[MarketIndex]:
        """Synchronous implementation of list_indices."""
        query = """
            SELECT
                i.id,
                i.code,
                i.name,
                i.description,
                i.source_url,
                i.last_updated,
                COUNT(ic.ticker) as constituent_count
            FROM indices i
            LEFT JOIN index_constituents ic ON i.id = ic.index_id
            GROUP BY i.id, i.code, i.name, i.description, i.source_url, i.last_updated
            ORDER BY i.name
        """
        with _get_connection(self.database_url) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query)
                rows = cur.fetchall()

        return [self._row_to_index(row) for row in rows]

    def _row_to_index(self, row: dict[str, Any]) -> MarketIndex:
        """Convert database row to MarketIndex domain model."""
        return MarketIndex(
            id=row["id"],
            code=row["code"],
            name=row["name"],
            description=row.get("description"),
            source_url=row.get("source_url"),
            last_updated=row.get("last_updated"),
            constituent_count=row.get("constituent_count", 0),
        )

    async def get_index(self, code: str) -> MarketIndex | None:
        """Get a specific index by code.

        Args:
            code: Index code (e.g., 'sp500', 'nasdaq100')

        Returns:
            MarketIndex object, or None if not found
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_index_sync, code)

    def _get_index_sync(self, code: str) -> MarketIndex | None:
        """Synchronous implementation of get_index."""
        query = """
            SELECT
                i.id,
                i.code,
                i.name,
                i.description,
                i.source_url,
                i.last_updated,
                COUNT(ic.ticker) as constituent_count
            FROM indices i
            LEFT JOIN index_constituents ic ON i.id = ic.index_id
            WHERE i.code = %s
            GROUP BY i.id, i.code, i.name, i.description, i.source_url, i.last_updated
        """
        with _get_connection(self.database_url) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (code.lower(),))
                row = cur.fetchone()

        return self._row_to_index(row) if row else None

    async def get_constituents(self, code: str) -> list[str]:
        """Get all tickers in an index.

        Args:
            code: Index code (e.g., 'sp500', 'nasdaq100')

        Returns:
            List of ticker symbols in the index
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_constituents_sync, code)

    def _get_constituents_sync(self, code: str) -> list[str]:
        """Synchronous implementation of get_constituents."""
        query = """
            SELECT ic.ticker
            FROM index_constituents ic
            JOIN indices i ON ic.index_id = i.id
            WHERE i.code = %s
            ORDER BY ic.ticker
        """
        with _get_connection(self.database_url) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (code.lower(),))
                rows = cur.fetchall()

        return [row["ticker"] for row in rows]

    async def is_member(self, ticker: str, index_code: str) -> bool:
        """Check if a ticker is a member of an index.

        Args:
            ticker: Stock symbol
            index_code: Index code

        Returns:
            True if ticker is in the index
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._is_member_sync, ticker, index_code)

    def _is_member_sync(self, ticker: str, index_code: str) -> bool:
        """Synchronous implementation of is_member."""
        query = """
            SELECT 1
            FROM index_constituents ic
            JOIN indices i ON ic.index_id = i.id
            WHERE ic.ticker = %s AND i.code = %s
        """
        with _get_connection(self.database_url) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (ticker.upper(), index_code.lower()))
                row = cur.fetchone()

        return row is not None

    async def get_ticker_indices(self, ticker: str) -> list[str]:
        """Get all indices a ticker belongs to.

        Args:
            ticker: Stock symbol

        Returns:
            List of index codes the ticker is a member of
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_ticker_indices_sync, ticker)

    def _get_ticker_indices_sync(self, ticker: str) -> list[str]:
        """Synchronous implementation of get_ticker_indices."""
        query = """
            SELECT i.code
            FROM indices i
            JOIN index_constituents ic ON i.id = ic.index_id
            WHERE ic.ticker = %s
            ORDER BY i.name
        """
        with _get_connection(self.database_url) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query, (ticker.upper(),))
                rows = cur.fetchall()

        return [row["code"] for row in rows]

    async def update_constituents(self, code: str, tickers: list[str]) -> int:
        """Update index constituents (replace existing).

        Args:
            code: Index code
            tickers: List of ticker symbols to set as constituents

        Returns:
            Number of constituents added

        Raises:
            ValueError: If index code not found
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._update_constituents_sync, code, tickers)

    def _update_constituents_sync(self, code: str, tickers: list[str]) -> int:
        """Synchronous implementation of update_constituents."""
        with _get_connection(self.database_url) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                # Get index ID
                cur.execute("SELECT id FROM indices WHERE code = %s", (code.lower(),))
                row = cur.fetchone()
                if not row:
                    raise ValueError(f"Index not found: {code}")
                index_id = row["id"]

                # Delete existing constituents
                cur.execute("DELETE FROM index_constituents WHERE index_id = %s", (index_id,))

                # Insert new constituents (only those that exist in companies table)
                added = 0
                for ticker in tickers:
                    ticker_upper = ticker.upper()
                    cur.execute(
                        """
                        INSERT INTO index_constituents (index_id, ticker)
                        SELECT %s, %s
                        WHERE EXISTS (SELECT 1 FROM companies WHERE ticker = %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (index_id, ticker_upper, ticker_upper),
                    )
                    if cur.rowcount > 0:
                        added += 1

                # Update last_updated timestamp
                cur.execute(
                    "UPDATE indices SET last_updated = CURRENT_TIMESTAMP WHERE id = %s",
                    (index_id,),
                )

                conn.commit()

        return added
