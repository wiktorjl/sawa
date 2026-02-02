"""Stock and market data queries."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sawa_tui.database import execute_query


@dataclass(frozen=True, slots=True)
class Company:
    """Company details."""

    ticker: str
    name: str
    description: str | None = None
    sector: str | None = None
    market_cap: float | None = None
    employees: int | None = None
    exchange: str | None = None
    cik: str | None = None
    homepage_url: str | None = None
    address: str | None = None
    active: bool = True

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "Company":
        """Create Company from database row."""
        address_parts = [
            row.get("address_address1"),
            row.get("address_city"),
            row.get("address_state"),
            row.get("address_postal_code"),
        ]
        address = ", ".join(p for p in address_parts if p)

        return cls(
            ticker=row["ticker"],
            name=row.get("name", ""),
            description=row.get("description"),
            sector=row.get("sic_description"),
            market_cap=float(row["market_cap"]) if row.get("market_cap") else None,
            employees=row.get("total_employees"),
            exchange=row.get("primary_exchange"),
            cik=row.get("cik"),
            homepage_url=row.get("homepage_url"),
            address=address if address else None,
            active=row.get("active", True),
        )


@dataclass(frozen=True, slots=True)
class StockPrice:
    """Daily stock price data."""

    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "StockPrice":
        """Create StockPrice from database row."""
        return cls(
            date=row["date"],
            open=float(row["open"]) if row.get("open") else 0,
            high=float(row["high"]) if row.get("high") else 0,
            low=float(row["low"]) if row.get("low") else 0,
            close=float(row["close"]) if row.get("close") else 0,
            volume=int(row["volume"]) if row.get("volume") else 0,
        )


@dataclass(frozen=True, slots=True)
class FinancialRatios:
    """Financial ratios and metrics."""

    date: date
    price: float | None = None
    pe_ratio: float | None = None
    pb_ratio: float | None = None
    ps_ratio: float | None = None
    debt_to_equity: float | None = None
    roe: float | None = None
    roa: float | None = None
    dividend_yield: float | None = None
    eps: float | None = None
    market_cap: float | None = None
    ev: float | None = None
    ev_to_ebitda: float | None = None
    ev_to_sales: float | None = None
    fcf: float | None = None
    current_ratio: float | None = None
    quick_ratio: float | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "FinancialRatios":
        """Create FinancialRatios from database row."""

        def to_float(val: Any) -> float | None:
            if val is None:
                return None
            if isinstance(val, Decimal):
                return float(val)
            return float(val)

        return cls(
            date=row["date"],
            price=to_float(row.get("price")),
            pe_ratio=to_float(row.get("price_to_earnings")),
            pb_ratio=to_float(row.get("price_to_book")),
            ps_ratio=to_float(row.get("price_to_sales")),
            debt_to_equity=to_float(row.get("debt_to_equity")),
            roe=to_float(row.get("return_on_equity")),
            roa=to_float(row.get("return_on_assets")),
            dividend_yield=to_float(row.get("dividend_yield")),
            eps=to_float(row.get("earnings_per_share")),
            market_cap=to_float(row.get("market_cap")),
            ev=to_float(row.get("enterprise_value")),
            ev_to_ebitda=to_float(row.get("ev_to_ebitda")),
            ev_to_sales=to_float(row.get("ev_to_sales")),
            fcf=to_float(row.get("free_cash_flow")),
            current_ratio=to_float(row.get("current")),
            quick_ratio=to_float(row.get("quick")),
        )


@dataclass(frozen=True, slots=True)
class IncomeStatement:
    """Income statement data."""

    period_end: date
    timeframe: str
    fiscal_year: int | None
    fiscal_quarter: int | None
    revenue: float | None = None
    cost_of_revenue: float | None = None
    gross_profit: float | None = None
    operating_income: float | None = None
    net_income: float | None = None
    eps: float | None = None
    ebitda: float | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "IncomeStatement":
        """Create IncomeStatement from database row."""

        def to_float(val: Any) -> float | None:
            if val is None:
                return None
            return float(val)

        return cls(
            period_end=row["period_end"],
            timeframe=row.get("timeframe", "quarterly"),
            fiscal_year=row.get("fiscal_year"),
            fiscal_quarter=row.get("fiscal_quarter"),
            revenue=to_float(row.get("revenue")),
            cost_of_revenue=to_float(row.get("cost_of_revenue")),
            gross_profit=to_float(row.get("gross_profit")),
            operating_income=to_float(row.get("operating_income")),
            net_income=to_float(row.get("net_income_loss_attributable_common_shareholders")),
            eps=to_float(row.get("diluted_earnings_per_share")),
            ebitda=to_float(row.get("ebitda")),
        )


@dataclass(frozen=True, slots=True)
class BalanceSheet:
    """Balance sheet data."""

    period_end: date
    timeframe: str
    fiscal_year: int | None
    fiscal_quarter: int | None
    total_assets: float | None = None
    total_liabilities: float | None = None
    total_equity: float | None = None
    cash: float | None = None
    total_debt: float | None = None
    current_assets: float | None = None
    current_liabilities: float | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "BalanceSheet":
        """Create BalanceSheet from database row."""

        def to_float(val: Any) -> float | None:
            if val is None:
                return None
            return float(val)

        total_debt = None
        debt_current = to_float(row.get("debt_current"))
        debt_lt = to_float(row.get("long_term_debt_and_capital_lease_obligations"))
        if debt_current is not None or debt_lt is not None:
            total_debt = (debt_current or 0) + (debt_lt or 0)

        return cls(
            period_end=row["period_end"],
            timeframe=row.get("timeframe", "quarterly"),
            fiscal_year=row.get("fiscal_year"),
            fiscal_quarter=row.get("fiscal_quarter"),
            total_assets=to_float(row.get("total_assets")),
            total_liabilities=to_float(row.get("total_liabilities")),
            total_equity=to_float(row.get("total_equity")),
            cash=to_float(row.get("cash_and_equivalents")),
            total_debt=total_debt,
            current_assets=to_float(row.get("total_current_assets")),
            current_liabilities=to_float(row.get("total_current_liabilities")),
        )


@dataclass(frozen=True, slots=True)
class CashFlow:
    """Cash flow statement data."""

    period_end: date
    timeframe: str
    fiscal_year: int | None
    fiscal_quarter: int | None
    operating_cash_flow: float | None = None
    investing_cash_flow: float | None = None
    financing_cash_flow: float | None = None
    net_change: float | None = None
    capex: float | None = None
    dividends: float | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "CashFlow":
        """Create CashFlow from database row."""

        def to_float(val: Any) -> float | None:
            if val is None:
                return None
            return float(val)

        return cls(
            period_end=row["period_end"],
            timeframe=row.get("timeframe", "quarterly"),
            fiscal_year=row.get("fiscal_year"),
            fiscal_quarter=row.get("fiscal_quarter"),
            operating_cash_flow=to_float(row.get("net_cash_from_operating_activities")),
            investing_cash_flow=to_float(row.get("net_cash_from_investing_activities")),
            financing_cash_flow=to_float(row.get("net_cash_from_financing_activities")),
            net_change=to_float(row.get("change_in_cash_and_equivalents")),
            capex=to_float(row.get("purchase_of_property_plant_and_equipment")),
            dividends=to_float(row.get("dividends")),
        )


@dataclass(frozen=True, slots=True)
class TreasuryYields:
    """Treasury yield curve data."""

    date: date
    yield_1m: float | None = None
    yield_3m: float | None = None
    yield_6m: float | None = None
    yield_1y: float | None = None
    yield_2y: float | None = None
    yield_5y: float | None = None
    yield_10y: float | None = None
    yield_30y: float | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "TreasuryYields":
        """Create TreasuryYields from database row."""

        def to_float(val: Any) -> float | None:
            if val is None:
                return None
            return float(val)

        return cls(
            date=row["date"],
            yield_1m=to_float(row.get("yield_1_month")),
            yield_3m=to_float(row.get("yield_3_month")),
            yield_6m=to_float(row.get("yield_6_month")),
            yield_1y=to_float(row.get("yield_1_year")),
            yield_2y=to_float(row.get("yield_2_year")),
            yield_5y=to_float(row.get("yield_5_year")),
            yield_10y=to_float(row.get("yield_10_year")),
            yield_30y=to_float(row.get("yield_30_year")),
        )


@dataclass(frozen=True, slots=True)
class Inflation:
    """Inflation data."""

    date: date
    cpi: float | None = None
    cpi_core: float | None = None
    cpi_yoy: float | None = None
    pce: float | None = None
    pce_core: float | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "Inflation":
        """Create Inflation from database row."""

        def to_float(val: Any) -> float | None:
            if val is None:
                return None
            return float(val)

        return cls(
            date=row["date"],
            cpi=to_float(row.get("cpi")),
            cpi_core=to_float(row.get("cpi_core")),
            cpi_yoy=to_float(row.get("cpi_year_over_year")),
            pce=to_float(row.get("pce")),
            pce_core=to_float(row.get("pce_core")),
        )


@dataclass(frozen=True, slots=True)
class LaborMarket:
    """Labor market data."""

    date: date
    unemployment_rate: float | None = None
    participation_rate: float | None = None
    avg_hourly_earnings: float | None = None
    job_openings: float | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "LaborMarket":
        """Create LaborMarket from database row."""

        def to_float(val: Any) -> float | None:
            if val is None:
                return None
            return float(val)

        return cls(
            date=row["date"],
            unemployment_rate=to_float(row.get("unemployment_rate")),
            participation_rate=to_float(row.get("labor_force_participation_rate")),
            avg_hourly_earnings=to_float(row.get("avg_hourly_earnings")),
            job_openings=to_float(row.get("job_openings")),
        )


@dataclass(frozen=True, slots=True)
class NewsArticle:
    """News article with sentiment."""

    id: str
    title: str
    published_utc: datetime
    author: str | None = None
    description: str | None = None
    article_url: str | None = None
    publisher_name: str | None = None
    sentiment: str | None = None  # positive, negative, neutral
    sentiment_reasoning: str | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "NewsArticle":
        """Create NewsArticle from database row."""
        return cls(
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


@dataclass(frozen=True, slots=True)
class ScreenerResult:
    """Unified data for screening."""

    ticker: str
    name: str
    sector: str
    price: float | None = None
    change_pct: float | None = None
    market_cap: float | None = None
    volume: int | None = None
    pe: float | None = None
    pb: float | None = None
    ps: float | None = None
    dividend_yield: float | None = None
    debt_to_equity: float | None = None
    roe: float | None = None
    eps: float | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "ScreenerResult":
        """Create from row."""

        def to_float(val: Any) -> float | None:
            if val is None:
                return None
            if isinstance(val, Decimal):
                return float(val)
            return float(val)

        return cls(
            ticker=row["ticker"],
            name=row["name"],
            sector=row.get("sic_description") or "Unknown",
            price=to_float(row.get("price")),
            change_pct=None,  # Need 2 days for this, maybe skip or calc later
            market_cap=to_float(row.get("market_cap")),
            volume=int(row.get("volume")) if row.get("volume") else None,
            pe=to_float(row.get("price_to_earnings")),
            pb=to_float(row.get("price_to_book")),
            ps=to_float(row.get("price_to_sales")),
            dividend_yield=to_float(row.get("dividend_yield")),
            debt_to_equity=to_float(row.get("debt_to_equity")),
            roe=to_float(row.get("return_on_equity")),
            eps=to_float(row.get("earnings_per_share")),
        )


class StockQueries:
    """Query methods for stock and market data."""

    @staticmethod
    def get_company(ticker: str) -> Company | None:
        """Get company details by ticker."""
        sql = """
            SELECT * FROM companies WHERE ticker = %(ticker)s
        """
        rows = execute_query(sql, {"ticker": ticker})
        return Company.from_row(rows[0]) if rows else None

    @staticmethod
    def search_companies(query: str, limit: int = 20) -> list[Company]:
        """Search companies by ticker or name."""
        sql = """
            SELECT * FROM companies
            WHERE ticker ILIKE %(q)s
               OR name ILIKE %(q)s
               OR sic_description ILIKE %(q)s
            ORDER BY
                CASE WHEN ticker ILIKE %(exact)s THEN 0 ELSE 1 END,
                market_cap DESC NULLS LAST
            LIMIT %(limit)s
        """
        rows = execute_query(sql, {"q": f"%{query}%", "exact": query, "limit": limit})
        return [Company.from_row(row) for row in rows]

    @staticmethod
    def list_companies(
        sector: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Company]:
        """List companies with optional sector filter."""
        if sector:
            sql = """
                SELECT * FROM companies
                WHERE sic_description ILIKE %(sector)s
                ORDER BY market_cap DESC NULLS LAST
                LIMIT %(limit)s OFFSET %(offset)s
            """
            params = {"sector": f"%{sector}%", "limit": limit, "offset": offset}
        else:
            sql = """
                SELECT * FROM companies
                ORDER BY market_cap DESC NULLS LAST
                LIMIT %(limit)s OFFSET %(offset)s
            """
            params = {"limit": limit, "offset": offset}
        rows = execute_query(sql, params)
        return [Company.from_row(row) for row in rows]

    @staticmethod
    def get_stock_prices(
        ticker: str,
        days: int = 60,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[StockPrice]:
        """Get stock prices for a ticker."""
        if start_date and end_date:
            sql = """
                SELECT date, open, high, low, close, volume
                FROM stock_prices
                WHERE ticker = %(ticker)s
                  AND date BETWEEN %(start)s AND %(end)s
                ORDER BY date DESC
            """
            params: dict[str, Any] = {"ticker": ticker, "start": start_date, "end": end_date}
        else:
            sql = """
                SELECT date, open, high, low, close, volume
                FROM stock_prices
                WHERE ticker = %(ticker)s
                ORDER BY date DESC
                LIMIT %(limit)s
            """
            params = {"ticker": ticker, "limit": days}
        rows = execute_query(sql, params)
        return [StockPrice.from_row(row) for row in rows]

    @staticmethod
    def get_latest_price(ticker: str) -> StockPrice | None:
        """Get the latest price for a ticker."""
        sql = """
            SELECT date, open, high, low, close, volume
            FROM stock_prices
            WHERE ticker = %(ticker)s
            ORDER BY date DESC
            LIMIT 1
        """
        rows = execute_query(sql, {"ticker": ticker})
        return StockPrice.from_row(rows[0]) if rows else None

    @staticmethod
    def get_financial_ratios(ticker: str, limit: int = 10) -> list[FinancialRatios]:
        """Get financial ratios for a ticker."""
        sql = """
            SELECT * FROM financial_ratios
            WHERE ticker = %(ticker)s
            ORDER BY date DESC
            LIMIT %(limit)s
        """
        rows = execute_query(sql, {"ticker": ticker, "limit": limit})
        return [FinancialRatios.from_row(row) for row in rows]

    @staticmethod
    def get_latest_ratios(ticker: str) -> FinancialRatios | None:
        """Get the latest financial ratios for a ticker."""
        ratios = StockQueries.get_financial_ratios(ticker, limit=1)
        return ratios[0] if ratios else None

    @staticmethod
    def get_income_statements(
        ticker: str,
        timeframe: str = "quarterly",
        limit: int = 8,
    ) -> list[IncomeStatement]:
        """Get income statements for a ticker."""
        sql = """
            SELECT * FROM income_statements
            WHERE ticker = %(ticker)s AND timeframe = %(timeframe)s
            ORDER BY period_end DESC
            LIMIT %(limit)s
        """
        rows = execute_query(sql, {"ticker": ticker, "timeframe": timeframe, "limit": limit})
        return [IncomeStatement.from_row(row) for row in rows]

    @staticmethod
    def get_balance_sheets(
        ticker: str,
        timeframe: str = "quarterly",
        limit: int = 8,
    ) -> list[BalanceSheet]:
        """Get balance sheets for a ticker."""
        sql = """
            SELECT * FROM balance_sheets
            WHERE ticker = %(ticker)s AND timeframe = %(timeframe)s
            ORDER BY period_end DESC
            LIMIT %(limit)s
        """
        rows = execute_query(sql, {"ticker": ticker, "timeframe": timeframe, "limit": limit})
        return [BalanceSheet.from_row(row) for row in rows]

    @staticmethod
    def get_cash_flows(
        ticker: str,
        timeframe: str = "quarterly",
        limit: int = 8,
    ) -> list[CashFlow]:
        """Get cash flow statements for a ticker."""
        sql = """
            SELECT * FROM cash_flows
            WHERE ticker = %(ticker)s AND timeframe = %(timeframe)s
            ORDER BY period_end DESC
            LIMIT %(limit)s
        """
        rows = execute_query(sql, {"ticker": ticker, "timeframe": timeframe, "limit": limit})
        return [CashFlow.from_row(row) for row in rows]

    @staticmethod
    def get_treasury_yields(limit: int = 30) -> list[TreasuryYields]:
        """Get treasury yields."""
        sql = """
            SELECT * FROM treasury_yields
            ORDER BY date DESC
            LIMIT %(limit)s
        """
        rows = execute_query(sql, {"limit": limit})
        return [TreasuryYields.from_row(row) for row in rows]

    @staticmethod
    def get_inflation(limit: int = 30) -> list[Inflation]:
        """Get inflation data."""
        sql = """
            SELECT * FROM inflation
            ORDER BY date DESC
            LIMIT %(limit)s
        """
        rows = execute_query(sql, {"limit": limit})
        return [Inflation.from_row(row) for row in rows]

    @staticmethod
    def get_labor_market(limit: int = 30) -> list[LaborMarket]:
        """Get labor market data."""
        sql = """
            SELECT * FROM labor_market
            ORDER BY date DESC
            LIMIT %(limit)s
        """
        rows = execute_query(sql, {"limit": limit})
        return [LaborMarket.from_row(row) for row in rows]

    @staticmethod
    def get_52_week_range(ticker: str) -> tuple[float | None, float | None]:
        """Get 52-week high and low for a ticker."""
        sql = """
            SELECT MAX(high) as high_52w, MIN(low) as low_52w
            FROM stock_prices
            WHERE ticker = %(ticker)s
              AND date >= CURRENT_DATE - INTERVAL '52 weeks'
        """
        rows = execute_query(sql, {"ticker": ticker})
        if rows:
            return (
                float(rows[0]["high_52w"]) if rows[0].get("high_52w") else None,
                float(rows[0]["low_52w"]) if rows[0].get("low_52w") else None,
            )
        return None, None

    @staticmethod
    def get_news(ticker: str, limit: int = 10) -> list[NewsArticle]:
        """Get news articles for a ticker with sentiment."""
        sql = """
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
            WHERE nat.ticker = %(ticker)s
            ORDER BY na.published_utc DESC
            LIMIT %(limit)s
        """
        rows = execute_query(sql, {"ticker": ticker, "limit": limit})
        return [NewsArticle.from_row(row) for row in rows]

    @staticmethod
    def get_news_sentiment_summary(ticker: str, days: int = 30) -> dict[str, int]:
        """Get sentiment summary counts for a ticker."""
        sql = """
            SELECT
                ns.sentiment,
                COUNT(*) as count
            FROM news_sentiment ns
            JOIN news_articles na ON ns.article_id = na.id
            WHERE ns.ticker = %(ticker)s
              AND na.published_utc >= NOW() - INTERVAL '%(days)s days'
            GROUP BY ns.sentiment
        """
        rows = execute_query(sql, {"ticker": ticker, "days": days})
        return {row["sentiment"]: row["count"] for row in rows if row.get("sentiment")}

    @staticmethod
    def get_screener_universe() -> list[ScreenerResult]:
        """Get full universe of data for screening."""
        sql = """
            SELECT
                c.ticker,
                c.name,
                c.sic_description,
                c.market_cap,
                fr.price,
                fr.price_to_earnings,
                fr.price_to_book,
                fr.price_to_sales,
                fr.dividend_yield,
                fr.debt_to_equity,
                fr.return_on_equity,
                fr.earnings_per_share,
                fr.enterprise_value,
                fr.average_volume as volume
            FROM companies c
            LEFT JOIN (
                SELECT DISTINCT ON (ticker) *
                FROM financial_ratios
                ORDER BY ticker, date DESC
            ) fr ON c.ticker = fr.ticker
            ORDER BY c.market_cap DESC NULLS LAST
        """
        rows = execute_query(sql)
        return [ScreenerResult.from_row(row) for row in rows]
