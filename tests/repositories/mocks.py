"""Mock repository implementations for testing.

These mock repositories can be used in unit tests to avoid making
real database or API calls.

Usage:
    from tests.repositories.mocks import MockPriceRepository

    repo = MockPriceRepository()
    repo.add_price(StockPrice(...))

    # In tests
    prices = await repo.get_prices("AAPL", start, end)
"""

from collections.abc import AsyncIterator
from datetime import date
from decimal import Decimal
from typing import Literal

from sawa.domain.models import (
    BalanceSheet,
    CashFlow,
    CompanyInfo,
    FinancialRatio,
    IncomeStatement,
    InflationData,
    LaborMarketData,
    StockPrice,
    TreasuryYield,
)
from sawa.repositories.base import (
    CompanyRepository,
    EconomyRepository,
    FundamentalRepository,
    RatiosRepository,
    StockPriceRepository,
)


class MockPriceRepository(StockPriceRepository):
    """Mock price repository for testing.

    Stores prices in memory and returns them on request.
    Tracks call counts for verification.
    """

    def __init__(self, prices: list[StockPrice] | None = None) -> None:
        """Initialize with optional list of prices."""
        self.prices: list[StockPrice] = prices or []
        self.call_count = 0
        self.last_ticker: str | None = None
        self.last_start_date: date | None = None
        self.last_end_date: date | None = None

    @property
    def provider_name(self) -> str:
        """Return provider name."""
        return "mock"

    @property
    def supports_historical_bulk(self) -> bool:
        """Return True for testing."""
        return True

    def add_price(self, price: StockPrice) -> None:
        """Add a price to the mock data."""
        self.prices.append(price)

    def add_prices(self, prices: list[StockPrice]) -> None:
        """Add multiple prices to the mock data."""
        self.prices.extend(prices)

    async def get_prices(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
    ) -> list[StockPrice]:
        """Return stored prices matching criteria."""
        self.call_count += 1
        self.last_ticker = ticker
        self.last_start_date = start_date
        self.last_end_date = end_date

        return [
            p
            for p in self.prices
            if p.ticker == ticker.upper() and start_date <= p.date <= end_date
        ]

    async def get_prices_stream(
        self,
        tickers: list[str],
        start_date: date,
        end_date: date,
    ) -> AsyncIterator[StockPrice]:
        """Stream stored prices."""
        tickers_upper = {t.upper() for t in tickers}
        for price in self.prices:
            if price.ticker in tickers_upper and start_date <= price.date <= end_date:
                yield price

    async def get_latest_price(self, ticker: str) -> StockPrice | None:
        """Return most recent price for ticker."""
        ticker_prices = [p for p in self.prices if p.ticker == ticker.upper()]
        if not ticker_prices:
            return None
        return max(ticker_prices, key=lambda p: p.date)

    async def get_prices_for_date(
        self,
        tickers: list[str],
        target_date: date,
    ) -> list[StockPrice]:
        """Return prices for tickers on date."""
        tickers_upper = {t.upper() for t in tickers}
        return [p for p in self.prices if p.ticker in tickers_upper and p.date == target_date]


class MockCompanyRepository(CompanyRepository):
    """Mock company repository for testing."""

    def __init__(self, companies: list[CompanyInfo] | None = None) -> None:
        """Initialize with optional list of companies."""
        self.companies: list[CompanyInfo] = companies or []
        self.call_count = 0

    @property
    def provider_name(self) -> str:
        """Return provider name."""
        return "mock"

    def add_company(self, company: CompanyInfo) -> None:
        """Add a company to the mock data."""
        self.companies.append(company)

    async def get_company_info(self, ticker: str) -> CompanyInfo | None:
        """Return company info for ticker."""
        self.call_count += 1
        for company in self.companies:
            if company.ticker == ticker.upper():
                return company
        return None

    async def search_companies(
        self,
        query: str,
        limit: int = 20,
    ) -> list[CompanyInfo]:
        """Search companies by name or ticker."""
        self.call_count += 1
        query_lower = query.lower()
        results = [
            c
            for c in self.companies
            if query_lower in c.ticker.lower() or query_lower in c.name.lower()
        ]
        return results[:limit]


class MockFundamentalRepository(FundamentalRepository):
    """Mock fundamental repository for testing."""

    def __init__(self) -> None:
        """Initialize empty mock."""
        self.income_statements: list[IncomeStatement] = []
        self.balance_sheets: list[BalanceSheet] = []
        self.cash_flows: list[CashFlow] = []
        self.call_count = 0

    @property
    def provider_name(self) -> str:
        """Return provider name."""
        return "mock"

    async def get_income_statements(
        self,
        ticker: str,
        timeframe: Literal["quarterly", "annual"],
        limit: int = 4,
    ) -> list[IncomeStatement]:
        """Return stored income statements."""
        self.call_count += 1
        results = [
            stmt
            for stmt in self.income_statements
            if stmt.ticker == ticker.upper() and stmt.timeframe == timeframe
        ]
        return sorted(results, key=lambda x: x.period_end, reverse=True)[:limit]

    async def get_balance_sheets(
        self,
        ticker: str,
        timeframe: Literal["quarterly", "annual"],
        limit: int = 4,
    ) -> list[BalanceSheet]:
        """Return stored balance sheets."""
        self.call_count += 1
        results = [
            bs
            for bs in self.balance_sheets
            if bs.ticker == ticker.upper() and bs.timeframe == timeframe
        ]
        return sorted(results, key=lambda x: x.period_end, reverse=True)[:limit]

    async def get_cash_flows(
        self,
        ticker: str,
        timeframe: Literal["quarterly", "annual"],
        limit: int = 4,
    ) -> list[CashFlow]:
        """Return stored cash flows."""
        self.call_count += 1
        results = [
            cf
            for cf in self.cash_flows
            if cf.ticker == ticker.upper() and cf.timeframe == timeframe
        ]
        return sorted(results, key=lambda x: x.period_end, reverse=True)[:limit]


class MockRatiosRepository(RatiosRepository):
    """Mock ratios repository for testing."""

    def __init__(self, ratios: list[FinancialRatio] | None = None) -> None:
        """Initialize with optional list of ratios."""
        self.ratios: list[FinancialRatio] = ratios or []
        self.call_count = 0

    @property
    def provider_name(self) -> str:
        """Return provider name."""
        return "mock"

    async def get_ratios(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
    ) -> list[FinancialRatio]:
        """Return stored ratios."""
        self.call_count += 1
        return [
            r
            for r in self.ratios
            if r.ticker == ticker.upper() and start_date <= r.date <= end_date
        ]

    async def get_latest_ratio(self, ticker: str) -> FinancialRatio | None:
        """Return most recent ratio for ticker."""
        self.call_count += 1
        ticker_ratios = [r for r in self.ratios if r.ticker == ticker.upper()]
        if not ticker_ratios:
            return None
        return max(ticker_ratios, key=lambda r: r.date)


class MockEconomyRepository(EconomyRepository):
    """Mock economy repository for testing."""

    def __init__(self) -> None:
        """Initialize empty mock."""
        self.yields: list[TreasuryYield] = []
        self.inflation: list[InflationData] = []
        self.labor: list[LaborMarketData] = []
        self.call_count = 0

    @property
    def provider_name(self) -> str:
        """Return provider name."""
        return "mock"

    async def get_treasury_yields(
        self,
        start_date: date,
        end_date: date,
    ) -> list[TreasuryYield]:
        """Return stored yields."""
        self.call_count += 1
        return [y for y in self.yields if start_date <= y.date <= end_date]

    async def get_inflation(
        self,
        start_date: date,
        end_date: date,
        indicator: str | None = None,
    ) -> list[InflationData]:
        """Return stored inflation data."""
        self.call_count += 1
        results = [i for i in self.inflation if start_date <= i.date <= end_date]
        if indicator:
            results = [i for i in results if i.indicator == indicator]
        return results

    async def get_labor_market(
        self,
        start_date: date,
        end_date: date,
        indicator: str | None = None,
    ) -> list[LaborMarketData]:
        """Return stored labor data."""
        self.call_count += 1
        results = [lm for lm in self.labor if start_date <= lm.date <= end_date]
        if indicator:
            results = [lm for lm in results if lm.indicator == indicator]
        return results


def create_sample_price(
    ticker: str = "AAPL",
    price_date: date | None = None,
    close: Decimal | None = None,
) -> StockPrice:
    """Create a sample StockPrice for testing.

    Args:
        ticker: Stock symbol
        price_date: Date (defaults to 2024-01-15)
        close: Closing price (defaults to 185.75)

    Returns:
        StockPrice instance
    """
    return StockPrice(
        ticker=ticker,
        date=price_date or date(2024, 1, 15),
        open=Decimal("185.50"),
        high=Decimal("186.00"),
        low=Decimal("184.00"),
        close=close or Decimal("185.75"),
        volume=50000000,
    )


def create_sample_company(
    ticker: str = "AAPL",
    name: str = "Apple Inc.",
    sector: str = "Technology",
) -> CompanyInfo:
    """Create a sample CompanyInfo for testing.

    Args:
        ticker: Stock symbol
        name: Company name
        sector: Company sector

    Returns:
        CompanyInfo instance
    """
    return CompanyInfo(
        ticker=ticker,
        name=name,
        sector=sector,
        industry="Consumer Electronics",
    )
