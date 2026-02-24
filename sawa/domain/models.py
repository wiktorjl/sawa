"""Domain models - provider-agnostic data structures.

These dataclasses represent the core business entities used throughout
the S&P 500 data pipeline. They are immutable (frozen) and memory-efficient
(slots=True).

Usage:
    from sawa.domain import StockPrice, CompanyInfo

    price = StockPrice(
        ticker="AAPL",
        date=date(2024, 1, 15),
        open=Decimal("185.50"),
        high=Decimal("186.00"),
        low=Decimal("184.00"),
        close=Decimal("185.75"),
        volume=50000000,
    )
"""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Literal


@dataclass(frozen=True, slots=True)
class StockPrice:
    """Stock price data for a single day.

    Attributes:
        ticker: Stock symbol (normalized to uppercase)
        date: Trading date
        open: Opening price
        high: High price
        low: Low price
        close: Closing price
        volume: Trading volume
        adjusted_close: Split/dividend adjusted close (optional)
    """

    ticker: str
    date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    adjusted_close: Decimal | None = None

    def __post_init__(self) -> None:
        """Normalize ticker to uppercase."""
        object.__setattr__(self, "ticker", self.ticker.upper())


@dataclass(frozen=True, slots=True)
class NewsArticle:
    """News article with optional sentiment.

    Attributes:
        id: Unique article identifier
        title: Article title
        published_utc: Publication timestamp
        article_url: Link to original article
        author: Article author (optional)
        description: Article description/snippet (optional)
        publisher_name: News publisher name (optional)
        sentiment: Categorical sentiment label (positive/negative/neutral)
        sentiment_reasoning: Explanation for sentiment classification (optional)
    """

    id: str
    title: str
    published_utc: datetime
    article_url: str | None = None
    author: str | None = None
    description: str | None = None
    publisher_name: str | None = None
    sentiment: Literal["positive", "negative", "neutral"] | None = None
    sentiment_reasoning: str | None = None


@dataclass(frozen=True, slots=True)
class IncomeStatement:
    """Income statement data.

    Attributes:
        ticker: Stock symbol
        period_end: End date of the reporting period
        timeframe: "quarterly" or "annual"
        fiscal_year: Fiscal year
        fiscal_quarter: Fiscal quarter (1-4, None for annual)
        revenue: Total revenue
        cost_of_revenue: Cost of goods sold
        gross_profit: Revenue minus cost of revenue
        research_development: R&D expenses
        selling_general_administrative: SG&A expenses
        operating_income: Operating income
        net_income: Net income
        basic_eps: Basic earnings per share
        diluted_eps: Diluted earnings per share
    """

    ticker: str
    period_end: date
    timeframe: Literal["quarterly", "annual"]
    fiscal_year: int
    fiscal_quarter: int | None = None

    # Revenue
    revenue: Decimal | None = None
    cost_of_revenue: Decimal | None = None
    gross_profit: Decimal | None = None

    # Operating
    research_development: Decimal | None = None
    selling_general_administrative: Decimal | None = None
    operating_income: Decimal | None = None

    # Net income
    net_income: Decimal | None = None

    # Per share
    basic_eps: Decimal | None = None
    diluted_eps: Decimal | None = None


@dataclass(frozen=True, slots=True)
class BalanceSheet:
    """Balance sheet data.

    Attributes:
        ticker: Stock symbol
        period_end: End date of the reporting period
        timeframe: "quarterly" or "annual"
        fiscal_year: Fiscal year
        fiscal_quarter: Fiscal quarter (1-4, None for annual)
        total_assets: Total assets
        total_current_assets: Current assets
        cash_and_equivalents: Cash and cash equivalents
        total_liabilities: Total liabilities
        total_current_liabilities: Current liabilities
        long_term_debt: Long-term debt
        total_equity: Total stockholders' equity
        retained_earnings: Retained earnings
    """

    ticker: str
    period_end: date
    timeframe: Literal["quarterly", "annual"]
    fiscal_year: int | None = None
    fiscal_quarter: int | None = None

    # Assets
    total_assets: Decimal | None = None
    total_current_assets: Decimal | None = None
    cash_and_equivalents: Decimal | None = None

    # Liabilities
    total_liabilities: Decimal | None = None
    total_current_liabilities: Decimal | None = None
    long_term_debt: Decimal | None = None

    # Equity
    total_equity: Decimal | None = None
    retained_earnings: Decimal | None = None


@dataclass(frozen=True, slots=True)
class CashFlow:
    """Cash flow statement data.

    Attributes:
        ticker: Stock symbol
        period_end: End date of the reporting period
        timeframe: "quarterly" or "annual"
        fiscal_year: Fiscal year
        fiscal_quarter: Fiscal quarter (1-4, None for annual)
        operating_cash_flow: Cash from operations
        capital_expenditure: Capital expenditures (usually negative)
        dividends_paid: Dividends paid (usually negative)
        free_cash_flow: Operating cash flow minus capex
    """

    ticker: str
    period_end: date
    timeframe: Literal["quarterly", "annual"]
    fiscal_year: int | None = None
    fiscal_quarter: int | None = None

    # Operating
    operating_cash_flow: Decimal | None = None

    # Investing
    capital_expenditure: Decimal | None = None

    # Financing
    dividends_paid: Decimal | None = None

    # Calculated
    free_cash_flow: Decimal | None = None


@dataclass(frozen=True, slots=True)
class CompanyInfo:
    """Company overview information.

    Attributes:
        ticker: Stock symbol
        name: Company name
        description: Business description
        sector: GICS sector
        industry: GICS industry
        market_cap: Market capitalization
        employees: Number of employees
        website: Company website URL
        ceo: CEO name
        headquarters: Headquarters location
    """

    ticker: str
    name: str
    description: str | None = None
    sector: str | None = None
    industry: str | None = None
    market_cap: Decimal | None = None
    employees: int | None = None
    website: str | None = None
    ceo: str | None = None
    headquarters: str | None = None


@dataclass(frozen=True, slots=True)
class FinancialRatio:
    """Financial ratios for a ticker on a date.

    Attributes:
        ticker: Stock symbol
        date: Date of the ratios
        pe_ratio: Price-to-earnings ratio
        pb_ratio: Price-to-book ratio
        ps_ratio: Price-to-sales ratio
        peg_ratio: Price/earnings-to-growth ratio
        roe: Return on equity
        roa: Return on assets
        profit_margin: Net profit margin
        operating_margin: Operating margin
        current_ratio: Current ratio
        quick_ratio: Quick ratio (acid test)
        debt_to_equity: Debt-to-equity ratio
        debt_to_assets: Debt-to-assets ratio
        asset_turnover: Asset turnover ratio
        inventory_turnover: Inventory turnover ratio
    """

    ticker: str
    date: date

    # Valuation
    pe_ratio: Decimal | None = None
    pb_ratio: Decimal | None = None
    ps_ratio: Decimal | None = None
    peg_ratio: Decimal | None = None

    # Profitability
    roe: Decimal | None = None
    roa: Decimal | None = None
    profit_margin: Decimal | None = None
    operating_margin: Decimal | None = None

    # Liquidity
    current_ratio: Decimal | None = None
    quick_ratio: Decimal | None = None

    # Leverage
    debt_to_equity: Decimal | None = None
    debt_to_assets: Decimal | None = None

    # Efficiency
    asset_turnover: Decimal | None = None
    inventory_turnover: Decimal | None = None


@dataclass(frozen=True, slots=True)
class TreasuryYield:
    """Treasury yield data for a date.

    Attributes:
        date: Date of the yields
        yield_1mo: 1-month treasury yield
        yield_3mo: 3-month treasury yield
        yield_6mo: 6-month treasury yield
        yield_1yr: 1-year treasury yield
        yield_2yr: 2-year treasury yield
        yield_5yr: 5-year treasury yield
        yield_10yr: 10-year treasury yield
        yield_30yr: 30-year treasury yield
    """

    date: date
    yield_1mo: Decimal | None = None
    yield_3mo: Decimal | None = None
    yield_6mo: Decimal | None = None
    yield_1yr: Decimal | None = None
    yield_2yr: Decimal | None = None
    yield_5yr: Decimal | None = None
    yield_10yr: Decimal | None = None
    yield_30yr: Decimal | None = None


@dataclass(frozen=True, slots=True)
class InflationData:
    """Inflation indicator data.

    Attributes:
        date: Date of the measurement
        indicator: Indicator name (CPI, PCE, etc.)
        value: Indicator value
        change_yoy: Year-over-year change percentage
    """

    date: date
    indicator: str
    value: Decimal
    change_yoy: Decimal | None = None


@dataclass(frozen=True, slots=True)
class LaborMarketData:
    """Labor market indicator data.

    Attributes:
        date: Date of the measurement
        indicator: Indicator name (unemployment_rate, nonfarm_payrolls, etc.)
        value: Indicator value
    """

    date: date
    indicator: str
    value: Decimal


@dataclass(frozen=True, slots=True)
class MarketSentiment:
    """Market sentiment for a ticker on a date.

    Attributes:
        ticker: Stock symbol
        date: Date of the sentiment
        overall_score: Sentiment score from -1.0 (bearish) to 1.0 (bullish)
        volume: Number of data points used in calculation
        source: Source of the sentiment data
        bullish_count: Number of bullish signals
        bearish_count: Number of bearish signals
    """

    ticker: str
    date: date
    overall_score: float
    volume: int
    source: str
    bullish_count: int = 0
    bearish_count: int = 0


@dataclass(frozen=True, slots=True)
class MarketIndex:
    """Market index definition.

    Attributes:
        id: Database ID
        code: Short code (e.g., 'sp500', 'nasdaq5000')
        name: Full name (e.g., 'S&P 500', 'NASDAQ-100')
        description: Index description
        source_url: URL for constituent data source
        last_updated: When constituents were last updated
        constituent_count: Number of stocks in the index
    """

    id: int
    code: str
    name: str
    description: str | None = None
    source_url: str | None = None
    last_updated: datetime | None = None
    constituent_count: int = 0
