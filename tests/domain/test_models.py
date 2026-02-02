"""Tests for domain models."""

from datetime import date, datetime
from decimal import Decimal

import pytest

from sp500_tools.domain.models import (
    BalanceSheet,
    CashFlow,
    CompanyInfo,
    FinancialRatio,
    IncomeStatement,
    InflationData,
    LaborMarketData,
    MarketSentiment,
    NewsArticle,
    StockPrice,
    TreasuryYield,
)


class TestStockPrice:
    """Tests for StockPrice model."""

    def test_basic_creation(self) -> None:
        """Test creating a StockPrice with required fields."""
        price = StockPrice(
            ticker="AAPL",
            date=date(2024, 1, 15),
            open=Decimal("185.50"),
            high=Decimal("186.00"),
            low=Decimal("184.00"),
            close=Decimal("185.75"),
            volume=50000000,
        )
        assert price.ticker == "AAPL"
        assert price.date == date(2024, 1, 15)
        assert price.open == Decimal("185.50")
        assert price.close == Decimal("185.75")
        assert price.volume == 50000000
        assert price.adjusted_close is None

    def test_ticker_normalized_to_uppercase(self) -> None:
        """Test that ticker is normalized to uppercase."""
        price = StockPrice(
            ticker="aapl",
            date=date(2024, 1, 15),
            open=Decimal("185.50"),
            high=Decimal("186.00"),
            low=Decimal("184.00"),
            close=Decimal("185.75"),
            volume=50000000,
        )
        assert price.ticker == "AAPL"

    def test_with_adjusted_close(self) -> None:
        """Test creating a StockPrice with adjusted close."""
        price = StockPrice(
            ticker="AAPL",
            date=date(2024, 1, 15),
            open=Decimal("185.50"),
            high=Decimal("186.00"),
            low=Decimal("184.00"),
            close=Decimal("185.75"),
            volume=50000000,
            adjusted_close=Decimal("185.50"),
        )
        assert price.adjusted_close == Decimal("185.50")

    def test_frozen(self) -> None:
        """Test that StockPrice is immutable."""
        price = StockPrice(
            ticker="AAPL",
            date=date(2024, 1, 15),
            open=Decimal("185.50"),
            high=Decimal("186.00"),
            low=Decimal("184.00"),
            close=Decimal("185.75"),
            volume=50000000,
        )
        with pytest.raises(AttributeError):
            price.ticker = "MSFT"  # type: ignore[misc]


class TestCompanyInfo:
    """Tests for CompanyInfo model."""

    def test_basic_creation(self) -> None:
        """Test creating CompanyInfo with required fields."""
        company = CompanyInfo(
            ticker="AAPL",
            name="Apple Inc.",
        )
        assert company.ticker == "AAPL"
        assert company.name == "Apple Inc."
        assert company.sector is None
        assert company.industry is None

    def test_with_all_fields(self) -> None:
        """Test creating CompanyInfo with all fields."""
        company = CompanyInfo(
            ticker="AAPL",
            name="Apple Inc.",
            description="Consumer electronics company",
            sector="Technology",
            industry="Consumer Electronics",
            market_cap=Decimal("3000000000000"),
            employees=161000,
            website="https://apple.com",
            ceo="Tim Cook",
            headquarters="Cupertino, CA",
        )
        assert company.sector == "Technology"
        assert company.employees == 161000
        assert company.ceo == "Tim Cook"


class TestIncomeStatement:
    """Tests for IncomeStatement model."""

    def test_quarterly(self) -> None:
        """Test creating quarterly income statement."""
        stmt = IncomeStatement(
            ticker="AAPL",
            period_end=date(2024, 3, 31),
            timeframe="quarterly",
            fiscal_year=2024,
            fiscal_quarter=2,
            revenue=Decimal("94836000000"),
            net_income=Decimal("23636000000"),
        )
        assert stmt.ticker == "AAPL"
        assert stmt.timeframe == "quarterly"
        assert stmt.fiscal_quarter == 2
        assert stmt.revenue == Decimal("94836000000")

    def test_annual(self) -> None:
        """Test creating annual income statement."""
        stmt = IncomeStatement(
            ticker="AAPL",
            period_end=date(2023, 9, 30),
            timeframe="annual",
            fiscal_year=2023,
            fiscal_quarter=None,
            revenue=Decimal("383285000000"),
        )
        assert stmt.timeframe == "annual"
        assert stmt.fiscal_quarter is None


class TestBalanceSheet:
    """Tests for BalanceSheet model."""

    def test_creation(self) -> None:
        """Test creating balance sheet."""
        bs = BalanceSheet(
            ticker="AAPL",
            period_end=date(2024, 3, 31),
            timeframe="quarterly",
            total_assets=Decimal("352755000000"),
            total_liabilities=Decimal("290437000000"),
            total_equity=Decimal("62318000000"),
        )
        assert bs.total_assets == Decimal("352755000000")
        assert bs.total_equity == Decimal("62318000000")


class TestCashFlow:
    """Tests for CashFlow model."""

    def test_creation(self) -> None:
        """Test creating cash flow statement."""
        cf = CashFlow(
            ticker="AAPL",
            period_end=date(2024, 3, 31),
            timeframe="quarterly",
            operating_cash_flow=Decimal("26380000000"),
            capital_expenditure=Decimal("-2767000000"),
            free_cash_flow=Decimal("23613000000"),
        )
        assert cf.operating_cash_flow == Decimal("26380000000")
        assert cf.free_cash_flow == Decimal("23613000000")


class TestFinancialRatio:
    """Tests for FinancialRatio model."""

    def test_creation(self) -> None:
        """Test creating financial ratios."""
        ratio = FinancialRatio(
            ticker="AAPL",
            date=date(2024, 1, 15),
            pe_ratio=Decimal("28.5"),
            pb_ratio=Decimal("47.2"),
            roe=Decimal("1.47"),
            debt_to_equity=Decimal("1.81"),
        )
        assert ratio.pe_ratio == Decimal("28.5")
        assert ratio.roe == Decimal("1.47")


class TestTreasuryYield:
    """Tests for TreasuryYield model."""

    def test_creation(self) -> None:
        """Test creating treasury yield data."""
        yields = TreasuryYield(
            date=date(2024, 1, 15),
            yield_1mo=Decimal("5.38"),
            yield_3mo=Decimal("5.40"),
            yield_2yr=Decimal("4.35"),
            yield_10yr=Decimal("4.10"),
            yield_30yr=Decimal("4.30"),
        )
        assert yields.yield_10yr == Decimal("4.10")
        assert yields.yield_5yr is None  # Not provided


class TestInflationData:
    """Tests for InflationData model."""

    def test_creation(self) -> None:
        """Test creating inflation data."""
        inflation = InflationData(
            date=date(2024, 1, 15),
            indicator="CPI",
            value=Decimal("309.685"),
            change_yoy=Decimal("3.4"),
        )
        assert inflation.indicator == "CPI"
        assert inflation.change_yoy == Decimal("3.4")


class TestLaborMarketData:
    """Tests for LaborMarketData model."""

    def test_creation(self) -> None:
        """Test creating labor market data."""
        labor = LaborMarketData(
            date=date(2024, 1, 15),
            indicator="unemployment_rate",
            value=Decimal("3.7"),
        )
        assert labor.indicator == "unemployment_rate"
        assert labor.value == Decimal("3.7")


class TestNewsArticle:
    """Tests for NewsArticle model."""

    def test_creation(self) -> None:
        """Test creating news article."""
        article = NewsArticle(
            id="abc123",
            ticker="AAPL",
            title="Apple Reports Q1 Earnings",
            content="Apple Inc. reported strong quarterly earnings...",
            published_at=datetime(2024, 1, 25, 16, 30, 0),
            source="Reuters",
            url="https://reuters.com/article/abc123",
            sentiment_score=0.75,
            sentiment_label="positive",
        )
        assert article.id == "abc123"
        assert article.sentiment_label == "positive"


class TestMarketSentiment:
    """Tests for MarketSentiment model."""

    def test_creation(self) -> None:
        """Test creating market sentiment."""
        sentiment = MarketSentiment(
            ticker="AAPL",
            date=date(2024, 1, 15),
            overall_score=0.65,
            volume=1500,
            source="social_media",
            bullish_count=1000,
            bearish_count=500,
        )
        assert sentiment.overall_score == 0.65
        assert sentiment.bullish_count == 1000
