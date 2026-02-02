"""Tests for TUI StockService."""

import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest

from sawa.domain import models as domain

# Add TUI to path for testing (must be before TUI imports)
tui_path = Path(__file__).parent.parent.parent.parent / "tui"
sys.path.insert(0, str(tui_path))

from tests.repositories.mocks import (  # noqa: E402
    MockCompanyRepository,
    MockFundamentalRepository,
    MockPriceRepository,
    MockRatiosRepository,
)


class TestStockService:
    """Tests for StockService."""

    @pytest.fixture
    def mock_factory(self):
        """Create a mock factory with mock repositories."""
        # Create mock repositories
        price_repo = MockPriceRepository()
        company_repo = MockCompanyRepository()
        fundamental_repo = MockFundamentalRepository()
        ratios_repo = MockRatiosRepository()

        # Use recent dates so queries with default date ranges work
        today = date.today()
        yesterday = today - timedelta(days=1)

        # Add test data with recent dates
        price_repo.add_prices(
            [
                domain.StockPrice(
                    ticker="AAPL",
                    date=yesterday,
                    open=Decimal("185.50"),
                    high=Decimal("186.00"),
                    low=Decimal("184.00"),
                    close=Decimal("185.75"),
                    volume=50000000,
                ),
                domain.StockPrice(
                    ticker="AAPL",
                    date=today,
                    open=Decimal("186.00"),
                    high=Decimal("187.00"),
                    low=Decimal("185.00"),
                    close=Decimal("186.50"),
                    volume=45000000,
                ),
            ]
        )

        company_repo.add_company(
            domain.CompanyInfo(
                ticker="AAPL",
                name="Apple Inc.",
                sector="Technology",
                industry="Consumer Electronics",
                market_cap=Decimal("2500000000000"),
            )
        )

        ratios_repo.ratios = [
            domain.FinancialRatio(
                ticker="AAPL",
                date=today,
                pe_ratio=Decimal("28.5"),
                pb_ratio=Decimal("45.0"),
            ),
        ]

        fundamental_repo.income_statements = [
            domain.IncomeStatement(
                ticker="AAPL",
                period_end=date(2024, 3, 31),
                timeframe="quarterly",
                fiscal_year=2024,
                fiscal_quarter=1,
                revenue=Decimal("90000000000"),
            ),
        ]

        # Create a mock factory that returns our mock repos
        class MockFactory:
            def get_price_repository(self, provider=None):
                return price_repo

            def get_company_repository(self, provider=None):
                return company_repo

            def get_fundamental_repository(self, provider=None):
                return fundamental_repo

            def get_ratios_repository(self, provider=None):
                return ratios_repo

            def get_economy_repository(self, provider=None):
                from tests.repositories.mocks import MockEconomyRepository

                return MockEconomyRepository()

        return MockFactory()

    def test_get_company(self, mock_factory):
        """Test getting company info."""
        with patch("sawa_tui.services.stock_service.get_factory", return_value=mock_factory):
            from sawa_tui.services import StockService

            service = StockService()
            result = service.get_company("AAPL")

            assert result is not None
            assert result.ticker == "AAPL"
            assert result.name == "Apple Inc."
            assert result.sector == "Technology"

    def test_get_company_not_found(self, mock_factory):
        """Test getting non-existent company."""
        with patch("sawa_tui.services.stock_service.get_factory", return_value=mock_factory):
            from sawa_tui.services import StockService

            service = StockService()
            result = service.get_company("NONEXISTENT")

            assert result is None

    def test_search_companies(self, mock_factory):
        """Test searching companies."""
        with patch("sawa_tui.services.stock_service.get_factory", return_value=mock_factory):
            from sawa_tui.services import StockService

            service = StockService()
            results = service.search_companies("Apple")

            assert len(results) == 1
            assert results[0].name == "Apple Inc."

    def test_get_stock_prices_with_days(self, mock_factory):
        """Test getting stock prices by days."""
        with patch("sawa_tui.services.stock_service.get_factory", return_value=mock_factory):
            from sawa_tui.services import StockService

            service = StockService()
            results = service.get_stock_prices("AAPL", days=30)

            assert len(results) == 2
            # Should be sorted by date descending
            today = date.today()
            yesterday = today - timedelta(days=1)
            assert results[0].date == today
            assert results[1].date == yesterday

    def test_get_stock_prices_with_date_range(self, mock_factory):
        """Test getting stock prices by date range."""
        with patch("sawa_tui.services.stock_service.get_factory", return_value=mock_factory):
            from sawa_tui.services import StockService

            service = StockService()
            today = date.today()
            yesterday = today - timedelta(days=1)
            results = service.get_stock_prices(
                "AAPL",
                start_date=yesterday,
                end_date=today,
            )

            assert len(results) == 2

    def test_get_latest_price(self, mock_factory):
        """Test getting latest price."""
        with patch("sawa_tui.services.stock_service.get_factory", return_value=mock_factory):
            from sawa_tui.services import StockService

            service = StockService()
            result = service.get_latest_price("AAPL")

            assert result is not None
            assert result.close == pytest.approx(186.50)

    def test_get_financial_ratios(self, mock_factory):
        """Test getting financial ratios."""
        with patch("sawa_tui.services.stock_service.get_factory", return_value=mock_factory):
            from sawa_tui.services import StockService

            service = StockService()
            results = service.get_financial_ratios("AAPL", limit=10)

            assert len(results) == 1
            assert results[0].pe_ratio == pytest.approx(28.5)

    def test_get_income_statements(self, mock_factory):
        """Test getting income statements."""
        with patch("sawa_tui.services.stock_service.get_factory", return_value=mock_factory):
            from sawa_tui.services import StockService

            service = StockService()
            results = service.get_income_statements("AAPL", timeframe="quarterly", limit=8)

            assert len(results) == 1
            assert results[0].revenue == pytest.approx(90000000000)

    def test_get_52_week_range(self, mock_factory):
        """Test getting 52-week range."""
        with patch("sawa_tui.services.stock_service.get_factory", return_value=mock_factory):
            from sawa_tui.services import StockService

            service = StockService()
            high, low = service.get_52_week_range("AAPL")

            assert high == pytest.approx(187.0)
            assert low == pytest.approx(184.0)

    def test_get_52_week_range_no_data(self, mock_factory):
        """Test getting 52-week range with no data."""
        with patch("sawa_tui.services.stock_service.get_factory", return_value=mock_factory):
            from sawa_tui.services import StockService

            service = StockService()
            high, low = service.get_52_week_range("NONEXISTENT")

            assert high is None
            assert low is None
