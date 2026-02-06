"""Tests for mock repositories."""

from datetime import date
from decimal import Decimal

import pytest

from sawa.domain.models import FinancialRatio
from tests.repositories.mocks import (
    MockCompanyRepository,
    MockPriceRepository,
    MockRatiosRepository,
    create_sample_company,
    create_sample_price,
)


class TestMockPriceRepository:
    """Tests for MockPriceRepository."""

    @pytest.mark.asyncio
    async def test_empty_repository(self) -> None:
        """Test querying empty repository."""
        repo = MockPriceRepository()
        prices = await repo.get_prices("AAPL", date(2024, 1, 1), date(2024, 1, 31))
        assert prices == []
        assert repo.call_count == 1

    @pytest.mark.asyncio
    async def test_add_and_get_price(self) -> None:
        """Test adding and retrieving a price."""
        repo = MockPriceRepository()
        price = create_sample_price("AAPL", date(2024, 1, 15))
        repo.add_price(price)

        prices = await repo.get_prices("AAPL", date(2024, 1, 1), date(2024, 1, 31))
        assert len(prices) == 1
        assert prices[0].ticker == "AAPL"
        assert prices[0].date == date(2024, 1, 15)

    @pytest.mark.asyncio
    async def test_filter_by_date_range(self) -> None:
        """Test filtering by date range."""
        repo = MockPriceRepository()
        repo.add_price(create_sample_price("AAPL", date(2024, 1, 10)))
        repo.add_price(create_sample_price("AAPL", date(2024, 1, 15)))
        repo.add_price(create_sample_price("AAPL", date(2024, 1, 20)))

        prices = await repo.get_prices("AAPL", date(2024, 1, 12), date(2024, 1, 18))
        assert len(prices) == 1
        assert prices[0].date == date(2024, 1, 15)

    @pytest.mark.asyncio
    async def test_filter_by_ticker(self) -> None:
        """Test filtering by ticker."""
        repo = MockPriceRepository()
        repo.add_price(create_sample_price("AAPL", date(2024, 1, 15)))
        repo.add_price(create_sample_price("MSFT", date(2024, 1, 15)))

        prices = await repo.get_prices("AAPL", date(2024, 1, 1), date(2024, 1, 31))
        assert len(prices) == 1
        assert prices[0].ticker == "AAPL"

    @pytest.mark.asyncio
    async def test_ticker_case_insensitive(self) -> None:
        """Test that ticker matching is case-insensitive."""
        repo = MockPriceRepository()
        repo.add_price(create_sample_price("AAPL", date(2024, 1, 15)))

        prices = await repo.get_prices("aapl", date(2024, 1, 1), date(2024, 1, 31))
        assert len(prices) == 1

    @pytest.mark.asyncio
    async def test_get_latest_price(self) -> None:
        """Test getting the latest price."""
        repo = MockPriceRepository()
        repo.add_price(create_sample_price("AAPL", date(2024, 1, 10)))
        repo.add_price(create_sample_price("AAPL", date(2024, 1, 20)))
        repo.add_price(create_sample_price("AAPL", date(2024, 1, 15)))

        latest = await repo.get_latest_price("AAPL")
        assert latest is not None
        assert latest.date == date(2024, 1, 20)

    @pytest.mark.asyncio
    async def test_get_latest_price_not_found(self) -> None:
        """Test getting latest price for non-existent ticker."""
        repo = MockPriceRepository()
        latest = await repo.get_latest_price("INVALID")
        assert latest is None

    @pytest.mark.asyncio
    async def test_get_prices_for_date(self) -> None:
        """Test getting prices for multiple tickers on a date."""
        repo = MockPriceRepository()
        repo.add_price(create_sample_price("AAPL", date(2024, 1, 15)))
        repo.add_price(create_sample_price("MSFT", date(2024, 1, 15)))
        repo.add_price(create_sample_price("GOOGL", date(2024, 1, 15)))
        repo.add_price(create_sample_price("AAPL", date(2024, 1, 16)))

        prices = await repo.get_prices_for_date(["AAPL", "MSFT"], date(2024, 1, 15))
        assert len(prices) == 2
        tickers = {p.ticker for p in prices}
        assert tickers == {"AAPL", "MSFT"}

    @pytest.mark.asyncio
    async def test_tracks_last_call(self) -> None:
        """Test that repository tracks the last call parameters."""
        repo = MockPriceRepository()
        await repo.get_prices("AAPL", date(2024, 1, 1), date(2024, 1, 31))

        assert repo.last_ticker == "AAPL"
        assert repo.last_start_date == date(2024, 1, 1)
        assert repo.last_end_date == date(2024, 1, 31)


class TestMockCompanyRepository:
    """Tests for MockCompanyRepository."""

    @pytest.mark.asyncio
    async def test_empty_repository(self) -> None:
        """Test querying empty repository."""
        repo = MockCompanyRepository()
        company = await repo.get_company_info("AAPL")
        assert company is None

    @pytest.mark.asyncio
    async def test_add_and_get_company(self) -> None:
        """Test adding and retrieving a company."""
        repo = MockCompanyRepository()
        company = create_sample_company("AAPL", "Apple Inc.")
        repo.add_company(company)

        result = await repo.get_company_info("AAPL")
        assert result is not None
        assert result.name == "Apple Inc."

    @pytest.mark.asyncio
    async def test_search_companies(self) -> None:
        """Test searching companies."""
        repo = MockCompanyRepository()
        repo.add_company(create_sample_company("AAPL", "Apple Inc."))
        repo.add_company(create_sample_company("MSFT", "Microsoft Corporation"))
        repo.add_company(create_sample_company("AMZN", "Amazon.com Inc."))

        # Search by ticker
        results = await repo.search_companies("AAPL")
        assert len(results) == 1
        assert results[0].ticker == "AAPL"

        # Search by name
        results = await repo.search_companies("Microsoft")
        assert len(results) == 1
        assert results[0].ticker == "MSFT"

        # Search partial - "a" matches AAPL (ticker), AMZN (ticker), MSFT (name contains 'a')
        results = await repo.search_companies("a")
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_search_with_limit(self) -> None:
        """Test search respects limit."""
        repo = MockCompanyRepository()
        for i in range(10):
            repo.add_company(create_sample_company(f"TEST{i}", f"Test Company {i}"))

        results = await repo.search_companies("Test", limit=3)
        assert len(results) == 3


class TestMockRatiosRepository:
    """Tests for MockRatiosRepository."""

    @pytest.mark.asyncio
    async def test_get_ratios(self) -> None:
        """Test getting ratios."""
        repo = MockRatiosRepository()
        repo.ratios.append(
            FinancialRatio(
                ticker="AAPL",
                date=date(2024, 1, 15),
                pe_ratio=Decimal("28.5"),
            )
        )

        ratios = await repo.get_ratios("AAPL", date(2024, 1, 1), date(2024, 1, 31))
        assert len(ratios) == 1
        assert ratios[0].pe_ratio == Decimal("28.5")

    @pytest.mark.asyncio
    async def test_get_latest_ratio(self) -> None:
        """Test getting latest ratio."""
        repo = MockRatiosRepository()
        repo.ratios.append(
            FinancialRatio(ticker="AAPL", date=date(2024, 1, 10), pe_ratio=Decimal("27.0"))
        )
        repo.ratios.append(
            FinancialRatio(ticker="AAPL", date=date(2024, 1, 20), pe_ratio=Decimal("28.5"))
        )

        latest = await repo.get_latest_ratio("AAPL")
        assert latest is not None
        assert latest.date == date(2024, 1, 20)
        assert latest.pe_ratio == Decimal("28.5")


class TestHelperFunctions:
    """Tests for helper functions."""

    def test_create_sample_price(self) -> None:
        """Test create_sample_price helper."""
        price = create_sample_price()
        assert price.ticker == "AAPL"
        assert price.date == date(2024, 1, 15)
        assert price.close == Decimal("185.75")

    def test_create_sample_price_custom(self) -> None:
        """Test create_sample_price with custom values."""
        price = create_sample_price(
            ticker="MSFT",
            price_date=date(2024, 2, 1),
            close=Decimal("400.00"),
        )
        assert price.ticker == "MSFT"
        assert price.date == date(2024, 2, 1)
        assert price.close == Decimal("400.00")

    def test_create_sample_company(self) -> None:
        """Test create_sample_company helper."""
        company = create_sample_company()
        assert company.ticker == "AAPL"
        assert company.name == "Apple Inc."
        assert company.sector == "Technology"

    def test_create_sample_company_custom(self) -> None:
        """Test create_sample_company with custom values."""
        company = create_sample_company(
            ticker="MSFT",
            name="Microsoft Corporation",
            sector="Software",
        )
        assert company.ticker == "MSFT"
        assert company.name == "Microsoft Corporation"
        assert company.sector == "Software"
