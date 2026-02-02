"""Tests for TUI service converters."""

import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from sawa.domain import models as domain

# Add TUI to path for testing (must be before TUI imports)
tui_path = Path(__file__).parent.parent.parent.parent / "tui"
sys.path.insert(0, str(tui_path))

from sawa_tui.models.queries import (  # noqa: E402
    BalanceSheet,
    CashFlow,
    Company,
    FinancialRatios,
    IncomeStatement,
    Inflation,
    LaborMarket,
    StockPrice,
    TreasuryYields,
)
from sawa_tui.services.converters import (  # noqa: E402
    balance_sheet_to_tui,
    cash_flow_to_tui,
    company_info_to_tui,
    financial_ratio_to_tui,
    income_statement_to_tui,
    inflation_to_tui,
    labor_market_to_tui,
    stock_price_to_tui,
    treasury_yield_to_tui,
)


class TestStockPriceConverter:
    """Tests for stock_price_to_tui converter."""

    def test_converts_all_fields(self):
        """Test that all fields are correctly converted."""
        domain_price = domain.StockPrice(
            ticker="AAPL",
            date=date(2024, 1, 15),
            open=Decimal("185.50"),
            high=Decimal("186.00"),
            low=Decimal("184.00"),
            close=Decimal("185.75"),
            volume=50000000,
        )

        result = stock_price_to_tui(domain_price)

        assert isinstance(result, StockPrice)
        assert result.date == date(2024, 1, 15)
        assert result.open == 185.50
        assert result.high == 186.00
        assert result.low == 184.00
        assert result.close == 185.75
        assert result.volume == 50000000

    def test_decimal_to_float_conversion(self):
        """Test that Decimal values are converted to float."""
        domain_price = domain.StockPrice(
            ticker="AAPL",
            date=date(2024, 1, 15),
            open=Decimal("0.001"),
            high=Decimal("999999.99"),
            low=Decimal("0.0"),
            close=Decimal("123.456789"),
            volume=1,
        )

        result = stock_price_to_tui(domain_price)

        assert result.open == pytest.approx(0.001)
        assert result.high == pytest.approx(999999.99)
        assert result.low == pytest.approx(0.0)
        assert result.close == pytest.approx(123.456789)


class TestCompanyInfoConverter:
    """Tests for company_info_to_tui converter."""

    def test_converts_basic_fields(self):
        """Test that basic fields are correctly converted."""
        domain_company = domain.CompanyInfo(
            ticker="AAPL",
            name="Apple Inc.",
            description="Technology company",
            sector="Technology",
            industry="Consumer Electronics",
            market_cap=Decimal("2500000000000"),
            employees=164000,
            website="https://apple.com",
        )

        result = company_info_to_tui(domain_company)

        assert isinstance(result, Company)
        assert result.ticker == "AAPL"
        assert result.name == "Apple Inc."
        assert result.description == "Technology company"
        assert result.sector == "Technology"
        assert result.market_cap == pytest.approx(2500000000000)
        assert result.employees == 164000
        assert result.homepage_url == "https://apple.com"

    def test_handles_none_values(self):
        """Test that None values are preserved."""
        domain_company = domain.CompanyInfo(
            ticker="AAPL",
            name="Apple Inc.",
        )

        result = company_info_to_tui(domain_company)

        assert result.description is None
        assert result.sector is None
        assert result.market_cap is None
        assert result.employees is None

    def test_sets_missing_tui_fields(self):
        """Test that TUI-specific fields are set to defaults."""
        domain_company = domain.CompanyInfo(
            ticker="AAPL",
            name="Apple Inc.",
        )

        result = company_info_to_tui(domain_company)

        assert result.exchange is None
        assert result.cik is None
        assert result.active is True


class TestFinancialRatioConverter:
    """Tests for financial_ratio_to_tui converter."""

    def test_converts_all_fields(self):
        """Test that all ratio fields are correctly converted."""
        domain_ratio = domain.FinancialRatio(
            ticker="AAPL",
            date=date(2024, 1, 15),
            pe_ratio=Decimal("28.5"),
            pb_ratio=Decimal("45.0"),
            ps_ratio=Decimal("7.5"),
            debt_to_equity=Decimal("1.5"),
            roe=Decimal("0.85"),
            roa=Decimal("0.25"),
            current_ratio=Decimal("1.1"),
            quick_ratio=Decimal("0.9"),
        )

        result = financial_ratio_to_tui(domain_ratio)

        assert isinstance(result, FinancialRatios)
        assert result.date == date(2024, 1, 15)
        assert result.pe_ratio == pytest.approx(28.5)
        assert result.pb_ratio == pytest.approx(45.0)
        assert result.ps_ratio == pytest.approx(7.5)
        assert result.debt_to_equity == pytest.approx(1.5)
        assert result.roe == pytest.approx(0.85)
        assert result.roa == pytest.approx(0.25)
        assert result.current_ratio == pytest.approx(1.1)
        assert result.quick_ratio == pytest.approx(0.9)

    def test_handles_none_values(self):
        """Test that None ratio values are preserved."""
        domain_ratio = domain.FinancialRatio(
            ticker="AAPL",
            date=date(2024, 1, 15),
        )

        result = financial_ratio_to_tui(domain_ratio)

        assert result.pe_ratio is None
        assert result.pb_ratio is None
        assert result.roe is None


class TestIncomeStatementConverter:
    """Tests for income_statement_to_tui converter."""

    def test_converts_quarterly_statement(self):
        """Test quarterly income statement conversion."""
        domain_stmt = domain.IncomeStatement(
            ticker="AAPL",
            period_end=date(2024, 3, 31),
            timeframe="quarterly",
            fiscal_year=2024,
            fiscal_quarter=1,
            revenue=Decimal("90000000000"),
            gross_profit=Decimal("40000000000"),
            operating_income=Decimal("25000000000"),
            net_income=Decimal("23000000000"),
            diluted_eps=Decimal("1.50"),
        )

        result = income_statement_to_tui(domain_stmt)

        assert isinstance(result, IncomeStatement)
        assert result.period_end == date(2024, 3, 31)
        assert result.timeframe == "quarterly"
        assert result.fiscal_year == 2024
        assert result.fiscal_quarter == 1
        assert result.revenue == pytest.approx(90000000000)
        assert result.gross_profit == pytest.approx(40000000000)
        assert result.net_income == pytest.approx(23000000000)
        assert result.eps == pytest.approx(1.50)


class TestBalanceSheetConverter:
    """Tests for balance_sheet_to_tui converter."""

    def test_converts_balance_sheet(self):
        """Test balance sheet conversion."""
        domain_sheet = domain.BalanceSheet(
            ticker="AAPL",
            period_end=date(2024, 3, 31),
            timeframe="quarterly",
            fiscal_year=2024,
            total_assets=Decimal("350000000000"),
            total_liabilities=Decimal("300000000000"),
            total_equity=Decimal("50000000000"),
            cash_and_equivalents=Decimal("30000000000"),
            long_term_debt=Decimal("100000000000"),
        )

        result = balance_sheet_to_tui(domain_sheet)

        assert isinstance(result, BalanceSheet)
        assert result.total_assets == pytest.approx(350000000000)
        assert result.total_liabilities == pytest.approx(300000000000)
        assert result.total_equity == pytest.approx(50000000000)
        assert result.cash == pytest.approx(30000000000)
        assert result.total_debt == pytest.approx(100000000000)


class TestCashFlowConverter:
    """Tests for cash_flow_to_tui converter."""

    def test_converts_cash_flow(self):
        """Test cash flow conversion."""
        domain_cf = domain.CashFlow(
            ticker="AAPL",
            period_end=date(2024, 3, 31),
            timeframe="quarterly",
            fiscal_year=2024,
            operating_cash_flow=Decimal("30000000000"),
            capital_expenditure=Decimal("-3000000000"),
            dividends_paid=Decimal("-3500000000"),
            free_cash_flow=Decimal("27000000000"),
        )

        result = cash_flow_to_tui(domain_cf)

        assert isinstance(result, CashFlow)
        assert result.operating_cash_flow == pytest.approx(30000000000)
        assert result.capex == pytest.approx(-3000000000)
        assert result.dividends == pytest.approx(-3500000000)


class TestTreasuryYieldConverter:
    """Tests for treasury_yield_to_tui converter."""

    def test_converts_yields(self):
        """Test treasury yield conversion."""
        domain_yield = domain.TreasuryYield(
            date=date(2024, 1, 15),
            yield_1mo=Decimal("5.25"),
            yield_3mo=Decimal("5.30"),
            yield_6mo=Decimal("5.15"),
            yield_1yr=Decimal("4.90"),
            yield_2yr=Decimal("4.50"),
            yield_5yr=Decimal("4.20"),
            yield_10yr=Decimal("4.10"),
            yield_30yr=Decimal("4.25"),
        )

        result = treasury_yield_to_tui(domain_yield)

        assert isinstance(result, TreasuryYields)
        assert result.date == date(2024, 1, 15)
        assert result.yield_1m == pytest.approx(5.25)
        assert result.yield_3m == pytest.approx(5.30)
        assert result.yield_10y == pytest.approx(4.10)
        assert result.yield_30y == pytest.approx(4.25)


class TestInflationConverter:
    """Tests for inflation_to_tui converter."""

    def test_pivots_inflation_records(self):
        """Test that multiple records are pivoted into one."""
        records = [
            domain.InflationData(date=date(2024, 1, 15), indicator="cpi", value=Decimal("3.1")),
            domain.InflationData(
                date=date(2024, 1, 15), indicator="cpi_core", value=Decimal("3.9")
            ),
            domain.InflationData(date=date(2024, 1, 15), indicator="pce", value=Decimal("2.6")),
        ]

        result = inflation_to_tui(records)

        assert isinstance(result, Inflation)
        assert result.date == date(2024, 1, 15)
        assert result.cpi == pytest.approx(3.1)
        assert result.cpi_core == pytest.approx(3.9)
        assert result.pce == pytest.approx(2.6)

    def test_returns_none_for_empty_list(self):
        """Test that empty list returns None."""
        result = inflation_to_tui([])
        assert result is None


class TestLaborMarketConverter:
    """Tests for labor_market_to_tui converter."""

    def test_pivots_labor_records(self):
        """Test that multiple records are pivoted into one."""
        records = [
            domain.LaborMarketData(
                date=date(2024, 1, 15),
                indicator="unemployment_rate",
                value=Decimal("3.7"),
            ),
            domain.LaborMarketData(
                date=date(2024, 1, 15),
                indicator="labor_force_participation_rate",
                value=Decimal("62.5"),
            ),
        ]

        result = labor_market_to_tui(records)

        assert isinstance(result, LaborMarket)
        assert result.date == date(2024, 1, 15)
        assert result.unemployment_rate == pytest.approx(3.7)
        assert result.participation_rate == pytest.approx(62.5)

    def test_returns_none_for_empty_list(self):
        """Test that empty list returns None."""
        result = labor_market_to_tui([])
        assert result is None
