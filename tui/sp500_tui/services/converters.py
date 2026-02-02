"""Converters from domain models to TUI dataclasses.

This module provides functions to convert sp500_tools.domain models
to sp500_tui.models.queries dataclasses, maintaining backward
compatibility with existing TUI code.
"""

from decimal import Decimal

from sp500_tools.domain import models as domain

from sp500_tui.models import queries as tui


def _decimal_to_float(value: Decimal | None) -> float | None:
    """Convert Decimal to float, returning None for null values."""
    if value is None:
        return None
    return float(value)


def stock_price_to_tui(price: domain.StockPrice) -> tui.StockPrice:
    """Convert domain StockPrice to TUI StockPrice.

    Args:
        price: Domain StockPrice object

    Returns:
        TUI StockPrice dataclass
    """
    return tui.StockPrice(
        date=price.date,
        open=float(price.open),
        high=float(price.high),
        low=float(price.low),
        close=float(price.close),
        volume=price.volume,
    )


def company_info_to_tui(info: domain.CompanyInfo) -> tui.Company:
    """Convert domain CompanyInfo to TUI Company.

    Args:
        info: Domain CompanyInfo object

    Returns:
        TUI Company dataclass

    Note:
        Some fields in TUI Company (address, logo_url, cik, exchange)
        are not available in the domain model and will be None.
    """
    return tui.Company(
        ticker=info.ticker,
        name=info.name,
        description=info.description,
        sector=info.sector,  # TUI uses 'sector' but shows sic_description
        market_cap=_decimal_to_float(info.market_cap),
        employees=info.employees,
        homepage_url=info.website,
        address=info.headquarters,
        # These fields don't exist in domain model
        exchange=None,
        cik=None,
        logo_url=None,
        active=True,
    )


def financial_ratio_to_tui(ratio: domain.FinancialRatio) -> tui.FinancialRatios:
    """Convert domain FinancialRatio to TUI FinancialRatios.

    Args:
        ratio: Domain FinancialRatio object

    Returns:
        TUI FinancialRatios dataclass

    Note:
        Some fields in domain model map to different names in TUI:
        - pe_ratio -> pe_ratio
        - pb_ratio -> pb_ratio
        - ps_ratio -> ps_ratio
        - debt_to_equity -> debt_to_equity
        - roe -> roe
        - roa -> roa
    """
    return tui.FinancialRatios(
        date=ratio.date,
        price=None,  # Not in domain FinancialRatio
        pe_ratio=_decimal_to_float(ratio.pe_ratio),
        pb_ratio=_decimal_to_float(ratio.pb_ratio),
        ps_ratio=_decimal_to_float(ratio.ps_ratio),
        debt_to_equity=_decimal_to_float(ratio.debt_to_equity),
        roe=_decimal_to_float(ratio.roe),
        roa=_decimal_to_float(ratio.roa),
        dividend_yield=None,  # Not in domain FinancialRatio
        eps=None,  # Not in domain FinancialRatio
        market_cap=None,  # Not in domain FinancialRatio
        ev=None,  # Not in domain FinancialRatio
        ev_to_ebitda=None,  # Not in domain FinancialRatio
        ev_to_sales=None,  # Not in domain FinancialRatio
        fcf=None,  # Not in domain FinancialRatio
        current_ratio=_decimal_to_float(ratio.current_ratio),
        quick_ratio=_decimal_to_float(ratio.quick_ratio),
    )


def income_statement_to_tui(stmt: domain.IncomeStatement) -> tui.IncomeStatement:
    """Convert domain IncomeStatement to TUI IncomeStatement.

    Args:
        stmt: Domain IncomeStatement object

    Returns:
        TUI IncomeStatement dataclass
    """
    return tui.IncomeStatement(
        period_end=stmt.period_end,
        timeframe=stmt.timeframe,
        fiscal_year=stmt.fiscal_year,
        fiscal_quarter=stmt.fiscal_quarter,
        revenue=_decimal_to_float(stmt.revenue),
        cost_of_revenue=_decimal_to_float(stmt.cost_of_revenue),
        gross_profit=_decimal_to_float(stmt.gross_profit),
        operating_income=_decimal_to_float(stmt.operating_income),
        net_income=_decimal_to_float(stmt.net_income),
        eps=_decimal_to_float(stmt.diluted_eps),
        ebitda=None,  # Not in domain model
    )


def balance_sheet_to_tui(sheet: domain.BalanceSheet) -> tui.BalanceSheet:
    """Convert domain BalanceSheet to TUI BalanceSheet.

    Args:
        sheet: Domain BalanceSheet object

    Returns:
        TUI BalanceSheet dataclass
    """
    # Calculate total_debt from long_term_debt (current not available)
    total_debt = _decimal_to_float(sheet.long_term_debt)

    return tui.BalanceSheet(
        period_end=sheet.period_end,
        timeframe=sheet.timeframe,
        fiscal_year=sheet.fiscal_year,
        fiscal_quarter=sheet.fiscal_quarter,
        total_assets=_decimal_to_float(sheet.total_assets),
        total_liabilities=_decimal_to_float(sheet.total_liabilities),
        total_equity=_decimal_to_float(sheet.total_equity),
        cash=_decimal_to_float(sheet.cash_and_equivalents),
        total_debt=total_debt,
        current_assets=_decimal_to_float(sheet.total_current_assets),
        current_liabilities=_decimal_to_float(sheet.total_current_liabilities),
    )


def cash_flow_to_tui(cf: domain.CashFlow) -> tui.CashFlow:
    """Convert domain CashFlow to TUI CashFlow.

    Args:
        cf: Domain CashFlow object

    Returns:
        TUI CashFlow dataclass
    """
    return tui.CashFlow(
        period_end=cf.period_end,
        timeframe=cf.timeframe,
        fiscal_year=cf.fiscal_year,
        fiscal_quarter=cf.fiscal_quarter,
        operating_cash_flow=_decimal_to_float(cf.operating_cash_flow),
        investing_cash_flow=None,  # Not in domain model
        financing_cash_flow=None,  # Not in domain model
        net_change=None,  # Not in domain model
        capex=_decimal_to_float(cf.capital_expenditure),
        dividends=_decimal_to_float(cf.dividends_paid),
    )


def treasury_yield_to_tui(ty: domain.TreasuryYield) -> tui.TreasuryYields:
    """Convert domain TreasuryYield to TUI TreasuryYields.

    Args:
        ty: Domain TreasuryYield object

    Returns:
        TUI TreasuryYields dataclass
    """
    return tui.TreasuryYields(
        date=ty.date,
        yield_1m=_decimal_to_float(ty.yield_1mo),
        yield_3m=_decimal_to_float(ty.yield_3mo),
        yield_6m=_decimal_to_float(ty.yield_6mo),
        yield_1y=_decimal_to_float(ty.yield_1yr),
        yield_2y=_decimal_to_float(ty.yield_2yr),
        yield_5y=_decimal_to_float(ty.yield_5yr),
        yield_10y=_decimal_to_float(ty.yield_10yr),
        yield_30y=_decimal_to_float(ty.yield_30yr),
    )


def inflation_to_tui(records: list[domain.InflationData]) -> tui.Inflation | None:
    """Convert domain InflationData list to TUI Inflation.

    The domain model has one record per indicator, while TUI expects
    a single record with all indicators. This function pivots the data.

    Args:
        records: List of InflationData objects (same date, different indicators)

    Returns:
        TUI Inflation dataclass, or None if records is empty
    """
    if not records:
        return None

    # Build a mapping of indicator -> value
    indicator_map: dict[str, Decimal | None] = {}
    date = records[0].date

    for record in records:
        indicator_map[record.indicator.lower()] = record.value

    return tui.Inflation(
        date=date,
        cpi=_decimal_to_float(indicator_map.get("cpi")),
        cpi_core=_decimal_to_float(indicator_map.get("cpi_core")),
        cpi_yoy=_decimal_to_float(indicator_map.get("cpi_yoy")),
        pce=_decimal_to_float(indicator_map.get("pce")),
        pce_core=_decimal_to_float(indicator_map.get("pce_core")),
    )


def labor_market_to_tui(records: list[domain.LaborMarketData]) -> tui.LaborMarket | None:
    """Convert domain LaborMarketData list to TUI LaborMarket.

    The domain model has one record per indicator, while TUI expects
    a single record with all indicators. This function pivots the data.

    Args:
        records: List of LaborMarketData objects (same date, different indicators)

    Returns:
        TUI LaborMarket dataclass, or None if records is empty
    """
    if not records:
        return None

    # Build a mapping of indicator -> value
    indicator_map: dict[str, Decimal] = {}
    date = records[0].date

    for record in records:
        indicator_map[record.indicator.lower()] = record.value

    return tui.LaborMarket(
        date=date,
        unemployment_rate=_decimal_to_float(indicator_map.get("unemployment_rate")),
        participation_rate=_decimal_to_float(indicator_map.get("labor_force_participation_rate")),
        avg_hourly_earnings=_decimal_to_float(indicator_map.get("avg_hourly_earnings")),
        job_openings=_decimal_to_float(indicator_map.get("job_openings")),
    )
