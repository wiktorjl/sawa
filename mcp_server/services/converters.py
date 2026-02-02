"""Converters from domain models to MCP-compatible dictionaries.

The MCP server returns raw dictionaries that match SQL query results.
These converters transform domain models back to that format.
"""

from decimal import Decimal
from typing import Any

from sawa.domain import models as domain


def _decimal_to_float(value: Decimal | None) -> float | None:
    """Convert Decimal to float for JSON serialization."""
    if value is None:
        return None
    return float(value)


def stock_price_to_dict(price: domain.StockPrice) -> dict[str, Any]:
    """Convert StockPrice domain model to MCP dict.

    Returns dict matching stock_prices SQL query result format.
    """
    return {
        "date": price.date,
        "open": _decimal_to_float(price.open),
        "high": _decimal_to_float(price.high),
        "low": _decimal_to_float(price.low),
        "close": _decimal_to_float(price.close),
        "volume": price.volume,
    }


def company_info_to_dict(company: domain.CompanyInfo) -> dict[str, Any]:
    """Convert CompanyInfo domain model to MCP dict.

    Returns dict matching companies SQL query result format.
    """
    return {
        "ticker": company.ticker,
        "name": company.name,
        "description": company.description,
        "market_cap": _decimal_to_float(company.market_cap),
        "sector": company.sector,
        "industry": company.industry,
        "employees": company.employees,
        "homepage_url": company.website,
    }


def company_to_list_dict(company: domain.CompanyInfo) -> dict[str, Any]:
    """Convert CompanyInfo to dict for list_companies result format."""
    return {
        "ticker": company.ticker,
        "name": company.name,
        "market_cap": _decimal_to_float(company.market_cap),
        "sector": company.sector,
        "exchange": None,  # Not in domain model
    }


def financial_ratio_to_dict(ratio: domain.FinancialRatio) -> dict[str, Any]:
    """Convert FinancialRatio domain model to MCP dict.

    Returns dict matching financial_ratios SQL query result format.
    """
    return {
        "date": ratio.date,
        "price": None,  # Not in domain model
        "pe_ratio": _decimal_to_float(ratio.pe_ratio),
        "pb_ratio": _decimal_to_float(ratio.pb_ratio),
        "ps_ratio": _decimal_to_float(ratio.ps_ratio),
        "pcf_ratio": None,  # Not in domain model
        "pfcf_ratio": None,  # Not in domain model
        "debt_to_equity": _decimal_to_float(ratio.debt_to_equity),
        "roe": _decimal_to_float(ratio.roe),
        "roa": _decimal_to_float(ratio.roa),
        "dividend_yield": None,  # Not in domain model
        "eps": None,  # Not in domain model
        "market_cap": None,  # Not in domain model
        "ev": None,  # Not in domain model
        "ev_to_ebitda": None,  # Not in domain model
        "ev_to_sales": None,  # Not in domain model
        "fcf": None,  # Not in domain model
        "average_volume": None,  # Not in domain model
    }


def income_statement_to_dict(stmt: domain.IncomeStatement) -> dict[str, Any]:
    """Convert IncomeStatement domain model to MCP dict."""
    return {
        "period_end": stmt.period_end,
        "fiscal_year": stmt.fiscal_year,
        "fiscal_quarter": stmt.fiscal_quarter,
        "total_revenue": _decimal_to_float(stmt.revenue),
        "cost_of_revenue": _decimal_to_float(stmt.cost_of_revenue),
        "gross_profit": _decimal_to_float(stmt.gross_profit),
        "operating_income": _decimal_to_float(stmt.operating_income),
        "net_income": _decimal_to_float(stmt.net_income),
        "basic_eps": _decimal_to_float(stmt.basic_eps),
        "diluted_eps": _decimal_to_float(stmt.diluted_eps),
        "ebitda": None,  # Not in domain model
        "gross_margin": None,  # Not in domain model (calculated)
        "operating_margin": None,  # Not in domain model (calculated)
        "profit_margin": None,  # Not in domain model (calculated)
    }


def balance_sheet_to_dict(sheet: domain.BalanceSheet) -> dict[str, Any]:
    """Convert BalanceSheet domain model to MCP dict."""
    return {
        "period_end": sheet.period_end,
        "fiscal_year": sheet.fiscal_year,
        "fiscal_quarter": sheet.fiscal_quarter,
        "total_assets": _decimal_to_float(sheet.total_assets),
        "total_liabilities": _decimal_to_float(sheet.total_liabilities),
        "total_equity": _decimal_to_float(sheet.total_equity),
        "cash_and_equivalents": _decimal_to_float(sheet.cash_and_equivalents),
        "total_current_assets": _decimal_to_float(sheet.total_current_assets),
        "total_current_liabilities": _decimal_to_float(sheet.total_current_liabilities),
        "long_term_debt": _decimal_to_float(sheet.long_term_debt),
    }


def cash_flow_to_dict(cf: domain.CashFlow) -> dict[str, Any]:
    """Convert CashFlow domain model to MCP dict."""
    return {
        "period_end": cf.period_end,
        "fiscal_year": cf.fiscal_year,
        "fiscal_quarter": cf.fiscal_quarter,
        "operating_cash_flow": _decimal_to_float(cf.operating_cash_flow),
        "investing_cash_flow": None,  # Not in domain model
        "financing_cash_flow": None,  # Not in domain model
        "capex": _decimal_to_float(cf.capital_expenditure),
        "free_cash_flow": _decimal_to_float(cf.free_cash_flow),
    }


def treasury_yield_to_dict(ty: domain.TreasuryYield) -> dict[str, Any]:
    """Convert TreasuryYield domain model to MCP dict."""
    return {
        "date": ty.date,
        "yield_1_month": _decimal_to_float(ty.yield_1mo),
        "yield_3_month": _decimal_to_float(ty.yield_3mo),
        "yield_6_month": _decimal_to_float(ty.yield_6mo),
        "yield_1_year": _decimal_to_float(ty.yield_1yr),
        "yield_2_year": _decimal_to_float(ty.yield_2yr),
        "yield_5_year": _decimal_to_float(ty.yield_5yr),
        "yield_10_year": _decimal_to_float(ty.yield_10yr),
        "yield_30_year": _decimal_to_float(ty.yield_30yr),
    }


def inflation_to_dict(records: list[domain.InflationData]) -> dict[str, Any] | None:
    """Convert InflationData list to single MCP dict.

    The domain model has one record per indicator, but MCP expects
    a single record with all indicators per date.
    """
    if not records:
        return None

    # Build mapping of indicator -> value
    indicator_map: dict[str, Decimal] = {}
    record_date = records[0].date

    for record in records:
        indicator_map[record.indicator.lower()] = record.value

    return {
        "date": record_date,
        "cpi": _decimal_to_float(indicator_map.get("cpi")),
        "cpi_core": _decimal_to_float(indicator_map.get("cpi_core")),
        "inflation_yoy": _decimal_to_float(indicator_map.get("cpi_yoy")),
        "pce": _decimal_to_float(indicator_map.get("pce")),
        "pce_core": _decimal_to_float(indicator_map.get("pce_core")),
    }


def labor_market_to_dict(records: list[domain.LaborMarketData]) -> dict[str, Any] | None:
    """Convert LaborMarketData list to single MCP dict.

    The domain model has one record per indicator, but MCP expects
    a single record with all indicators per date.
    """
    if not records:
        return None

    # Build mapping of indicator -> value
    indicator_map: dict[str, Decimal] = {}
    record_date = records[0].date

    for record in records:
        indicator_map[record.indicator.lower()] = record.value

    return {
        "date": record_date,
        "unemployment_rate": _decimal_to_float(indicator_map.get("unemployment_rate")),
        "labor_force_participation_rate": _decimal_to_float(
            indicator_map.get("labor_force_participation_rate")
        ),
        "avg_hourly_earnings": _decimal_to_float(indicator_map.get("avg_hourly_earnings")),
        "job_openings": _decimal_to_float(indicator_map.get("job_openings")),
    }
