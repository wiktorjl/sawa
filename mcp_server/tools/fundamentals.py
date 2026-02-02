"""Fundamentals MCP tools (balance sheets, cash flows, income statements)."""

import logging
from typing import Any

from ..database import execute_query

logger = logging.getLogger(__name__)


# --- Async service-based implementations ---


async def get_fundamentals_async(
    ticker: str,
    timeframe: str = "quarterly",
    limit: int = 4,
) -> dict[str, Any]:
    """Get fundamentals via service layer (async)."""
    from ..services import get_stock_service

    if timeframe not in ("quarterly", "annual"):
        raise ValueError("timeframe must be 'quarterly' or 'annual'")

    service = get_stock_service()
    return await service.get_fundamentals(ticker, timeframe, min(limit, 20))


# --- Sync SQL-based implementations (original) ---


def get_fundamentals(
    ticker: str,
    timeframe: str = "quarterly",
    limit: int = 4,
) -> dict[str, Any]:
    """
    Get latest balance sheet, cash flow, and income statement data.

    Args:
        ticker: Stock ticker symbol (e.g., "AAPL")
        timeframe: "quarterly" or "annual" (default: "quarterly")
        limit: Number of periods to return (default: 4, max: 20)

    Returns:
        Dictionary with balance_sheets, cash_flows, and income_statements keys
    """
    if timeframe not in ("quarterly", "annual"):
        raise ValueError("timeframe must be 'quarterly' or 'annual'")

    limit = min(limit, 20)
    ticker_upper = ticker.upper()

    # Get balance sheets
    balance_sheets_sql = """
        SELECT 
            period_end,
            filing_date,
            fiscal_year,
            fiscal_quarter,
            total_assets,
            total_liabilities,
            total_equity,
            cash_and_equivalents,
            total_current_assets,
            total_current_liabilities,
            long_term_debt_and_capital_lease_obligations as long_term_debt
        FROM balance_sheets
        WHERE ticker = %(ticker)s
            AND timeframe = %(timeframe)s
        ORDER BY period_end DESC
        LIMIT %(limit)s
    """

    # Get cash flows
    cash_flows_sql = """
        SELECT 
            period_end,
            filing_date,
            fiscal_year,
            fiscal_quarter,
            net_cash_from_operating_activities as operating_cash_flow,
            net_cash_from_investing_activities as investing_cash_flow,
            net_cash_from_financing_activities as financing_cash_flow,
            purchase_of_property_plant_and_equipment as capex,
            depreciation_depletion_and_amortization as dda,
            (net_cash_from_operating_activities + purchase_of_property_plant_and_equipment) as free_cash_flow
        FROM cash_flows
        WHERE ticker = %(ticker)s
            AND timeframe = %(timeframe)s
        ORDER BY period_end DESC
        LIMIT %(limit)s
    """

    # Get income statements
    income_statements_sql = """
        SELECT 
            period_end,
            filing_date,
            fiscal_year,
            fiscal_quarter,
            total_revenue,
            cost_of_revenue,
            gross_profit,
            operating_expenses,
            operating_income,
            net_income,
            basic_eps,
            diluted_eps,
            ebitda,
            gross_margin,
            operating_margin,
            profit_margin
        FROM income_statements
        WHERE ticker = %(ticker)s
            AND timeframe = %(timeframe)s
        ORDER BY period_end DESC
        LIMIT %(limit)s
    """

    params = {
        "ticker": ticker_upper,
        "timeframe": timeframe,
        "limit": limit,
    }

    return {
        "balance_sheets": execute_query(balance_sheets_sql, params),
        "cash_flows": execute_query(cash_flows_sql, params),
        "income_statements": execute_query(income_statements_sql, params),
    }


def get_balance_sheet(
    ticker: str,
    timeframe: str = "quarterly",
    limit: int = 4,
) -> list[dict[str, Any]]:
    """
    Get balance sheet data for a ticker.

    Args:
        ticker: Stock ticker symbol
        timeframe: "quarterly" or "annual"
        limit: Number of periods (max: 20)

    Returns:
        List of balance sheet records
    """
    if timeframe not in ("quarterly", "annual"):
        raise ValueError("timeframe must be 'quarterly' or 'annual'")

    limit = min(limit, 20)

    sql = """
        SELECT 
            period_end,
            filing_date,
            fiscal_year,
            fiscal_quarter,
            total_assets,
            total_liabilities,
            total_equity,
            cash_and_equivalents,
            short_term_investments,
            receivables,
            inventories,
            total_current_assets,
            property_plant_equipment_net as ppe_net,
            goodwill,
            intangible_assets_net,
            accounts_payable,
            total_current_liabilities,
            long_term_debt_and_capital_lease_obligations as long_term_debt
        FROM balance_sheets
        WHERE ticker = %(ticker)s
            AND timeframe = %(timeframe)s
        ORDER BY period_end DESC
        LIMIT %(limit)s
    """

    params = {
        "ticker": ticker.upper(),
        "timeframe": timeframe,
        "limit": limit,
    }

    return execute_query(sql, params)


def get_income_statement(
    ticker: str,
    timeframe: str = "quarterly",
    limit: int = 4,
) -> list[dict[str, Any]]:
    """
    Get income statement data for a ticker.

    Args:
        ticker: Stock ticker symbol
        timeframe: "quarterly" or "annual"
        limit: Number of periods (max: 20)

    Returns:
        List of income statement records
    """
    if timeframe not in ("quarterly", "annual"):
        raise ValueError("timeframe must be 'quarterly' or 'annual'")

    limit = min(limit, 20)

    sql = """
        SELECT 
            period_end,
            filing_date,
            fiscal_year,
            fiscal_quarter,
            total_revenue,
            cost_of_revenue,
            gross_profit,
            research_and_development as r&d,
            selling_general_and_administrative as sg&a,
            operating_expenses,
            operating_income,
            interest_income,
            interest_expense,
            income_before_tax,
            income_tax_expense,
            net_income,
            basic_eps,
            diluted_eps,
            ebitda,
            gross_margin,
            operating_margin,
            profit_margin
        FROM income_statements
        WHERE ticker = %(ticker)s
            AND timeframe = %(timeframe)s
        ORDER BY period_end DESC
        LIMIT %(limit)s
    """

    params = {
        "ticker": ticker.upper(),
        "timeframe": timeframe,
        "limit": limit,
    }

    return execute_query(sql, params)
