"""Fundamentals MCP tools (balance sheets, cash flows, income statements)."""

import logging
from typing import Any, Literal

from ..database import execute_query

logger = logging.getLogger(__name__)

Timeframe = Literal["quarterly", "annual"]


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
            (net_cash_from_operating_activities
             + purchase_of_property_plant_and_equipment) as free_cash_flow
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
            revenue AS total_revenue,
            cost_of_revenue,
            gross_profit,
            total_operating_expenses AS operating_expenses,
            operating_income,
            consolidated_net_income_loss AS net_income,
            basic_earnings_per_share AS basic_eps,
            diluted_earnings_per_share AS diluted_eps,
            ebitda,
            CASE WHEN revenue > 0 THEN ROUND(gross_profit / revenue * 100, 2) END AS gross_margin,
            CASE WHEN revenue > 0 THEN ROUND(operating_income / revenue * 100, 2) END AS operating_margin,
            CASE WHEN revenue > 0 THEN ROUND(consolidated_net_income_loss / revenue * 100, 2) END AS profit_margin
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
