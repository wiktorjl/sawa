"""Fundamentals routes for financial statements display."""

import logging
from decimal import Decimal
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from sawa_web.database.connection import execute_query
from sawa_web.dependencies import CurrentUser

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/fundamentals", tags=["fundamentals"])

TEMPLATES_DIR = Path(__file__).parent.parent / "templates"
templates = Jinja2Templates(directory=TEMPLATES_DIR)


def format_number(value, compact: bool = True) -> str:
    """Format a number for display."""
    if value is None:
        return "-"

    if isinstance(value, Decimal):
        value = float(value)

    if compact:
        abs_value = abs(value)
        if abs_value >= 1_000_000_000_000:
            return f"${value / 1_000_000_000_000:.2f}T"
        elif abs_value >= 1_000_000_000:
            return f"${value / 1_000_000_000:.2f}B"
        elif abs_value >= 1_000_000:
            return f"${value / 1_000_000:.2f}M"
        elif abs_value >= 1_000:
            return f"${value / 1_000:.2f}K"
        else:
            return f"${value:.2f}"
    else:
        return f"${value:,.2f}"


@router.get("", response_class=HTMLResponse)
async def fundamentals_page(request: Request, user: CurrentUser, ticker: str = ""):
    """Render the main fundamentals page."""
    ticker = ticker.upper() if ticker else ""

    company = None
    if ticker:
        company = await execute_query(
            "SELECT ticker, name, sic_description as sector FROM companies WHERE ticker = $1",
            ticker,
            fetch_one=True,
        )

    # Get user's default timeframe setting
    timeframe_setting = await execute_query(
        "SELECT value FROM user_settings WHERE user_id = $1 AND key = 'fundamentals_timeframe'",
        user["id"],
        fetch_one=True,
    )
    timeframe = timeframe_setting["value"] if timeframe_setting else "quarterly"

    return templates.TemplateResponse(
        "fundamentals/index.html",
        {
            "request": request,
            "user": user,
            "active_page": "fundamentals",
            "page_title": f"Fundamentals - {ticker}" if ticker else "Fundamentals",
            "ticker": ticker,
            "company": company,
            "timeframe": timeframe,
            "active_tab": "income",
        },
    )


@router.get("/search", response_class=HTMLResponse)
async def search_tickers(request: Request, user: CurrentUser, q: str = ""):
    """Search for tickers."""
    if len(q) < 1:
        return HTMLResponse("")

    results = await execute_query(
        """
        SELECT ticker, name
        FROM companies
        WHERE ticker ILIKE $1 OR name ILIKE $1
        ORDER BY
            CASE WHEN ticker ILIKE $2 THEN 0 ELSE 1 END,
            ticker
        LIMIT 10
        """,
        f"%{q}%",
        f"{q}%",
    )

    return templates.TemplateResponse(
        "fundamentals/partials/ticker_search.html",
        {
            "request": request,
            "results": results or [],
            "query": q,
        },
    )


@router.get("/{ticker}", response_class=HTMLResponse)
async def fundamentals_for_ticker(request: Request, user: CurrentUser, ticker: str):
    """Redirect to fundamentals page with ticker."""
    ticker = ticker.upper()

    company = await execute_query(
        "SELECT ticker, name, sic_description as sector FROM companies WHERE ticker = $1",
        ticker,
        fetch_one=True,
    )

    if not company:
        return templates.TemplateResponse(
            "fundamentals/index.html",
            {
                "request": request,
                "user": user,
                "active_page": "fundamentals",
                "page_title": "Fundamentals",
                "ticker": "",
                "company": None,
                "timeframe": "quarterly",
                "active_tab": "income",
                "error": f"Company '{ticker}' not found",
            },
        )

    # Get user's default timeframe setting
    timeframe_setting = await execute_query(
        "SELECT value FROM user_settings WHERE user_id = $1 AND key = 'fundamentals_timeframe'",
        user["id"],
        fetch_one=True,
    )
    timeframe = timeframe_setting["value"] if timeframe_setting else "quarterly"

    return templates.TemplateResponse(
        "fundamentals/index.html",
        {
            "request": request,
            "user": user,
            "active_page": "fundamentals",
            "page_title": f"Fundamentals - {ticker}",
            "ticker": ticker,
            "company": company,
            "timeframe": timeframe,
            "active_tab": "income",
        },
    )


@router.get("/{ticker}/income", response_class=HTMLResponse)
async def income_statement(
    request: Request,
    user: CurrentUser,
    ticker: str,
    timeframe: str = "quarterly",
):
    """Get income statement data partial."""
    ticker = ticker.upper()

    if timeframe not in ("quarterly", "annual"):
        timeframe = "quarterly"

    data = await execute_query(
        """
        SELECT
            period_end,
            fiscal_quarter,
            fiscal_year,
            revenue,
            cost_of_revenue,
            gross_profit,
            research_development,
            selling_general_administrative,
            total_operating_expenses,
            operating_income,
            interest_income,
            interest_expense,
            other_income_expense,
            income_before_income_taxes,
            income_taxes,
            consolidated_net_income_loss as net_income,
            basic_earnings_per_share as eps,
            ebitda
        FROM income_statements
        WHERE ticker = $1 AND timeframe = $2
        ORDER BY period_end DESC
        LIMIT 12
        """,
        ticker,
        timeframe,
    )

    # Define the metrics to display
    metrics = [
        ("Revenue", "revenue"),
        ("Cost of Revenue", "cost_of_revenue"),
        ("Gross Profit", "gross_profit"),
        ("R&D Expenses", "research_development"),
        ("SG&A Expenses", "selling_general_administrative"),
        ("Operating Expenses", "total_operating_expenses"),
        ("Operating Income", "operating_income"),
        ("Interest Income", "interest_income"),
        ("Interest Expense", "interest_expense"),
        ("Other Income/Expense", "other_income_expense"),
        ("Pre-Tax Income", "income_before_income_taxes"),
        ("Income Taxes", "income_taxes"),
        ("Net Income", "net_income"),
        ("EPS (Basic)", "eps"),
        ("EBITDA", "ebitda"),
    ]

    return templates.TemplateResponse(
        "fundamentals/partials/income.html",
        {
            "request": request,
            "ticker": ticker,
            "timeframe": timeframe,
            "data": data or [],
            "metrics": metrics,
            "format_number": format_number,
        },
    )


@router.get("/{ticker}/balance", response_class=HTMLResponse)
async def balance_sheet(
    request: Request,
    user: CurrentUser,
    ticker: str,
    timeframe: str = "quarterly",
):
    """Get balance sheet data partial."""
    ticker = ticker.upper()

    if timeframe not in ("quarterly", "annual"):
        timeframe = "quarterly"

    data = await execute_query(
        """
        SELECT
            period_end,
            fiscal_quarter,
            fiscal_year,
            cash_and_equivalents,
            short_term_investments,
            receivables,
            inventories,
            total_current_assets,
            property_plant_equipment_net,
            goodwill,
            intangible_assets_net,
            total_assets,
            accounts_payable,
            debt_current,
            total_current_liabilities,
            long_term_debt_and_capital_lease_obligations as long_term_debt,
            total_liabilities,
            common_stock,
            retained_earnings_deficit as retained_earnings,
            total_equity,
            total_liabilities_and_equity
        FROM balance_sheets
        WHERE ticker = $1 AND timeframe = $2
        ORDER BY period_end DESC
        LIMIT 12
        """,
        ticker,
        timeframe,
    )

    # Define the metrics to display
    metrics = [
        ("Cash & Equivalents", "cash_and_equivalents"),
        ("Short-term Investments", "short_term_investments"),
        ("Receivables", "receivables"),
        ("Inventories", "inventories"),
        ("Total Current Assets", "total_current_assets"),
        ("PP&E (Net)", "property_plant_equipment_net"),
        ("Goodwill", "goodwill"),
        ("Intangible Assets", "intangible_assets_net"),
        ("Total Assets", "total_assets"),
        ("Accounts Payable", "accounts_payable"),
        ("Current Debt", "debt_current"),
        ("Total Current Liabilities", "total_current_liabilities"),
        ("Long-term Debt", "long_term_debt"),
        ("Total Liabilities", "total_liabilities"),
        ("Common Stock", "common_stock"),
        ("Retained Earnings", "retained_earnings"),
        ("Total Equity", "total_equity"),
        ("Total Liabilities & Equity", "total_liabilities_and_equity"),
    ]

    return templates.TemplateResponse(
        "fundamentals/partials/balance.html",
        {
            "request": request,
            "ticker": ticker,
            "timeframe": timeframe,
            "data": data or [],
            "metrics": metrics,
            "format_number": format_number,
        },
    )


@router.get("/{ticker}/cashflow", response_class=HTMLResponse)
async def cash_flow(
    request: Request,
    user: CurrentUser,
    ticker: str,
    timeframe: str = "quarterly",
):
    """Get cash flow data partial."""
    ticker = ticker.upper()

    if timeframe not in ("quarterly", "annual"):
        timeframe = "quarterly"

    data = await execute_query(
        """
        SELECT
            period_end,
            fiscal_quarter,
            fiscal_year,
            net_income,
            depreciation_depletion_and_amortization as depreciation,
            change_in_other_operating_assets_and_liabilities_net as working_capital_changes,
            net_cash_from_operating_activities as operating_cf,
            purchase_of_property_plant_and_equipment as capex,
            sale_of_property_plant_and_equipment as asset_sales,
            other_investing_activities,
            net_cash_from_investing_activities as investing_cf,
            long_term_debt_issuances_repayments as debt_activity,
            dividends,
            other_financing_activities,
            net_cash_from_financing_activities as financing_cf,
            change_in_cash_and_equivalents as net_change
        FROM cash_flows
        WHERE ticker = $1 AND timeframe = $2
        ORDER BY period_end DESC
        LIMIT 12
        """,
        ticker,
        timeframe,
    )

    # Define the metrics to display
    metrics = [
        ("Net Income", "net_income"),
        ("Depreciation & Amortization", "depreciation"),
        ("Working Capital Changes", "working_capital_changes"),
        ("Operating Cash Flow", "operating_cf"),
        ("Capital Expenditures", "capex"),
        ("Asset Sales", "asset_sales"),
        ("Other Investing", "other_investing_activities"),
        ("Investing Cash Flow", "investing_cf"),
        ("Debt Issuance/Repayment", "debt_activity"),
        ("Dividends Paid", "dividends"),
        ("Other Financing", "other_financing_activities"),
        ("Financing Cash Flow", "financing_cf"),
        ("Net Change in Cash", "net_change"),
    ]

    return templates.TemplateResponse(
        "fundamentals/partials/cashflow.html",
        {
            "request": request,
            "ticker": ticker,
            "timeframe": timeframe,
            "data": data or [],
            "metrics": metrics,
            "format_number": format_number,
        },
    )
