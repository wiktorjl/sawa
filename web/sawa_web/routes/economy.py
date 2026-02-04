"""Economy routes for macroeconomic data display."""

import logging
from decimal import Decimal
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from sawa_web.database.connection import execute_query
from sawa_web.dependencies import CurrentUser

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/economy", tags=["economy"])

TEMPLATES_DIR = Path(__file__).parent.parent / "templates"
templates = Jinja2Templates(directory=TEMPLATES_DIR)


def format_percent(value, decimals: int = 2) -> str:
    """Format a decimal as percentage."""
    if value is None:
        return "-"
    if isinstance(value, Decimal):
        value = float(value)
    return f"{value:.{decimals}f}%"


def format_number(value, suffix: str = "") -> str:
    """Format a number for display."""
    if value is None:
        return "-"
    if isinstance(value, Decimal):
        value = float(value)
    return f"{value:,.2f}{suffix}"


@router.get("", response_class=HTMLResponse)
async def economy_page(request: Request, user: CurrentUser):
    """Render the main economy page."""
    # Get latest data for summary cards
    latest_yields = await execute_query(
        """
        SELECT date, yield_2_year, yield_10_year, yield_30_year
        FROM treasury_yields
        ORDER BY date DESC
        LIMIT 1
        """,
        fetch_one=True,
    )

    latest_inflation = await execute_query(
        """
        SELECT date, cpi_year_over_year
        FROM inflation
        WHERE cpi_year_over_year IS NOT NULL
        ORDER BY date DESC
        LIMIT 1
        """,
        fetch_one=True,
    )

    latest_labor = await execute_query(
        """
        SELECT date, unemployment_rate
        FROM labor_market
        WHERE unemployment_rate IS NOT NULL
        ORDER BY date DESC
        LIMIT 1
        """,
        fetch_one=True,
    )

    # Check for yield curve inversion
    yield_inverted = False
    if latest_yields and latest_yields.get("yield_2_year") and latest_yields.get("yield_10_year"):
        yield_inverted = float(latest_yields["yield_2_year"]) > float(latest_yields["yield_10_year"])

    return templates.TemplateResponse(
        "economy/index.html",
        {
            "request": request,
            "user": user,
            "active_page": "economy",
            "page_title": "Economy",
            "latest_yields": latest_yields,
            "latest_inflation": latest_inflation,
            "latest_labor": latest_labor,
            "yield_inverted": yield_inverted,
            "format_percent": format_percent,
            "active_tab": "yields",
        },
    )


@router.get("/yields", response_class=HTMLResponse)
async def treasury_yields(request: Request, user: CurrentUser):
    """Get treasury yields data partial."""
    data = await execute_query(
        """
        SELECT
            date,
            yield_1_month,
            yield_3_month,
            yield_6_month,
            yield_1_year,
            yield_2_year,
            yield_5_year,
            yield_10_year,
            yield_20_year,
            yield_30_year
        FROM treasury_yields
        ORDER BY date DESC
        LIMIT 20
        """,
    )

    # Calculate yield curve spread (10Y - 2Y)
    yield_spreads = []
    for row in (data or []):
        spread = None
        if row.get("yield_10_year") and row.get("yield_2_year"):
            spread = float(row["yield_10_year"]) - float(row["yield_2_year"])
        yield_spreads.append(spread)

    return templates.TemplateResponse(
        "economy/partials/yields.html",
        {
            "request": request,
            "data": data or [],
            "yield_spreads": yield_spreads,
            "format_percent": format_percent,
        },
    )


@router.get("/inflation", response_class=HTMLResponse)
async def inflation_data(request: Request, user: CurrentUser):
    """Get inflation data partial."""
    data = await execute_query(
        """
        SELECT
            date,
            cpi,
            cpi_core,
            cpi_year_over_year,
            pce,
            pce_core
        FROM inflation
        ORDER BY date DESC
        LIMIT 20
        """,
    )

    return templates.TemplateResponse(
        "economy/partials/inflation.html",
        {
            "request": request,
            "data": data or [],
            "format_percent": format_percent,
            "format_number": format_number,
        },
    )


@router.get("/labor", response_class=HTMLResponse)
async def labor_market(request: Request, user: CurrentUser):
    """Get labor market data partial."""
    data = await execute_query(
        """
        SELECT
            date,
            unemployment_rate,
            labor_force_participation_rate,
            avg_hourly_earnings,
            job_openings
        FROM labor_market
        ORDER BY date DESC
        LIMIT 20
        """,
    )

    return templates.TemplateResponse(
        "economy/partials/labor.html",
        {
            "request": request,
            "data": data or [],
            "format_percent": format_percent,
            "format_number": format_number,
        },
    )
