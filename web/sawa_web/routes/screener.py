"""Screener routes for stock filtering."""

import logging
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from sawa_web.dependencies import CurrentUser
from sawa_web.services.screener_service import (
    ScreenerError,
    execute_screener,
    get_query_help,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/screener", tags=["screener"])

TEMPLATES_DIR = Path(__file__).parent.parent / "templates"
templates = Jinja2Templates(directory=TEMPLATES_DIR)


def format_number(value, compact: bool = True) -> str:
    """Format a number for display."""
    if value is None:
        return "-"

    try:
        value = float(value)
    except (ValueError, TypeError):
        return "-"

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


def format_ratio(value, is_percent: bool = False) -> str:
    """Format a ratio for display."""
    if value is None:
        return "-"

    try:
        value = float(value)
    except (ValueError, TypeError):
        return "-"

    if is_percent:
        return f"{value * 100:.2f}%"
    else:
        return f"{value:.2f}"


@router.get("", response_class=HTMLResponse)
async def screener_page(request: Request, user: CurrentUser):
    """Render the main screener page."""
    help_info = get_query_help()

    return templates.TemplateResponse(
        "screener/index.html",
        {
            "request": request,
            "user": user,
            "active_page": "screener",
            "page_title": "Screener",
            "help_info": help_info,
        },
    )


@router.post("/run", response_class=HTMLResponse)
async def run_screener(
    request: Request,
    user: CurrentUser,
    query: str = Form(""),
):
    """Execute a screener query and return results."""
    if not query or not query.strip():
        return HTMLResponse(
            '<div class="alert alert-warning">Please enter a query</div>',
            status_code=400,
        )

    try:
        results = await execute_screener(query)

        return templates.TemplateResponse(
            "screener/partials/results.html",
            {
                "request": request,
                "results": results,
                "query": query,
                "count": len(results),
                "format_number": format_number,
                "format_ratio": format_ratio,
            },
        )

    except ScreenerError as e:
        return HTMLResponse(
            f'<div class="alert alert-danger">{e}</div>',
            status_code=400,
        )
    except Exception as e:
        logger.error(f"Screener error: {e}")
        return HTMLResponse(
            '<div class="alert alert-danger">An error occurred while processing your query</div>',
            status_code=500,
        )


@router.get("/help", response_class=HTMLResponse)
async def screener_help(request: Request, user: CurrentUser):
    """Get screener help panel."""
    help_info = get_query_help()

    return templates.TemplateResponse(
        "screener/partials/help.html",
        {
            "request": request,
            "help_info": help_info,
        },
    )
