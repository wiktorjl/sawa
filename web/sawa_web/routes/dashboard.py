"""Dashboard routes."""

from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from sawa_web.database.connection import execute_query
from sawa_web.dependencies import CurrentUser

router = APIRouter(tags=["dashboard"])

TEMPLATES_DIR = Path(__file__).parent.parent / "templates"
templates = Jinja2Templates(directory=TEMPLATES_DIR)


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, user: CurrentUser):
    """Render the main dashboard page."""

    # Get market summary stats
    market_stats = await get_market_stats()

    # Get user's watchlist summary
    watchlist_summary = await get_watchlist_summary(user["id"])

    # Get recent market movers
    top_gainers = await get_top_movers(order="DESC", limit=5)
    top_losers = await get_top_movers(order="ASC", limit=5)

    return templates.TemplateResponse(
        "dashboard/home.html",
        {
            "request": request,
            "user": user,
            "active_page": "dashboard",
            "page_title": "Dashboard",
            "market_stats": market_stats,
            "watchlist_summary": watchlist_summary,
            "top_gainers": top_gainers,
            "top_losers": top_losers,
        },
    )


async def get_market_stats() -> dict:
    """Get overall market statistics."""
    result = await execute_query(
        """
        SELECT
            COUNT(DISTINCT c.ticker) as total_companies,
            COALESCE(SUM(c.market_cap), 0) as total_market_cap,
            COALESCE(AVG(fr.price_to_earnings), 0) as avg_pe_ratio,
            COALESCE(AVG(fr.dividend_yield), 0) as avg_dividend_yield
        FROM companies c
        LEFT JOIN LATERAL (
            SELECT price_to_earnings, dividend_yield
            FROM financial_ratios
            WHERE ticker = c.ticker
            ORDER BY date DESC
            LIMIT 1
        ) fr ON true
        """,
        fetch_one=True,
    )
    return result or {
        "total_companies": 0,
        "total_market_cap": 0,
        "avg_pe_ratio": 0,
        "avg_dividend_yield": 0,
    }


async def get_watchlist_summary(user_id: int) -> dict:
    """Get summary of user's watchlist."""
    result = await execute_query(
        """
        SELECT
            COUNT(ws.ticker) as stock_count,
            w.name as watchlist_name
        FROM watchlists w
        LEFT JOIN watchlist_symbols ws ON w.id = ws.watchlist_id
        WHERE w.user_id = $1 AND w.is_default = TRUE
        GROUP BY w.id, w.name
        """,
        user_id,
        fetch_one=True,
    )
    return result or {"stock_count": 0, "watchlist_name": "Default"}


async def get_top_movers(order: str = "DESC", limit: int = 5) -> list:
    """Get top market movers by daily change."""
    result = await execute_query(
        f"""
        WITH latest_prices AS (
            SELECT DISTINCT ON (ticker)
                ticker, date, close, open
            FROM stock_prices
            ORDER BY ticker, date DESC
        )
        SELECT
            c.ticker,
            c.name,
            lp.close as price,
            CASE WHEN lp.open > 0 THEN ((lp.close - lp.open) / lp.open * 100) ELSE 0 END as change_percent
        FROM companies c
        JOIN latest_prices lp ON c.ticker = lp.ticker
        WHERE lp.open > 0
        ORDER BY change_percent {order}
        LIMIT $1
        """,
        limit,
    )
    return result or []
