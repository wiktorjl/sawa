"""Stock routes for watchlist and stock details."""

import json
import logging
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from fastapi import Form

from sawa_web.database.connection import execute_query, execute_write, execute_write_returning
from sawa_web.dependencies import CurrentUser
from sawa_web.services.ai_service import (
    ZAIError,
    generate_company_overview,
    save_company_overview,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/stocks", tags=["stocks"])

TEMPLATES_DIR = Path(__file__).parent.parent / "templates"
templates = Jinja2Templates(directory=TEMPLATES_DIR)


@router.get("", response_class=HTMLResponse)
async def stocks_list(request: Request, user: CurrentUser):
    """Render the stocks/watchlist page."""
    watchlists = await get_user_watchlists(user["id"])
    default_watchlist = next((w for w in watchlists if w["is_default"]), watchlists[0] if watchlists else None)

    stocks = []
    if default_watchlist:
        stocks = await get_watchlist_stocks(default_watchlist["id"])

    return templates.TemplateResponse(
        "stocks/list.html",
        {
            "request": request,
            "user": user,
            "active_page": "stocks",
            "page_title": "Stocks",
            "watchlists": watchlists,
            "current_watchlist": default_watchlist,
            "stocks": stocks,
        },
    )


@router.get("/watchlist-preview", response_class=HTMLResponse)
async def watchlist_preview(request: Request, user: CurrentUser):
    """Get a preview of the user's watchlist for the dashboard."""
    watchlist = await execute_query(
        """
        SELECT id FROM watchlists
        WHERE user_id = $1 AND is_default = TRUE
        LIMIT 1
        """,
        user["id"],
        fetch_one=True,
    )

    stocks = []
    if watchlist:
        stocks = await get_watchlist_stocks(watchlist["id"], limit=5)

    return templates.TemplateResponse(
        "stocks/partials/watchlist_preview.html",
        {
            "request": request,
            "stocks": stocks,
        },
    )


@router.get("/watchlist/{watchlist_id}", response_class=HTMLResponse)
async def watchlist_stocks_partial(request: Request, user: CurrentUser, watchlist_id: int):
    """Get stocks for a specific watchlist (partial for HTMX)."""
    # Verify watchlist belongs to user
    watchlist = await execute_query(
        """
        SELECT id FROM watchlists
        WHERE id = $1 AND user_id = $2
        """,
        watchlist_id,
        user["id"],
        fetch_one=True,
    )

    if not watchlist:
        return HTMLResponse("<p class='text-muted text-center py-4'>Watchlist not found.</p>")

    stocks = await get_watchlist_stocks(watchlist_id)

    return templates.TemplateResponse(
        "stocks/partials/stocks_table.html",
        {
            "request": request,
            "stocks": stocks,
        },
    )


@router.get("/search", response_class=HTMLResponse)
async def search_stocks(request: Request, user: CurrentUser, q: str = ""):
    """Search for stocks by ticker or name."""
    if len(q) < 1:
        return HTMLResponse("")

    results = await execute_query(
        """
        SELECT ticker, name, sic_description as sector
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
        "stocks/partials/search_results.html",
        {
            "request": request,
            "results": results,
            "query": q,
        },
    )


@router.get("/{ticker}", response_class=HTMLResponse)
async def stock_detail(request: Request, user: CurrentUser, ticker: str):
    """Render the stock detail page."""
    ticker = ticker.upper()

    # Get company info
    company = await execute_query(
        """
        SELECT c.*,
               c.sic_description as sector,
               c.total_employees as employees,
               c.homepage_url as website,
               fr.price_to_earnings as pe_ratio,
               fr.price_to_book,
               fr.dividend_yield,
               fr.return_on_equity as roe,
               fr.debt_to_equity,
               fr.current as current_ratio
        FROM companies c
        LEFT JOIN LATERAL (
            SELECT price_to_earnings, price_to_book, dividend_yield,
                   return_on_equity, debt_to_equity, current
            FROM financial_ratios
            WHERE ticker = c.ticker
            ORDER BY date DESC
            LIMIT 1
        ) fr ON true
        WHERE c.ticker = $1
        """,
        ticker,
        fetch_one=True,
    )

    if not company:
        return templates.TemplateResponse(
            "stocks/not_found.html",
            {
                "request": request,
                "user": user,
                "active_page": "stocks",
                "page_title": "Not Found",
                "ticker": ticker,
            },
            status_code=404,
        )

    # Get latest price
    latest_price = await execute_query(
        """
        SELECT date, open, high, low, close, volume,
               CASE WHEN open > 0 THEN ((close - open) / open * 100) ELSE 0 END as change_percent
        FROM stock_prices
        WHERE ticker = $1
        ORDER BY date DESC
        LIMIT 1
        """,
        ticker,
        fetch_one=True,
    )

    # Get recent prices for chart (last 60 days)
    price_history_raw = await execute_query(
        """
        SELECT date, close
        FROM stock_prices
        WHERE ticker = $1
        ORDER BY date DESC
        LIMIT 60
        """,
        ticker,
    )
    # Convert Decimal to float and date to string for JSON serialization
    price_history = [
        {"date": str(p["date"]), "close": float(p["close"]) if p["close"] else 0}
        for p in (price_history_raw or [])
    ]

    # Check if in watchlist
    in_watchlist = await execute_query(
        """
        SELECT 1 FROM watchlist_symbols ws
        JOIN watchlists w ON ws.watchlist_id = w.id
        WHERE w.user_id = $1 AND ws.ticker = $2
        LIMIT 1
        """,
        user["id"],
        ticker,
        fetch_one=True,
    )

    # Get recent news
    news = await execute_query(
        """
        SELECT na.title, na.author, na.published_utc, na.article_url, na.description
        FROM news_articles na
        JOIN news_article_tickers nat ON na.id = nat.article_id
        WHERE nat.ticker = $1
        ORDER BY na.published_utc DESC
        LIMIT 5
        """,
        ticker,
    )

    # Get AI overview if available
    overview_raw = await execute_query(
        """
        SELECT main_product, revenue_model, headwinds, tailwinds,
               sector_outlook, competitive_position, generated_at
        FROM company_overviews
        WHERE ticker = $1
        ORDER BY generated_at DESC
        LIMIT 1
        """,
        ticker,
        fetch_one=True,
    )

    # Process JSONB fields - asyncpg may return them as strings
    overview = None
    if overview_raw:
        import json
        overview = dict(overview_raw)
        # Parse JSON strings if needed
        for field in ['headwinds', 'tailwinds']:
            if overview.get(field) and isinstance(overview[field], str):
                try:
                    overview[field] = json.loads(overview[field])
                except json.JSONDecodeError:
                    overview[field] = []

    return templates.TemplateResponse(
        "stocks/detail.html",
        {
            "request": request,
            "user": user,
            "active_page": "stocks",
            "page_title": ticker,
            "ticker": ticker,
            "company": company,
            "latest_price": latest_price,
            "price_history": list(reversed(price_history)),
            "in_watchlist": in_watchlist is not None,
            "news": news or [],
            "overview": overview,
        },
    )


@router.post("/{ticker}/watchlist", response_class=HTMLResponse)
async def toggle_watchlist(request: Request, user: CurrentUser, ticker: str):
    """Add or remove a stock from the user's default watchlist."""
    ticker = ticker.upper()

    # Get default watchlist
    watchlist = await execute_query(
        """
        SELECT id FROM watchlists
        WHERE user_id = $1 AND is_default = TRUE
        """,
        user["id"],
        fetch_one=True,
    )

    if not watchlist:
        return HTMLResponse("No watchlist found", status_code=400)

    # Check if already in watchlist
    existing = await execute_query(
        """
        SELECT 1 FROM watchlist_symbols
        WHERE watchlist_id = $1 AND ticker = $2
        """,
        watchlist["id"],
        ticker,
        fetch_one=True,
    )

    if existing:
        # Remove from watchlist
        await execute_write(
            """
            DELETE FROM watchlist_symbols
            WHERE watchlist_id = $1 AND ticker = $2
            """,
            watchlist["id"],
            ticker,
        )
        in_watchlist = False
    else:
        # Add to watchlist
        await execute_write(
            """
            INSERT INTO watchlist_symbols (watchlist_id, ticker, sort_order)
            VALUES ($1, $2, (SELECT COALESCE(MAX(sort_order), 0) + 1 FROM watchlist_symbols WHERE watchlist_id = $1))
            """,
            watchlist["id"],
            ticker,
        )
        in_watchlist = True

    return templates.TemplateResponse(
        "stocks/partials/watchlist_button.html",
        {
            "request": request,
            "ticker": ticker,
            "in_watchlist": in_watchlist,
        },
    )


@router.post("/{ticker}/generate-overview", response_class=HTMLResponse)
async def generate_overview(request: Request, user: CurrentUser, ticker: str):
    """Generate AI overview for a stock."""
    ticker = ticker.upper()

    # Get company info for the AI prompt
    company = await execute_query(
        """
        SELECT ticker, name, sic_description as sector
        FROM companies
        WHERE ticker = $1
        """,
        ticker,
        fetch_one=True,
    )

    if not company:
        return HTMLResponse(
            '<p class="text-danger text-center py-4">Company not found.</p>',
            status_code=404,
        )

    try:
        # Generate the overview using AI
        overview_obj = await generate_company_overview(
            ticker=company["ticker"],
            company_name=company["name"],
            sector=company.get("sector"),
            user_id=user["id"],
        )

        # Save to database
        await save_company_overview(overview_obj)

        # Convert to dict for template
        overview = {
            "main_product": overview_obj.main_product,
            "revenue_model": overview_obj.revenue_model,
            "headwinds": overview_obj.headwinds,
            "tailwinds": overview_obj.tailwinds,
            "sector_outlook": overview_obj.sector_outlook,
            "competitive_position": overview_obj.competitive_position,
            "generated_at": overview_obj.generated_at,
        }

        return templates.TemplateResponse(
            "stocks/partials/overview_content.html",
            {
                "request": request,
                "ticker": ticker,
                "overview": overview,
            },
        )

    except ZAIError as e:
        logger.error(f"Failed to generate overview for {ticker}: {e}")
        return HTMLResponse(
            f'<p class="text-danger text-center py-4">Failed to generate overview: {e.message}</p>',
            status_code=500,
        )
    except Exception as e:
        logger.error(f"Unexpected error generating overview for {ticker}: {e}")
        return HTMLResponse(
            '<p class="text-danger text-center py-4">An unexpected error occurred. Please try again.</p>',
            status_code=500,
        )


async def get_user_watchlists(user_id: int) -> list:
    """Get all watchlists for a user."""
    return await execute_query(
        """
        SELECT id, name, is_default,
               (SELECT COUNT(*) FROM watchlist_symbols WHERE watchlist_id = watchlists.id) as stock_count
        FROM watchlists
        WHERE user_id = $1
        ORDER BY is_default DESC, name
        """,
        user_id,
    ) or []


async def get_watchlist_stocks(watchlist_id: int, limit: int | None = None) -> list:
    """Get stocks in a watchlist with current prices."""
    limit_clause = f"LIMIT {limit}" if limit else ""
    return await execute_query(
        f"""
        SELECT
            c.ticker,
            c.name,
            c.sic_description as sector,
            sp.close as price,
            CASE WHEN sp.open > 0 THEN ((sp.close - sp.open) / sp.open * 100) ELSE 0 END as change_percent,
            sp.volume
        FROM watchlist_symbols ws
        JOIN companies c ON ws.ticker = c.ticker
        LEFT JOIN LATERAL (
            SELECT open, close, volume
            FROM stock_prices
            WHERE ticker = c.ticker
            ORDER BY date DESC
            LIMIT 1
        ) sp ON true
        WHERE ws.watchlist_id = $1
        ORDER BY ws.sort_order
        {limit_clause}
        """,
        watchlist_id,
    ) or []


# ============================================
# WATCHLIST CRUD OPERATIONS
# ============================================


@router.post("/watchlists/create", response_class=HTMLResponse)
async def create_watchlist(
    request: Request,
    user: CurrentUser,
    name: str = Form(...),
):
    """Create a new watchlist."""
    name = name.strip()
    if not name:
        return HTMLResponse(
            '<div class="alert alert-danger">Watchlist name cannot be empty</div>',
            status_code=400,
        )

    # Check if name already exists for this user
    existing = await execute_query(
        "SELECT 1 FROM watchlists WHERE user_id = $1 AND name = $2",
        user["id"],
        name,
        fetch_one=True,
    )

    if existing:
        return HTMLResponse(
            '<div class="alert alert-danger">A watchlist with this name already exists</div>',
            status_code=400,
        )

    # Create the watchlist
    new_watchlist = await execute_write_returning(
        """
        INSERT INTO watchlists (user_id, name, is_default)
        VALUES ($1, $2, FALSE)
        RETURNING id, name, is_default
        """,
        user["id"],
        name,
    )

    # Return updated watchlist tabs
    watchlists = await get_user_watchlists(user["id"])

    return templates.TemplateResponse(
        "stocks/partials/watchlist_tabs.html",
        {
            "request": request,
            "watchlists": watchlists,
            "current_watchlist": new_watchlist,
            "created": True,
        },
    )


@router.post("/watchlists/{watchlist_id}/rename", response_class=HTMLResponse)
async def rename_watchlist(
    request: Request,
    user: CurrentUser,
    watchlist_id: int,
    name: str = Form(...),
):
    """Rename a watchlist."""
    name = name.strip()
    if not name:
        return HTMLResponse(
            '<div class="alert alert-danger">Watchlist name cannot be empty</div>',
            status_code=400,
        )

    # Verify watchlist belongs to user
    watchlist = await execute_query(
        "SELECT id, name, is_default FROM watchlists WHERE id = $1 AND user_id = $2",
        watchlist_id,
        user["id"],
        fetch_one=True,
    )

    if not watchlist:
        return HTMLResponse(
            '<div class="alert alert-danger">Watchlist not found</div>',
            status_code=404,
        )

    # Check if new name already exists
    existing = await execute_query(
        "SELECT 1 FROM watchlists WHERE user_id = $1 AND name = $2 AND id != $3",
        user["id"],
        name,
        watchlist_id,
        fetch_one=True,
    )

    if existing:
        return HTMLResponse(
            '<div class="alert alert-danger">A watchlist with this name already exists</div>',
            status_code=400,
        )

    # Update the name
    await execute_write(
        "UPDATE watchlists SET name = $1 WHERE id = $2",
        name,
        watchlist_id,
    )

    # Return updated watchlist tabs
    watchlists = await get_user_watchlists(user["id"])
    current = {"id": watchlist_id, "name": name, "is_default": watchlist["is_default"]}

    return templates.TemplateResponse(
        "stocks/partials/watchlist_tabs.html",
        {
            "request": request,
            "watchlists": watchlists,
            "current_watchlist": current,
        },
    )


@router.delete("/watchlists/{watchlist_id}", response_class=HTMLResponse)
async def delete_watchlist(
    request: Request,
    user: CurrentUser,
    watchlist_id: int,
):
    """Delete a watchlist."""
    # Verify watchlist belongs to user and is not default
    watchlist = await execute_query(
        "SELECT id, name, is_default FROM watchlists WHERE id = $1 AND user_id = $2",
        watchlist_id,
        user["id"],
        fetch_one=True,
    )

    if not watchlist:
        return HTMLResponse(
            '<div class="alert alert-danger">Watchlist not found</div>',
            status_code=404,
        )

    if watchlist["is_default"]:
        return HTMLResponse(
            '<div class="alert alert-danger">Cannot delete the default watchlist</div>',
            status_code=400,
        )

    # Delete the watchlist (cascade will delete symbols)
    await execute_write(
        "DELETE FROM watchlists WHERE id = $1",
        watchlist_id,
    )

    # Return updated watchlist tabs with default selected
    watchlists = await get_user_watchlists(user["id"])
    default_watchlist = next((w for w in watchlists if w["is_default"]), watchlists[0] if watchlists else None)

    return templates.TemplateResponse(
        "stocks/partials/watchlist_tabs.html",
        {
            "request": request,
            "watchlists": watchlists,
            "current_watchlist": default_watchlist,
            "deleted": True,
        },
    )


@router.post("/watchlists/{watchlist_id}/set-default", response_class=HTMLResponse)
async def set_default_watchlist(
    request: Request,
    user: CurrentUser,
    watchlist_id: int,
):
    """Set a watchlist as the default."""
    # Verify watchlist belongs to user
    watchlist = await execute_query(
        "SELECT id, name FROM watchlists WHERE id = $1 AND user_id = $2",
        watchlist_id,
        user["id"],
        fetch_one=True,
    )

    if not watchlist:
        return HTMLResponse(
            '<div class="alert alert-danger">Watchlist not found</div>',
            status_code=404,
        )

    # Unset current default
    await execute_write(
        "UPDATE watchlists SET is_default = FALSE WHERE user_id = $1",
        user["id"],
    )

    # Set new default
    await execute_write(
        "UPDATE watchlists SET is_default = TRUE WHERE id = $1",
        watchlist_id,
    )

    # Return updated watchlist tabs
    watchlists = await get_user_watchlists(user["id"])
    current = {"id": watchlist_id, "name": watchlist["name"], "is_default": True}

    return templates.TemplateResponse(
        "stocks/partials/watchlist_tabs.html",
        {
            "request": request,
            "watchlists": watchlists,
            "current_watchlist": current,
        },
    )
