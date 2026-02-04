"""ORION Web Dashboard - FastAPI Application."""

import logging
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from sawa_web.config import get_settings
from sawa_web.database.connection import close_pool, init_pool

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler for startup/shutdown."""
    # Startup
    logger.info("Starting ORION Web Dashboard...")
    await init_pool()
    yield
    # Shutdown
    logger.info("Shutting down ORION Web Dashboard...")
    await close_pool()


# Create FastAPI app
app = FastAPI(
    title="ORION Dashboard",
    description="Sci-fi themed S&P 500 data dashboard",
    version="0.1.0",
    lifespan=lifespan,
)

# Mount static files
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# Set up Jinja2 templates
templates = Jinja2Templates(directory=TEMPLATES_DIR)


# Include routers
from sawa_web.auth.routes import router as auth_router
from sawa_web.routes.dashboard import router as dashboard_router
from sawa_web.routes.stocks import router as stocks_router
from sawa_web.routes.settings import router as settings_router
from sawa_web.routes.fundamentals import router as fundamentals_router
from sawa_web.routes.economy import router as economy_router
from sawa_web.routes.glossary import router as glossary_router
from sawa_web.routes.screener import router as screener_router
from sawa_web.routes.admin import router as admin_router

app.include_router(auth_router)
app.include_router(dashboard_router)
app.include_router(stocks_router)
app.include_router(settings_router)
app.include_router(fundamentals_router)
app.include_router(economy_router)
app.include_router(glossary_router)
app.include_router(screener_router)
app.include_router(admin_router)


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    """Root redirect to dashboard or login."""
    # Check if user is authenticated (session check)
    session = request.cookies.get(get_settings().session_cookie_name)
    if session:
        return RedirectResponse(url="/dashboard", status_code=302)
    return RedirectResponse(url="/login", status_code=302)


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "orion-dashboard"}


def run():
    """Run the application with uvicorn."""
    settings = get_settings()
    uvicorn.run(
        "sawa_web.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
    )


if __name__ == "__main__":
    run()
