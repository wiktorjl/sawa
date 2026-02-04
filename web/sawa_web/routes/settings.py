"""Settings routes for user preferences and account management."""

import logging
from pathlib import Path

import bcrypt
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from sawa_web.database.connection import execute_query, execute_write
from sawa_web.dependencies import CurrentUser

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/settings", tags=["settings"])

TEMPLATES_DIR = Path(__file__).parent.parent / "templates"
templates = Jinja2Templates(directory=TEMPLATES_DIR)

# Setting definitions with defaults and descriptions
SETTING_DEFINITIONS = {
    "chart_period_days": {
        "default": "60",
        "type": "select",
        "options": ["30", "60", "90", "180", "365"],
        "label": "Chart Period (Days)",
        "description": "Number of days to display in stock price charts",
    },
    "number_format": {
        "default": "compact",
        "type": "select",
        "options": ["compact", "full"],
        "label": "Number Format",
        "description": "How to display large numbers (compact: 1.5M, full: 1,500,000)",
    },
    "fundamentals_timeframe": {
        "default": "quarterly",
        "type": "select",
        "options": ["quarterly", "annual"],
        "label": "Default Timeframe",
        "description": "Default view for financial statements",
    },
    "chart_detail": {
        "default": "normal",
        "type": "select",
        "options": ["minimal", "normal", "detailed"],
        "label": "Chart Detail Level",
        "description": "Amount of information shown on charts",
    },
    "theme": {
        "default": "osaka-jade",
        "type": "select",
        "options": ["osaka-jade"],
        "label": "Theme",
        "description": "Color theme for the dashboard",
    },
}


@router.get("", response_class=HTMLResponse)
async def settings_page(request: Request, user: CurrentUser):
    """Render the main settings page."""
    # Get user settings
    user_settings = await get_user_settings(user["id"])

    # Merge with defaults
    settings = {}
    for key, definition in SETTING_DEFINITIONS.items():
        settings[key] = {
            "value": user_settings.get(key, definition["default"]),
            **definition,
        }

    # Get API key (masked)
    zai_api_key = user_settings.get("zai_api_key", "")
    has_api_key = bool(zai_api_key)

    return templates.TemplateResponse(
        "settings/index.html",
        {
            "request": request,
            "user": user,
            "active_page": "settings",
            "page_title": "Settings",
            "settings": settings,
            "has_api_key": has_api_key,
            "active_tab": "display",
        },
    )


@router.get("/display", response_class=HTMLResponse)
async def display_settings(request: Request, user: CurrentUser):
    """Get display settings partial."""
    user_settings = await get_user_settings(user["id"])

    settings = {}
    for key in ["number_format", "theme"]:
        definition = SETTING_DEFINITIONS[key]
        settings[key] = {
            "value": user_settings.get(key, definition["default"]),
            **definition,
        }

    return templates.TemplateResponse(
        "settings/partials/display.html",
        {
            "request": request,
            "settings": settings,
        },
    )


@router.get("/charts", response_class=HTMLResponse)
async def charts_settings(request: Request, user: CurrentUser):
    """Get chart settings partial."""
    user_settings = await get_user_settings(user["id"])

    settings = {}
    for key in ["chart_period_days", "chart_detail"]:
        definition = SETTING_DEFINITIONS[key]
        settings[key] = {
            "value": user_settings.get(key, definition["default"]),
            **definition,
        }

    return templates.TemplateResponse(
        "settings/partials/charts.html",
        {
            "request": request,
            "settings": settings,
        },
    )


@router.get("/behavior", response_class=HTMLResponse)
async def behavior_settings(request: Request, user: CurrentUser):
    """Get behavior settings partial."""
    user_settings = await get_user_settings(user["id"])

    settings = {}
    for key in ["fundamentals_timeframe"]:
        definition = SETTING_DEFINITIONS[key]
        settings[key] = {
            "value": user_settings.get(key, definition["default"]),
            **definition,
        }

    return templates.TemplateResponse(
        "settings/partials/behavior.html",
        {
            "request": request,
            "settings": settings,
        },
    )


@router.get("/api", response_class=HTMLResponse)
async def api_settings(request: Request, user: CurrentUser):
    """Get API keys settings partial."""
    user_settings = await get_user_settings(user["id"])

    zai_api_key = user_settings.get("zai_api_key", "")
    has_api_key = bool(zai_api_key)

    return templates.TemplateResponse(
        "settings/partials/api.html",
        {
            "request": request,
            "has_api_key": has_api_key,
        },
    )


@router.get("/account", response_class=HTMLResponse)
async def account_settings(request: Request, user: CurrentUser):
    """Get account settings partial."""
    return templates.TemplateResponse(
        "settings/partials/account.html",
        {
            "request": request,
            "user": user,
        },
    )


@router.post("/update", response_class=HTMLResponse)
async def update_setting(
    request: Request,
    user: CurrentUser,
    key: str = Form(...),
    value: str = Form(...),
):
    """Update a single setting."""
    # Validate key
    if key not in SETTING_DEFINITIONS and key != "zai_api_key":
        return HTMLResponse(
            '<span class="text-danger">Invalid setting key</span>',
            status_code=400,
        )

    # Validate value for predefined settings
    if key in SETTING_DEFINITIONS:
        definition = SETTING_DEFINITIONS[key]
        if definition["type"] == "select" and value not in definition["options"]:
            return HTMLResponse(
                '<span class="text-danger">Invalid value</span>',
                status_code=400,
            )

    # Upsert the setting
    await execute_write(
        """
        INSERT INTO user_settings (user_id, key, value, updated_at)
        VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
        ON CONFLICT (user_id, key)
        DO UPDATE SET value = $3, updated_at = CURRENT_TIMESTAMP
        """,
        user["id"],
        key,
        value,
    )

    return HTMLResponse('<span class="text-success">Saved</span>')


@router.post("/api-key", response_class=HTMLResponse)
async def update_api_key(
    request: Request,
    user: CurrentUser,
    zai_api_key: str = Form(""),
):
    """Update the Z.AI API key."""
    # Upsert the API key
    await execute_write(
        """
        INSERT INTO user_settings (user_id, key, value, updated_at)
        VALUES ($1, 'zai_api_key', $2, CURRENT_TIMESTAMP)
        ON CONFLICT (user_id, key)
        DO UPDATE SET value = $2, updated_at = CURRENT_TIMESTAMP
        """,
        user["id"],
        zai_api_key,
    )

    has_api_key = bool(zai_api_key)

    return templates.TemplateResponse(
        "settings/partials/api.html",
        {
            "request": request,
            "has_api_key": has_api_key,
            "save_success": True,
        },
    )


@router.post("/password", response_class=HTMLResponse)
async def change_password(
    request: Request,
    user: CurrentUser,
    current_password: str = Form(...),
    new_password: str = Form(...),
    confirm_password: str = Form(...),
):
    """Change the user's password."""
    # Validate passwords match
    if new_password != confirm_password:
        return HTMLResponse(
            '<div class="alert alert-danger">New passwords do not match</div>',
            status_code=400,
        )

    # Validate password length
    if len(new_password) < 4:
        return HTMLResponse(
            '<div class="alert alert-danger">Password must be at least 4 characters</div>',
            status_code=400,
        )

    # Get current password hash
    user_data = await execute_query(
        "SELECT password_hash FROM users WHERE id = $1",
        user["id"],
        fetch_one=True,
    )

    if not user_data or not user_data.get("password_hash"):
        return HTMLResponse(
            '<div class="alert alert-danger">Unable to verify current password</div>',
            status_code=400,
        )

    # Verify current password
    if not bcrypt.checkpw(
        current_password.encode("utf-8"),
        user_data["password_hash"].encode("utf-8"),
    ):
        return HTMLResponse(
            '<div class="alert alert-danger">Current password is incorrect</div>',
            status_code=400,
        )

    # Hash new password
    new_hash = bcrypt.hashpw(new_password.encode("utf-8"), bcrypt.gensalt())

    # Update password
    await execute_write(
        "UPDATE users SET password_hash = $1 WHERE id = $2",
        new_hash.decode("utf-8"),
        user["id"],
    )

    return HTMLResponse('<div class="alert alert-success">Password updated successfully</div>')


async def get_user_settings(user_id: int) -> dict:
    """Get all settings for a user as a dict."""
    rows = await execute_query(
        "SELECT key, value FROM user_settings WHERE user_id = $1",
        user_id,
    )
    return {row["key"]: row["value"] for row in (rows or [])}
