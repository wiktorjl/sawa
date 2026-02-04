"""Authentication routes for login/logout."""

from pathlib import Path
from typing import Annotated

import bcrypt
from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from itsdangerous import URLSafeTimedSerializer

from sawa_web.config import Settings, get_settings
from sawa_web.database.connection import execute_query, execute_write
from sawa_web.dependencies import OptionalUser

router = APIRouter(tags=["auth"])

# Templates
TEMPLATES_DIR = Path(__file__).parent.parent / "templates"
templates = Jinja2Templates(directory=TEMPLATES_DIR)


@router.get("/login", response_class=HTMLResponse)
async def login_page(
    request: Request,
    user: OptionalUser,
    error: str | None = None,
):
    """Render the login page."""
    # Redirect to dashboard if already logged in
    if user:
        return RedirectResponse(url="/dashboard", status_code=302)

    return templates.TemplateResponse(
        "auth/login.html",
        {
            "request": request,
            "error": error,
        },
    )


@router.post("/login")
async def login(
    request: Request,
    username: Annotated[str, Form()],
    password: Annotated[str, Form()],
    settings: Annotated[Settings, Depends(get_settings)],
):
    """Handle login form submission."""
    # Find user by name (case-insensitive)
    user = await execute_query(
        "SELECT id, name, is_admin, password_hash FROM users WHERE LOWER(name) = LOWER($1)",
        username,
        fetch_one=True,
    )

    if not user:
        return templates.TemplateResponse(
            "auth/login.html",
            {
                "request": request,
                "error": "Invalid username or password",
            },
            status_code=401,
        )

    # Check password
    password_hash = user.get("password_hash")
    if not password_hash:
        # User exists but has no password set (TUI-only user)
        # Allow login with any password for now, or set the password
        # For initial setup, we'll set the password on first web login
        hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt())
        await execute_write(
            "UPDATE users SET password_hash = $1 WHERE id = $2",
            hashed.decode(),
            user["id"],
        )
    else:
        # Verify password
        if not bcrypt.checkpw(password.encode(), password_hash.encode()):
            return templates.TemplateResponse(
                "auth/login.html",
                {
                    "request": request,
                    "error": "Invalid username or password",
                },
                status_code=401,
            )

    # Create session
    serializer = URLSafeTimedSerializer(settings.secret_key)
    session_token = serializer.dumps(user["id"])

    response = RedirectResponse(url="/dashboard", status_code=302)
    response.set_cookie(
        key=settings.session_cookie_name,
        value=session_token,
        max_age=settings.session_max_age,
        httponly=True,
        samesite="lax",
    )

    return response


@router.get("/logout")
async def logout(
    settings: Annotated[Settings, Depends(get_settings)],
):
    """Log out the current user."""
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie(key=settings.session_cookie_name)
    return response


@router.post("/logout")
async def logout_post(
    settings: Annotated[Settings, Depends(get_settings)],
):
    """Log out via POST (for HTMX)."""
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie(key=settings.session_cookie_name)
    return response
