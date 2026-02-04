"""Admin routes for user management."""

import logging
from pathlib import Path

import bcrypt
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from sawa_web.database.connection import execute_query, execute_write, execute_write_returning
from sawa_web.dependencies import AdminUser

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])

TEMPLATES_DIR = Path(__file__).parent.parent / "templates"
templates = Jinja2Templates(directory=TEMPLATES_DIR)


@router.get("/users", response_class=HTMLResponse)
async def users_page(request: Request, user: AdminUser):
    """Render the user management page."""
    users = await get_all_users()

    return templates.TemplateResponse(
        "admin/users.html",
        {
            "request": request,
            "user": user,
            "active_page": "admin",
            "page_title": "User Management",
            "users": users,
        },
    )


@router.get("/users/list", response_class=HTMLResponse)
async def users_list(request: Request, user: AdminUser):
    """Get the user list partial."""
    users = await get_all_users()

    return templates.TemplateResponse(
        "admin/partials/user_list.html",
        {
            "request": request,
            "current_user": user,
            "users": users,
        },
    )


@router.post("/users/create", response_class=HTMLResponse)
async def create_user(
    request: Request,
    user: AdminUser,
    username: str = Form(...),
    password: str = Form(...),
    is_admin: bool = Form(False),
):
    """Create a new user."""
    username = username.strip()

    if not username:
        return HTMLResponse(
            '<div class="alert alert-danger">Username cannot be empty</div>',
            status_code=400,
        )

    if len(password) < 4:
        return HTMLResponse(
            '<div class="alert alert-danger">Password must be at least 4 characters</div>',
            status_code=400,
        )

    # Check if username exists
    existing = await execute_query(
        "SELECT 1 FROM users WHERE name = $1",
        username,
        fetch_one=True,
    )

    if existing:
        return HTMLResponse(
            '<div class="alert alert-danger">Username already exists</div>',
            status_code=400,
        )

    # Hash password
    password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())

    # Create user
    new_user = await execute_write_returning(
        """
        INSERT INTO users (name, password_hash, is_admin)
        VALUES ($1, $2, $3)
        RETURNING id, name, is_admin
        """,
        username,
        password_hash.decode("utf-8"),
        is_admin,
    )

    # Create default watchlist for user
    await execute_write(
        """
        INSERT INTO watchlists (user_id, name, is_default)
        VALUES ($1, 'Default', TRUE)
        """,
        new_user["id"],
    )

    # Copy default settings
    await execute_write(
        """
        INSERT INTO user_settings (user_id, key, value)
        SELECT $1, key, value FROM default_settings
        ON CONFLICT (user_id, key) DO NOTHING
        """,
        new_user["id"],
    )

    # Return updated user list
    users = await get_all_users()

    return templates.TemplateResponse(
        "admin/partials/user_list.html",
        {
            "request": request,
            "current_user": user,
            "users": users,
            "created": username,
        },
    )


@router.post("/users/{user_id}/rename", response_class=HTMLResponse)
async def rename_user(
    request: Request,
    user: AdminUser,
    user_id: int,
    username: str = Form(...),
):
    """Rename a user."""
    username = username.strip()

    if not username:
        return HTMLResponse(
            '<div class="alert alert-danger">Username cannot be empty</div>',
            status_code=400,
        )

    # Verify user exists
    target_user = await execute_query(
        "SELECT id, name FROM users WHERE id = $1",
        user_id,
        fetch_one=True,
    )

    if not target_user:
        return HTMLResponse(
            '<div class="alert alert-danger">User not found</div>',
            status_code=404,
        )

    # Check if new name already exists
    existing = await execute_query(
        "SELECT 1 FROM users WHERE name = $1 AND id != $2",
        username,
        user_id,
        fetch_one=True,
    )

    if existing:
        return HTMLResponse(
            '<div class="alert alert-danger">Username already exists</div>',
            status_code=400,
        )

    # Update username
    await execute_write(
        "UPDATE users SET name = $1 WHERE id = $2",
        username,
        user_id,
    )

    # Return updated user list
    users = await get_all_users()

    return templates.TemplateResponse(
        "admin/partials/user_list.html",
        {
            "request": request,
            "current_user": user,
            "users": users,
        },
    )


@router.post("/users/{user_id}/toggle-admin", response_class=HTMLResponse)
async def toggle_admin(
    request: Request,
    user: AdminUser,
    user_id: int,
):
    """Toggle admin status for a user."""
    # Verify user exists
    target_user = await execute_query(
        "SELECT id, name, is_admin FROM users WHERE id = $1",
        user_id,
        fetch_one=True,
    )

    if not target_user:
        return HTMLResponse(
            '<div class="alert alert-danger">User not found</div>',
            status_code=404,
        )

    # Cannot demote yourself
    if user_id == user["id"] and target_user["is_admin"]:
        return HTMLResponse(
            '<div class="alert alert-danger">Cannot remove your own admin privileges</div>',
            status_code=400,
        )

    # Check if this would leave no admins
    if target_user["is_admin"]:
        admin_count = await execute_query(
            "SELECT COUNT(*) as count FROM users WHERE is_admin = TRUE",
            fetch_one=True,
        )
        if admin_count and admin_count["count"] <= 1:
            return HTMLResponse(
                '<div class="alert alert-danger">Cannot demote the last admin</div>',
                status_code=400,
            )

    # Toggle admin status
    new_status = not target_user["is_admin"]
    await execute_write(
        "UPDATE users SET is_admin = $1 WHERE id = $2",
        new_status,
        user_id,
    )

    # Return updated user list
    users = await get_all_users()

    return templates.TemplateResponse(
        "admin/partials/user_list.html",
        {
            "request": request,
            "current_user": user,
            "users": users,
        },
    )


@router.delete("/users/{user_id}", response_class=HTMLResponse)
async def delete_user(
    request: Request,
    user: AdminUser,
    user_id: int,
):
    """Delete a user."""
    # Cannot delete yourself
    if user_id == user["id"]:
        return HTMLResponse(
            '<div class="alert alert-danger">Cannot delete your own account</div>',
            status_code=400,
        )

    # Verify user exists
    target_user = await execute_query(
        "SELECT id, name, is_admin FROM users WHERE id = $1",
        user_id,
        fetch_one=True,
    )

    if not target_user:
        return HTMLResponse(
            '<div class="alert alert-danger">User not found</div>',
            status_code=404,
        )

    # Check if this would leave no admins
    if target_user["is_admin"]:
        admin_count = await execute_query(
            "SELECT COUNT(*) as count FROM users WHERE is_admin = TRUE",
            fetch_one=True,
        )
        if admin_count and admin_count["count"] <= 1:
            return HTMLResponse(
                '<div class="alert alert-danger">Cannot delete the last admin</div>',
                status_code=400,
            )

    # Delete user (cascade will handle watchlists, settings, etc.)
    await execute_write(
        "DELETE FROM users WHERE id = $1",
        user_id,
    )

    # Return updated user list
    users = await get_all_users()

    return templates.TemplateResponse(
        "admin/partials/user_list.html",
        {
            "request": request,
            "current_user": user,
            "users": users,
            "deleted": target_user["name"],
        },
    )


async def get_all_users() -> list:
    """Get all users with their stats."""
    return await execute_query(
        """
        SELECT
            u.id,
            u.name,
            u.is_admin,
            u.created_at,
            (SELECT COUNT(*) FROM watchlists WHERE user_id = u.id) as watchlist_count,
            (SELECT COUNT(*) FROM watchlist_symbols ws
             JOIN watchlists w ON ws.watchlist_id = w.id
             WHERE w.user_id = u.id) as stock_count
        FROM users u
        ORDER BY u.created_at
        """,
    ) or []
