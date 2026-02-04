"""FastAPI dependencies for authentication and database access."""

from typing import Annotated

from fastapi import Cookie, Depends, HTTPException, Request, status
from itsdangerous import BadSignature, URLSafeTimedSerializer

from sawa_web.config import Settings, get_settings
from sawa_web.database.connection import execute_query


def get_serializer(settings: Settings = Depends(get_settings)) -> URLSafeTimedSerializer:
    """Get the session serializer."""
    return URLSafeTimedSerializer(settings.secret_key)


async def get_current_user_optional(
    request: Request,
    settings: Annotated[Settings, Depends(get_settings)],
) -> dict | None:
    """
    Get the current user from session cookie, or None if not authenticated.

    Returns:
        User dict with id, name, is_admin, or None
    """
    session_cookie = request.cookies.get(settings.session_cookie_name)
    if not session_cookie:
        return None

    serializer = URLSafeTimedSerializer(settings.secret_key)
    try:
        user_id = serializer.loads(
            session_cookie,
            max_age=settings.session_max_age,
        )
    except BadSignature:
        return None

    # Fetch user from database
    user = await execute_query(
        "SELECT id, name, is_admin, created_at FROM users WHERE id = $1",
        user_id,
        fetch_one=True,
    )

    return user


async def get_current_user(
    user: Annotated[dict | None, Depends(get_current_user_optional)],
) -> dict:
    """
    Get the current user, or raise 401 if not authenticated.

    Use this dependency for protected routes.
    """
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"HX-Redirect": "/login"},
        )
    return user


async def get_admin_user(
    user: Annotated[dict, Depends(get_current_user)],
) -> dict:
    """
    Get the current user if they are an admin, or raise 403.

    Use this dependency for admin-only routes.
    """
    if not user.get("is_admin"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required",
        )
    return user


# Type aliases for cleaner route signatures
CurrentUser = Annotated[dict, Depends(get_current_user)]
OptionalUser = Annotated[dict | None, Depends(get_current_user_optional)]
AdminUser = Annotated[dict, Depends(get_admin_user)]
