"""User data model and CRUD operations for multi-user support."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sawa_tui.database import execute_query, execute_write, execute_write_returning


@dataclass(frozen=True, slots=True)
class User:
    """Represents a user in the system."""

    id: int
    name: str
    is_admin: bool
    created_at: datetime

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "User":
        """Create a User from a database row."""
        return cls(
            id=row["id"],
            name=row["name"],
            is_admin=row["is_admin"],
            created_at=row["created_at"],
        )


class UserManager:
    """Manager for user CRUD operations."""

    @staticmethod
    def get_all() -> list[User]:
        """Get all users ordered by admin status and name."""
        sql = """
            SELECT id, name, is_admin, created_at
            FROM users
            ORDER BY is_admin DESC, name ASC
        """
        rows = execute_query(sql)
        return [User.from_row(row) for row in rows]

    @staticmethod
    def get_by_id(user_id: int) -> User | None:
        """Get a user by ID."""
        sql = "SELECT id, name, is_admin, created_at FROM users WHERE id = %(id)s"
        rows = execute_query(sql, {"id": user_id})
        return User.from_row(rows[0]) if rows else None

    @staticmethod
    def get_by_name(name: str) -> User | None:
        """Get a user by name (case-insensitive)."""
        sql = "SELECT id, name, is_admin, created_at FROM users WHERE LOWER(name) = LOWER(%(name)s)"
        rows = execute_query(sql, {"name": name})
        return User.from_row(rows[0]) if rows else None

    @staticmethod
    def get_active() -> User | None:
        """Get the currently active user."""
        sql = """
            SELECT u.id, u.name, u.is_admin, u.created_at
            FROM users u
            JOIN active_user au ON u.id = au.user_id
            WHERE au.id = 1
        """
        rows = execute_query(sql)
        return User.from_row(rows[0]) if rows else None

    @staticmethod
    def set_active(user_id: int) -> bool:
        """Set the active user."""
        # Verify user exists
        if not UserManager.get_by_id(user_id):
            return False

        sql = "UPDATE active_user SET user_id = %(user_id)s WHERE id = 1"
        return execute_write(sql, {"user_id": user_id}) > 0

    @staticmethod
    def create(name: str, is_admin: bool = False) -> User | None:
        """
        Create a new user.

        This will:
        1. Create the user record
        2. Create a default watchlist for the user
        3. Copy settings from default_settings template

        Returns:
            Created User object, or None if creation failed
        """
        # Check if name already exists
        if UserManager.get_by_name(name):
            return None

        # Create user
        sql = """
            INSERT INTO users (name, is_admin)
            VALUES (%(name)s, %(is_admin)s)
            RETURNING id, name, is_admin, created_at
        """
        row = execute_write_returning(sql, {"name": name, "is_admin": is_admin})
        if not row:
            return None

        user = User.from_row(row)

        # Create default watchlist for user
        watchlist_sql = """
            INSERT INTO watchlists (user_id, name, is_default)
            VALUES (%(user_id)s, 'Default', TRUE)
        """
        execute_write(watchlist_sql, {"user_id": user.id})

        # Copy default settings to user_settings
        settings_sql = """
            INSERT INTO user_settings (user_id, key, value)
            SELECT %(user_id)s, key, value
            FROM default_settings
        """
        execute_write(settings_sql, {"user_id": user.id})

        return user

    @staticmethod
    def delete(user_id: int) -> tuple[bool, str]:
        """
        Delete a user.

        Validates:
        - User exists
        - User is not the active user (enforced by database trigger)
        - At least one other user exists

        CASCADE deletes:
        - All watchlists and their symbols
        - All user settings
        - All glossary term overrides

        Returns:
            Tuple of (success, error_message)
        """
        # Check user exists
        user = UserManager.get_by_id(user_id)
        if not user:
            return False, "User not found"

        # Check if active user
        active = UserManager.get_active()
        if active and active.id == user_id:
            return False, "Cannot delete the currently active user"

        # Check at least one other user exists
        all_users = UserManager.get_all()
        if len(all_users) <= 1:
            return False, "Cannot delete the last user"

        # If admin, check at least one other admin exists
        if user.is_admin:
            admin_count = sum(1 for u in all_users if u.is_admin)
            if admin_count <= 1:
                return False, "Cannot delete the last admin user"

        # Delete user (CASCADE handles related data)
        sql = "DELETE FROM users WHERE id = %(id)s"
        success = execute_write(sql, {"id": user_id}) > 0
        return (True, "") if success else (False, "Failed to delete user")

    @staticmethod
    def toggle_admin(user_id: int) -> tuple[bool, str]:
        """
        Toggle admin status for a user.

        Validates:
        - User exists
        - If demoting, at least one other admin exists (enforced by database trigger)

        Returns:
            Tuple of (success, error_message)
        """
        user = UserManager.get_by_id(user_id)
        if not user:
            return False, "User not found"

        # If currently admin, check there's another admin
        if user.is_admin:
            all_users = UserManager.get_all()
            admin_count = sum(1 for u in all_users if u.is_admin)
            if admin_count <= 1:
                return False, "Cannot demote the last admin user"

        # Toggle admin status
        sql = "UPDATE users SET is_admin = NOT is_admin WHERE id = %(id)s"
        success = execute_write(sql, {"id": user_id}) > 0
        return (True, "") if success else (False, "Failed to toggle admin status")

    @staticmethod
    def rename(user_id: int, new_name: str) -> tuple[bool, str]:
        """
        Rename a user.

        Validates:
        - User exists
        - New name is not empty
        - New name is not already taken

        Returns:
            Tuple of (success, error_message)
        """
        if not new_name or not new_name.strip():
            return False, "Name cannot be empty"

        new_name = new_name.strip()

        # Check user exists
        user = UserManager.get_by_id(user_id)
        if not user:
            return False, "User not found"

        # Check new name not already taken
        existing = UserManager.get_by_name(new_name)
        if existing and existing.id != user_id:
            return False, f"Name '{new_name}' is already taken"

        # Rename
        sql = "UPDATE users SET name = %(name)s WHERE id = %(id)s"
        success = execute_write(sql, {"id": user_id, "name": new_name}) > 0
        return (True, "") if success else (False, "Failed to rename user")

    @staticmethod
    def count_admins() -> int:
        """Count the number of admin users."""
        sql = "SELECT COUNT(*) as count FROM users WHERE is_admin = TRUE"
        rows = execute_query(sql)
        return rows[0]["count"] if rows else 0

    @staticmethod
    def ensure_active_user() -> User:
        """
        Ensure there is an active user, creating default if needed.

        Returns:
            The active user
        """
        active = UserManager.get_active()
        if active:
            return active

        # No active user - try to set first user as active
        users = UserManager.get_all()
        if users:
            UserManager.set_active(users[0].id)
            return users[0]

        # No users at all - create default admin user
        default_user = UserManager.create("Default", is_admin=True)
        if default_user:
            UserManager.set_active(default_user.id)
            return default_user

        raise RuntimeError("Failed to ensure active user")
