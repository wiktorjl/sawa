"""User management and switcher views."""

from rich.panel import Panel
from rich.text import Text

from sawa_tui.components import COLORS, render_empty_state
from sawa_tui.state import AppState
from sawa_tui.themes import get_theme


def render_user_management_view(state: AppState) -> Panel:
    """Render user management view (admin only)."""
    theme = get_theme()
    content = Text()

    if not state.current_user or not state.current_user.is_admin:
        content.append_text(
            render_empty_state("Admin access required", "Only admins can manage users")
        )
        return Panel(
            content, title=f"[{theme.header}]User Management[/]", border_style=theme.border
        )

    if not state.user_mgmt_users:
        content.append_text(render_empty_state("No users found", "Press 'n' to create a user"))
        return Panel(
            content, title=f"[{theme.header}]User Management[/]", border_style=theme.border
        )

    content.append("\n")
    content.append(f"  Total Users: {len(state.user_mgmt_users)}\n", style=theme.text_muted)
    content.append(f"  Current User: {state.current_user.name}\n\n", style=theme.info)

    for i, user in enumerate(state.user_mgmt_users):
        is_selected = i == state.user_mgmt_selected_idx
        is_current = user.id == state.current_user.id

        # User indicator
        if is_current:
            indicator = "→ "
            style = theme.positive
        else:
            indicator = "  "
            style = theme.text if not is_selected else COLORS["selected"]

        # Selection marker
        if is_selected:
            prefix = "> "
            name_style = f"bold {COLORS['selected']}"
        else:
            prefix = "  "
            name_style = style

        # Admin badge
        admin_badge = " [ADMIN]" if user.is_admin else ""

        content.append(f"{prefix}{indicator}{user.name}{admin_badge}", style=name_style)
        content.append(f"  (ID: {user.id})\n", style=theme.text_muted)

    content.append("\n")

    if state.user_mgmt_confirm_delete and state.user_mgmt_users:
        user = state.user_mgmt_users[state.user_mgmt_selected_idx]
        content.append(f"  ⚠ Delete user '{user.name}'? (y/n)\n", style=f"bold {theme.warning}")

    subtitle = (
        f"[{theme.text_muted}]"
        "↑↓:Select  Enter:Switch  n:New  d:Delete  t:Toggle Admin  r:Rename  Esc:Back"
        "[/]"
    )
    return Panel(
        content,
        title=f"[{theme.header}]User Management[/]",
        border_style=theme.border,
        subtitle=subtitle,
        subtitle_align="left",
    )


def render_user_switcher_view(state: AppState) -> Panel:
    """Render user switcher popup."""
    theme = get_theme()
    content = Text()

    if not state.user_switcher_users:
        content.append_text(render_empty_state("No users found", ""))
        return Panel(content, title=f"[{theme.header}]Switch User[/]", border_style=theme.border)

    content.append("\n")
    content.append("  Select a user to switch to:\n\n", style=f"bold {theme.header}")

    for i, user in enumerate(state.user_switcher_users):
        is_selected = i == state.user_switcher_selected_idx
        is_current = state.current_user and user.id == state.current_user.id

        if is_current:
            indicator = "✓ "
            style = theme.positive
        else:
            indicator = "  "
            style = theme.text

        if is_selected:
            prefix = "> "
            name_style = f"bold {COLORS['selected']}"
        else:
            prefix = "  "
            name_style = style

        admin_badge = " [ADMIN]" if user.is_admin else ""

        content.append(f"{prefix}{indicator}{user.name}{admin_badge}\n", style=name_style)

    content.append("\n")

    return Panel(
        content,
        title=f"[{theme.header}]Switch User[/]",
        border_style=theme.border,
        subtitle=f"[{theme.text_muted}]↑↓:Select  Enter:Switch  Esc:Cancel[/]",
        subtitle_align="left",
    )
