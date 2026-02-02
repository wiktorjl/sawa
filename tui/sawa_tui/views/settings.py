"""Settings view with categories and configuration options."""

from rich.layout import Layout
from rich.panel import Panel
from rich.text import Text

from sawa_tui.components import COLORS, TAB_HEADER_HEIGHT, render_empty_state, render_tabs
from sawa_tui.state import AppState, SettingsCategory
from sawa_tui.themes import get_theme

# Settings definitions per category
SETTINGS_ITEMS: dict[SettingsCategory, list[tuple[str, str, str, list | None]]] = {
    SettingsCategory.DISPLAY: [
        (
            "theme",
            "Theme",
            "choice",
            [
                "default",
                "osaka-jade",
                "mono",
                "high-contrast",
                "dracula",
                "catppuccin",
                "gruvbox",
                "nord",
                "tokyo-night",
                "solarized",
                "one-dark",
            ],
        ),
        ("chart_period_days", "Chart Period (days)", "int", [30, 60, 90, 180, 365]),
        ("number_format", "Number Format", "choice", ["compact", "full"]),
        ("logo_enabled", "Show Company Logos", "bool", None),
        ("logo_width", "Logo Width (chars)", "int", [10, 20, 24, 28, 32, 36, 40]),
        ("logo_height", "Logo Height (lines)", "int", [5, 6, 8, 10, 12, 15, 18]),
    ],
    SettingsCategory.CHARTS: [
        ("chart_detail", "Chart Detail Level", "choice", ["compact", "normal", "detailed"]),
    ],
    SettingsCategory.BEHAVIOR: [
        ("fundamentals_timeframe", "Default Timeframe", "choice", ["quarterly", "annual"]),
    ],
    SettingsCategory.API: [
        ("zai_api_key", "Z.AI API Key", "secret", None),
    ],
    SettingsCategory.USERS: [],  # Handled specially in _render_settings_content
}


def render_settings_view(state: AppState) -> Layout:
    """Render the settings view with horizontal tabs."""
    layout = Layout()

    layout.split_column(
        Layout(name="tabs", size=TAB_HEADER_HEIGHT),
        Layout(name="content"),
    )

    # Tab header
    layout["tabs"].update(_render_settings_tabs(state))

    # Settings content (single panel, no sidebar)
    layout["content"].update(_render_settings_content(state))

    return layout


def _render_settings_tabs(state: AppState) -> Panel:
    """Render settings category tabs."""
    theme = get_theme()

    tabs = [
        ("1", "Display", state.settings_category == SettingsCategory.DISPLAY),
        ("2", "Charts", state.settings_category == SettingsCategory.CHARTS),
        ("3", "Behavior", state.settings_category == SettingsCategory.BEHAVIOR),
        ("4", "API Keys", state.settings_category == SettingsCategory.API),
        ("5", "Users", state.settings_category == SettingsCategory.USERS),
    ]

    text = Text()
    text.append(" SETTINGS ", style=f"bold {theme.header}")
    text.append("  ")
    text.append_text(render_tabs(tabs))

    return Panel(text, border_style=theme.border, height=TAB_HEADER_HEIGHT)


def _render_users_settings_panel(state: AppState) -> Panel:
    """Render user management info in the settings panel."""
    theme = get_theme()
    content = Text()

    # Current user info
    state.ensure_user()
    if state.current_user:
        content.append("\n Current User\n", style=f"bold {theme.info}")
        content.append(" " + "─" * 50 + "\n", style=theme.border)
        content.append(f"  Name: {state.current_user.name}\n", style=theme.text_bright)
        content.append(
            f"  Admin: {'Yes' if state.current_user.is_admin else 'No'}\n", style=theme.text_bright
        )
        content.append("\n")

    # Instructions
    content.append(" User Management\n", style=f"bold {theme.warning}")
    content.append(" " + "─" * 50 + "\n", style=theme.border)
    content.append("\n")
    content.append("  Press ", style=theme.text_muted)
    content.append("Ctrl+U", style=theme.warning)
    content.append(" to quickly switch between users\n", style=theme.text_muted)
    content.append("\n")

    if state.current_user and state.current_user.is_admin:
        content.append("  Press ", style=theme.text_muted)
        content.append("Ctrl+P", style=theme.warning)
        content.append(" to manage users (create, delete, rename)\n", style=theme.text_muted)
    else:
        content.append("  User management requires admin privileges\n", style=theme.text_muted)

    content.append("\n")
    content.append(" Features\n", style=f"bold {theme.info}")
    content.append(" " + "─" * 50 + "\n", style=theme.border)
    content.append("  • Each user has their own settings and watchlists\n", style=theme.text_bright)
    content.append("  • Admins can manage users and access all features\n", style=theme.text_bright)
    content.append("  • Switch users anytime without logging out\n", style=theme.text_bright)

    return Panel(
        content,
        title=f"[{theme.header}]User Management[/]",
        border_style=theme.border,
        padding=(0, 2),
    )


def _render_settings_content(state: AppState) -> Panel:
    """Render settings items for current category."""
    from sawa_tui.models.settings import SettingsManager

    theme = get_theme()

    # Special handling for USERS category
    if state.settings_category == SettingsCategory.USERS:
        return _render_users_settings_panel(state)

    items = SETTINGS_ITEMS.get(state.settings_category, [])

    content = Text()

    if not items:
        content.append_text(
            render_empty_state("No settings in this category.", "Use 1-5 to switch categories")
        )
        return Panel(content, title=f"[{theme.header}]Settings[/]", border_style=theme.text_muted)

    # If popup is open, show it instead
    if state.settings_popup_open:
        return _render_settings_popup(state)

    # Ensure user is loaded
    state.ensure_user()
    if not state.current_user:
        content.append_text(render_empty_state("No active user", "Cannot load settings"))
        return Panel(content, title=f"[{theme.header}]Settings[/]", border_style=theme.text_muted)

    content.append("\n")

    for i, (key, label, value_type, choices) in enumerate(items):
        # Get current value from database
        current_value = SettingsManager.get(state.current_user.id, key)

        # Format value for display
        if value_type == "bool":
            value_str = "On" if current_value else "Off"
        elif value_type == "secret":
            # Mask secret values
            if current_value:
                value_str = (
                    "*" * 8 + str(current_value)[-4:] if len(str(current_value)) > 4 else "****"
                )
            else:
                value_str = "(not set)"
        else:
            value_str = str(current_value) if current_value else "(not set)"

        is_selected = i == state.settings_selected_idx
        is_editing = is_selected and state.settings_editing

        if is_editing:
            content.append(f"  > {label}: ", style=f"bold {theme.warning}")
            content.append(
                f"[{state.settings_edit_value}]",
                style=f"bold {theme.selected_text} on {theme.selected}",
            )
            content.append("_", style="blink")
            content.append("\n")
        elif is_selected:
            content.append(f"  > {label}: ", style=COLORS["selected"])
            if choices and (value_type == "choice" or value_type == "int"):
                content.append("< ", style=f"{theme.info}")
                content.append(f"{value_str}", style=theme.text_bright)
                content.append(" >", style=f"{theme.info}")
            else:
                content.append(f"{value_str}", style=theme.text_bright)
            content.append("  [Enter to select]", style=theme.text_muted)
            content.append("\n")
        else:
            content.append(f"    {label}: ", style=theme.text_muted)
            content.append(f"{value_str}", style=theme.text)
            content.append("\n")

    content.append("\n")

    category_name = {
        SettingsCategory.DISPLAY: "Display Settings",
        SettingsCategory.CHARTS: "Chart Settings",
        SettingsCategory.BEHAVIOR: "Behavior Settings",
        SettingsCategory.API: "API Keys",
        SettingsCategory.USERS: "User Management",
    }[state.settings_category]

    return Panel(
        content,
        title=f"[{theme.header}]{category_name}[/]",
        title_align="left",
        border_style=theme.border,
    )


def _render_settings_popup(state: AppState) -> Panel:
    """Render the popup menu for selecting a choice."""
    theme = get_theme()
    content = Text()

    content.append(f" Select {state.settings_popup_label}:\n\n", style=f"bold {theme.header}")

    for i, choice in enumerate(state.settings_popup_choices):
        if i == state.settings_popup_idx:
            content.append(f"   > {choice}\n", style=f"{theme.selected_text} {theme.selected}")
        else:
            content.append(f"     {choice}\n", style=theme.text)

    content.append("\n", style=theme.text_muted)
    content.append(" [Up/Down] Navigate  [Enter] Select  [Esc] Cancel\n", style=theme.text_muted)

    return Panel(
        content,
        title=f"[{theme.accent}]{state.settings_popup_label}[/]",
        title_align="left",
        border_style=theme.accent,
    )
