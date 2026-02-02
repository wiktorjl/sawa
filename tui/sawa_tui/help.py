"""Help overlay rendering."""

from rich.panel import Panel
from rich.text import Text

from sawa_tui.state import View


def render_help_overlay(current_view: View, term_width: int, term_height: int) -> Panel:
    """
    Render full-screen help overlay with keyboard shortcuts.

    Args:
        current_view: Current view for context-specific help
        term_width: Terminal width for sizing
        term_height: Terminal height for sizing

    Returns:
        Rich Panel with help content
    """

    # Global shortcuts
    global_help = [
        ("F1-F5", "Switch between main views"),
        ("Ctrl+U", "Quick user switcher"),
        ("Ctrl+P", "User management (admin only)"),
        ("q", "Quit application"),
        ("r", "Refresh current view"),
        ("?", "Toggle this help"),
        ("/", "Search (where available)"),
        ("Esc", "Go back / Cancel"),
    ]

    # View-specific shortcuts
    view_help = {
        View.STOCKS: [
            ("Tab", "Switch focus between watchlists and stocks"),
            ("Up/Down", "Navigate list"),
            ("Left/Right", "Switch focus"),
            ("Enter", "Select watchlist / View stock detail"),
            ("n", "Create new watchlist"),
            ("d", "Delete selected watchlist"),
            ("a", "Add stock to current watchlist"),
            ("x", "Remove stock from watchlist"),
        ],
        View.STOCK_DETAIL: [
            ("Esc", "Return to stocks list"),
            ("a", "Add stock to current watchlist"),
            ("v", "Toggle news pane visibility"),
            ("V", "Open news in fullscreen"),
        ],
        View.NEWS_FULLSCREEN: [
            ("Esc", "Return to stock detail"),
            ("Up/Down", "Navigate articles"),
            ("Enter", "Open article in browser"),
        ],
        View.FUNDAMENTALS: [
            ("1", "Income Statement tab"),
            ("2", "Balance Sheet tab"),
            ("3", "Cash Flow tab"),
            ("t", "Toggle Quarterly/Annual"),
            ("/", "Search for ticker"),
        ],
        View.ECONOMY: [
            ("1", "Treasury Yields tab"),
            ("2", "Inflation tab"),
            ("3", "Labor Market tab"),
        ],
        View.SETTINGS: [
            ("1-4", "Switch category"),
            ("Up/Down", "Navigate settings"),
            ("Left/Right", "Cycle through options"),
            ("Enter/Space", "Edit value / Toggle"),
        ],
        View.GLOSSARY: [
            ("Tab", "Switch focus between terms and definition"),
            ("Up/Down", "Navigate terms"),
            ("/", "Search terms"),
            ("Enter", "Generate definition"),
            ("n", "Add new term"),
            ("d", "Delete user-added term"),
            ("g", "Regenerate with options"),
            ("1-5", "Jump to related term"),
        ],
        View.USER_SWITCHER: [
            ("Up/Down", "Navigate users"),
            ("Enter", "Switch to selected user"),
            ("Esc", "Close switcher"),
        ],
        View.USER_MANAGEMENT: [
            ("Up/Down", "Navigate users"),
            ("Enter", "Switch to selected user"),
            ("n", "Create new user"),
            ("d", "Delete selected user"),
            ("r", "Rename selected user"),
            ("t", "Toggle admin status"),
            ("Esc", "Close user management"),
        ],
    }

    content = Text()

    # Global section
    content.append(" GLOBAL SHORTCUTS\n", style="bold yellow")
    content.append("-" * 50 + "\n", style="dim")
    for key, desc in global_help:
        content.append(f"  {key:12}", style="green")
        content.append(f"{desc}\n", style="white")

    content.append("\n")

    # Current view section
    view_name = current_view.name.replace("_", " ").title()
    content.append(f" {view_name.upper()} SHORTCUTS\n", style="bold yellow")
    content.append("-" * 50 + "\n", style="dim")

    for key, desc in view_help.get(current_view, []):
        content.append(f"  {key:12}", style="green")
        content.append(f"{desc}\n", style="white")

    content.append("\n")
    content.append(" Press ? or Esc to close", style="dim")

    # Calculate appropriate size - make it wider and taller for better visibility
    help_width = min(70, term_width - 10)
    help_height = min(len(global_help) + len(view_help.get(current_view, [])) + 14, term_height - 8)

    return Panel(
        content,
        title="[bold yellow]KEYBOARD SHORTCUTS[/]",
        border_style="yellow",
        width=help_width,
        height=help_height,
        padding=(1, 2),
    )
