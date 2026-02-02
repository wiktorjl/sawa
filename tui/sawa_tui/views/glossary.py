"""Glossary view for financial term definitions."""

from rich.layout import Layout
from rich.panel import Panel
from rich.text import Text

from sawa_tui.components import SIDEBAR_WIDTH, panel_title, render_scroll_indicator
from sawa_tui.state import AppState
from sawa_tui.themes import get_theme


def render_glossary_view(state: AppState) -> Layout:
    """Render the glossary view."""
    layout = Layout()

    layout.split_row(
        Layout(name="sidebar", size=SIDEBAR_WIDTH),
        Layout(name="main"),
    )

    # Sidebar: term list
    layout["sidebar"].update(_render_glossary_sidebar(state))

    # Main: definition or loading/error state
    layout["main"].update(_render_glossary_definition(state))

    return layout


def _render_glossary_sidebar(state: AppState) -> Panel:
    """Render the glossary term list sidebar."""
    theme = get_theme()
    content = Text()

    # Search box
    if state.glossary_search:
        content.append(f" / {state.glossary_search}", style=theme.warning)
    else:
        content.append(" / Search...", style=theme.text_muted)
    content.append("\n\n", style=theme.text_muted)

    # Term list with scrolling
    visible_rows = state.get_visible_glossary_rows()
    start = state.glossary_scroll_offset
    end = start + visible_rows

    for i, term in enumerate(state.glossary_filtered[start:end], start=start):
        # Category prefix
        cat_abbrev = ""
        if term.category:
            cat_map = {
                "Valuation": "VAL",
                "Profitability": "PRF",
                "Liquidity": "LIQ",
                "Leverage": "LEV",
                "Cash Flow": "CF",
                "Dividends": "DIV",
                "Growth": "GRW",
                "Trading": "TRD",
                "User Added": "USR",
                "Related": "REL",
            }
            cat_abbrev = cat_map.get(term.category, term.category[:3].upper())

        # Indicator for cached definition
        cached = "*" if term.has_definition else " "

        line = f"{cached}[{cat_abbrev:>3}] {term.term}"

        if i == state.selected_term_idx and state.glossary_focus_sidebar:
            content.append(line + "\n", style=f"{theme.selected_text} {theme.selected}")
        elif i == state.selected_term_idx:
            content.append(line + "\n", style=theme.primary)
        else:
            content.append(
                line + "\n", style=theme.text_muted if not term.has_definition else theme.text
            )

    # Calculate scroll info using shared component
    total = len(state.glossary_filtered)
    scroll_info = render_scroll_indicator(start, total, visible_rows) if total > 0 else ""

    # Use focused panel title
    title = panel_title("TERMS", state.glossary_focus_sidebar, scroll_info)

    return Panel(
        content,
        title=title,
        title_align="left",
        border_style=theme.border_focus if state.glossary_focus_sidebar else theme.text_muted,
    )


def _render_glossary_definition(state: AppState) -> Panel:
    """Render the glossary definition panel."""
    theme = get_theme()
    # Show regeneration menu if active
    if state.glossary_show_regen_menu:
        return _render_regen_menu(state)

    # Loading state with streaming content
    if state.glossary_loading:
        content = Text()
        content.append(" Generating definition...", style=f"bold {theme.warning}")
        content.append("\n\n", style=theme.text_muted)

        # Spinner animation (will be static but indicates activity)
        content.append(" ", style=theme.text_muted)
        content.append("[", style=theme.text_muted)
        # Simple progress indicator
        content.append("=" * 20, style=theme.positive)
        content.append(">", style=f"bold {theme.positive}")
        content.append(" " * 10, style=theme.text_muted)
        content.append("]", style=theme.text_muted)
        content.append("\n\n", style=theme.text_muted)

        # Show streaming content as it arrives
        if state.glossary_stream_content:
            content.append(state.glossary_stream_content, style=theme.text)

        return Panel(
            content,
            title=f"[{theme.warning}]Generating...[/]",
            border_style=theme.warning,
        )

    # Error state
    if state.glossary_error:
        content = Text()
        content.append(" Error\n\n", style=f"bold {theme.negative}")
        content.append(f" {state.glossary_error}\n\n", style=theme.negative)
        content.append(" Press Enter to retry", style=theme.text_muted)

        return Panel(
            content,
            title=f"[{theme.negative}]Error[/]",
            border_style=theme.negative,
        )

    # No term selected
    term = state.current_glossary_term()
    if not term:
        content = Text()
        content.append("\n Select a term from the list\n", style=theme.text_muted)
        content.append(" Press Enter to generate definition\n", style=theme.text_muted)

        return Panel(
            content,
            title=f"[{theme.header}]Definition[/]",
            border_style=theme.text_muted,
        )

    # No definition yet
    definition = state.glossary_definition
    if not definition:
        content = Text()
        content.append(f"\n {term.term}\n\n", style=f"bold {theme.primary}")
        content.append(" No definition cached.\n", style=theme.text_muted)
        content.append(" Press Enter to generate.\n", style=theme.text_muted)

        return Panel(
            content,
            title=f"[{theme.header}]{term.term}[/]",
            border_style=theme.text_muted,
        )

    # Render the full definition
    content = Text()

    # Official definition
    content.append(" OFFICIAL DEFINITION\n", style=f"bold {theme.info}")
    content.append(f" {definition.official_definition}\n\n", style=theme.text)

    # Plain English
    content.append(" WHAT IT ACTUALLY MEANS\n", style=f"bold {theme.info}")
    content.append(f" {definition.plain_english}\n\n", style=theme.text)

    # Examples
    if definition.examples:
        content.append(" EXAMPLES\n", style=f"bold {theme.info}")
        for i, example in enumerate(definition.examples, 1):
            content.append(f" {i}. {example}\n", style=theme.text)
        content.append("\n", style=theme.text_muted)

    # Related terms with numbers for quick jump
    if definition.related_terms:
        content.append(" RELATED TERMS\n", style=f"bold {theme.info}")
        for i, related in enumerate(definition.related_terms[:5], 1):
            content.append(f" [{i}] ", style=theme.warning)
            content.append(f"{related}  ", style=theme.positive)
        content.append("\n\n", style=theme.text_muted)

    # Learn more links
    if definition.learn_more:
        content.append(" LEARN MORE\n", style=f"bold {theme.info}")
        for url in definition.learn_more[:3]:
            # Truncate long URLs
            display_url = url
            if len(display_url) > 60:
                display_url = display_url[:57] + "..."
            content.append(f" {display_url}\n", style=theme.info)

    # Show if regenerated with custom prompt
    if definition.custom_prompt:
        content.append("\n", style=theme.text_muted)
        content.append(f" [Customized: {definition.custom_prompt[:30]}...]", style=theme.text_muted)

    return Panel(
        content,
        title=f"[{theme.header}]{definition.term}[/]",
        border_style=theme.border if not state.glossary_focus_sidebar else theme.text_muted,
    )


def _render_regen_menu(state: AppState) -> Panel:
    """Render the regeneration options menu."""
    theme = get_theme()
    content = Text()
    content.append("\n Regenerate definition with:\n\n", style=f"bold {theme.warning}")

    options = [
        ("1", "More technical", "Use more technical language and formulas"),
        ("2", "Simpler explanation", "Make it easier for beginners"),
        ("3", "Add more examples", "Include 4-5 practical examples"),
        ("4", "Focus on practical use", "How investors use this metric"),
        ("c", "Custom instructions", "Enter your own prompt"),
    ]

    for key, label, desc in options:
        content.append(f"  [{key}] ", style=theme.warning)
        content.append(f"{label}\n", style=f"bold {theme.text}")
        content.append(f"      {desc}\n\n", style=theme.text_muted)

    content.append("\n  Press Esc to cancel", style=theme.text_muted)

    term = state.current_glossary_term()
    title = f"Regenerate: {term.term}" if term else "Regenerate"

    return Panel(
        content,
        title=f"[{theme.warning}]{title}[/]",
        border_style=theme.warning,
    )
