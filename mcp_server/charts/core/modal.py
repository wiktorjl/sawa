"""Terminal width warning modal."""

from ..themes import Theme, get_theme


def render_width_warning(
    current_width: int,
    required_width: int,
    theme: Theme | None = None,
) -> str:
    """
    Render a warning modal when terminal is too narrow.

    Args:
        current_width: Current terminal width
        required_width: Minimum required width
        theme: Theme to use for styling

    Returns:
        Formatted warning message
    """
    if theme is None:
        theme = get_theme()

    box = theme.box
    symbols = theme.symbols

    # Build the modal content
    lines = []

    # Calculate modal width (fits within current width or uses minimum)
    modal_width = min(current_width - 4, 60)
    if modal_width < 40:
        modal_width = 40

    inner_width = modal_width - 4  # Account for borders and padding

    def center(text: str) -> str:
        padding = (inner_width - len(text)) // 2
        return " " * padding + text

    def border_line(char: str) -> str:
        return char * modal_width

    # Top border
    lines.append(box.top_left + box.horizontal * (modal_width - 2) + box.top_right)

    # Empty line
    lines.append(box.vertical + " " * (modal_width - 2) + box.vertical)

    # Warning icon and title
    warning_title = f"{symbols.warning}  TERMINAL TOO NARROW"
    warning_line = center(warning_title)
    lines.append(box.vertical + " " + warning_line.ljust(modal_width - 3) + box.vertical)

    # Empty line
    lines.append(box.vertical + " " * (modal_width - 2) + box.vertical)

    # Width info
    current_line = f"Current width:  {current_width} columns"
    lines.append(box.vertical + " " + center(current_line).ljust(modal_width - 3) + box.vertical)

    required_line = f"Minimum needed: {required_width} columns"
    lines.append(box.vertical + " " + center(required_line).ljust(modal_width - 3) + box.vertical)

    # Empty line
    lines.append(box.vertical + " " * (modal_width - 2) + box.vertical)

    # Instructions
    instruction1 = "Please resize your terminal window"
    lines.append(box.vertical + " " + center(instruction1).ljust(modal_width - 3) + box.vertical)

    instruction2 = "or reduce font size to view charts."
    lines.append(box.vertical + " " + center(instruction2).ljust(modal_width - 3) + box.vertical)

    # Empty line
    lines.append(box.vertical + " " * (modal_width - 2) + box.vertical)

    # Visual width indicator
    indicator_width = min(inner_width - 4, required_width)
    arrow_line = f"{symbols.arrow_left}{box.light_h * indicator_width}{symbols.arrow_right}"
    lines.append(box.vertical + " " + center(arrow_line).ljust(modal_width - 3) + box.vertical)

    expand_text = "Expand to this width"
    lines.append(box.vertical + " " + center(expand_text).ljust(modal_width - 3) + box.vertical)

    # Empty line
    lines.append(box.vertical + " " * (modal_width - 2) + box.vertical)

    # Bottom border
    lines.append(box.bottom_left + box.horizontal * (modal_width - 2) + box.bottom_right)

    # Join and colorize
    modal = "\n".join(lines)

    # Apply warning color to the whole modal
    return theme.warning_text(modal)


def check_width_and_warn(
    current_width: int,
    required_width: int,
    theme: Theme | None = None,
) -> str | None:
    """
    Check if terminal width is sufficient, return warning if not.

    Args:
        current_width: Current terminal width
        required_width: Minimum required width
        theme: Theme to use

    Returns:
        Warning message string if width insufficient, None otherwise
    """
    if current_width >= required_width:
        return None

    return render_width_warning(current_width, required_width, theme)
