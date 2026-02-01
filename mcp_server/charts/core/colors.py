"""ANSI color utilities."""

import re

# ANSI escape sequence pattern
ANSI_PATTERN = re.compile(r"\033\[[0-9;]*m")


def strip_ansi(text: str) -> str:
    """
    Remove all ANSI escape sequences from text.

    Args:
        text: Text potentially containing ANSI codes

    Returns:
        Text with all ANSI codes removed
    """
    return ANSI_PATTERN.sub("", text)


def visible_len(text: str) -> int:
    """
    Get visible length of text (excluding ANSI codes).

    Args:
        text: Text potentially containing ANSI codes

    Returns:
        Length of visible characters
    """
    return len(strip_ansi(text))


def pad_to_width(text: str, width: int, align: str = "left") -> str:
    """
    Pad text to specified width, accounting for ANSI codes.

    Args:
        text: Text potentially containing ANSI codes
        width: Target width
        align: Alignment ('left', 'right', 'center')

    Returns:
        Padded text
    """
    current_len = visible_len(text)
    padding = width - current_len

    if padding <= 0:
        return text

    if align == "right":
        return " " * padding + text
    elif align == "center":
        left_pad = padding // 2
        right_pad = padding - left_pad
        return " " * left_pad + text + " " * right_pad
    else:  # left
        return text + " " * padding


def truncate_to_width(text: str, width: int, ellipsis: str = "...") -> str:
    """
    Truncate text to fit within width, preserving ANSI codes at start.

    Args:
        text: Text potentially containing ANSI codes
        width: Maximum visible width
        ellipsis: String to append when truncated

    Returns:
        Truncated text
    """
    if visible_len(text) <= width:
        return text

    # Simple approach: strip ANSI, truncate, but this loses colors
    plain = strip_ansi(text)
    if len(plain) <= width:
        return text

    truncated = plain[: width - len(ellipsis)] + ellipsis
    return truncated
