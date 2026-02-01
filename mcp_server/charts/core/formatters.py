"""Number and text formatting utilities."""

from datetime import date, datetime
from decimal import Decimal
from typing import Any


def format_currency(
    value: float | int | Decimal | None,
    symbol: str = "$",
    decimals: int = 2,
) -> str:
    """
    Format a number as currency.

    Args:
        value: Numeric value
        symbol: Currency symbol
        decimals: Number of decimal places

    Returns:
        Formatted currency string (e.g., "$123.45")
    """
    if value is None:
        return f"{symbol}--"

    try:
        num = float(value)
        return f"{symbol}{num:,.{decimals}f}"
    except (ValueError, TypeError):
        return f"{symbol}--"


def format_large_number(
    value: float | int | Decimal | None,
    decimals: int = 1,
    prefix: str = "",
    suffix: str = "",
) -> str:
    """
    Format large numbers with K/M/B/T suffixes.

    Args:
        value: Numeric value
        decimals: Number of decimal places
        prefix: Prefix string (e.g., "$")
        suffix: Suffix string

    Returns:
        Formatted string (e.g., "$1.5B", "45.2M")
    """
    if value is None:
        return f"{prefix}--{suffix}"

    try:
        num = float(value)
    except (ValueError, TypeError):
        return f"{prefix}--{suffix}"

    if num == 0:
        return f"{prefix}0{suffix}"

    # Handle negative numbers
    sign = ""
    if num < 0:
        sign = "-"
        num = abs(num)

    # Determine suffix
    if num >= 1_000_000_000_000:
        formatted = f"{num / 1_000_000_000_000:.{decimals}f}T"
    elif num >= 1_000_000_000:
        formatted = f"{num / 1_000_000_000:.{decimals}f}B"
    elif num >= 1_000_000:
        formatted = f"{num / 1_000_000:.{decimals}f}M"
    elif num >= 1_000:
        formatted = f"{num / 1_000:.{decimals}f}K"
    else:
        formatted = f"{num:.{decimals}f}"

    return f"{prefix}{sign}{formatted}{suffix}"


def format_percent(
    value: float | int | Decimal | None,
    decimals: int = 1,
    include_sign: bool = False,
) -> str:
    """
    Format a number as percentage.

    Args:
        value: Numeric value (0.15 = 15%)
        decimals: Number of decimal places
        include_sign: Include + for positive values

    Returns:
        Formatted percentage string (e.g., "15.0%", "+15.0%")
    """
    if value is None:
        return "--%"

    try:
        num = float(value)
        # If value looks like it's already a percentage (> 1 or < -1), don't multiply
        if abs(num) <= 1:
            num = num * 100

        sign = ""
        if include_sign and num > 0:
            sign = "+"

        return f"{sign}{num:.{decimals}f}%"
    except (ValueError, TypeError):
        return "--%"


def format_change(
    value: float | int | Decimal | None,
    is_percent: bool = False,
    decimals: int = 2,
    prefix: str = "",
) -> str:
    """
    Format a change value with +/- sign.

    Args:
        value: Numeric change value
        is_percent: If True, format as percentage
        decimals: Number of decimal places
        prefix: Prefix string (e.g., "$")

    Returns:
        Formatted change string (e.g., "+$5.25", "-2.5%")
    """
    if value is None:
        return "--"

    try:
        num = float(value)
    except (ValueError, TypeError):
        return "--"

    sign = "+" if num >= 0 else ""

    if is_percent:
        return f"{sign}{num:.{decimals}f}%"
    else:
        return f"{sign}{prefix}{num:.{decimals}f}"


def format_date_range(
    start: str | date | datetime | None,
    end: str | date | datetime | None,
    fmt: str = "%Y-%m-%d",
) -> str:
    """
    Format a date range.

    Args:
        start: Start date
        end: End date
        fmt: Date format string

    Returns:
        Formatted range string (e.g., "2024-01-01 to 2024-06-01")
    """
    start_str = _format_date(start, fmt) or "?"
    end_str = _format_date(end, fmt) or "?"
    return f"{start_str} \u2192 {end_str}"  # Using arrow symbol


def _format_date(value: Any, fmt: str) -> str | None:
    """Format a single date value."""
    if value is None:
        return None

    if isinstance(value, str):
        return value

    if isinstance(value, (date, datetime)):
        return value.strftime(fmt)

    return str(value)


def format_number(
    value: float | int | Decimal | None,
    decimals: int = 2,
    thousands_sep: bool = True,
) -> str:
    """
    Format a plain number.

    Args:
        value: Numeric value
        decimals: Number of decimal places
        thousands_sep: Include thousands separator

    Returns:
        Formatted number string
    """
    if value is None:
        return "--"

    try:
        num = float(value)
        if thousands_sep:
            return f"{num:,.{decimals}f}"
        else:
            return f"{num:.{decimals}f}"
    except (ValueError, TypeError):
        return "--"
