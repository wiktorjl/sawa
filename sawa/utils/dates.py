"""Date parsing and range calculation utilities."""

import argparse
from datetime import date, datetime, timezone

from dateutil.relativedelta import relativedelta

DATE_FORMAT = "%Y-%m-%d"
DEFAULT_YEARS = 5


def parse_date(date_str: str) -> date:
    """
    Parse YYYY-MM-DD date string for argparse.

    Args:
        date_str: Date string in YYYY-MM-DD format

    Returns:
        Parsed date object

    Raises:
        argparse.ArgumentTypeError: If date format is invalid
    """
    try:
        return datetime.strptime(date_str, DATE_FORMAT).date()
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Use YYYY-MM-DD."
        ) from e


def calculate_date_range(
    start_date: date | None = None,
    end_date: date | None = None,
    years: int | None = None,
) -> tuple[date, date]:
    """
    Calculate start and end dates from various input options.

    Uses relativedelta for accurate leap year handling.

    Args:
        start_date: Explicit start date (takes priority)
        end_date: Explicit end date (defaults to today)
        years: Number of years back from end_date

    Returns:
        Tuple of (start_date, end_date)

    Raises:
        ValueError: If start_date >= end_date
    """
    calc_end = end_date or date.today()

    if start_date:
        calc_start = start_date
    elif years:
        calc_start = calc_end - relativedelta(years=years)
    else:
        calc_start = calc_end - relativedelta(years=DEFAULT_YEARS)

    if calc_start >= calc_end:
        raise ValueError(f"Start date {calc_start} must be before end date {calc_end}")

    return calc_start, calc_end


def timestamp_to_date(timestamp_ms: int) -> date:
    """
    Convert millisecond timestamp to date (timezone-aware).

    Args:
        timestamp_ms: Unix timestamp in milliseconds

    Returns:
        Date object in UTC
    """
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).date()
