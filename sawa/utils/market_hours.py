"""Market hours utilities for US stock market."""

from datetime import date, datetime

import pytz

ET = pytz.timezone("America/New_York")


def get_market_date() -> date:
    """
    Get today's date in US Eastern Time.

    Use this instead of date.today() for market-related logic
    to avoid timezone mismatches on UTC servers.
    """
    return datetime.now(ET).date()


def is_market_open() -> bool:
    """
    Check if US stock market is currently open (9:30 AM - 4:00 PM ET).

    Returns:
        True if market is open, False otherwise
    """
    et = pytz.timezone("America/New_York")
    now_et = datetime.now(et)

    # Weekend check
    if now_et.weekday() >= 5:  # Sat=5, Sun=6
        return False

    # Time check
    market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)

    return market_open <= now_et <= market_close


def is_after_market_close() -> bool:
    """
    Check if current time is after 5:00 PM ET (settlement time).

    Used to determine when to fetch today's EOD data.

    Returns:
        True if after 5:00 PM ET, False otherwise
    """
    et = pytz.timezone("America/New_York")
    now_et = datetime.now(et)

    # Weekend - consider "after close"
    if now_et.weekday() >= 5:
        return True

    # After 5:00 PM ET
    settlement_time = now_et.replace(hour=17, minute=0, second=0, microsecond=0)
    return now_et >= settlement_time
