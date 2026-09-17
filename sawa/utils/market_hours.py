"""Market hours utilities for US stock market."""

from datetime import date, datetime, timedelta

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


# ── Trading calendar ─────────────────────────────────────────────────────────
#
# NYSE regular-session calendar: weekdays minus the nine fixed holidays and
# Good Friday. Observed-day rules follow the exchange: a Saturday holiday is
# observed on the preceding Friday and a Sunday holiday on the following
# Monday, except New Year's Day, which is never moved into the prior year.
# Unscheduled closures (days of mourning, weather) cannot be derived; add them
# to EXTRA_MARKET_CLOSURES when the exchange announces them.

EXTRA_MARKET_CLOSURES: frozenset[date] = frozenset(
    {
        date(2025, 1, 9),  # National day of mourning (President Carter)
    }
)


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    last = date(year + (month == 12), (month % 12) + 1, 1) - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def _easter(year: int) -> date:
    """Gregorian Easter Sunday (anonymous / Meeus-Jones-Butcher algorithm)."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    m = (32 + 2 * e + 2 * i - h - k) % 7
    n = (a + 11 * h + 22 * m) // 451
    month, day = divmod(h + m - 7 * n + 114, 31)
    return date(year, month, day + 1)


def _observed(holiday: date) -> date:
    if holiday.weekday() == 5:
        return holiday - timedelta(days=1)
    if holiday.weekday() == 6:
        return holiday + timedelta(days=1)
    return holiday


def nyse_holidays(year: int) -> frozenset[date]:
    """Full-day NYSE closures for ``year`` (regular holidays only)."""
    holidays: set[date] = set()
    new_year = date(year, 1, 1)
    if new_year.weekday() == 6:
        holidays.add(new_year + timedelta(days=1))
    elif new_year.weekday() != 5:
        holidays.add(new_year)
    holidays.add(_nth_weekday(year, 1, 0, 3))  # Martin Luther King Jr. Day
    holidays.add(_nth_weekday(year, 2, 0, 3))  # Presidents' Day
    holidays.add(_easter(year) - timedelta(days=2))  # Good Friday
    holidays.add(_last_weekday(year, 5, 0))  # Memorial Day
    holidays.add(_observed(date(year, 6, 19)))  # Juneteenth
    holidays.add(_observed(date(year, 7, 4)))  # Independence Day
    holidays.add(_nth_weekday(year, 9, 0, 1))  # Labor Day
    holidays.add(_nth_weekday(year, 11, 3, 4))  # Thanksgiving
    holidays.add(_observed(date(year, 12, 25)))  # Christmas
    return frozenset(holidays)


def is_trading_day(day: date) -> bool:
    """True when the NYSE holds a regular session on ``day``."""
    return (
        day.weekday() < 5
        and day not in nyse_holidays(day.year)
        and day not in EXTRA_MARKET_CLOSURES
    )


def previous_trading_day(day: date) -> date:
    """Most recent trading day strictly before ``day``."""
    candidate = day - timedelta(days=1)
    while not is_trading_day(candidate):
        candidate -= timedelta(days=1)
    return candidate


def expected_latest_eod_date(
    now_et: datetime | None = None, *, settled_hour: int = 20
) -> date:
    """Most recent session whose EOD data should already be in the database.

    The daily job starts at 17:00 ET and normally finishes before 19:00 ET, so
    after ``settled_hour`` on a trading day today's session is expected; at any
    other time (mornings, weekends, holidays) the previous trading day is.
    Callers that run right after the daily job pass ``settled_hour=17``.
    """
    now_et = now_et or datetime.now(ET)
    today = now_et.date()
    if is_trading_day(today) and now_et.hour >= settled_hour:
        return today
    return previous_trading_day(today)
