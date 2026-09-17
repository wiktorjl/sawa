"""NYSE trading-calendar helpers used by the doctor freshness checks."""

from __future__ import annotations

from datetime import date, datetime

import pytest

from sawa.utils.market_hours import (
    ET,
    expected_latest_eod_date,
    is_trading_day,
    nyse_holidays,
    previous_trading_day,
)


def _et(year: int, month: int, day: int, hour: int, minute: int = 0) -> datetime:
    return ET.localize(datetime(year, month, day, hour, minute))


def test_2026_holidays_match_the_published_nyse_calendar() -> None:
    assert sorted(nyse_holidays(2026)) == [
        date(2026, 1, 1),  # New Year's Day
        date(2026, 1, 19),  # MLK Day
        date(2026, 2, 16),  # Presidents' Day
        date(2026, 4, 3),  # Good Friday
        date(2026, 5, 25),  # Memorial Day
        date(2026, 6, 19),  # Juneteenth
        date(2026, 7, 3),  # Independence Day observed (Jul 4 is a Saturday)
        date(2026, 9, 7),  # Labor Day
        date(2026, 11, 26),  # Thanksgiving
        date(2026, 12, 25),  # Christmas
    ]


def test_2027_observed_rules_shift_weekend_holidays() -> None:
    holidays = nyse_holidays(2027)
    assert date(2027, 6, 18) in holidays  # Juneteenth (Sat) -> Friday
    assert date(2027, 7, 5) in holidays  # Independence Day (Sun) -> Monday
    assert date(2027, 12, 24) in holidays  # Christmas (Sat) -> Friday
    assert date(2027, 3, 26) in holidays  # Good Friday (Easter 2027-03-28)


def test_new_years_day_on_saturday_is_not_observed_on_friday() -> None:
    # 2022-01-01 was a Saturday; the NYSE was open on Friday 2021-12-31.
    assert date(2021, 12, 31) not in nyse_holidays(2021)
    assert date(2022, 1, 1) not in nyse_holidays(2022)
    assert is_trading_day(date(2021, 12, 31))


def test_extra_closures_are_not_trading_days() -> None:
    assert not is_trading_day(date(2025, 1, 9))  # day of mourning
    assert is_trading_day(date(2025, 1, 10))


@pytest.mark.parametrize(
    ("day", "expected"),
    [
        (date(2026, 9, 8), date(2026, 9, 4)),  # Tuesday after Labor Day -> Friday
        (date(2026, 9, 14), date(2026, 9, 11)),  # Monday -> Friday
        (date(2026, 9, 16), date(2026, 9, 15)),  # midweek
        (date(2026, 1, 2), date(2025, 12, 31)),  # across New Year
    ],
)
def test_previous_trading_day(day: date, expected: date) -> None:
    assert previous_trading_day(day) == expected


def test_expected_eod_is_previous_session_before_settlement() -> None:
    assert expected_latest_eod_date(_et(2026, 9, 15, 8, 30)) == date(2026, 9, 14)
    assert expected_latest_eod_date(_et(2026, 9, 15, 19, 59)) == date(2026, 9, 14)


def test_expected_eod_is_today_after_settlement_on_a_trading_day() -> None:
    assert expected_latest_eod_date(_et(2026, 9, 15, 20, 0)) == date(2026, 9, 15)
    assert expected_latest_eod_date(_et(2026, 9, 15, 17, 30), settled_hour=17) == date(
        2026, 9, 15
    )


def test_expected_eod_over_a_long_weekend() -> None:
    # Labor Day 2026-09-07: Saturday through Tuesday morning expect Friday.
    for stamp in (
        _et(2026, 9, 5, 12),
        _et(2026, 9, 6, 22),
        _et(2026, 9, 7, 22),
        _et(2026, 9, 8, 8, 30),
    ):
        assert expected_latest_eod_date(stamp) == date(2026, 9, 4)
