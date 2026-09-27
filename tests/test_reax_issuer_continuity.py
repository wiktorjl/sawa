"""Offline checks for REAX's reviewed merger successor, not ticker reuse."""

from datetime import date
from fractions import Fraction
from unittest import mock

import pytest

from sawa import split_adjust
from sawa.domain.issuer_continuity import REVIEWED_ISSUER_SUCCESSORS

DATES = {date(2021, 6, 15), date(2026, 9, 25)}
OLD = {
    "cik": "0001862461",
    "composite_figi": "BBG00VNNPD72",
    "share_class_figi": "BBG00KRN3NQ3",
}
NEW = {
    "cik": "0002136387",
    "composite_figi": "BBG024M90BY3",
    "share_class_figi": "BBG024M90C26",
}


def _check(*, conversion=(10, 1), old=None, new=None, dates=None, ticker="REAX"):
    client = mock.Mock()
    client.get_single.side_effect = [old or OLD, new or NEW]
    conn = mock.MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = conversion
    result = split_adjust.known_identity_conflict(
        client, ticker, dates or DATES, mock.Mock(), conn=conn
    )
    conn.commit.assert_not_called()
    return result, cursor


def test_reax_successor_requires_recorded_reverse_split_on_first_new_trading_date():
    result, cursor = _check()

    assert result is None
    assert cursor.execute.call_args.args[1] == ("REAX", date(2026, 8, 25))
    assert "execution_date = %s" in cursor.execute.call_args.args[0]


@pytest.mark.parametrize("conversion", [None, (1, 1), (1, 10), (10, 2)])
def test_reax_without_exact_ledger_conversion_stays_blocked(conversion):
    result, _ = _check(conversion=conversion)
    assert result is not None


@pytest.mark.parametrize("side", ["old", "new"])
@pytest.mark.parametrize("field", ["cik", "composite_figi", "share_class_figi"])
def test_reax_mapping_rejects_different_identity_pins(side, field):
    old, new = dict(OLD), dict(NEW)
    (old if side == "old" else new)[field] = "999999"

    result, cursor = _check(old=old, new=new)

    assert result is not None
    cursor.execute.assert_not_called()


@pytest.mark.parametrize(
    "dates",
    [
        {date(2021, 6, 15), date(2026, 8, 24)},
        {date(2026, 8, 25), date(2026, 9, 25)},
    ],
)
def test_reax_mapping_requires_history_crossing_the_conversion(dates):
    result, cursor = _check(dates=dates)
    assert result is not None
    cursor.execute.assert_not_called()


def test_reax_mapping_does_not_extend_to_other_tickers():
    result, cursor = _check(ticker="ICON")
    assert result is not None
    cursor.execute.assert_not_called()


def test_reax_successor_retains_exact_share_conversion_and_primary_source():
    successor = next(item for item in REVIEWED_ISSUER_SUCCESSORS if item.ticker == "REAX")
    assert successor.share_ratio == Fraction(1, 10)
    assert successor.execution_date == date(2026, 8, 25)
    assert (
        "https://www.sec.gov/Archives/edgar/data/1862461/"
        "000110465926100333/tm2623609d5_6k.htm"
    ) in successor.primary_sources
