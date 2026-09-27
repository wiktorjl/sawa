"""Fail-closed issuer continuity checks, with no provider or database access."""

import logging
from datetime import date
from unittest import mock

import pytest

from sawa import split_adjust
from sawa.domain.issuer_continuity import (
    REVIEWED_IDENTITY_CORRECTIONS,
    REVIEWED_ISSUER_SUCCESSORS,
)

DATES = {date(2021, 2, 18), date(2026, 9, 25)}
CLBK_OLD = {
    "cik": "0001723596",
    "composite_figi": "BBG003222R31",
    "share_class_figi": "BBG003222R95",
}
CLBK_NEW = {
    "cik": "0002115119",
    "composite_figi": "BBG023TM45W0",
    "share_class_figi": "BBG023TM45X9",
}


def _check(old, new, *, ticker="CLBK", dates=None, conn=None):
    client = mock.Mock()
    client.get_single.side_effect = [old, new]
    return split_adjust.known_identity_conflict(
        client, ticker, dates or DATES, mock.Mock(), conn=conn,
    )


def _ledger(conversion=(5, 11)):
    conn = mock.MagicMock()
    conn.cursor.return_value.__enter__.return_value.fetchone.return_value = conversion
    return conn


def test_snfca_historical_cik_correction_with_both_figis_unchanged_is_safe():
    # SEC's 2020 10-K (filed 2021-03-31) already identifies SNFCA under CIK 318673:
    # https://www.sec.gov/Archives/edgar/data/318673/000109690621000634/snfca_10k.htm
    figis = {"composite_figi": "BBG000C3JXB4", "share_class_figi": "BBG001S6HG45"}
    conn = _ledger()
    assert _check(
        {"cik": "0000936217", **figis}, {"cik": "0000318673", **figis},
        ticker="SNFCA", conn=conn,
    ) is None
    conn.cursor.assert_not_called()


@pytest.mark.parametrize("field", ["composite_figi", "share_class_figi"])
@pytest.mark.parametrize("value", [None, "", "   ", "DIFFERENT", 123])
def test_one_matching_figi_is_not_enough_to_accept_a_cik_change(field, value):
    old = {"cik": "123", "composite_figi": "COMPOSITE", "share_class_figi": "CLASS"}
    new = {**old, "cik": "456", field: value}
    assert _check(old, new, ticker="UNREVIEWED") is not None


@pytest.mark.parametrize("blank", [None, "", "   "])
def test_matching_but_blank_figis_are_not_evidence(blank):
    figis = {"composite_figi": blank, "share_class_figi": blank}
    assert _check({"cik": "123", **figis}, {"cik": "456", **figis}) is not None


@pytest.mark.parametrize("conversion", [(5, 11), (10, 22)])
def test_reviewed_clbk_successor_requires_exact_recorded_conversion(conversion):
    conn = _ledger(conversion)
    assert _check(CLBK_OLD, CLBK_NEW, conn=conn) is None
    cursor = conn.cursor.return_value.__enter__.return_value
    assert cursor.execute.call_args.args[1] == ("CLBK", date(2026, 7, 21))
    assert "execution_date = %s" in cursor.execute.call_args.args[0]
    conn.commit.assert_not_called()


@pytest.mark.parametrize("conversion", [None, (1, 2), (11, 5), (0, 11), (-5, -11)])
def test_reviewed_clbk_without_the_required_ledger_ratio_is_still_blocked(conversion):
    assert _check(CLBK_OLD, CLBK_NEW, conn=_ledger(conversion)) is not None


def test_reviewed_clbk_without_a_database_connection_is_still_blocked():
    assert _check(CLBK_OLD, CLBK_NEW) is not None


@pytest.mark.parametrize("side", ["old", "new"])
@pytest.mark.parametrize("field", ["cik", "composite_figi", "share_class_figi"])
def test_clbk_successor_rule_does_not_accept_different_identity_pins(side, field):
    old, new = dict(CLBK_OLD), dict(CLBK_NEW)
    (old if side == "old" else new)[field] = "999999"
    conn = _ledger()
    assert _check(old, new, conn=conn) is not None
    conn.cursor.assert_not_called()


@pytest.mark.parametrize("dates", [
    {date(2021, 2, 18), date(2026, 7, 20)},
    {date(2026, 7, 21), date(2026, 9, 25)},
])
def test_clbk_successor_rule_requires_dates_straddling_the_conversion(dates):
    conn = _ledger()
    assert _check(CLBK_OLD, CLBK_NEW, dates=dates, conn=conn) is not None
    conn.cursor.assert_not_called()


def test_clbk_successor_rule_does_not_extend_to_another_ticker():
    assert _check(CLBK_OLD, CLBK_NEW, ticker="OTHER", conn=_ledger()) is not None


def test_dfns_unrelated_issuer_reuse_remains_blocked_even_with_a_conversion_ledger():
    conflict = _check(
        {"cik": "0001777946"}, {"cik": "0001787518"}, ticker="DFNS", conn=_ledger(),
    )
    assert conflict["old_cik"] == "1777946"
    assert conflict["current_cik"] == "1787518"


def test_database_failure_does_not_bypass_the_reviewed_successor_ledger_check():
    conn = _ledger()
    conn.cursor.side_effect = RuntimeError("database unavailable")
    with pytest.raises(RuntimeError, match="database unavailable"):
        _check(CLBK_OLD, CLBK_NEW, conn=conn)


def test_reviewed_successor_retains_primary_source_provenance():
    successor = next(item for item in REVIEWED_ISSUER_SUCCESSORS if item.ticker == "CLBK")
    assert successor.ticker == "CLBK"
    assert any(url.startswith("https://www.sec.gov/") for url in successor.primary_sources)
    assert any(url.startswith("https://www.nasdaqtrader.com/") for url in successor.primary_sources)


def _correction_details(correction):
    return (
        {"cik": correction.old_cik, "composite_figi": correction.old_composite_figi,
         "share_class_figi": correction.share_class_figi},
        {"cik": correction.current_cik, "composite_figi": correction.current_composite_figi,
         "share_class_figi": correction.share_class_figi},
    )


@pytest.mark.parametrize("correction", REVIEWED_IDENTITY_CORRECTIONS)
def test_reviewed_proshares_metadata_correction_does_not_require_a_fictitious_split(correction):
    old, new = _correction_details(correction)
    conn = _ledger()
    assert _check(old, new, ticker=correction.ticker, conn=conn) is None
    conn.cursor.assert_not_called()
    assert len(correction.primary_sources) >= 2
    assert all(url.startswith("https://www.sec.gov/") for url in correction.primary_sources)


@pytest.mark.parametrize("correction", REVIEWED_IDENTITY_CORRECTIONS)
@pytest.mark.parametrize("side", [0, 1])
@pytest.mark.parametrize("field", ["cik", "composite_figi", "share_class_figi"])
def test_proshares_metadata_correction_requires_every_reviewed_identity_pin(
    correction, side, field,
):
    details = _correction_details(correction)
    details[side][field] = "999999"
    assert _check(*details, ticker=correction.ticker) is not None


@pytest.mark.parametrize("correction", REVIEWED_IDENTITY_CORRECTIONS)
def test_proshares_metadata_correction_does_not_extend_to_other_tickers(correction):
    old, new = _correction_details(correction)
    assert _check(old, new, ticker="UNREVIEWED") is not None


@pytest.mark.parametrize("correction", REVIEWED_IDENTITY_CORRECTIONS)
def test_proshares_metadata_correction_cannot_be_applied_in_reverse(correction):
    old, new = _correction_details(correction)
    assert _check(new, old, ticker=correction.ticker) is not None


def test_split_refresh_passes_its_connection_to_identity_verification():
    conn = mock.MagicMock()
    conn.__enter__.return_value = conn
    with (
        mock.patch.object(split_adjust.psycopg, "connect", return_value=conn),
        mock.patch.object(split_adjust, "PolygonClient"),
        mock.patch.object(split_adjust, "SyncRateLimiter"),
        mock.patch.object(split_adjust, "get_earliest_price_date", return_value=min(DATES)),
        mock.patch.object(split_adjust, "get_existing_price_dates", return_value={"CLBK": DATES}),
        mock.patch.object(
            split_adjust, "known_identity_conflict", side_effect=RuntimeError("stop before fetch"),
        ) as check,
        mock.patch.object(split_adjust, "fetch_prices_via_api") as fetch,
    ):
        result = split_adjust.refresh_split_adjusted_prices(
            "unused", "unused", ["CLBK"], logger=logging.getLogger(__name__),
        )
    assert check.call_args.kwargs["conn"] is conn
    assert result["success"] is False
    fetch.assert_not_called()
