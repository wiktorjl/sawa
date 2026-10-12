"""Distinguish confirmed unavailable bars from incomplete local ingestion."""

import logging
from datetime import date
from unittest import mock

import pytest

from sawa import stock_character_batch as batch

EMPTY = {
    "ticker": "THIN", "status": "DELAYED", "adjusted": True,
    "queryCount": 0, "resultsCount": 0, "request_id": "reviewable-request",
}
STALE = [{"ticker": "THIN", "source_price_date": date(2026, 9, 25)}]
THROUGH = date(2026, 9, 28)


@pytest.mark.parametrize("response,accepted", [
    (EMPTY, True),
    ({**EMPTY, "status": "OK", "results": []}, True),
    ({}, False),
    (None, False),
    ({**EMPTY, "ticker": "DIFFERENT"}, False),
    ({**EMPTY, "status": "ERROR"}, False),
    ({**EMPTY, "queryCount": False}, False),
    ({**EMPTY, "resultsCount": False}, False),
    ({**EMPTY, "resultsCount": 1}, False),
    ({**EMPTY, "results": [{"c": 10}]}, False),
    ({**EMPTY, "results": None}, False),
    ({**EMPTY, "adjusted": False}, False),
])
def test_only_explicit_complete_empty_response_confirms_unavailable(response, accepted):
    client = mock.MagicMock()
    client.__enter__.return_value = client
    client.get.return_value = response
    with (
        mock.patch.object(batch, "PolygonClient", return_value=client),
        mock.patch.object(batch, "SyncRateLimiter"),
    ):
        confirmed, errors = batch.verify_empty_price_windows(
            "test", STALE, THROUGH, logging.getLogger(__name__),
        )
    assert bool(confirmed) is accepted
    assert not errors
    assert client.get.call_args.kwargs["path_params"] == {
        "ticker": "THIN", "start": "2026-09-26", "end": "2026-09-28",
    }
    if accepted:
        assert confirmed[0]["verified_through"] == THROUGH
        assert confirmed[0]["source_price_date"] == date(2026, 9, 25)


def test_provider_failure_cannot_confirm_unavailability():
    client = mock.MagicMock()
    client.__enter__.return_value = client
    client.get.side_effect = RuntimeError("request failed")
    with (
        mock.patch.object(batch, "PolygonClient", return_value=client),
        mock.patch.object(batch, "SyncRateLimiter"),
    ):
        confirmed, errors = batch.verify_empty_price_windows(
            "test", STALE, THROUGH, logging.getLogger(__name__),
        )
    assert not confirmed
    assert errors == {"THIN": "request failed"}


@pytest.mark.parametrize("confirmed", [True, False])
def test_skipped_stale_inputs_are_never_reclassified_and_only_verified_empty_is_healthy(confirmed):
    conn = mock.MagicMock()
    rows = [
        {"ticker": "GOOD", "classified": True, "character": "trending"},
        {**STALE[0], "classified": False, "stale_source": True},
    ]
    with (
        mock.patch.object(batch.psycopg, "connect", return_value=conn),
        mock.patch.object(batch, "_fetch_benchmark_prices", return_value={}),
        mock.patch.object(batch, "_process_ticker", side_effect=rows),
        mock.patch.object(batch, "verify_empty_price_windows",
                          return_value=(STALE if confirmed else [], {})),
    ):
        result = batch.run_stock_character_batch(
            "test", ["GOOD", "THIN"], workers=1, run_date=THROUGH, api_key="key",
        )
    assert result["success"] is True
    assert result["classified"] == 1
    assert result["stale_sources"] == 1
    assert result["unresolved_stale_sources"] == int(not confirmed)
    assert result["degraded"] is not confirmed
