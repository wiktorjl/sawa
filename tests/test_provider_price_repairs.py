"""Rounding recovery needs positive raw observations and a verified split basis."""

import logging
from decimal import Decimal
from unittest import mock

import pytest

from sawa.domain.exceptions import ProviderError
from sawa.provider_price_repairs import recover_rounded_adjusted_bars


def responses():
    bars = [
        {"t": 1723608000000 + i * 86400000, "o": 2, "h": 3, "l": 1, "c": 2, "v": 110}
        for i in range(5)
    ]
    tiny = {"t": 1724385600000, "o": 0.000001, "h": 0.0001,
            "l": 0.000001, "c": 0.000001, "v": 345}
    raw = {"ticker": "ELOX", "adjusted": False, "status": "OK", "results": [*bars, tiny]}
    adjusted = {"ticker": "ELOX", "adjusted": True, "status": "DELAYED", "results": [
        {**b, **{k: b[k] * 11 for k in ("o", "h", "l", "c")}, "v": 10} for b in bars
    ] + [{**tiny, "o": 0, "h": 0.0011, "l": 0, "c": 0, "v": 31.363636}]}
    client = mock.Mock()
    client.get.return_value = raw
    client.get_splits.return_value = [{
        "ticker": "ELOX", "execution_date": "2026-06-01", "split_from": 11, "split_to": 1,
    }]
    return client, adjusted, raw


def repair(client, adjusted):
    return recover_rounded_adjusted_bars(
        client, "ELOX", adjusted, "2024-08-01", "2026-09-29",
    )


def test_reconstructs_only_rounding_loss_and_preserves_provider_input():
    client, adjusted, _ = responses()
    rows, count = repair(client, adjusted)
    assert count == 1
    assert rows[-1]["o"] == rows[-1]["l"] == rows[-1]["c"] == Decimal("0.00001100")
    assert rows[-1]["h"] == Decimal("0.00110000")
    assert rows[-1]["v"] == 31
    assert adjusted["results"][-1]["o"] == 0
    assert rows[:-1] == adjusted["results"][:-1]
    assert client.get.call_args.kwargs["params"]["adjusted"] == "false"


@pytest.mark.parametrize("defect", ["zero_raw", "wrong_split", "too_few_probes", "duplicate_probes",
                                  "wrong_volume",
                                  "not_rounding", "missing_raw", "changed_probe"])
def test_uncorroborated_prices_remain_invalid(defect):
    client, adjusted, raw = responses()
    if defect == "zero_raw":
        raw["results"][-1]["o"] = 0
    elif defect == "wrong_split":
        client.get_splits.return_value[0]["split_from"] = 10
    elif defect == "too_few_probes":
        adjusted["results"].pop(0)
    elif defect == "duplicate_probes":
        adjusted["results"] = [adjusted["results"][0]] * 5 + [adjusted["results"][-1]]
    elif defect == "wrong_volume":
        adjusted["results"][-1]["v"] = 100
    elif defect == "not_rounding":
        raw["results"][-1]["o"] = 0.00001
        raw["results"][-1]["h"] = 0.0001
    elif defect == "missing_raw":
        raw["results"].pop()
    else:
        adjusted["results"][0]["o"] = 21
    rows, count = repair(client, adjusted)
    assert count == 0
    assert rows == adjusted["results"]


@pytest.mark.parametrize("defect", ["wrong_ticker", "adjusted_raw", "duplicate_raw",
                                  "wrong_ledger_ticker", "duplicate_split"])
def test_ambiguous_or_mismatched_provider_evidence_fails(defect):
    client, adjusted, raw = responses()
    if defect == "wrong_ticker":
        raw["ticker"] = "OTHER"
    elif defect == "adjusted_raw":
        raw["adjusted"] = True
    elif defect == "duplicate_raw":
        raw["results"].append(dict(raw["results"][0]))
    elif defect == "wrong_ledger_ticker":
        client.get_splits.return_value[0]["ticker"] = "OTHER"
    else:
        client.get_splits.return_value.append(dict(client.get_splits.return_value[0]))
    with pytest.raises(ProviderError):
        repair(client, adjusted)


def test_shared_price_fetch_counts_recovered_rows_as_valid_observations():
    from sawa.daily import fetch_prices_via_api

    client, adjusted, raw = responses()
    client.get.side_effect = [adjusted, raw]
    stats = {}
    rows = fetch_prices_via_api(
        client, ["ELOX"], "2024-08-01", "2026-09-29", logging.getLogger(__name__),
        stats=stats,
    )
    assert len(rows) == 6
    assert rows[-1]["open"] == Decimal("0.00001100")
    assert stats["rounded_price_rows_recovered"] == {"ELOX": 1}
    assert stats["failed_symbols"] == 0
    assert stats.get("invalid_price_rows", 0) == 0


def test_rounding_recovery_outage_remains_a_fetch_failure():
    from sawa.daily import fetch_prices_via_api

    client, adjusted, _ = responses()
    client.get.side_effect = [adjusted, RuntimeError("raw provider unavailable")]
    stats = {}
    rows = fetch_prices_via_api(
        client, ["ELOX"], "2024-08-01", "2026-09-29", logging.getLogger(__name__),
        stats=stats,
    )
    assert not rows
    assert stats["failed_tickers"] == ["ELOX"]
    assert stats["fetch_errors"] == 1
