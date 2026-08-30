"""Tests for the CBOE delayed-quotes client and same-day internals merge."""

from typing import Any
from unittest.mock import MagicMock, patch

import httpx

from sawa.api.cboe import CboeClient, CboeMarketInternalsResult
from sawa.daily import merge_cboe_internals


def _ok_response(payload: dict[str, Any]) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = 200
    resp.json.return_value = payload
    resp.raise_for_status.return_value = None
    return resp


def _quote_payload(symbol: str, close: float, last_trade: str) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "data": {
            "symbol": f"^{symbol.lstrip('_')}",
            "close": close,
            "last_trade_time": last_trade,
        },
    }


class TestCboeClient:
    def test_get_market_internals_merges_by_date(self) -> None:
        client = CboeClient()
        with patch.object(client.client, "get") as mock_get:
            mock_get.side_effect = [
                _ok_response(_quote_payload("_VIX", 22.22, "2026-06-10T16:15:01")),
                _ok_response(_quote_payload("_VIX3M", 22.89, "2026-06-10T16:15:01")),
            ]
            rows = client.get_market_internals()

        assert rows == [{"date": "2026-06-10", "vix": 22.22, "vix3m": 22.89}]

    def test_get_quote_rejects_missing_close(self) -> None:
        client = CboeClient()
        payload = _quote_payload("_VIX", 22.22, "2026-06-10T16:15:01")
        payload["data"]["close"] = 0
        with patch.object(client.client, "get", return_value=_ok_response(payload)):
            assert client.get_quote("_VIX") is None

    def test_get_quote_rejects_missing_trade_time(self) -> None:
        client = CboeClient()
        payload = _quote_payload("_VIX", 22.22, "")
        with patch.object(client.client, "get", return_value=_ok_response(payload)):
            assert client.get_quote("_VIX") is None

    def test_one_symbol_failing_keeps_the_other(self) -> None:
        client = CboeClient()
        with patch.object(client.client, "get") as mock_get:
            mock_get.side_effect = [
                httpx.ReadTimeout("timed out"),
                _ok_response(_quote_payload("_VIX3M", 22.89, "2026-06-10T16:15:01")),
            ]
            rows = client.get_market_internals()

        assert rows == [{"date": "2026-06-10", "vix3m": 22.89}]
        assert isinstance(rows, CboeMarketInternalsResult)
        assert rows.all_quotes_failed is False
        assert rows.failure_details == [
            {
                "symbol": "_VIX",
                "field": "vix",
                "error_type": "ReadTimeout",
                "message": "timed out",
            }
        ]

    def test_mismatched_trade_dates_merge_into_one_row(self) -> None:
        """A stale quote must never be relabeled onto the other feed's date."""
        client = CboeClient()
        with patch.object(client.client, "get") as mock_get:
            mock_get.side_effect = [
                _ok_response(_quote_payload("_VIX", 22.22, "2026-06-09T16:15:01")),
                _ok_response(_quote_payload("_VIX3M", 22.89, "2026-06-10T16:15:01")),
            ]
            rows = client.get_market_internals()

        assert rows == [
            {"date": "2026-06-09", "vix": 22.22},
            {"date": "2026-06-10", "vix3m": 22.89},
        ]

    def test_both_symbols_failing_returns_empty(self) -> None:
        client = CboeClient()
        with patch.object(client.client, "get") as mock_get:
            mock_get.side_effect = [
                httpx.ReadTimeout("timed out"),
                httpx.ReadTimeout("timed out"),
            ]
            result = client.get_market_internals()

        assert result == []
        assert result.all_quotes_failed is True
        assert [failure.field for failure in result.failures] == ["vix", "vix3m"]

    def test_unusable_quote_is_a_typed_batch_failure(self) -> None:
        client = CboeClient()
        bad = _quote_payload("_VIX", 22.22, "2026-06-10T16:15:01")
        bad["data"]["close"] = float("nan")
        with patch.object(client.client, "get") as mock_get:
            mock_get.side_effect = [
                _ok_response(bad),
                _ok_response(_quote_payload("_VIX3M", 22.89, "2026-06-10T16:15:01")),
            ]
            result = client.get_market_internals()

        assert result == [{"date": "2026-06-10", "vix3m": 22.89}]
        assert result.failure_details[0]["symbol"] == "_VIX"
        assert result.failure_details[0]["error_type"] == "ProviderError"

    def test_mixed_quote_identity_is_a_typed_failure(self) -> None:
        client = CboeClient()
        mismatched = _quote_payload("_VIX", 22.22, "2026-06-10T16:15:01")
        mismatched["data"]["symbol"] = "^VIX3M"
        with patch.object(client.client, "get") as mock_get:
            mock_get.side_effect = [
                _ok_response(mismatched),
                _ok_response(
                    _quote_payload("_VIX3M", 22.89, "2026-06-10T16:15:01")
                ),
            ]
            result = client.get_market_internals()

        assert result == [{"date": "2026-06-10", "vix3m": 22.89}]
        assert result.failure_details[0]["field"] == "vix"
        assert "symbol does not match" in result.failure_details[0]["message"]

    def test_strict_and_plausible_quote_dates_are_enforced(self) -> None:
        client = CboeClient()
        malformed = _quote_payload("_VIX", 22.22, "2026-6-10T16:15:01")
        future = _quote_payload("_VIX3M", 22.89, "2999-01-01T16:15:01")
        with patch.object(client.client, "get") as mock_get:
            mock_get.side_effect = [_ok_response(malformed), _ok_response(future)]
            result = client.get_market_internals()

        assert result == []
        assert result.all_quotes_failed is True
        assert all(failure.error_type == "ProviderError" for failure in result.failures)


class TestMergeCboeInternals:
    def test_appends_same_day_row_fred_lacks(self) -> None:
        fred = [{"date": "2026-06-09", "vix": "19.87", "vix3m": "21.31", "hy_spread": "2.75"}]
        cboe = [{"date": "2026-06-10", "vix": 22.22, "vix3m": 22.89}]

        merged = merge_cboe_internals(fred, cboe)

        assert merged[-1] == {
            "date": "2026-06-10",
            "vix": 22.22,
            "vix3m": 22.89,
            "hy_spread": None,
        }

    def test_fred_values_win_on_overlapping_dates(self) -> None:
        fred = [{"date": "2026-06-10", "vix": "22.20", "vix3m": "22.90", "hy_spread": "2.75"}]
        cboe = [{"date": "2026-06-10", "vix": 22.22, "vix3m": 22.89}]

        merged = merge_cboe_internals(fred, cboe)

        assert merged == [
            {"date": "2026-06-10", "vix": "22.20", "vix3m": "22.90", "hy_spread": "2.75"}
        ]

    def test_fills_holes_without_touching_hy_spread(self) -> None:
        fred = [{"date": "2026-06-10", "vix": "22.20", "vix3m": None, "hy_spread": "2.75"}]
        cboe = [{"date": "2026-06-10", "vix": 22.22, "vix3m": 22.89}]

        merged = merge_cboe_internals(fred, cboe)

        assert merged == [
            {"date": "2026-06-10", "vix": "22.20", "vix3m": 22.89, "hy_spread": "2.75"}
        ]

    def test_no_cboe_rows_is_a_noop(self) -> None:
        fred = [{"date": "2026-06-09", "vix": "19.87", "vix3m": "21.31", "hy_spread": None}]
        assert merge_cboe_internals(fred, []) == fred

    def test_appends_even_when_fred_is_empty(self) -> None:
        merged = merge_cboe_internals([], [{"date": "2026-06-10", "vix": 22.22}])
        assert merged == [
            {"date": "2026-06-10", "vix": 22.22, "vix3m": None, "hy_spread": None}
        ]
