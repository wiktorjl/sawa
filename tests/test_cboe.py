"""Tests for the CBOE delayed-quotes client and same-day internals merge."""

from typing import Any
from unittest.mock import MagicMock, patch

import httpx

from sawa.api.cboe import CboeClient
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

    def test_mismatched_trade_dates_merge_into_one_row(self) -> None:
        """If the two feeds disagree on the trade date, both fields still land
        on a single row keyed by the later settlement date."""
        client = CboeClient()
        with patch.object(client.client, "get") as mock_get:
            mock_get.side_effect = [
                _ok_response(_quote_payload("_VIX", 22.22, "2026-06-09T16:15:01")),
                _ok_response(_quote_payload("_VIX3M", 22.89, "2026-06-10T16:15:01")),
            ]
            rows = client.get_market_internals()

        assert rows == [{"date": "2026-06-10", "vix": 22.22, "vix3m": 22.89}]

    def test_both_symbols_failing_returns_empty(self) -> None:
        client = CboeClient()
        with patch.object(client.client, "get") as mock_get:
            mock_get.side_effect = [
                httpx.ReadTimeout("timed out"),
                httpx.ReadTimeout("timed out"),
            ]
            assert client.get_market_internals() == []


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
