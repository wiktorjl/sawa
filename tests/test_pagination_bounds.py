"""Offline regression tests for bounded Polygon pagination."""

import logging
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

import sawa.api.client as client_module
import sawa.utils.symbols as symbols_module
from sawa.api.client import PolygonClient
from sawa.domain.exceptions import ProviderError
from sawa.utils.symbols import (
    fetch_nasdaq_active_from_polygon,
    fetch_nasdaq_listed_symbols,
    fetch_us_active_from_polygon,
)

SymbolFetcher = Callable[..., list[str]]
SYMBOL_FETCHERS = (
    fetch_nasdaq_active_from_polygon,
    fetch_us_active_from_polygon,
)


def test_current_nasdaq_refresh_can_disable_stale_snapshot_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_live(*_args: object, **_kwargs: object) -> list[str]:
        raise RuntimeError("live source unavailable")

    monkeypatch.setattr(symbols_module, "fetch_nasdaq_active_from_polygon", fail_live)

    with pytest.raises(RuntimeError, match="live source unavailable"):
        fetch_nasdaq_listed_symbols(
            logging.getLogger(__name__),
            api_key="explicit-key",
            allow_fallback=False,
        )


def _response(payload: object) -> MagicMock:
    response = MagicMock(spec=httpx.Response)
    response.status_code = 200
    response.raise_for_status.return_value = None
    response.json.return_value = payload
    return response


def _page(*tickers: str, next_url: str | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": "OK",
        "results": [
            {"ticker": ticker, "primary_exchange": "XNAS"} for ticker in tickers
        ],
    }
    if next_url is not None:
        payload["next_url"] = next_url
    return payload


def test_get_rejects_list_json_as_provider_error() -> None:
    client = PolygonClient("test-key")
    try:
        with patch.object(client.client, "get", return_value=_response(["not-an-object"])) as get:
            with pytest.raises(ProviderError, match="Invalid response object"):
                client.get("news")
    finally:
        client.close()

    assert get.call_count == 1


def test_get_paginated_rejects_two_page_url_cycle_before_third_request() -> None:
    client = PolygonClient("test-key")
    pages = [
        _response(_page("AAPL", next_url="/pagination/page-two")),
        _response(_page("MSFT", next_url="/v2/reference/news")),
    ]
    try:
        with patch.object(client.client, "get", side_effect=pages) as get:
            with pytest.raises(ProviderError, match="Repeated pagination URL"):
                client.get_paginated("news")
    finally:
        client.close()

    assert get.call_count == 2


def test_get_paginated_allows_exact_page_ceiling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(client_module, "MAX_PAGINATION_PAGES", 2)
    client = PolygonClient("test-key")
    pages = [
        _response(_page("AAPL", next_url="/pagination/page-two")),
        _response(_page("MSFT")),
    ]
    try:
        with patch.object(client.client, "get", side_effect=pages) as get:
            results = client.get_paginated("news")
    finally:
        client.close()

    assert [row["ticker"] for row in results] == ["AAPL", "MSFT"]
    assert get.call_count == 2


def test_get_paginated_rejects_page_beyond_ceiling_before_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(client_module, "MAX_PAGINATION_PAGES", 2)
    client = PolygonClient("test-key")
    pages = [
        _response(_page("AAPL", next_url="/pagination/page-two")),
        _response(_page("MSFT", next_url="/pagination/page-three")),
    ]
    try:
        with patch.object(client.client, "get", side_effect=pages) as get:
            with pytest.raises(ProviderError, match="maximum of 2 pages"):
                client.get_paginated("news")
    finally:
        client.close()

    assert get.call_count == 2


def test_get_paginated_allows_exact_result_ceiling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(client_module, "MAX_PAGINATION_RESULTS", 2)
    client = PolygonClient("test-key")
    pages = [
        _response(_page("AAPL", next_url="/pagination/page-two")),
        _response(_page("MSFT")),
    ]
    try:
        with patch.object(client.client, "get", side_effect=pages) as get:
            results = client.get_paginated("news")
    finally:
        client.close()

    assert len(results) == 2
    assert get.call_count == 2


def test_get_paginated_rejects_cumulative_results_beyond_ceiling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(client_module, "MAX_PAGINATION_RESULTS", 2)
    client = PolygonClient("test-key")
    pages = [
        _response(_page("AAPL", "MSFT", next_url="/pagination/page-two")),
        _response(_page("NVDA")),
    ]
    try:
        with patch.object(client.client, "get", side_effect=pages) as get:
            with pytest.raises(ProviderError, match="maximum of 2 results"):
                client.get_paginated("news")
    finally:
        client.close()

    assert get.call_count == 2


@pytest.mark.parametrize("fetcher", SYMBOL_FETCHERS)
def test_symbol_fetcher_rejects_two_page_url_cycle_before_third_request(
    fetcher: SymbolFetcher,
) -> None:
    pages = [
        _response(_page("AAPL", next_url="?cursor=page-two")),
        _response(_page("MSFT", next_url="/v3/reference/tickers")),
    ]

    with patch("sawa.utils.symbols.requests.get", side_effect=pages) as get, patch(
        "sawa.utils.symbols.time.sleep"
    ):
        with pytest.raises(ProviderError, match="Repeated Polygon pagination URL"):
            fetcher(logging.getLogger(__name__), types=("CS",), api_key="test-key")

    assert get.call_count == 2


@pytest.mark.parametrize("fetcher", SYMBOL_FETCHERS)
def test_symbol_fetcher_allows_exact_page_ceiling(
    fetcher: SymbolFetcher,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(symbols_module, "MAX_PAGINATION_PAGES", 2)
    pages = [
        _response(_page("AAPL", next_url="?cursor=page-two")),
        _response(_page("MSFT")),
    ]

    with patch("sawa.utils.symbols.requests.get", side_effect=pages) as get, patch(
        "sawa.utils.symbols.time.sleep"
    ):
        results = fetcher(
            logging.getLogger(__name__),
            types=("CS",),
            api_key="test-key",
        )

    assert results == ["AAPL", "MSFT"]
    assert get.call_count == 2


@pytest.mark.parametrize("fetcher", SYMBOL_FETCHERS)
def test_symbol_fetcher_rejects_page_beyond_ceiling_before_request(
    fetcher: SymbolFetcher,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(symbols_module, "MAX_PAGINATION_PAGES", 2)
    pages = [
        _response(_page("AAPL", next_url="?cursor=page-two")),
        _response(_page("MSFT", next_url="?cursor=page-three")),
    ]

    with patch("sawa.utils.symbols.requests.get", side_effect=pages) as get, patch(
        "sawa.utils.symbols.time.sleep"
    ):
        with pytest.raises(ProviderError, match="maximum of 2 pages"):
            fetcher(logging.getLogger(__name__), types=("CS",), api_key="test-key")

    assert get.call_count == 2


@pytest.mark.parametrize("fetcher", SYMBOL_FETCHERS)
def test_symbol_fetcher_allows_exact_result_ceiling(
    fetcher: SymbolFetcher,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(symbols_module, "MAX_PAGINATION_RESULTS", 2)
    pages = [
        _response(_page("AAPL", next_url="?cursor=page-two")),
        _response(_page("MSFT")),
    ]

    with patch("sawa.utils.symbols.requests.get", side_effect=pages) as get, patch(
        "sawa.utils.symbols.time.sleep"
    ):
        results = fetcher(
            logging.getLogger(__name__),
            types=("CS",),
            api_key="test-key",
        )

    assert results == ["AAPL", "MSFT"]
    assert get.call_count == 2


@pytest.mark.parametrize("fetcher", SYMBOL_FETCHERS)
def test_symbol_fetcher_rejects_cumulative_results_beyond_ceiling(
    fetcher: SymbolFetcher,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(symbols_module, "MAX_PAGINATION_RESULTS", 2)
    pages = [
        _response(_page("AAPL", "MSFT", next_url="?cursor=page-two")),
        _response(_page("NVDA")),
    ]

    with patch("sawa.utils.symbols.requests.get", side_effect=pages) as get, patch(
        "sawa.utils.symbols.time.sleep"
    ):
        with pytest.raises(ProviderError, match="maximum of 2 results"):
            fetcher(logging.getLogger(__name__), types=("CS",), api_key="test-key")

    assert get.call_count == 2
