"""Credential-safe provider pagination for live symbol universes."""

import logging
from unittest.mock import MagicMock, patch

import pytest

from sawa.utils.symbols import (
    fetch_nasdaq_active_from_polygon,
    fetch_us_active_from_polygon,
)


@pytest.mark.parametrize(
    "fetcher",
    [fetch_nasdaq_active_from_polygon, fetch_us_active_from_polygon],
)
def test_symbol_fetcher_rejects_off_origin_next_url_before_second_request(
    fetcher,
) -> None:
    first_page = MagicMock()
    first_page.json.return_value = {
        "status": "OK",
        "results": [{"ticker": "AAPL", "primary_exchange": "XNAS"}],
        "next_url": "https://attacker.example/collect",
    }

    with patch("sawa.utils.symbols.requests.get", return_value=first_page) as get:
        with pytest.raises(ValueError, match="outside the configured HTTPS API origin"):
            fetcher(
                logging.getLogger(__name__),
                types=("CS",),
                api_key="symbol-secret",
            )

    assert get.call_count == 1
    assert get.call_args.kwargs["allow_redirects"] is False


@pytest.mark.parametrize(
    "fetcher",
    [fetch_nasdaq_active_from_polygon, fetch_us_active_from_polygon],
)
def test_symbol_fetcher_keeps_relative_page_on_origin_and_sends_key_as_params(
    fetcher,
) -> None:
    first_page = MagicMock()
    first_page.json.return_value = {
        "status": "OK",
        "results": [{"ticker": "AAPL", "primary_exchange": "XNAS"}],
        "next_url": "/v3/reference/tickers?cursor=next",
    }
    second_page = MagicMock()
    second_page.json.return_value = {
        "status": "OK",
        "results": [{"ticker": "MSFT", "primary_exchange": "XNAS"}],
    }

    with patch(
        "sawa.utils.symbols.requests.get",
        side_effect=[first_page, second_page],
    ) as get, patch("sawa.utils.symbols.time.sleep"):
        result = fetcher(
            logging.getLogger(__name__),
            types=("CS",),
            api_key="symbol-secret",
        )

    assert result == ["AAPL", "MSFT"]
    assert get.call_args_list[1].args[0] == (
        "https://api.polygon.io/v3/reference/tickers?cursor=next"
    )
    assert get.call_args_list[1].kwargs["params"] == {"apiKey": "symbol-secret"}
    assert get.call_args_list[1].kwargs["allow_redirects"] is False
