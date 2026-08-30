"""Live MCP price formatting is UTC-stable and credential-safe."""

import os
import time
from datetime import datetime, timezone
from unittest import mock

import pytest

from mcp_server.tools.market_data import (
    get_live_price_async,
    get_live_prices_batch_async,
)


def _live_result(ticker: str, timestamp_ms: int) -> dict[str, object]:
    return {
        "ticker": ticker,
        "current_price": 101.0,
        "current_date": "2026-01-02",
        "change_percent": 1.0,
        "history": [
            {
                "t": timestamp_ms,
                "o": 100.0,
                "h": 102.0,
                "l": 99.0,
                "c": 101.0,
                "v": 100,
            }
        ],
    }


@pytest.mark.skipif(not hasattr(time, "tzset"), reason="process TZ switching unavailable")
@pytest.mark.asyncio
async def test_single_and_batch_history_dates_are_utc_under_pacific_timezone() -> None:
    timestamp_ms = int(
        datetime(2026, 1, 2, 0, 30, tzinfo=timezone.utc).timestamp() * 1000
    )
    prior_tz = os.environ.get("TZ")
    os.environ["TZ"] = "America/Los_Angeles"
    time.tzset()
    try:
        with mock.patch(
            "sawa.get_live_price",
            new=mock.AsyncMock(return_value=_live_result("AAPL", timestamp_ms)),
        ), mock.patch(
            "sawa.get_live_prices_batch",
            new=mock.AsyncMock(
                return_value={"AAPL": _live_result("AAPL", timestamp_ms)}
            ),
        ):
            single = await get_live_price_async("AAPL")
            batch = await get_live_prices_batch_async(["AAPL"])
    finally:
        if prior_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = prior_tz
        time.tzset()

    assert single["history"][0]["date"] == "2026-01-02"
    assert batch["AAPL"]["history"][0]["date"] == "2026-01-02"
    assert str(single["fetched_at"]).endswith("+00:00")
    assert str(batch["AAPL"]["fetched_at"]).endswith("+00:00")


@pytest.mark.asyncio
async def test_batch_live_error_is_redacted_before_success_payload() -> None:
    secret = "batch-json-secret"
    with mock.patch(
        "sawa.get_live_prices_batch",
        new=mock.AsyncMock(
            return_value={
                "AAPL": {
                    "error": str({"apiKey": secret, "status": "failed"}),
                    "error_type": "ProviderError",
                }
            }
        ),
    ):
        result = await get_live_prices_batch_async(["AAPL"])

    assert secret not in result["AAPL"]["error"]
    assert "<redacted>" in result["AAPL"]["error"]
