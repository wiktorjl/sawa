"""In-memory MCP v2 contract tests for discovery, calls, and failures."""

import inspect
import json
import threading
import time
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from mcp import Client, MCPError
from mcp.types import INVALID_PARAMS

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 compatibility
    import tomli as tomllib


def test_mcp_package_pins_compatible_sawa_release() -> None:
    pyproject = Path(__file__).parents[1] / "mcp_server" / "pyproject.toml"
    config = tomllib.loads(pyproject.read_text(encoding="utf-8"))

    assert "sawa>=0.3.0,<0.4" in config["project"]["dependencies"]


def test_raw_query_dispatch_is_physically_absent(mcp_server_module) -> None:
    source = inspect.getsource(mcp_server_module.call_tool)
    assert 'name == "execute_query"' not in source


@pytest.fixture
def mcp_server_module(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("MCP_LOG_DIR", str(tmp_path / "mcp-logs"))
    import mcp_server.server as server

    return server


def _tool_error(result) -> dict:
    """Decode an error envelope, which intentionally has no structured output."""
    assert result.is_error is True
    payload = json.loads(result.content[0].text)
    assert result.structured_content is None
    assert set(payload) == {"error", "metadata"}
    assert set(payload["error"]) == {"type", "message"}
    assert {"tool", "schema_version"} <= set(payload["metadata"])
    return payload["error"]


@pytest.mark.asyncio
async def test_v2_discovery_has_typed_outputs_and_read_only_annotations(
    mcp_server_module,
) -> None:
    async with Client(mcp_server_module.app) as client:
        result = await client.list_tools()
        assert client.protocol_version == "2026-07-28"

    assert result.tools
    assert "execute_query" not in {tool.name for tool in result.tools}
    open_world = {
        tool.name
        for tool in result.tools
        if tool.annotations is not None and tool.annotations.open_world_hint
    }
    assert open_world == {
        "get_live_price",
        "get_live_prices_batch",
    }
    for tool in result.tools:
        Draft202012Validator.check_schema(tool.input_schema)
        Draft202012Validator.check_schema(tool.output_schema)
        assert tool.input_schema["additionalProperties"] is False
        assert tool.output_schema == mcp_server_module._TOOL_OUTPUT_SCHEMA
        assert tool.annotations is not None
        assert tool.annotations.read_only_hint is True
        assert tool.annotations.destructive_hint is False
        for field in ("date", "start_date", "end_date", "target_date", "since_date"):
            if field in tool.input_schema.get("properties", {}):
                field_schema = tool.input_schema["properties"][field]
                assert field_schema["minLength"] == 10
                assert field_schema["maxLength"] == 10
        for field in ("ticker", "benchmark"):
            if field in tool.input_schema.get("properties", {}):
                ticker_schema = tool.input_schema["properties"][field]
                assert ticker_schema["minLength"] == 1
                assert ticker_schema["maxLength"] == 10

        def assert_bounded_strings(value) -> None:
            if isinstance(value, dict):
                if value.get("type") == "string":
                    assert value["maxLength"] <= 256
                for item in value.values():
                    assert_bounded_strings(item)
            elif isinstance(value, list):
                for item in value:
                    assert_bounded_strings(item)

        assert_bounded_strings(tool.input_schema)


@pytest.mark.asyncio
async def test_v2_specialized_schemas_match_tool_domain_bounds(
    mcp_server_module,
) -> None:
    tools = {tool.name: tool for tool in await mcp_server_module.list_tools()}

    assert tools["detect_volume_anomalies"].input_schema["properties"][
        "lookback_days"
    ]["minimum"] == 20
    assert tools["detect_volume_anomalies"].input_schema["properties"][
        "threshold_multiplier"
    ]["minimum"] == 1.1
    assert tools["get_advanced_volume_indicators"].input_schema["properties"][
        "lookback_days"
    ]["minimum"] == 5

    monthly_validator = Draft202012Validator(
        tools["get_weekly_monthly_candles"].input_schema
    )
    assert monthly_validator.is_valid(
        {"ticker": "AAPL", "timeframe": "monthly", "periods": 120}
    )
    assert not monthly_validator.is_valid(
        {"ticker": "AAPL", "timeframe": "monthly", "periods": 121}
    )

    ytd_tickers = tools["get_ytd_returns"].input_schema["properties"]["tickers"]
    assert ytd_tickers["minItems"] == 1
    assert ytd_tickers["maxItems"] == 50
    assert ytd_tickers["uniqueItems"] is True

    scan_index = tools["scan_ytd_performance"].input_schema["properties"]["index"]
    assert scan_index["enum"] == mcp_server_module._SCANNER_INDEX_CODES

    generic_index = tools["list_companies"].input_schema["properties"]["index"]
    validator = Draft202012Validator(generic_index)
    assert validator.is_valid("sp500")
    assert not validator.is_valid("all")
    assert not validator.is_valid("both")
    assert not validator.is_valid("nasdaq5000")
    assert tools["list_companies"].input_schema["properties"]["offset"][
        "maximum"
    ] == 10_000

    scan_properties = tools["scan_ytd_performance"].input_schema["properties"]
    assert scan_properties["top_n"]["minimum"] == 1
    assert scan_properties["bottom_n"]["maximum"] == 50


@pytest.mark.asyncio
async def test_v2_success_returns_content_and_structured_content(
    mcp_server_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outcomes = []
    monkeypatch.setattr(
        mcp_server_module,
        "list_companies",
        lambda **_kwargs: [{"ticker": "AAPL", "name": "Apple Inc."}],
    )
    monkeypatch.setattr(
        mcp_server_module,
        "record_call_outcome",
        lambda tool, **kwargs: outcomes.append((tool, kwargs["success"])),
    )

    async with Client(mcp_server_module.app) as client:
        result = await client.call_tool("list_companies", {"limit": 1})

    assert result.is_error is False
    assert result.structured_content is not None
    assert result.structured_content["data"] == [
        {"ticker": "AAPL", "name": "Apple Inc."}
    ]
    Draft202012Validator(mcp_server_module._TOOL_OUTPUT_SCHEMA).validate(
        result.structured_content
    )
    assert json.loads(result.content[0].text) == result.structured_content
    assert outcomes == [("list_companies", True)]


@pytest.mark.asyncio
async def test_non_chart_tool_ignores_invalid_chart_theme(
    mcp_server_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MCP_CHART_THEME", "definitely-invalid")
    monkeypatch.setattr(
        mcp_server_module,
        "list_companies",
        lambda **_kwargs: [{"ticker": "AAPL"}],
    )

    async with Client(mcp_server_module.app) as client:
        result = await client.call_tool("list_companies", {"limit": 1})

    assert result.is_error is False
    assert result.structured_content["data"] == [{"ticker": "AAPL"}]


@pytest.mark.asyncio
async def test_chart_runtime_honors_no_color(
    mcp_server_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setenv("NO_COLOR", "1")
    monkeypatch.delenv("MCP_CHART_THEME", raising=False)
    monkeypatch.setattr(
        mcp_server_module,
        "get_stock_prices",
        lambda **_kwargs: [{"date": "2026-08-28", "close": 100}],
    )

    def render(result, ticker, layout, theme):
        captured["colors_enabled"] = theme.colors_enabled
        return "plain chart"

    monkeypatch.setattr(mcp_server_module, "render_price_chart", render)

    async with Client(mcp_server_module.app) as client:
        result = await client.call_tool(
            "get_stock_prices",
            {"ticker": "AAPL", "start_date": "2026-08-28"},
        )

    assert result.is_error is False
    assert result.structured_content["chart"] == "plain chart"
    assert captured == {"colors_enabled": False}


@pytest.mark.asyncio
async def test_live_batch_partial_failure_is_success_with_explicit_warning(
    mcp_server_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def partial_batch(**_kwargs):
        return {
            "AAPL": {"ticker": "AAPL", "latest_price": 100.0},
            "MSFT": {
                "ticker": "MSFT",
                "error": "provider unavailable",
                "error_type": "provider_error",
            },
        }

    monkeypatch.setattr(
        mcp_server_module,
        "get_live_prices_batch_async",
        partial_batch,
    )

    async with Client(mcp_server_module.app) as client:
        result = await client.call_tool(
            "get_live_prices_batch", {"tickers": ["AAPL", "MSFT"], "days": 7}
        )

    assert result.is_error is False
    assert result.structured_content["data"]["MSFT"]["error"] == "provider unavailable"
    assert result.structured_content["warnings"] == [
        "Live-price provider failed for 1/2 tickers: MSFT"
    ]


@pytest.mark.asyncio
async def test_live_batch_total_provider_outage_is_tool_error(
    mcp_server_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outcomes = []

    async def failed_batch(**_kwargs):
        return {
            ticker: {
                "ticker": ticker,
                "error": "provider unavailable",
                "error_type": "provider_error",
            }
            for ticker in ("AAPL", "MSFT")
        }

    monkeypatch.setattr(
        mcp_server_module,
        "get_live_prices_batch_async",
        failed_batch,
    )
    monkeypatch.setattr(
        mcp_server_module,
        "record_call_outcome",
        lambda tool, **kwargs: outcomes.append((tool, kwargs["success"])),
    )

    async with Client(mcp_server_module.app) as client:
        result = await client.call_tool(
            "get_live_prices_batch", {"tickers": ["AAPL", "MSFT"], "days": 7}
        )

    assert result.is_error is True
    assert "every requested ticker" in _tool_error(result)["message"]
    assert outcomes == [("get_live_prices_batch", False)]


@pytest.mark.asyncio
async def test_live_batch_valid_no_data_is_success_not_provider_failure(
    mcp_server_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def empty_batch(**_kwargs):
        return {
            "AAPL": {
                "ticker": "AAPL",
                "error": "No data found for AAPL",
                "error_type": "no_data",
            }
        }

    monkeypatch.setattr(
        mcp_server_module,
        "get_live_prices_batch_async",
        empty_batch,
    )

    async with Client(mcp_server_module.app) as client:
        result = await client.call_tool(
            "get_live_prices_batch", {"tickers": ["AAPL"], "days": 7}
        )

    assert result.is_error is False
    assert result.structured_content["warnings"] == [
        "No live-price data for 1/1 tickers: AAPL"
    ]


@pytest.mark.asyncio
async def test_advertised_batch_ticker_arrays_are_bounded(
    mcp_server_module,
) -> None:
    tools = {tool.name: tool for tool in await mcp_server_module.list_tools()}

    live_tickers = tools["get_live_prices_batch"].input_schema["properties"]["tickers"]
    intraday_tickers = tools["get_intraday_bars"].input_schema["properties"]["tickers"]
    assert (live_tickers["minItems"], live_tickers["maxItems"]) == (1, 50)
    assert (intraday_tickers["minItems"], intraday_tickers["maxItems"]) == (1, 20)
    assert live_tickers["items"]["maxLength"] == 10


@pytest.mark.asyncio
async def test_screen_filter_schema_matches_domain_and_rejects_unknown(
    mcp_server_module,
) -> None:
    tools = {tool.name: tool for tool in await mcp_server_module.list_tools()}
    filters_schema = tools["screen_stocks"].input_schema["properties"]["filters"]

    assert filters_schema["additionalProperties"] is False
    assert set(filters_schema["properties"]) == set(mcp_server_module.FILTER_SPECS)


@pytest.mark.asyncio
async def test_v2_schema_validation_returns_tool_error_before_execution(
    mcp_server_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False
    outcomes = []

    def should_not_run(**_kwargs):
        nonlocal called
        called = True
        raise AssertionError("tool implementation should not run")

    monkeypatch.setattr(mcp_server_module, "list_companies", should_not_run)
    monkeypatch.setattr(
        mcp_server_module,
        "record_call_outcome",
        lambda tool, **kwargs: outcomes.append((tool, kwargs["success"])),
    )

    async with Client(mcp_server_module.app) as client:
        result = await client.call_tool("list_companies", {"limt": 1})

    assert result.is_error is True
    assert called is False
    error = _tool_error(result)
    assert error["type"] == "ValueError"
    assert "additionalProperties" in error["message"]
    assert outcomes == [("list_companies", False)]


@pytest.mark.asyncio
async def test_corporate_action_schema_bounds_match_domain_validation(
    mcp_server_module,
) -> None:
    tools = {tool.name: tool for tool in await mcp_server_module.list_tools()}
    limit_maxima = {
        "get_stock_splits": 500,
        "get_dividends": 500,
        "get_ex_dividend_calendar": 500,
        "get_dividend_yield_leaders": 200,
        "get_earnings_calendar": 500,
        "get_earnings_history": 40,
    }

    for name, maximum in limit_maxima.items():
        schema = tools[name].input_schema
        assert schema["properties"]["limit"]["maximum"] == maximum
        with pytest.raises(ValueError, match="Limit too large"):
            mcp_server_module.validate_tool_arguments(name, {"limit": maximum + 1})

    recent_splits_schema = tools["get_recent_splits"].input_schema
    assert recent_splits_schema["properties"]["days"]["maximum"] == 30
    with pytest.raises(ValueError, match="days too large"):
        mcp_server_module.validate_tool_arguments("get_recent_splits", {"days": 31})


@pytest.mark.asyncio
async def test_v2_failure_is_error_and_redacts_secrets(
    mcp_server_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "protocol-secret"
    outcomes = []

    def fail(**_kwargs):
        raise RuntimeError(str({"apiKey": secret, "status": "failed"}))

    monkeypatch.setattr(mcp_server_module, "list_companies", fail)
    monkeypatch.setattr(
        mcp_server_module,
        "record_call_outcome",
        lambda tool, **kwargs: outcomes.append((tool, kwargs["success"])),
    )

    async with Client(mcp_server_module.app) as client:
        result = await client.call_tool("list_companies", {"limit": 1})

    rendered = result.content[0].text
    assert result.is_error is True
    assert _tool_error(result)["type"] == "RuntimeError"
    assert secret not in rendered
    assert "apiKey': <redacted>" in rendered
    assert outcomes == [("list_companies", False)]


@pytest.mark.asyncio
async def test_v2_large_utf8_provider_error_is_redacted_and_response_capped(
    mcp_server_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "provider-secret"
    monitored_errors: list[BaseException | None] = []

    def fail(**_kwargs):
        raise RuntimeError(f"api_key={secret} " + "界" * 20_000)

    monkeypatch.setattr(mcp_server_module, "list_companies", fail)
    monkeypatch.setattr(
        mcp_server_module.database_runtime,
        "MAX_RESULT_BYTES",
        512,
    )
    monkeypatch.setattr(
        mcp_server_module,
        "record_call_outcome",
        lambda _tool, **kwargs: monitored_errors.append(kwargs.get("error")),
    )

    async with Client(mcp_server_module.app) as client:
        result = await client.call_tool("list_companies", {"limit": 1})

    rendered = result.content[0].text
    assert result.is_error is True
    assert len(rendered.encode("utf-8")) <= 512
    _tool_error(result)
    assert secret not in rendered
    assert monitored_errors and monitored_errors[0] is not None
    assert len(str(monitored_errors[0]).encode("utf-8")) <= 2048
    assert secret not in str(monitored_errors[0])


@pytest.mark.asyncio
async def test_v2_oversized_date_rejected_by_schema_before_execution(
    mcp_server_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    def should_not_run(**_kwargs):
        nonlocal called
        called = True
        return []

    monkeypatch.setattr(mcp_server_module, "get_stock_prices", should_not_run)
    async with Client(mcp_server_module.app) as client:
        result = await client.call_tool(
            "get_stock_prices",
            {"ticker": "AAPL", "start_date": "界" * 100_000},
        )

    assert result.is_error is True
    assert called is False
    assert len(result.content[0].text.encode("utf-8")) <= (
        mcp_server_module.database_runtime.MAX_RESULT_BYTES
    )
    assert "maxLength constraint failed" in _tool_error(result)["message"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool_name", "field_name", "implementation_name"),
    [
        ("search_companies", "query", "search_companies"),
        ("calculate_relative_strength", "benchmark", "calculate_relative_strength"),
        ("get_52week_extremes", "since_date", "get_52week_extremes"),
    ],
)
async def test_v2_oversized_strings_rejected_before_execution(
    mcp_server_module,
    monkeypatch: pytest.MonkeyPatch,
    tool_name: str,
    field_name: str,
    implementation_name: str,
) -> None:
    called = False

    def should_not_run(*_args, **_kwargs):
        nonlocal called
        called = True
        return []

    monkeypatch.setattr(mcp_server_module, implementation_name, should_not_run)
    arguments = {field_name: "界" * 100_000}
    if tool_name == "calculate_relative_strength":
        arguments["ticker"] = "AAPL"

    async with Client(mcp_server_module.app) as client:
        result = await client.call_tool(tool_name, arguments)

    assert result.is_error is True
    assert called is False
    assert "maxLength constraint failed" in _tool_error(result)["message"]


def test_direct_argument_validation_bounds_semantic_and_free_text_fields(
    mcp_server_module,
) -> None:
    arguments = mcp_server_module.validate_tool_arguments(
        "calculate_relative_strength", {"ticker": "aapl", "benchmark": "spy"}
    )
    assert arguments == {"ticker": "AAPL", "benchmark": "SPY"}

    with pytest.raises(ValueError, match="Invalid since_date"):
        mcp_server_module.validate_tool_arguments(
            "get_52week_extremes", {"since_date": "2026-02-30"}
        )
    with pytest.raises(ValueError, match="query is too long"):
        mcp_server_module.validate_tool_arguments(
            "search_companies", {"query": "x" * 257}
        )


@pytest.mark.asyncio
async def test_v2_cross_field_pattern_window_rejected_before_execution(
    mcp_server_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    def should_not_run(**_kwargs):
        nonlocal called
        called = True
        return {}

    monkeypatch.setattr(mcp_server_module, "detect_chart_patterns", should_not_run)
    async with Client(mcp_server_module.app) as client:
        result = await client.call_tool(
            "detect_chart_patterns",
            {"ticker": "AAPL", "lookback_days": 20, "min_pattern_days": 21},
        )

    assert result.is_error is True
    assert called is False
    assert "between 5 and lookback_days" in _tool_error(result)["message"]


@pytest.mark.asyncio
async def test_scan_ytd_dispatch_uses_independent_tail_sizes(
    mcp_server_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    received: dict[str, object] = {}

    async def scan(**kwargs):
        received.update(kwargs)
        return {"successful": 0, "errors": []}

    monkeypatch.setattr(mcp_server_module, "scan_ytd_performance_async", scan)
    async with Client(mcp_server_module.app) as client:
        result = await client.call_tool(
            "scan_ytd_performance",
            {"index": "sp500", "top_n": 3, "bottom_n": 7},
        )

    assert result.is_error is False
    assert received["top_n"] == 3
    assert received["bottom_n"] == 7


@pytest.mark.asyncio
async def test_v2_non_finite_numbers_normalize_before_both_representations(
    mcp_server_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        mcp_server_module,
        "list_companies",
        lambda **_kwargs: [
            {"nan": float("nan"), "positive": float("inf"), "negative": -float("inf")}
        ],
    )

    async with Client(mcp_server_module.app) as client:
        result = await client.call_tool("list_companies", {"limit": 1})

    assert result.is_error is False
    assert result.structured_content["data"] == [
        {"nan": None, "positive": None, "negative": None}
    ]
    assert json.loads(result.content[0].text) == result.structured_content
    assert "NaN" not in result.content[0].text
    assert "Infinity" not in result.content[0].text


@pytest.mark.asyncio
async def test_v2_exact_serialized_response_cap_becomes_tool_error(
    mcp_server_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        mcp_server_module,
        "list_companies",
        lambda **_kwargs: [{"ticker": "A" * 500}],
    )
    monkeypatch.setattr(
        mcp_server_module.database_runtime,
        "MAX_RESULT_BYTES",
        200,
    )

    async with Client(mcp_server_module.app) as client:
        result = await client.call_tool("list_companies", {"limit": 1})

    assert result.is_error is True
    error = _tool_error(result)
    assert error["type"] == "ValueError"
    assert "maximum serialized size of 200 bytes" in (
        error["message"]
    )


@pytest.mark.asyncio
async def test_v2_serialization_failure_records_one_failure_not_success(
    mcp_server_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outcomes = []

    class Unserializable:
        def __str__(self) -> str:
            raise RuntimeError("serialization sentinel")

    monkeypatch.setattr(
        mcp_server_module,
        "list_companies",
        lambda **_kwargs: [Unserializable()],
    )
    monkeypatch.setattr(
        mcp_server_module,
        "record_call_outcome",
        lambda tool, **kwargs: outcomes.append((tool, kwargs["success"])),
    )

    async with Client(mcp_server_module.app) as client:
        result = await client.call_tool("list_companies", {"limit": 1})

    assert result.is_error is True
    assert _tool_error(result)["type"] == "RuntimeError"
    assert outcomes == [("list_companies", False)]


@pytest.mark.asyncio
async def test_corrupt_monitoring_state_cannot_change_successful_protocol_result(
    mcp_server_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import mcp_server.monitoring as monitoring

    with monitoring.open_private_text(monitoring._state_file(), "w") as state_file:
        state_file.write('{"list_companies": NaN}')
    monkeypatch.setattr(
        mcp_server_module,
        "list_companies",
        lambda **_kwargs: [{"ticker": "AAPL"}],
    )

    async with Client(mcp_server_module.app) as client:
        result = await client.call_tool("list_companies", {"limit": 1})

    assert result.is_error is False
    assert result.structured_content["data"] == [{"ticker": "AAPL"}]


@pytest.mark.asyncio
async def test_monitoring_exception_cannot_change_successful_protocol_result(
    mcp_server_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        mcp_server_module,
        "list_companies",
        lambda **_kwargs: [{"ticker": "AAPL"}],
    )

    def monitoring_failure(*_args, **_kwargs) -> None:
        raise RuntimeError("monitoring sentinel")

    monkeypatch.setattr(mcp_server_module, "record_call_outcome", monitoring_failure)

    async with Client(mcp_server_module.app) as client:
        result = await client.call_tool("list_companies", {"limit": 1})

    assert result.is_error is False
    assert result.structured_content["data"] == [{"ticker": "AAPL"}]


@pytest.mark.asyncio
async def test_v2_raw_query_call_is_an_invalid_params_protocol_error(
    mcp_server_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outcomes = []
    monkeypatch.setattr(
        mcp_server_module,
        "record_call_outcome",
        lambda tool, **kwargs: outcomes.append((tool, kwargs["success"])),
    )
    async with Client(mcp_server_module.app) as client:
        with pytest.raises(MCPError) as caught:
            await client.call_tool("execute_query", {"sql": "SELECT 1"})

    assert caught.value.code == INVALID_PARAMS
    assert caught.value.message == "Unknown tool"
    assert outcomes == []


@pytest.mark.asyncio
async def test_failure_alert_delivery_does_not_delay_protocol_error(
    mcp_server_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import mcp_server.monitoring as monitoring

    started = threading.Event()
    release = threading.Event()

    class SlowNotifier:
        def send(self, **_kwargs: object) -> bool:
            started.set()
            release.wait(timeout=2)
            return True

    monkeypatch.setattr(monitoring, "_FAILURE_ALERT_THRESHOLD", 1)
    monkeypatch.setattr(monitoring, "get_notifier", lambda _logger=None: SlowNotifier())
    monkeypatch.setattr(
        mcp_server_module,
        "list_companies",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("provider failed")),
    )

    began = time.monotonic()
    async with Client(mcp_server_module.app) as client:
        result = await client.call_tool("list_companies", {"limit": 1})
    elapsed = time.monotonic() - began

    try:
        assert result.is_error is True
        assert started.wait(timeout=1)
        assert elapsed < 0.5
    finally:
        release.set()


@pytest.mark.asyncio
async def test_monitoring_retains_original_exception_type(
    mcp_server_module,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outcomes: list[tuple[BaseException | None, str | None]] = []
    monkeypatch.setattr(
        mcp_server_module,
        "record_call_outcome",
        lambda _tool, **kwargs: outcomes.append(
            (kwargs.get("error"), kwargs.get("error_type"))
        ),
    )

    monkeypatch.setattr(
        mcp_server_module,
        "list_companies",
        lambda **_kwargs: (_ for _ in ()).throw(ValueError("provider failed")),
    )
    async with Client(mcp_server_module.app) as client:
        result = await client.call_tool("list_companies", {"limit": 1})

    assert result.is_error is True
    assert len(outcomes) == 1
    assert isinstance(outcomes[0][0], RuntimeError)
    assert outcomes[0][1] == "ValueError"
