"""Tests for the bounded, database-backed MCP market scanner."""

from __future__ import annotations

from typing import Any

import pytest

import mcp_server.tools.scanner as scanner


def test_scan_uses_three_bounded_queries_and_independent_tails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []
    responses = iter(
        [
            [{"total_symbols": 4, "successful": 4}],
            [
                {
                    "ticker": "WIN",
                    "sector": "Tech",
                    "change_percent": 20,
                    "is_large_cap": True,
                    "gain_rank": 1,
                    "loss_rank": 4,
                },
                {
                    "ticker": "LOSE",
                    "sector": "Tech",
                    "change_percent": -10,
                    "is_large_cap": True,
                    "gain_rank": 4,
                    "loss_rank": 1,
                },
            ],
            [
                {
                    "is_large_cap": True,
                    "sector": "Tech",
                    "count": 4,
                    "average_change_percent": 3,
                }
            ],
        ]
    )

    def execute(query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        calls.append((query, params.copy()))
        return next(responses)

    monkeypatch.setattr(scanner, "execute_query", execute)

    result = scanner._scan_ytd_performance_local(
        index="sp500",
        start_date="2026-01-01",
        large_cap_threshold=100,
        top_n=3,
        bottom_n=7,
    )

    assert len(calls) == 3
    assert all(call[1]["index_codes"] == ["sp500"] for call in calls)
    assert all(call[1]["top_n"] == 3 for call in calls)
    assert all(call[1]["bottom_n"] == 7 for call in calls)
    assert "ROW_NUMBER()" in calls[1][0]
    assert "gain_rank <= %(top_n)s" in calls[1][0]
    assert result["large_cap"]["top_gainers"][0]["ticker"] == "WIN"
    assert result["large_cap"]["top_gainers"][0]["is_large_cap"] is True
    assert result["large_cap"]["top_losers"][0]["ticker"] == "LOSE"
    assert [row["ticker"] for row in result["large_cap"]["by_sector"]["Tech"]] == [
        "WIN",
        "LOSE",
    ]
    assert result["large_cap"]["sector_summary"]["Tech"]["count"] == 4
    assert result["large_cap"]["by_sector_truncated"] is True
    assert result["source"] == "database"
    assert result["data_schema_version"] == "scan_ytd_performance.v2"


def test_scan_both_uses_union_of_stored_universes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []
    responses = iter(
        [
            [{"total_symbols": 1, "successful": 1}],
            [],
            [],
        ]
    )

    def execute(_query: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        calls.append(params.copy())
        return next(responses)

    monkeypatch.setattr(scanner, "execute_query", execute)

    scanner._scan_ytd_performance_local(
        index="both",
        start_date="2026-01-01",
        large_cap_threshold=100,
        top_n=1,
        bottom_n=1,
    )

    assert calls[0]["index_codes"] == ["sp500", "nasdaq_listed"]


def test_scan_without_stored_constituents_stops_after_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def execute(_query: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        nonlocal calls
        calls += 1
        return [{"total_symbols": 0, "successful": 0}]

    monkeypatch.setattr(scanner, "execute_query", execute)

    result = scanner._scan_ytd_performance_local(
        index="sp500",
        start_date="2026-01-01",
        large_cap_threshold=100,
        top_n=10,
        bottom_n=10,
    )

    assert calls == 1
    assert "No stored constituents" in result["error"]
