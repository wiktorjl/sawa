"""Bounds tests for company-list MCP tools."""

from __future__ import annotations

from typing import Any

import pytest

import mcp_server.tools.companies as companies


def test_list_companies_clamps_runtime_pagination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def execute(_query: object, params: dict[str, Any]) -> list[dict[str, Any]]:
        captured.update(params)
        return []

    monkeypatch.setattr(companies, "execute_query", execute)

    companies.list_companies(limit=100_000, offset=100_000)

    assert captured["limit"] == 1_000
    assert captured["offset"] == companies.MAX_COMPANY_OFFSET
