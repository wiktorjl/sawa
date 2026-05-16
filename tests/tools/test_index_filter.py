"""Tests for the shared MCP index-filter helper."""

import pytest

from mcp_server.tools._index_filter import build_index_filter


def _render(fragment) -> str:
    """psycopg.sql.Composable doesn't have a stable __str__ for assertion."""
    return fragment.as_string(None)


@pytest.mark.parametrize("sentinel", ["all", "both", "", None])
def test_no_filter_for_sentinels(sentinel):
    params: dict = {}
    fragment = build_index_filter(sentinel, "c", params)
    assert _render(fragment) == ""
    assert params == {}


@pytest.mark.parametrize(
    "code",
    ["sp500", "nasdaq_listed", "us_active", "nasdaq100", "dow30", "mag7"],
)
def test_known_index_codes_build_filter(code):
    params: dict = {}
    fragment = build_index_filter(code, "c", params)
    rendered = _render(fragment)
    assert "AND c.ticker IN" in rendered
    assert "FROM index_constituents ic" in rendered
    assert "WHERE i.code = %(index_code)s" in rendered
    assert params == {"index_code": code}


def test_table_alias_is_used():
    params: dict = {}
    fragment = build_index_filter("sp500", "d", params)
    rendered = _render(fragment)
    assert "AND d.ticker IN" in rendered
    assert "AND c.ticker IN" not in rendered


def test_custom_param_name_preserved():
    params: dict = {"existing": 1}
    fragment = build_index_filter("sp500", "x", params, param_name="x_index")
    rendered = _render(fragment)
    assert "%(x_index)s" in rendered
    assert params == {"existing": 1, "x_index": "sp500"}


def test_arbitrary_index_codes_pass_through():
    """The helper doesn't enforce an enum — any code from `indices` works.

    This is the design property the refactor relies on: adding new
    indices in PR 2 (and later) shouldn't require touching every
    consumer.
    """
    params: dict = {}
    fragment = build_index_filter("some_future_code", "c", params)
    assert _render(fragment).count("%(index_code)s") == 1
    assert params == {"index_code": "some_future_code"}
