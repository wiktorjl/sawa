"""Tests for the yfinance → GICS sector mapping in the backfill script.

The yfinance sector vocabulary is the most likely thing to drift —
Yahoo occasionally renames buckets ("Financial" became "Financial
Services" some years back). Pinning the mapping with tests catches the
problem at CI time rather than mid-backfill.
"""

import importlib.util
from pathlib import Path

import pytest


def _load_module():
    """Load scripts/backfill_gics_overrides.py without running main()."""
    path = Path(__file__).parents[1] / "scripts" / "backfill_gics_overrides.py"
    spec = importlib.util.spec_from_file_location("backfill_gics_overrides", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def bf():
    return _load_module()


@pytest.mark.parametrize(
    "yahoo,expected",
    [
        ("Technology",           "Information Technology"),
        ("technology",           "Information Technology"),
        ("  Technology  ",       "Information Technology"),
        ("Healthcare",           "Health Care"),
        ("Financial",            "Financials"),
        ("Financial Services",   "Financials"),
        ("Consumer Cyclical",    "Consumer Discretionary"),
        ("Consumer Defensive",   "Consumer Staples"),
        ("Communication Services","Communication Services"),
        ("Basic Materials",      "Materials"),
        ("Energy",               "Energy"),
        ("Industrials",          "Industrials"),
        ("Real Estate",          "Real Estate"),
        ("Utilities",            "Utilities"),
    ],
)
def test_yfinance_to_gics_known_sectors(bf, yahoo, expected):
    assert bf._map_to_gics(yahoo) == expected


@pytest.mark.parametrize("falsy", [None, "", "  "])
def test_yfinance_to_gics_falsy_inputs(bf, falsy):
    assert bf._map_to_gics(falsy) is None


def test_yfinance_to_gics_unknown_returns_none(bf):
    """Unknown Yahoo sectors must surface as None so the script logs a
    warning instead of silently inserting a wrong sector."""
    assert bf._map_to_gics("Crypto Mining Hyperstuff") is None


def test_all_gics_sectors_covered(bf):
    """Every Yahoo sector in the constant maps to one of the 11 official
    GICS sectors (or, after a Yahoo rename, ambiguously aliases to the
    same target as another Yahoo string — e.g. Financial ↔ Financial
    Services both → Financials)."""
    official_gics = {
        "Energy", "Materials", "Industrials", "Consumer Discretionary",
        "Consumer Staples", "Health Care", "Financials",
        "Information Technology", "Communication Services",
        "Utilities", "Real Estate",
    }
    for yahoo, gics in bf.YFINANCE_TO_GICS_SECTOR.items():
        assert gics in official_gics, f"{yahoo!r} → {gics!r} isn't a real GICS sector"
