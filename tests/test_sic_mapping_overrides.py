"""Tests for the DB-backed ticker GICS override lookup added in PR 3.3.

PR 3.3 replaced the hard-coded ``TICKER_GICS_OVERRIDES`` dict access in
``map_sic_to_gics`` / ``get_sic_industry`` with a DB-backed lookup that
falls back to the dict when the database is unavailable. These tests
exercise both paths.
"""

from sawa.utils import sic_mapping
from sawa.utils.sic_mapping import (
    GICSMapping,
    _get_ticker_override,
    clear_cache,
    get_sic_industry,
    map_sic_to_gics,
)


def setup_function(_func):
    """Ensure each test starts with empty caches."""
    clear_cache()


def teardown_function(_func):
    clear_cache()


def test_fallback_dict_used_when_db_unavailable(monkeypatch):
    """If load_overrides_from_db never populated the cache and the
    single-ticker DB lookup returns None, the static dict is consulted."""
    monkeypatch.setattr(sic_mapping, "_get_override_from_db", lambda t: None)
    # Module-cached DB lookup is patched but the lru_cache wrapper isn't —
    # ensure the cache is empty so the lookup actually runs.
    sic_mapping._db_overrides_cache = None

    # The dict has 6 ADRs from the legacy CASE block; ASML is one of them.
    assert map_sic_to_gics(None, ticker="ASML") == "Information Technology"
    assert get_sic_industry(None, ticker="ASML") == "Semiconductor Equipment"


def test_db_cache_takes_precedence(monkeypatch):
    """A cached DB override must win over the static fallback dict."""
    monkeypatch.setattr(sic_mapping, "_get_override_from_db", lambda t: None)
    sic_mapping._db_overrides_cache = {
        "ASML": GICSMapping(
            gics_sector="Communication Services",   # deliberately "wrong" to prove precedence
            gics_industry="Cable & Satellite",
            confidence="medium",
            notes="test fixture",
        ),
    }
    assert map_sic_to_gics(None, ticker="ASML") == "Communication Services"
    assert get_sic_industry(None, ticker="ASML") == "Cable & Satellite"


def test_db_single_lookup_used_when_not_cached(monkeypatch):
    """When the bulk cache isn't loaded, ``_get_override_from_db`` is
    consulted before the dict."""
    sic_mapping._db_overrides_cache = None
    monkeypatch.setattr(
        sic_mapping,
        "_get_override_from_db",
        lambda t: GICSMapping(
            gics_sector="Energy",
            gics_industry="Oil & Gas E&P",
            confidence="high",
            notes="test fixture",
        ) if t == "PDD" else None,
    )
    assert map_sic_to_gics(None, ticker="PDD") == "Energy"


def test_ticker_normalisation_via_lower_to_upper(monkeypatch):
    """Tickers are upper-cased on the way in (Polygon convention)."""
    monkeypatch.setattr(sic_mapping, "_get_override_from_db", lambda t: None)
    sic_mapping._db_overrides_cache = None
    assert map_sic_to_gics(None, ticker="asml") == "Information Technology"


def test_no_override_falls_through_to_sic(monkeypatch):
    """When no override exists, lookup proceeds to SIC mapping."""
    monkeypatch.setattr(sic_mapping, "_get_override_from_db", lambda t: None)
    sic_mapping._db_overrides_cache = None
    # 7372 = software, mapped to IT in the fallback dict
    result = map_sic_to_gics("7372", ticker="UNKNOWN_TICKER")
    assert result == "Information Technology"


def test_get_ticker_override_helper(monkeypatch):
    """The new ``_get_ticker_override`` helper is the unified entry point."""
    monkeypatch.setattr(sic_mapping, "_get_override_from_db", lambda t: None)
    sic_mapping._db_overrides_cache = None
    override = _get_ticker_override("CCEP")
    assert override is not None
    assert override["gics_sector"] == "Consumer Staples"
    # Unknown ticker → None
    assert _get_ticker_override("BOGUS_TICKER_XYZ") is None
