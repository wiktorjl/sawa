"""Offline tests for the sawa.utils.symbols index dispatcher and fetchers."""

import logging

import pytest

from sawa.utils.symbols import fetch_index_symbols, fetch_mag7_symbols


@pytest.fixture
def logger():
    return logging.getLogger("test_index_fetchers")


def test_mag7_is_eight_stable_tickers(logger):
    """The Magnificent 7 is informal — fixing it via test pins the cohort.

    Eight tickers because Alphabet trades as both GOOGL and GOOG.
    """
    assert fetch_mag7_symbols(logger) == [
        "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "NVDA", "META", "TSLA",
    ]


def test_dispatcher_routes_to_mag7(logger):
    assert fetch_index_symbols("mag7", logger) == fetch_mag7_symbols(logger)


@pytest.mark.parametrize(
    "alias",
    [
        "MAG7",
        "mag-7",
        "magnificent_7",
        "Magnificent 7",
        "magnificent-seven",
    ],
)
def test_dispatcher_accepts_mag7_aliases(alias, logger):
    assert fetch_index_symbols(alias, logger) == fetch_mag7_symbols(logger)


@pytest.mark.parametrize(
    "alias,expected_code",
    [
        ("S&P 500", "sp500"),
        ("sp_500", "sp500"),
        ("Nasdaq-100", "nasdaq100"),
        ("dow 30", "dow30"),
        ("DJIA", "dow30"),
        ("Dow Jones Industrial Average", "dow30"),
    ],
)
def test_dispatcher_normalises_aliases(alias, expected_code, logger, monkeypatch):
    """The dispatcher should reduce common spelling variants before
    looking up the fetcher.

    We monkeypatch the registry so the test stays offline: each alias
    must resolve to ``expected_code`` and call its fetcher.
    """
    calls = []

    def make_stub(code):
        def stub(lg):
            calls.append(code)
            return [code]
        return stub

    monkeypatch.setattr(
        "sawa.utils.symbols._INDEX_FETCHERS",
        {code: make_stub(code) for code in [
            "sp500", "nasdaq_listed", "us_active",
            "nasdaq100", "dow30", "russell1000", "russell2000", "mag7",
        ]},
    )

    result = fetch_index_symbols(alias, logger)
    assert result == [expected_code]
    assert calls == [expected_code]


def test_dispatcher_rejects_legacy_nasdaq5000(logger):
    """The old ``nasdaq5000`` code was renamed in PR 1; the dispatcher
    must NOT silently re-route it (hard rename, no alias)."""
    with pytest.raises(ValueError, match="Unknown index"):
        fetch_index_symbols("nasdaq5000", logger)


def test_dispatcher_rejects_unknown_code(logger):
    with pytest.raises(ValueError, match="Unknown index"):
        fetch_index_symbols("bogus_index", logger)
