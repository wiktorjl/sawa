"""Symbol file loading and validation utilities."""

import logging
import os
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from sawa.utils.resources import packaged_resource_path, project_root

NASDAQ_POLYGON_TYPES = ("CS", "ETF", "ADRC")

TICKER_PATTERN = re.compile(r"^[A-Z]{1,7}(\.[A-Z])?$")


def validate_ticker(ticker: str) -> str:
    """
    Validate and normalize ticker symbol.

    Args:
        ticker: Raw ticker symbol

    Returns:
        Normalized uppercase ticker

    Raises:
        ValueError: If ticker format is invalid
    """
    ticker = ticker.upper().strip()
    if not TICKER_PATTERN.match(ticker):
        raise ValueError(f"Invalid ticker format: {ticker}")
    return ticker


def load_symbols(
    filepath: str | Path,
    logger: logging.Logger | None = None,
    validate: bool = True,
) -> list[str]:
    """
    Load stock symbols from a text file.

    Args:
        filepath: Path to file containing symbols (one per line)
        logger: Optional logger for progress reporting
        validate: Whether to validate ticker format

    Returns:
        List of ticker symbols

    Raises:
        FileNotFoundError: If file does not exist
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"Symbols file not found: {filepath}")

    symbols = []
    with open(filepath) as f:
        for line_num, line in enumerate(f, 1):
            symbol = line.strip()
            if not symbol or symbol.startswith("#"):
                continue
            symbol = symbol.upper()
            if validate:
                try:
                    symbol = validate_ticker(symbol)
                except ValueError as e:
                    if logger:
                        logger.warning(f"Line {line_num}: {e}")
                    continue
            symbols.append(symbol)

    if logger:
        logger.info(f"Loaded {len(symbols)} symbols from {filepath}")
    return symbols


_WIKI_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SawaDataBot/1.0)"}
_TICKER_HEADER_PATTERN = re.compile(r"^(ticker|symbol)( symbol)?$", re.IGNORECASE)


def _fetch_wikipedia_constituents(
    url: str,
    name: str,
    logger: logging.Logger,
    table_id: str = "constituents",
) -> list[str]:
    """
    Scrape a Wikipedia "List of …" article for constituent tickers.

    Locates the table by ``id`` (default ``"constituents"`` — the
    convention every major US index article uses), inspects the header
    row to find the "Ticker" or "Symbol" column, then collects each
    row's cell from that column.

    Args:
        url: Wikipedia URL to scrape.
        name: Human-readable index name (for logs and error messages).
        logger: Logger instance.
        table_id: HTML ``id`` of the constituents table.

    Returns:
        List of ticker symbols.

    Raises:
        ValueError: If the table or a ticker column can't be found.
        requests.RequestException: On network failure.
    """
    logger.info(f"Fetching {name} symbols from Wikipedia ({url})...")
    response = requests.get(url, headers=_WIKI_HEADERS, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", {"id": table_id})
    if not table:
        raise ValueError(f"Could not find {name} constituents table (id={table_id})")

    header_cells = table.find("tr").find_all(["th", "td"])
    ticker_col = next(
        (i for i, c in enumerate(header_cells)
         if _TICKER_HEADER_PATTERN.match(c.get_text(strip=True))),
        None,
    )
    if ticker_col is None:
        raise ValueError(
            f"Could not locate a ticker/symbol column in {name} table; "
            f"headers were: {[c.get_text(strip=True) for c in header_cells]}"
        )

    symbols: list[str] = []
    # Wikipedia sometimes renders the first body cell as ``<th scope="row">``
    # rather than ``<td>``. To keep header and body indices aligned we
    # collect both ``td`` and ``th`` from each row.
    for row in table.find_all("tr")[1:]:
        cells = row.find_all(["td", "th"])
        if len(cells) > ticker_col:
            ticker = cells[ticker_col].get_text(strip=True)
            if ticker:
                symbols.append(ticker)

    logger.info(f"Found {len(symbols)} {name} symbols")
    return symbols


def fetch_sp500_symbols(logger: logging.Logger) -> list[str]:
    """Fetch current S&P 500 symbols from Wikipedia."""
    return _fetch_wikipedia_constituents(
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        "S&P 500",
        logger,
    )


def fetch_nasdaq100_symbols(logger: logging.Logger) -> list[str]:
    """Fetch current NASDAQ-100 symbols from Wikipedia."""
    return _fetch_wikipedia_constituents(
        "https://en.wikipedia.org/wiki/Nasdaq-100",
        "NASDAQ-100",
        logger,
    )


def fetch_dow30_symbols(logger: logging.Logger) -> list[str]:
    """Fetch current Dow Jones Industrial Average symbols from Wikipedia."""
    return _fetch_wikipedia_constituents(
        "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average",
        "Dow 30",
        logger,
    )


def fetch_russell1000_symbols(logger: logging.Logger) -> list[str]:
    """Fetch current Russell 1000 symbols from Wikipedia.

    Wikipedia maintains a full ~1,003-row constituent table (the
    Russell 1000 is the 1,000 largest US stocks; the row count is
    slightly over 1,000 due to dual-class share entries). Russell
    only reconstitutes annually in late June, so this list is stable
    enough to live on Wikipedia.
    """
    return _fetch_wikipedia_constituents(
        "https://en.wikipedia.org/wiki/Russell_1000_Index",
        "Russell 1000",
        logger,
    )


# The Magnificent Seven is an informal cohort, not an official index, so
# constituents live in code. GOOGL and GOOG are both included because
# both Alphabet share classes trade and most index providers count them
# separately.
_MAG7 = ("AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "NVDA", "META", "TSLA")


def fetch_mag7_symbols(logger: logging.Logger) -> list[str]:
    """Return the Magnificent Seven (hard-coded)."""
    logger.info(f"Loaded {len(_MAG7)} Magnificent 7 symbols (hard-coded)")
    return list(_MAG7)


# Russell 2000 is still deferred. Both iShares (IWM holdings CSV) and
# Vanguard (VTWO holdings) gate programmatic access — iShares with a
# JS consent page, Vanguard with React-SPA fetches behind a session.
# Wikipedia has only a 12-row "notable constituents" snippet for
# Russell 2000 (vs. the full 1,003-row table for Russell 1000), so we
# can't reuse the _fetch_wikipedia_constituents helper. Realistic
# future sources: bundled snapshot CSV in data/, SEC EDGAR N-PORT
# filings from IWM, or a third-party feed.


def fetch_nasdaq_active_from_polygon(
    logger: logging.Logger,
    types: tuple[str, ...] = NASDAQ_POLYGON_TYPES,
    api_key: str | None = None,
) -> list[str]:
    """
    Fetch all currently-active NASDAQ-listed tickers from Polygon, filtered
    to common stock, ETFs, and ADRs by default.

    Polygon's ``type`` filter only accepts a single value, so this function
    paginates through the endpoint once per type and returns the merged
    deduplicated list.

    Args:
        logger: Logger instance
        types: Polygon ticker types to include. Defaults to (CS, ETF, ADRC).
            Other available types include WARRANT, UNIT, RIGHT, PFD, FUND.
        api_key: Polygon API key. Falls back to ``POLYGON_API_KEY`` env var.

    Returns:
        Sorted list of ticker symbols, deduplicated across types.

    Raises:
        ValueError: If no API key available.
        requests.RequestException: If the Polygon endpoint is unreachable.
    """
    api_key = api_key or os.environ.get("POLYGON_API_KEY")
    if not api_key:
        raise ValueError("POLYGON_API_KEY required (env var or argument)")

    base_url = "https://api.polygon.io/v3/reference/tickers"
    seen: set[str] = set()

    for ticker_type in types:
        params: dict[str, str | int] = {
            "market": "stocks",
            "exchange": "XNAS",
            "active": "true",
            "type": ticker_type,
            "limit": 1000,
            "apiKey": api_key,
        }
        url: str | None = base_url
        page = 0
        type_count = 0
        while url:
            page += 1
            resp = requests.get(
                url,
                params=params if page == 1 else None,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            for row in data.get("results", []):
                ticker = row.get("ticker")
                if ticker:
                    seen.add(ticker.upper())
                    type_count += 1
            next_url = data.get("next_url")
            if not next_url:
                break
            sep = "&" if "?" in next_url else "?"
            url = f"{next_url}{sep}apiKey={api_key}"
            time.sleep(0.05)
        logger.info(f"  Polygon XNAS type={ticker_type}: {type_count} tickers")

    symbols = sorted(seen)
    logger.info(f"Polygon XNAS total ({'+'.join(types)}): {len(symbols)} tickers")
    return symbols


def fetch_us_active_from_polygon(
    logger: logging.Logger,
    types: tuple[str, ...] = ("CS", "ETF", "ADRC"),
    api_key: str | None = None,
) -> list[str]:
    """
    Fetch all currently-active US-tradeable tickers from Polygon across
    every major exchange (no ``exchange`` filter), filtered to common
    stock, ETFs, and ADRs by default.

    This is the broadest US equity universe Sawa tracks. ETFs are the
    dominant gap that ``fetch_nasdaq_active_from_polygon()`` misses —
    only ~24% of US ETFs are XNAS-listed; the rest live on NYSE Arca
    (ARCX) and Cboe BZX (BATS).

    Like the NASDAQ fetcher, paginates once per type and merges. Drops
    XASE-listed CS (NYSE American — almost entirely microcap) by
    post-filtering.

    Args:
        logger: Logger instance
        types: Polygon ticker types to include. Defaults to (CS, ETF, ADRC).
        api_key: Polygon API key. Falls back to ``POLYGON_API_KEY`` env var.

    Returns:
        Sorted list of ticker symbols, deduplicated across types.

    Raises:
        ValueError: If no API key available.
        requests.RequestException: If the Polygon endpoint is unreachable.
    """
    api_key = api_key or os.environ.get("POLYGON_API_KEY")
    if not api_key:
        raise ValueError("POLYGON_API_KEY required (env var or argument)")

    base_url = "https://api.polygon.io/v3/reference/tickers"
    seen: set[str] = set()

    # Exchanges to drop entirely for type=CS — XASE is NYSE American
    # (microcap noise). BATS-CS is intentionally NOT excluded: the only
    # BATS-listed CS is CBOE (Cboe Global Markets), an S&P 500 member.
    cs_exclude_exchanges = {"XASE"}

    for ticker_type in types:
        params: dict[str, str | int] = {
            "market": "stocks",
            "active": "true",
            "type": ticker_type,
            "limit": 1000,
            "apiKey": api_key,
        }
        url: str | None = base_url
        page = 0
        type_count = 0
        type_dropped = 0
        while url:
            page += 1
            resp = requests.get(
                url,
                params=params if page == 1 else None,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            for row in data.get("results", []):
                ticker = row.get("ticker")
                if not ticker:
                    continue
                exchange = row.get("primary_exchange", "")
                if ticker_type == "CS" and exchange in cs_exclude_exchanges:
                    type_dropped += 1
                    continue
                seen.add(ticker.upper())
                type_count += 1
            next_url = data.get("next_url")
            if not next_url:
                break
            sep = "&" if "?" in next_url else "?"
            url = f"{next_url}{sep}apiKey={api_key}"
            time.sleep(0.05)
        suffix = f", dropped {type_dropped} from XASE" if type_dropped else ""
        logger.info(f"  Polygon type={ticker_type} (any exchange): {type_count} tickers{suffix}")

    symbols = sorted(seen)
    logger.info(f"Polygon US active total ({'+'.join(types)}): {len(symbols)} tickers")
    return symbols


def fetch_nasdaq_listed_symbols(logger: logging.Logger) -> list[str]:
    """
    Load NASDAQ-listed tickers, primary source Polygon REST.

    Tries the Polygon ``/v3/reference/tickers`` endpoint filtered to
    ``exchange=XNAS, active=true, type IN (CS, ETF, ADRC)`` first. On any
    failure (network, missing API key, etc.) falls back to the bundled
    ``data/nasdaq1000_symbols.txt`` file.

    The bundled file is a 2021-era snapshot kept as a recovery fallback; it
    contains warrant/unit/right tickers that the Polygon path now filters
    out, so the two sources are not 1:1 — expect a ~12% reduction when
    switching from bundled to Polygon (Polygon: ~4,681; bundled: ~5,316).

    Args:
        logger: Logger instance

    Returns:
        List of ticker symbols
    """
    try:
        return fetch_nasdaq_active_from_polygon(logger)
    except Exception as e:
        logger.warning(
            f"Polygon NASDAQ fetch failed ({e}); falling back to bundled file"
        )

    candidates = [
        project_root() / "data" / "nasdaq1000_symbols.txt",
        packaged_resource_path("nasdaq1000_symbols.txt"),
    ]
    symbols_file = next((c for c in candidates if c.exists()), None)
    if symbols_file is None:
        raise FileNotFoundError(
            "NASDAQ-listed symbols file not found in any of: "
            + ", ".join(str(c) for c in candidates)
        )

    symbols: list[str] = []
    with open(symbols_file) as f:
        for line in f:
            sym = line.strip().upper()
            if sym and not sym.startswith("#"):
                symbols.append(sym)

    logger.info(f"Loaded {len(symbols)} NASDAQ symbols from {symbols_file}")
    return symbols


_INDEX_FETCHERS: dict[str, "callable[[logging.Logger], list[str]]"] = {
    "sp500": fetch_sp500_symbols,
    "nasdaq_listed": fetch_nasdaq_listed_symbols,
    "us_active": fetch_us_active_from_polygon,
    "nasdaq100": fetch_nasdaq100_symbols,
    "dow30": fetch_dow30_symbols,
    "russell1000": fetch_russell1000_symbols,
    "mag7": fetch_mag7_symbols,
}


def fetch_index_symbols(index: str, logger: logging.Logger) -> list[str]:
    """
    Fetch symbols for a market index.

    Recognized codes (case-insensitive): ``sp500``, ``nasdaq_listed``,
    ``us_active``, ``nasdaq100``, ``dow30``, ``russell1000``,
    ``russell2000``, ``mag7``. Common dashed/spaced variants are
    accepted (e.g. ``"S&P 500"``, ``"nasdaq-100"``, ``"Russell 1000"``).

    Args:
        index: Index code.
        logger: Logger instance.

    Returns:
        List of ticker symbols.

    Raises:
        ValueError: If index not recognized.
    """
    normalized = re.sub(r"[\s\-&]+", "_", index.lower()).strip("_")
    aliases = {
        "sp_500": "sp500",
        "s_p500": "sp500",
        "s_p_500": "sp500",
        "nasdaq_100": "nasdaq100",
        "dow_30": "dow30",
        "dow_jones": "dow30",
        "dow_jones_industrial_average": "dow30",
        "djia": "dow30",
        "russell_1000": "russell1000",
        "russell1000_index": "russell1000",
        "mag_7": "mag7",
        "magnificent_7": "mag7",
        "magnificent_seven": "mag7",
        "us_active": "us_active",
    }
    code = aliases.get(normalized, normalized)

    fetcher = _INDEX_FETCHERS.get(code)
    if fetcher is None:
        raise ValueError(
            f"Unknown index: {index!r}. Valid codes: "
            f"{sorted(_INDEX_FETCHERS)}"
        )
    return fetcher(logger)
