"""
Sawa - S&P 500 Data Downloader and Analysis Package.

This package provides tools for downloading, storing, and analyzing S&P 500
market data from Polygon.io API.

High-level API:
    from sawa import get_live_price, scan_ytd_performance

    # Get live price
    price_data = await get_live_price("AAPL", days=7)

    # Scan market
    scan_results = await scan_ytd_performance(index="sp500")

CLI Commands:
    sawa coldstart --years 5        # Full database setup
    sawa daily                      # Daily price update
    sawa weekly                     # Weekly fundamentals update
    sawa add-symbol PLTR COIN       # Add new symbols
"""

__version__ = "0.2.0"


# Lazy imports to avoid circular dependencies
def __getattr__(name: str):
    """Lazy import for high-level functions."""
    if name == "get_live_price":
        from sawa.live import get_live_price

        return get_live_price
    elif name == "get_live_prices_batch":
        from sawa.live import get_live_prices_batch

        return get_live_prices_batch
    elif name == "scan_ytd_performance":
        from sawa.scanner import scan_ytd_performance

        return scan_ytd_performance
    elif name == "fetch_sp500_symbols":
        from sawa.utils.symbols import fetch_sp500_symbols

        return fetch_sp500_symbols
    elif name == "fetch_nasdaq100_symbols":
        from sawa.utils.symbols import fetch_nasdaq100_symbols

        return fetch_nasdaq100_symbols
    elif name == "fetch_index_symbols":
        from sawa.utils.symbols import fetch_index_symbols

        return fetch_index_symbols
    elif name == "PolygonClient":
        from sawa.api import PolygonClient

        return PolygonClient
    elif name == "PolygonS3Client":
        from sawa.api import PolygonS3Client

        return PolygonS3Client
    elif name == "AsyncPolygonClient":
        from sawa.api import AsyncPolygonClient

        return AsyncPolygonClient
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


__all__ = [
    # Version
    "__version__",
    # Live data
    "get_live_price",
    "get_live_prices_batch",
    # Scanner
    "scan_ytd_performance",
    # Symbol fetching
    "fetch_sp500_symbols",
    "fetch_nasdaq100_symbols",
    "fetch_index_symbols",
    # API clients
    "PolygonClient",
    "PolygonS3Client",
    "AsyncPolygonClient",
]
