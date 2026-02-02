"""Service layer for TUI data access.

This module provides synchronous service classes that wrap the async
repository layer from sawa.repositories. The services handle
async-to-sync conversion and model mapping.

Usage:
    from sawa_tui.services import StockService, EconomyService

    # Get stock data
    stock_service = StockService()
    company = stock_service.get_company("AAPL")
    prices = stock_service.get_prices("AAPL", days=60)

    # Get economy data
    economy_service = EconomyService()
    yields = economy_service.get_treasury_yields(limit=30)
"""

from sawa_tui.services.economy_service import EconomyService
from sawa_tui.services.stock_service import StockService

__all__ = [
    "StockService",
    "EconomyService",
]
