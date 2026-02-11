"""MCP service layer for repository-based data access.

This module provides services that wrap the sawa repository layer,
converting domain models to MCP-compatible dictionaries.

Usage:
    from mcp_server.services import get_stock_service, get_economy_service

    stock_service = get_stock_service()
    prices = stock_service.get_prices("AAPL", "2024-01-01", "2024-01-31")
"""

from mcp_server.services.economy_service import EconomyService
from mcp_server.services.stock_service import StockService

# Singleton instances
_stock_service: StockService | None = None
_economy_service: EconomyService | None = None


def get_stock_service() -> StockService:
    """Get or create stock service singleton."""
    global _stock_service
    if _stock_service is None:
        _stock_service = StockService()
    return _stock_service


def get_economy_service() -> EconomyService:
    """Get or create economy service singleton."""
    global _economy_service
    if _economy_service is None:
        _economy_service = EconomyService()
    return _economy_service


__all__ = [
    "StockService",
    "EconomyService",
    "get_stock_service",
    "get_economy_service",
]
