"""Economy service for MCP server using repository layer.

DEPRECATED: This service is not used by the MCP server and relies on
repository methods that are not compatible with the current database schema.

The MCP server uses direct SQL queries in mcp_server/tools/economy.py instead.
This module is kept for backwards compatibility but should not be used.
"""

from collections import defaultdict
from datetime import date
from typing import Any

from sawa.repositories import get_factory

from mcp_server.services.converters import (
    inflation_to_dict,
    labor_market_to_dict,
    treasury_yield_to_dict,
)


class EconomyService:
    """Async service for economic data access via repository layer.

    DEPRECATED: This service relies on repository methods (get_inflation,
    get_labor_market) that are not compatible with the current database schema.
    These methods expect a narrow schema with an 'indicator' column, but the
    actual schema uses a wide format with separate columns per indicator.

    The MCP server uses direct SQL queries in mcp_server/tools/economy.py instead.
    Do not use this service - it will raise NotImplementedError at runtime.

    Example:
        service = EconomyService()
        yields = await service.get_treasury_yields("2024-01-01", "2024-01-31")  # Works
        inflation = await service.get_inflation("2024-01-01", "2024-01-31")  # Raises NotImplementedError
    """

    def __init__(self) -> None:
        """Initialize with repository factory."""
        self._factory = get_factory()

    async def get_treasury_yields(
        self,
        start_date: str,
        end_date: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Get treasury yield data.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD), defaults to today
            limit: Maximum rows

        Returns:
            List of yield dicts matching MCP format
        """
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date) if end_date else date.today()

        repo = self._factory.get_economy_repository()
        yields = await repo.get_treasury_yields(start, end)

        result = [treasury_yield_to_dict(y) for y in yields]
        return result[:limit]

    async def get_inflation(
        self,
        start_date: str,
        end_date: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Get inflation data.

        The domain model returns one record per indicator per date,
        but MCP expects a single record with all indicators per date.
        This method pivots the data accordingly.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD), defaults to today
            limit: Maximum rows

        Returns:
            List of inflation dicts matching MCP format
        """
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date) if end_date else date.today()

        repo = self._factory.get_economy_repository()
        records = await repo.get_inflation(start, end)

        # Group by date
        by_date: dict[date, list] = defaultdict(list)
        for record in records:
            by_date[record.date].append(record)

        # Convert each date group
        result = []
        for dt in sorted(by_date.keys()):
            inflation_dict = inflation_to_dict(by_date[dt])
            if inflation_dict:
                result.append(inflation_dict)
            if len(result) >= limit:
                break

        return result

    async def get_labor_market(
        self,
        start_date: str,
        end_date: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Get labor market data.

        The domain model returns one record per indicator per date,
        but MCP expects a single record with all indicators per date.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD), defaults to today
            limit: Maximum rows

        Returns:
            List of labor market dicts matching MCP format
        """
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date) if end_date else date.today()

        repo = self._factory.get_economy_repository()
        records = await repo.get_labor_market(start, end)

        # Group by date
        by_date: dict[date, list] = defaultdict(list)
        for record in records:
            by_date[record.date].append(record)

        # Convert each date group
        result = []
        for dt in sorted(by_date.keys()):
            labor_dict = labor_market_to_dict(by_date[dt])
            if labor_dict:
                result.append(labor_dict)
            if len(result) >= limit:
                break

        return result
