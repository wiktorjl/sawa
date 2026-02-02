"""Economy service - synchronous wrapper around async repositories.

This service provides synchronous methods for accessing economic data
(treasury yields, inflation, labor market) via the repository layer.

Usage:
    from sawa_tui.services import EconomyService

    service = EconomyService()
    yields = service.get_treasury_yields(limit=30)
    inflation = service.get_inflation(limit=30)
    labor = service.get_labor_market(limit=30)
"""

import asyncio
from collections import defaultdict
from datetime import date, timedelta

from sawa.repositories import get_factory

from sawa_tui.models.queries import Inflation, LaborMarket, TreasuryYields
from sawa_tui.services.converters import (
    inflation_to_tui,
    labor_market_to_tui,
    treasury_yield_to_tui,
)


class EconomyService:
    """Synchronous service for economic data access.

    This service wraps the async economy repository and provides
    synchronous methods compatible with the TUI's synchronous
    architecture.

    Attributes:
        _factory: Repository factory instance

    Example:
        service = EconomyService()
        yields = service.get_treasury_yields(limit=30)
        for y in yields:
            print(f"{y.date}: 10Y = {y.yield_10y}%")
    """

    def __init__(self) -> None:
        """Initialize the service with repository factory."""
        self._factory = get_factory()

    def _run_async(self, coro):
        """Run an async coroutine synchronously.

        Args:
            coro: Coroutine to execute

        Returns:
            Result of the coroutine
        """
        return asyncio.run(coro)

    def get_treasury_yields(self, limit: int = 30) -> list[TreasuryYields]:
        """Get treasury yields.

        Args:
            limit: Maximum number of records (days)

        Returns:
            List of TreasuryYields objects, sorted by date descending
        """
        # Calculate date range (assume ~1.5x limit to account for weekends/holidays)
        end_date = date.today()
        start_date = end_date - timedelta(days=int(limit * 1.5))

        repo = self._factory.get_economy_repository()
        results = self._run_async(repo.get_treasury_yields(start_date, end_date))

        # Convert, reverse to desc order, and limit
        yields = [treasury_yield_to_tui(r) for r in results]
        return list(reversed(yields))[:limit]

    def get_inflation(self, limit: int = 30) -> list[Inflation]:
        """Get inflation data.

        The domain model returns one record per indicator per date,
        but TUI expects a single record with all indicators per date.
        This method pivots the data accordingly.

        Args:
            limit: Maximum number of records (months typically)

        Returns:
            List of Inflation objects, sorted by date descending
        """
        # Go back further for inflation (monthly data)
        end_date = date.today()
        start_date = end_date - timedelta(days=limit * 45)  # ~1.5 months per record

        repo = self._factory.get_economy_repository()
        results = self._run_async(repo.get_inflation(start_date, end_date))

        # Group by date
        by_date: dict[date, list] = defaultdict(list)
        for record in results:
            by_date[record.date].append(record)

        # Convert each date group to TUI Inflation
        inflation_list = []
        for dt in sorted(by_date.keys(), reverse=True):
            inflation = inflation_to_tui(by_date[dt])
            if inflation:
                inflation_list.append(inflation)
            if len(inflation_list) >= limit:
                break

        return inflation_list

    def get_labor_market(self, limit: int = 30) -> list[LaborMarket]:
        """Get labor market data.

        The domain model returns one record per indicator per date,
        but TUI expects a single record with all indicators per date.
        This method pivots the data accordingly.

        Args:
            limit: Maximum number of records (months typically)

        Returns:
            List of LaborMarket objects, sorted by date descending
        """
        # Go back further for labor market (monthly data)
        end_date = date.today()
        start_date = end_date - timedelta(days=limit * 45)  # ~1.5 months per record

        repo = self._factory.get_economy_repository()
        results = self._run_async(repo.get_labor_market(start_date, end_date))

        # Group by date
        by_date: dict[date, list] = defaultdict(list)
        for record in results:
            by_date[record.date].append(record)

        # Convert each date group to TUI LaborMarket
        labor_list = []
        for dt in sorted(by_date.keys(), reverse=True):
            labor = labor_market_to_tui(by_date[dt])
            if labor:
                labor_list.append(labor)
            if len(labor_list) >= limit:
                break

        return labor_list

    def get_latest_treasury_yields(self) -> TreasuryYields | None:
        """Get the most recent treasury yields.

        Returns:
            Most recent TreasuryYields, or None if not available
        """
        yields = self.get_treasury_yields(limit=1)
        return yields[0] if yields else None

    def get_latest_inflation(self) -> Inflation | None:
        """Get the most recent inflation data.

        Returns:
            Most recent Inflation, or None if not available
        """
        inflation = self.get_inflation(limit=1)
        return inflation[0] if inflation else None

    def get_latest_labor_market(self) -> LaborMarket | None:
        """Get the most recent labor market data.

        Returns:
            Most recent LaborMarket, or None if not available
        """
        labor = self.get_labor_market(limit=1)
        return labor[0] if labor else None
