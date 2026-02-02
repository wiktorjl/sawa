"""Repository layer for S&P 500 data pipeline.

This module provides the repository pattern implementation for accessing
stock market data from various providers (database, Polygon.io, etc.).

Key Components:
    - base: Abstract repository interfaces
    - config: Repository configuration
    - factory: Factory for creating repository instances
    - cache: In-memory LRU cache with TTL
    - rate_limiter: Token bucket rate limiter
    - database: Database repository implementations

Usage:
    from sp500_tools.repositories import get_factory

    factory = get_factory()
    price_repo = factory.get_price_repository()
    prices = await price_repo.get_prices("AAPL", start_date, end_date)
"""

from sp500_tools.repositories.base import (
    CompanyRepository,
    EconomyRepository,
    FundamentalRepository,
    NewsRepository,
    RatiosRepository,
    Repository,
    StockPriceRepository,
)
from sp500_tools.repositories.cache import InMemoryCache, NullCache
from sp500_tools.repositories.config import RepositoryConfig, get_config
from sp500_tools.repositories.factory import (
    RepositoryFactory,
    get_factory,
    reset_factory,
    set_factory,
)

__all__ = [
    # Interfaces
    "Repository",
    "StockPriceRepository",
    "FundamentalRepository",
    "CompanyRepository",
    "NewsRepository",
    "RatiosRepository",
    "EconomyRepository",
    # Configuration
    "RepositoryConfig",
    "get_config",
    # Factory
    "RepositoryFactory",
    "get_factory",
    "set_factory",
    "reset_factory",
    # Cache
    "InMemoryCache",
    "NullCache",
]
