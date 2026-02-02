"""Factory for creating repository instances.

This module provides the RepositoryFactory class which creates and caches
repository instances based on configuration. It also provides module-level
functions for accessing a global factory instance.

Usage:
    from sawa.repositories import get_factory

    # Get the global factory (configured from environment)
    factory = get_factory()

    # Get repositories
    price_repo = factory.get_price_repository()
    company_repo = factory.get_company_repository()

    # Use a specific provider
    polygon_repo = factory.get_price_repository(provider="polygon")

    # For testing
    from sawa.repositories import set_factory, reset_factory
    set_factory(mock_factory)
    # ... run tests ...
    reset_factory()
"""

from sawa.repositories.base import (
    CompanyRepository,
    EconomyRepository,
    FundamentalRepository,
    RatiosRepository,
    StockPriceRepository,
)
from sawa.repositories.cache import InMemoryCache, NullCache
from sawa.repositories.config import RepositoryConfig


class RepositoryFactory:
    """Factory for creating repository instances.

    This factory creates and caches repository instances based on the
    provided configuration. It supports multiple providers and can
    switch between them at runtime.

    Attributes:
        config: Repository configuration

    Example:
        config = RepositoryConfig(database_url="postgresql://...")
        factory = RepositoryFactory(config)
        repo = factory.get_price_repository()
    """

    def __init__(self, config: RepositoryConfig) -> None:
        """Initialize factory with configuration.

        Args:
            config: Repository configuration
        """
        self.config = config
        self._cache: InMemoryCache | NullCache | None = None
        if config.cache_enabled:
            self._cache = InMemoryCache(
                max_size=config.cache_max_size,
                default_ttl_seconds=config.cache_ttl_seconds,
            )
        self._instances: dict[str, object] = {}

    def get_price_repository(
        self,
        provider: str | None = None,
    ) -> StockPriceRepository:
        """Get price repository instance.

        Args:
            provider: Provider name ("database" or "polygon").
                     Defaults to config.default_price_provider.

        Returns:
            StockPriceRepository instance

        Raises:
            ValueError: If provider is unknown or not configured
        """
        provider = provider or self.config.default_price_provider
        cache_key = f"price:{provider}"

        if cache_key not in self._instances:
            if provider == "database":
                from sawa.repositories.database import DatabasePriceRepository

                if not self.config.database_url:
                    raise ValueError("DATABASE_URL not configured")

                self._instances[cache_key] = DatabasePriceRepository(
                    database_url=self.config.database_url
                )
            elif provider == "polygon":
                from sawa.repositories.polygon_prices import (
                    PolygonPriceRepository,
                )

                if not self.config.polygon_api_key:
                    raise ValueError("Polygon API key not configured")

                self._instances[cache_key] = PolygonPriceRepository(
                    api_key=self.config.polygon_api_key,
                    s3_access_key=self.config.polygon_s3_access_key,
                    s3_secret_key=self.config.polygon_s3_secret_key,
                    cache=self._cache,
                    rate_limit=self.config.polygon_rate_limit,
                )
            else:
                raise ValueError(f"Unknown price provider: {provider}")

        return self._instances[cache_key]  # type: ignore[return-value]

    def get_fundamental_repository(
        self,
        provider: str | None = None,
    ) -> FundamentalRepository:
        """Get fundamental repository instance.

        Args:
            provider: Provider name. Defaults to config.default_fundamental_provider.

        Returns:
            FundamentalRepository instance

        Raises:
            ValueError: If provider is unknown or not configured
        """
        provider = provider or self.config.default_fundamental_provider
        cache_key = f"fundamental:{provider}"

        if cache_key not in self._instances:
            if provider == "database":
                from sawa.repositories.database import (
                    DatabaseFundamentalRepository,
                )

                if not self.config.database_url:
                    raise ValueError("DATABASE_URL not configured")

                self._instances[cache_key] = DatabaseFundamentalRepository(
                    database_url=self.config.database_url
                )
            else:
                raise ValueError(f"Unknown fundamental provider: {provider}")

        return self._instances[cache_key]  # type: ignore[return-value]

    def get_company_repository(
        self,
        provider: str | None = None,
    ) -> CompanyRepository:
        """Get company repository instance.

        Args:
            provider: Provider name. Defaults to config.default_company_provider.

        Returns:
            CompanyRepository instance

        Raises:
            ValueError: If provider is unknown or not configured
        """
        provider = provider or self.config.default_company_provider
        cache_key = f"company:{provider}"

        if cache_key not in self._instances:
            if provider == "database":
                from sawa.repositories.database import DatabaseCompanyRepository

                if not self.config.database_url:
                    raise ValueError("DATABASE_URL not configured")

                self._instances[cache_key] = DatabaseCompanyRepository(
                    database_url=self.config.database_url
                )
            else:
                raise ValueError(f"Unknown company provider: {provider}")

        return self._instances[cache_key]  # type: ignore[return-value]

    def get_ratios_repository(
        self,
        provider: str | None = None,
    ) -> RatiosRepository:
        """Get ratios repository instance.

        Args:
            provider: Provider name. Defaults to config.default_ratios_provider.

        Returns:
            RatiosRepository instance

        Raises:
            ValueError: If provider is unknown or not configured
        """
        provider = provider or self.config.default_ratios_provider
        cache_key = f"ratios:{provider}"

        if cache_key not in self._instances:
            if provider == "database":
                from sawa.repositories.database import DatabaseRatiosRepository

                if not self.config.database_url:
                    raise ValueError("DATABASE_URL not configured")

                self._instances[cache_key] = DatabaseRatiosRepository(
                    database_url=self.config.database_url
                )
            else:
                raise ValueError(f"Unknown ratios provider: {provider}")

        return self._instances[cache_key]  # type: ignore[return-value]

    def get_economy_repository(
        self,
        provider: str | None = None,
    ) -> EconomyRepository:
        """Get economy repository instance.

        Args:
            provider: Provider name. Defaults to config.default_economy_provider.

        Returns:
            EconomyRepository instance

        Raises:
            ValueError: If provider is unknown or not configured
        """
        provider = provider or self.config.default_economy_provider
        cache_key = f"economy:{provider}"

        if cache_key not in self._instances:
            if provider == "database":
                from sawa.repositories.database import DatabaseEconomyRepository

                if not self.config.database_url:
                    raise ValueError("DATABASE_URL not configured")

                self._instances[cache_key] = DatabaseEconomyRepository(
                    database_url=self.config.database_url
                )
            else:
                raise ValueError(f"Unknown economy provider: {provider}")

        return self._instances[cache_key]  # type: ignore[return-value]

    def clear_cache(self) -> None:
        """Clear all repository caches."""
        if self._cache:
            self._cache.clear()


# Module-level singleton factory
_factory: RepositoryFactory | None = None


def get_factory() -> RepositoryFactory:
    """Get or create the global factory instance.

    The factory is created on first call using configuration from
    environment variables.

    Returns:
        RepositoryFactory instance
    """
    global _factory
    if _factory is None:
        from sawa.repositories.config import get_config

        config = get_config()
        _factory = RepositoryFactory(config)
    return _factory


def set_factory(factory: RepositoryFactory) -> None:
    """Set the global factory instance.

    This is primarily used for testing to inject a mock factory.

    Args:
        factory: Factory instance to use
    """
    global _factory
    _factory = factory


def reset_factory() -> None:
    """Reset the global factory instance.

    After calling this, the next call to get_factory() will create
    a new factory from the current environment configuration.
    """
    global _factory
    _factory = None
