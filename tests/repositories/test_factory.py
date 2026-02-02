"""Tests for repository factory."""

import pytest

from sp500_tools.repositories.config import RepositoryConfig
from sp500_tools.repositories.factory import (
    RepositoryFactory,
    get_factory,
    reset_factory,
    set_factory,
)


class TestRepositoryFactory:
    """Tests for RepositoryFactory."""

    def test_create_factory(self) -> None:
        """Test creating a factory."""
        config = RepositoryConfig(database_url="postgresql://test:test@localhost/test")
        factory = RepositoryFactory(config)
        assert factory.config == config

    def test_get_price_repository_database(self) -> None:
        """Test getting database price repository."""
        config = RepositoryConfig(
            database_url="postgresql://test:test@localhost/test",
            default_price_provider="database",
        )
        factory = RepositoryFactory(config)

        repo = factory.get_price_repository()
        assert repo.provider_name == "database"

    def test_get_price_repository_missing_url(self) -> None:
        """Test error when database URL is missing."""
        config = RepositoryConfig(
            database_url=None,
            default_price_provider="database",
        )
        factory = RepositoryFactory(config)

        with pytest.raises(ValueError, match="DATABASE_URL"):
            factory.get_price_repository()

    def test_get_price_repository_unknown_provider(self) -> None:
        """Test error for unknown provider."""
        config = RepositoryConfig(
            database_url="postgresql://test:test@localhost/test",
            default_price_provider="unknown",
        )
        factory = RepositoryFactory(config)

        with pytest.raises(ValueError, match="Unknown price provider"):
            factory.get_price_repository()

    def test_explicit_provider_override(self) -> None:
        """Test explicitly specifying a provider."""
        config = RepositoryConfig(
            database_url="postgresql://test:test@localhost/test",
            default_price_provider="polygon",  # Default is polygon
        )
        factory = RepositoryFactory(config)

        # Explicitly request database
        repo = factory.get_price_repository(provider="database")
        assert repo.provider_name == "database"

    def test_repository_caching(self) -> None:
        """Test that repositories are cached."""
        config = RepositoryConfig(database_url="postgresql://test:test@localhost/test")
        factory = RepositoryFactory(config)

        repo1 = factory.get_price_repository()
        repo2 = factory.get_price_repository()
        assert repo1 is repo2

    def test_different_providers_different_instances(self) -> None:
        """Test that different providers return different instances."""
        config = RepositoryConfig(
            database_url="postgresql://test:test@localhost/test",
            polygon_api_key="test_key",
        )
        factory = RepositoryFactory(config)

        db_repo = factory.get_price_repository(provider="database")
        polygon_repo = factory.get_price_repository(provider="polygon")

        assert db_repo is not polygon_repo
        assert db_repo.provider_name == "database"
        assert polygon_repo.provider_name == "polygon.io"

    def test_get_company_repository(self) -> None:
        """Test getting company repository."""
        config = RepositoryConfig(database_url="postgresql://test:test@localhost/test")
        factory = RepositoryFactory(config)

        repo = factory.get_company_repository()
        assert repo.provider_name == "database"

    def test_get_fundamental_repository(self) -> None:
        """Test getting fundamental repository."""
        config = RepositoryConfig(database_url="postgresql://test:test@localhost/test")
        factory = RepositoryFactory(config)

        repo = factory.get_fundamental_repository()
        assert repo.provider_name == "database"

    def test_get_ratios_repository(self) -> None:
        """Test getting ratios repository."""
        config = RepositoryConfig(database_url="postgresql://test:test@localhost/test")
        factory = RepositoryFactory(config)

        repo = factory.get_ratios_repository()
        assert repo.provider_name == "database"

    def test_get_economy_repository(self) -> None:
        """Test getting economy repository."""
        config = RepositoryConfig(database_url="postgresql://test:test@localhost/test")
        factory = RepositoryFactory(config)

        repo = factory.get_economy_repository()
        assert repo.provider_name == "database"

    def test_clear_cache(self) -> None:
        """Test clearing repository cache."""
        config = RepositoryConfig(
            database_url="postgresql://test:test@localhost/test",
            cache_enabled=True,
        )
        factory = RepositoryFactory(config)

        # Get a repository to populate the internal cache
        factory.get_price_repository()

        # Clear should not raise
        factory.clear_cache()

    def test_cache_disabled(self) -> None:
        """Test factory with cache disabled."""
        config = RepositoryConfig(
            database_url="postgresql://test:test@localhost/test",
            cache_enabled=False,
        )
        factory = RepositoryFactory(config)

        # Should still work, just without caching
        repo = factory.get_price_repository()
        assert repo.provider_name == "database"


class TestGlobalFactory:
    """Tests for global factory functions."""

    def teardown_method(self) -> None:
        """Reset factory after each test."""
        reset_factory()

    def test_set_and_get_factory(self) -> None:
        """Test setting and getting global factory."""
        config = RepositoryConfig(database_url="postgresql://test:test@localhost/test")
        factory = RepositoryFactory(config)

        set_factory(factory)
        assert get_factory() is factory

    def test_reset_factory(self) -> None:
        """Test resetting global factory."""
        config = RepositoryConfig(database_url="postgresql://test:test@localhost/test")
        factory = RepositoryFactory(config)

        set_factory(factory)
        reset_factory()

        # Next call should create new factory (may fail without env vars)
        # We just verify reset doesn't raise
        reset_factory()
