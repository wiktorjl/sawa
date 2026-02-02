"""Tests for domain exceptions."""

import pytest

from sp500_tools.domain.exceptions import (
    AuthenticationError,
    NotFoundError,
    ProviderError,
    RateLimitError,
    RepositoryError,
    ValidationError,
)


class TestRepositoryError:
    """Tests for base RepositoryError."""

    def test_basic_error(self) -> None:
        """Test creating basic repository error."""
        error = RepositoryError("Something went wrong")
        assert str(error) == "Something went wrong"

    def test_is_exception(self) -> None:
        """Test that RepositoryError is an Exception."""
        error = RepositoryError("test")
        assert isinstance(error, Exception)


class TestProviderError:
    """Tests for ProviderError."""

    def test_basic_error(self) -> None:
        """Test creating provider error."""
        error = ProviderError("API failed", "polygon.io")
        assert "API failed" in str(error)
        assert error.provider == "polygon.io"
        assert error.original_error is None

    def test_with_original_error(self) -> None:
        """Test creating provider error with original exception."""
        original = ValueError("invalid response")
        error = ProviderError("API failed", "polygon.io", original)
        assert error.original_error == original
        assert "caused by" in str(error)

    def test_inherits_repository_error(self) -> None:
        """Test that ProviderError inherits from RepositoryError."""
        error = ProviderError("test", "provider")
        assert isinstance(error, RepositoryError)


class TestRateLimitError:
    """Tests for RateLimitError."""

    def test_basic_error(self) -> None:
        """Test creating rate limit error."""
        error = RateLimitError("polygon.io")
        assert "Rate limit exceeded" in str(error)
        assert error.provider == "polygon.io"
        assert error.retry_after is None

    def test_with_retry_after(self) -> None:
        """Test creating rate limit error with retry-after."""
        error = RateLimitError("polygon.io", retry_after=60)
        assert error.retry_after == 60

    def test_inherits_provider_error(self) -> None:
        """Test that RateLimitError inherits from ProviderError."""
        error = RateLimitError("provider")
        assert isinstance(error, ProviderError)
        assert isinstance(error, RepositoryError)


class TestAuthenticationError:
    """Tests for AuthenticationError."""

    def test_basic_error(self) -> None:
        """Test creating authentication error."""
        error = AuthenticationError("polygon.io")
        assert "Authentication failed" in str(error)
        assert error.provider == "polygon.io"

    def test_with_custom_message(self) -> None:
        """Test creating authentication error with custom message."""
        error = AuthenticationError("polygon.io", "API key expired")
        assert "API key expired" in str(error)

    def test_inherits_provider_error(self) -> None:
        """Test that AuthenticationError inherits from ProviderError."""
        error = AuthenticationError("provider")
        assert isinstance(error, ProviderError)


class TestNotFoundError:
    """Tests for NotFoundError."""

    def test_basic_error(self) -> None:
        """Test creating not found error."""
        error = NotFoundError("ticker")
        assert "ticker not found" in str(error)
        assert error.resource == "ticker"
        assert error.identifier is None

    def test_with_identifier(self) -> None:
        """Test creating not found error with identifier."""
        error = NotFoundError("ticker", "INVALID")
        assert "INVALID" in str(error)
        assert error.identifier == "INVALID"

    def test_inherits_repository_error(self) -> None:
        """Test that NotFoundError inherits from RepositoryError."""
        error = NotFoundError("resource")
        assert isinstance(error, RepositoryError)


class TestValidationError:
    """Tests for ValidationError."""

    def test_basic_error(self) -> None:
        """Test creating validation error."""
        error = ValidationError("Invalid date format")
        assert "Invalid date format" in str(error)
        assert error.field is None

    def test_with_field(self) -> None:
        """Test creating validation error with field."""
        error = ValidationError("Must be positive", field="volume")
        assert error.field == "volume"

    def test_inherits_repository_error(self) -> None:
        """Test that ValidationError inherits from RepositoryError."""
        error = ValidationError("test")
        assert isinstance(error, RepositoryError)


class TestExceptionHierarchy:
    """Tests for exception hierarchy."""

    def test_catch_all_with_repository_error(self) -> None:
        """Test that all errors can be caught with RepositoryError."""
        errors = [
            RepositoryError("base"),
            ProviderError("provider", "test"),
            RateLimitError("provider"),
            AuthenticationError("provider"),
            NotFoundError("resource"),
            ValidationError("invalid"),
        ]

        for error in errors:
            with pytest.raises(RepositoryError):
                raise error

    def test_catch_provider_errors(self) -> None:
        """Test catching all provider-related errors."""
        errors = [
            ProviderError("provider", "test"),
            RateLimitError("provider"),
            AuthenticationError("provider"),
        ]

        for error in errors:
            with pytest.raises(ProviderError):
                raise error
