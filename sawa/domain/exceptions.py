"""Repository and provider exceptions.

This module defines the exception hierarchy for the repository layer.
All repository operations should raise these exceptions rather than
provider-specific exceptions.

Exception Hierarchy:
    RepositoryError (base)
    ├── ProviderError (API/external failures)
    │   ├── RateLimitError (rate limit exceeded)
    │   └── AuthenticationError (invalid credentials)
    ├── NotFoundError (data not found)
    └── ValidationError (invalid data)

Usage:
    from sawa.domain.exceptions import NotFoundError, ProviderError

    try:
        prices = await repo.get_prices("INVALID", start, end)
    except NotFoundError:
        print("Ticker not found")
    except ProviderError as e:
        print(f"Provider {e.provider} failed: {e}")
"""


class RepositoryError(Exception):
    """Base exception for repository errors.

    All exceptions raised by repositories should inherit from this class.
    """

    pass


class ProviderError(RepositoryError):
    """Error from data provider (API failure, network error, etc.).

    Attributes:
        provider: Name of the provider that failed
        original_error: The underlying exception (if any)
    """

    def __init__(
        self,
        message: str,
        provider: str,
        original_error: Exception | None = None,
    ) -> None:
        super().__init__(message)
        self.provider = provider
        self.original_error = original_error

    def __str__(self) -> str:
        base = super().__str__()
        if self.original_error:
            return f"{base} (provider: {self.provider}, caused by: {self.original_error})"
        return f"{base} (provider: {self.provider})"


class RateLimitError(ProviderError):
    """Rate limit exceeded.

    Attributes:
        retry_after: Seconds to wait before retrying (if known)
    """

    def __init__(self, provider: str, retry_after: int | None = None) -> None:
        super().__init__(
            f"Rate limit exceeded for {provider}",
            provider,
        )
        self.retry_after = retry_after


class AuthenticationError(ProviderError):
    """API key invalid or missing.

    Raised when the API key is not configured, expired, or invalid.
    """

    def __init__(self, provider: str, message: str | None = None) -> None:
        super().__init__(
            message or f"Authentication failed for {provider}",
            provider,
        )


class NotFoundError(RepositoryError):
    """Requested data not found.

    Raised when the requested ticker, date range, or resource does not exist.
    """

    def __init__(self, resource: str, identifier: str | None = None) -> None:
        if identifier:
            message = f"{resource} not found: {identifier}"
        else:
            message = f"{resource} not found"
        super().__init__(message)
        self.resource = resource
        self.identifier = identifier


class ValidationError(RepositoryError):
    """Data validation failed.

    Raised when data from a provider fails validation or conversion.
    """

    def __init__(self, message: str, field: str | None = None) -> None:
        super().__init__(message)
        self.field = field
