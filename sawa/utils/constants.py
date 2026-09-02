"""Shared constants for sawa package."""

# HTTP request timeouts
DEFAULT_HTTP_TIMEOUT = 30  # seconds

# Database batch processing
DEFAULT_BATCH_SIZE = 1000  # rows per batch insert

# API rate limiting
DEFAULT_API_RATE_LIMIT = 5.0  # requests per second

# News fetching defaults
DEFAULT_NEWS_DAYS = 30  # days of history
# FRED publishes with a lag and revises recent values, so every market-internals
# fetch re-pulls at least this much history rather than starting at the last
# stored date.
MARKET_INTERNALS_OVERLAP_DAYS = 30
DEFAULT_NEWS_LIMIT_PER_SYMBOL = 50  # articles per symbol
