"""Shared constants for sawa package."""

# HTTP request timeouts
DEFAULT_HTTP_TIMEOUT = 30  # seconds

# Database batch processing
DEFAULT_BATCH_SIZE = 1000  # rows per batch insert

# API rate limiting
DEFAULT_API_RATE_LIMIT = 5.0  # requests per second

# News fetching defaults
DEFAULT_NEWS_DAYS = 30  # days of history
DEFAULT_NEWS_LIMIT_PER_SYMBOL = 50  # articles per symbol

# Tickers excluded from split-adjusted history refresh. Polygon returns
# bogus pre-IPO bars for these; once multiplied by a multi-million-x
# cumulative reverse-split factor the adjusted prices overflow
# NUMERIC(16,4) and abort the whole batch insert.
SPLIT_ADJUST_BLACKLIST: frozenset[str] = frozenset({"ADTX"})
