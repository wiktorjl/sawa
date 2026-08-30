"""
News data loader for PostgreSQL database.

Fetches news articles from Polygon.io API and loads them into the database.

Usage:
    python -m sawa.database.news --ticker AAPL --days 30
"""

import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from psycopg import sql

from sawa.api.client import PolygonClient
from sawa.domain.exceptions import ProviderError
from sawa.utils import setup_logging
from sawa.utils.cli import add_common_args, create_parser
from sawa.utils.config import get_polygon_api_key
from sawa.utils.security import redact_sensitive_text

from .connection import get_connection, get_connection_params

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class NewsRequestFailure:
    """Safe failure details for one news provider request."""

    ticker: str | None
    error_type: str
    message: str

    def to_dict(self) -> dict[str, str | None]:
        return {
            "ticker": self.ticker,
            "error_type": self.error_type,
            "message": self.message,
        }


class NewsLoadResult(int):
    """Distinct loaded articles plus request and persistence provenance."""

    requested: int
    succeeded: int
    empty: int
    fetched_articles: int
    persisted_articles: int
    rejected_articles: int
    failures: tuple[NewsRequestFailure, ...]

    def __new__(
        cls,
        loaded: int,
        *,
        requested: int,
        succeeded: int,
        empty: int,
        fetched_articles: int,
        persisted_articles: int,
        rejected_articles: int,
        failures: tuple[NewsRequestFailure, ...] = (),
    ) -> "NewsLoadResult":
        result = super().__new__(cls, loaded)
        result.requested = requested
        result.succeeded = succeeded
        result.empty = empty
        result.fetched_articles = fetched_articles
        result.persisted_articles = persisted_articles
        result.rejected_articles = rejected_articles
        result.failures = failures
        return result

    @property
    def failed(self) -> int:
        return len(self.failures)

    @property
    def all_requests_failed(self) -> bool:
        return self.requested > 0 and self.failed == self.requested

    @property
    def all_successful_empty(self) -> bool:
        return (
            self.requested > 0
            and self.succeeded == self.requested
            and self.failed == 0
            and self.fetched_articles == 0
        )

    @property
    def no_articles_fetched(self) -> bool:
        """Whether successful requests still produced no usable fresh articles."""
        return self.requested > 0 and self.succeeded > 0 and self.fetched_articles == 0

    @property
    def persistence_failed(self) -> bool:
        return self.rejected_articles > 0

    @property
    def total_persistence_failure(self) -> bool:
        return self.fetched_articles > 0 and self.persisted_articles == 0

    @property
    def partial_persistence_failure(self) -> bool:
        return self.rejected_articles > 0 and self.persisted_articles > 0

    @property
    def failure_details(self) -> list[dict[str, str | None]]:
        return [failure.to_dict() for failure in self.failures]

    def summary(self) -> dict[str, Any]:
        return {
            "requested": self.requested,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "empty": self.empty,
            "fetched_articles": self.fetched_articles,
            "persisted_articles": self.persisted_articles,
            "rejected_articles": self.rejected_articles,
            "unique_loaded_articles": int(self),
            "failures": self.failure_details,
        }


def load_news_article(conn, article: dict[str, Any]) -> bool:
    """Load a single news article and its related data."""
    article_id = article.get("id")
    if not article_id:
        return False

    # Insert or update article
    article_sql = sql.SQL("""
        INSERT INTO news_articles (
            id, title, author, description, article_url, image_url,
            published_utc, publisher_name, publisher_logo_url,
            publisher_homepage_url, keywords
        ) VALUES (
            %(id)s, %(title)s, %(author)s, %(description)s, %(article_url)s,
            %(image_url)s, %(published_utc)s, %(publisher_name)s,
            %(publisher_logo_url)s, %(publisher_homepage_url)s, %(keywords)s
        )
        ON CONFLICT (id) DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            keywords = EXCLUDED.keywords
    """)

    publisher = article.get("publisher", {})
    article_params = {
        "id": article_id,
        "title": article.get("title", ""),
        "author": article.get("author"),
        "description": article.get("description"),
        "article_url": article.get("article_url"),
        "image_url": article.get("image_url"),
        "published_utc": article.get("published_utc"),
        "publisher_name": publisher.get("name"),
        "publisher_logo_url": publisher.get("logo_url"),
        "publisher_homepage_url": publisher.get("homepage_url"),
        "keywords": article.get("keywords"),
    }

    with conn.cursor() as cur:
        cur.execute(article_sql, article_params)

    # Insert ticker associations
    tickers = article.get("tickers", [])
    if tickers:
        ticker_sql = sql.SQL("""
            INSERT INTO news_article_tickers (article_id, ticker)
            VALUES (%s, %s)
            ON CONFLICT (article_id, ticker) DO NOTHING
        """)
        ticker_data = [(article_id, ticker) for ticker in tickers]
        with conn.cursor() as cur:
            cur.executemany(ticker_sql, ticker_data)

    # Insert sentiment insights
    insights = article.get("insights", [])
    if insights:
        sentiment_sql = sql.SQL("""
            INSERT INTO news_sentiment (article_id, ticker, sentiment, sentiment_reasoning)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (article_id, ticker) DO UPDATE SET
                sentiment = EXCLUDED.sentiment,
                sentiment_reasoning = EXCLUDED.sentiment_reasoning
        """)
        sentiment_data = [
            (
                article_id,
                insight.get("ticker"),
                insight.get("sentiment"),
                insight.get("sentiment_reasoning"),
            )
            for insight in insights
            if insight.get("ticker")
        ]
        if sentiment_data:
            with conn.cursor() as cur:
                cur.executemany(sentiment_sql, sentiment_data)
    return True


def fetch_and_load_news(
    conn,
    client: PolygonClient,
    ticker: str | None = None,
    days: int = 30,
    limit: int = 1000,
    log: logging.Logger | None = None,
    loaded_ids: set[str] | None = None,
) -> NewsLoadResult:
    """
    Fetch news from API and load into database.

    Args:
        conn: Database connection
        client: Polygon API client
        ticker: Optional ticker to filter by
        days: Number of days of history to fetch
        limit: Max articles per request
        log: Logger instance
        loaded_ids: Optional set of article ids already loaded this run. Ids
            loaded by this call are added to it so a multi-symbol caller can
            count distinct articles (Polygon returns each article once per
            in-universe ticker, so summing per-symbol counts overstates the
            true article total).

    Returns:
        Number of distinct articles loaded by this call.
    """
    log = log or logger
    # Calculate date range. Use timezone-aware UTC so the 'Z' suffix sent to
    # Polygon's published_utc filter is accurate regardless of host timezone.
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days)

    log.info(f"Fetching news from {start_date.date()} to {end_date.date()}")
    if ticker:
        log.info(f"Filtering by ticker: {ticker}")

    # Fetch articles
    articles = client.get_news(
        ticker=ticker,
        published_utc_gte=start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        published_utc_lte=end_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        limit=limit,
    )
    if not isinstance(articles, list):
        raise ProviderError("Provider returned a non-list news response", provider="polygon")

    log.info(f"Fetched {len(articles)} articles")

    # Load each article. Wrap each in a SAVEPOINT so a single bad article rolls
    # back only its own writes, not every article already inserted in this batch.
    # Track distinct ids loaded this call so the reported total is a real
    # article count rather than a sum of per-ticker upserts.
    call_ids: set[str] = set()
    persisted_articles = 0
    rejected_articles = 0
    with conn.cursor() as sp_cur:
        for article in articles:
            article_id = article.get("id") if isinstance(article, dict) else None
            try:
                sp_cur.execute("SAVEPOINT article_insert")
                if not isinstance(article, dict) or not load_news_article(conn, article):
                    raise ValueError("news article has no usable identity")
                sp_cur.execute("RELEASE SAVEPOINT article_insert")
                persisted_articles += 1
                if article_id is not None:
                    call_ids.add(str(article_id))
            except Exception as e:
                rejected_articles += 1
                safe_error = redact_sensitive_text(e)
                log.warning(
                    f"Failed to load article {article_id}: "
                    f"{type(e).__name__}: {safe_error}"
                )
                sp_cur.execute("ROLLBACK TO SAVEPOINT article_insert")
                sp_cur.execute("RELEASE SAVEPOINT article_insert")
                continue

    conn.commit()
    if loaded_ids is not None:
        loaded_ids.update(call_ids)
    loaded = len(call_ids)
    log.info(f"Loaded {loaded} articles")
    return NewsLoadResult(
        loaded,
        requested=1,
        succeeded=1,
        empty=int(not articles),
        fetched_articles=len(articles),
        persisted_articles=persisted_articles,
        rejected_articles=rejected_articles,
    )


def fetch_news_for_symbols(
    conn,
    client: PolygonClient,
    symbols: list[str],
    days: int = 30,
    limit_per_symbol: int = 100,
    log: logging.Logger | None = None,
) -> NewsLoadResult:
    """
    Fetch news for multiple symbols.

    Args:
        conn: Database connection
        client: Polygon API client
        symbols: List of ticker symbols
        days: Number of days of history
        limit_per_symbol: Max articles per symbol
        log: Logger instance

    Returns:
        Total number of distinct articles loaded across all symbols. Polygon
        returns each article once per in-universe ticker, so the same article
        is fetched and re-upserted under several symbols; deduping on article id
        keeps this from overstating the true article count.
    """
    log = log or logger
    loaded_ids: set[str] = set()
    succeeded = 0
    empty = 0
    fetched_articles = 0
    persisted_articles = 0
    rejected_articles = 0
    failures: list[NewsRequestFailure] = []
    for i, symbol in enumerate(symbols, 1):
        log.info(f"[{i}/{len(symbols)}] Fetching news for {symbol}")
        try:
            result = fetch_and_load_news(
                conn,
                client,
                ticker=symbol,
                days=days,
                limit=limit_per_symbol,
                log=log,
                loaded_ids=loaded_ids,
            )
            succeeded += 1
            empty += result.empty
            fetched_articles += result.fetched_articles
            persisted_articles += result.persisted_articles
            rejected_articles += result.rejected_articles
        except Exception as e:
            safe_error = redact_sensitive_text(e)
            failures.append(
                NewsRequestFailure(
                    ticker=symbol,
                    error_type=type(e).__name__,
                    message=safe_error,
                )
            )
            log.error(
                f"Failed to fetch news for {symbol}: "
                f"{type(e).__name__}: {safe_error}"
            )
            try:
                conn.rollback()
            except Exception:
                pass
            continue

    return NewsLoadResult(
        len(loaded_ids),
        requested=len(symbols),
        succeeded=succeeded,
        empty=empty,
        fetched_articles=fetched_articles,
        persisted_articles=persisted_articles,
        rejected_articles=rejected_articles,
        failures=tuple(failures),
    )


def main() -> int:
    """Main entry point."""
    from dotenv import load_dotenv

    load_dotenv()

    parser = create_parser(
        "Fetch and load news articles into PostgreSQL database.",
        epilog="""\
Examples:
  %(prog)s --ticker AAPL --days 30
  %(prog)s --symbols-file symbols.txt --days 7
  %(prog)s --days 14  # Fetch all recent news

Environment: POLYGON_API_KEY, PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
""",
    )

    parser.add_argument("--ticker", help="Single ticker to fetch news for")
    parser.add_argument("--symbols-file", type=Path, help="File with symbols (one per line)")
    parser.add_argument("--days", type=int, default=30, help="Days of history (default: 30)")
    parser.add_argument("--limit", type=int, default=100, help="Max articles per symbol")
    parser.add_argument("--api-key", help="Polygon API key (overrides env)")
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--database")
    parser.add_argument("--user")
    parser.add_argument("--password")
    add_common_args(parser)

    args = parser.parse_args()
    log = setup_logging(args.verbose)

    log.info("=" * 60)
    log.info("News Data Loader")
    log.info("=" * 60)

    try:
        # Get API key
        api_key = args.api_key or get_polygon_api_key()
        if not api_key:
            log.error("POLYGON_API_KEY not set")
            return 1

        # Connect to database
        conn_params = get_connection_params(
            args.host, args.port, args.database, args.user, args.password
        )
        conn = get_connection(conn_params)
        log.info(
            f"Connected to {conn_params['host']}:{conn_params['port']}/{conn_params['dbname']}"
        )

        # Create API client
        client = PolygonClient(api_key, logger=log)

        # Determine symbols to fetch
        if args.ticker:
            symbols = [args.ticker.upper()]
        elif args.symbols_file:
            if not args.symbols_file.exists():
                log.error(f"Symbols file not found: {args.symbols_file}")
                return 1
            with open(args.symbols_file) as f:
                symbols = [line.strip().upper() for line in f if line.strip()]
        else:
            # Fetch general news (no ticker filter)
            symbols = []

        # Fetch and load news
        if symbols:
            total = fetch_news_for_symbols(
                conn, client, symbols, days=args.days, limit_per_symbol=args.limit, log=log
            )
        else:
            total = fetch_and_load_news(
                conn, client, days=args.days, limit=args.limit * 10, log=log
            )

        conn.close()
        log.info(f"\nTotal articles loaded: {total}")
        if isinstance(total, NewsLoadResult) and (
            total.all_requests_failed
            or total.no_articles_fetched
            or total.total_persistence_failure
        ):
            log.error("News run produced no trustworthy fresh result")
            return 1
        return 0

    except Exception as e:
        log.error(
            f"Error: {type(e).__name__}: {redact_sensitive_text(e)}"
        )
        if args.verbose:
            raise
        return 1


if __name__ == "__main__":
    sys.exit(main())
