"""
News data loader for PostgreSQL database.

Fetches news articles from Polygon.io API and loads them into the database.

Usage:
    python -m sawa.database.news --ticker AAPL --days 30
"""

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from sawa.api.client import PolygonClient
from sawa.utils import setup_logging
from sawa.utils.cli import add_common_args, create_parser
from sawa.utils.config import get_polygon_api_key

from .connection import get_connection, get_connection_params

logger = logging.getLogger(__name__)


def load_news_article(conn, article: dict[str, Any]) -> None:
    """Load a single news article and its related data."""
    article_id = article.get("id")
    if not article_id:
        return

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


def fetch_and_load_news(
    conn,
    client: PolygonClient,
    ticker: str | None = None,
    days: int = 30,
    limit: int = 1000,
    log: logging.Logger | None = None,
) -> int:
    """
    Fetch news from API and load into database.

    Args:
        conn: Database connection
        client: Polygon API client
        ticker: Optional ticker to filter by
        days: Number of days of history to fetch
        limit: Max articles per request
        log: Logger instance

    Returns:
        Number of articles loaded
    """
    log = log or logger
    # Calculate date range
    end_date = datetime.now()
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

    log.info(f"Fetched {len(articles)} articles")

    # Load each article
    loaded = 0
    for article in articles:
        try:
            load_news_article(conn, article)
            loaded += 1
        except psycopg.Error as e:
            log.warning(f"Failed to load article {article.get('id')}: {e}")
            conn.rollback()
            continue

    conn.commit()
    log.info(f"Loaded {loaded} articles")
    return loaded


def fetch_news_for_symbols(
    conn,
    client: PolygonClient,
    symbols: list[str],
    days: int = 30,
    limit_per_symbol: int = 100,
    log: logging.Logger | None = None,
) -> int:
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
        Total number of articles loaded
    """
    log = log or logger
    total = 0
    for i, symbol in enumerate(symbols, 1):
        log.info(f"[{i}/{len(symbols)}] Fetching news for {symbol}")
        try:
            count = fetch_and_load_news(
                conn,
                client,
                ticker=symbol,
                days=days,
                limit=limit_per_symbol,
                log=log,
            )
            total += count
        except Exception as e:
            log.error(f"Failed to fetch news for {symbol}: {e}")
            continue

    return total


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
        return 0

    except Exception as e:
        log.error(f"Error: {e}")
        if args.verbose:
            raise
        return 1


if __name__ == "__main__":
    sys.exit(main())
