"""News sentiment MCP tools."""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from ..database import execute_query

logger = logging.getLogger(__name__)

# The daily job loads news every evening, seven days a week, so a newest
# article older than this is a stalled pipeline, not a quiet market.
NEWS_STALE_AFTER_DAYS = 2.0


def _as_utc(value: Any) -> datetime | None:
    """Coerce a timestamptz row value (datetime or ISO string) to aware UTC."""
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def get_recent_news_sentiment(
    ticker: str,
    days_back: int = 14,
    max_articles: int = 10,
) -> dict[str, Any]:
    """
    Get recent news articles with sentiment analysis for a ticker.

    The data is read from the local news_articles tables, which the daily job
    refreshes; it is not a live provider call. The ``freshness`` block and
    ``warnings`` list say how current the underlying feed is.

    Args:
        ticker: Stock ticker symbol
        days_back: Number of days to look back (default: 14, max: 90)
        max_articles: Maximum articles to return (default: 10, max: 50)

    Returns:
        Dict with articles list, aggregated sentiment metrics, freshness and
        warnings
    """
    days_back = min(max(days_back, 1), 90)
    max_articles = min(max(max_articles, 1), 50)
    ticker = ticker.upper()

    # Fetch articles with sentiment for this ticker
    articles_query = """
        SELECT
            na.title,
            na.published_utc,
            na.author,
            na.publisher_name,
            na.article_url,
            ns.sentiment,
            ns.sentiment_reasoning
        FROM news_articles na
        JOIN news_article_tickers nat ON na.id = nat.article_id
        LEFT JOIN news_sentiment ns ON na.id = ns.article_id AND ns.ticker = nat.ticker
        WHERE nat.ticker = %(ticker)s
          AND na.published_utc >= CURRENT_TIMESTAMP - make_interval(days => %(days_back)s)
        ORDER BY na.published_utc DESC
        LIMIT %(max_articles)s
    """

    articles = execute_query(
        articles_query,
        {"ticker": ticker, "days_back": days_back, "max_articles": max_articles},
    )
    for article in articles:
        published = article.get("published_utc")
        if isinstance(published, datetime):
            article["published_utc"] = published.isoformat()

    # Fetch aggregated sentiment counts plus the watermarks that let the
    # caller tell a stale feed from a quiet ticker. The two scalar subqueries
    # are index-backward scans (idx_news_articles_published,
    # idx_news_article_tickers_ticker) and cost well under a millisecond.
    agg_query = """
        SELECT
            COUNT(*) as total_articles,
            COUNT(*) FILTER (WHERE ns.sentiment = 'positive') as positive_count,
            COUNT(*) FILTER (WHERE ns.sentiment = 'negative') as negative_count,
            COUNT(*) FILTER (WHERE ns.sentiment = 'neutral') as neutral_count,
            MAX(na.published_utc) as latest_in_window,
            (SELECT MAX(published_utc) FROM news_articles) as latest_any_ticker,
            (
                SELECT MAX(na2.published_utc)
                FROM news_articles na2
                JOIN news_article_tickers nat2 ON na2.id = nat2.article_id
                WHERE nat2.ticker = %(ticker)s
            ) as latest_for_ticker
        FROM news_articles na
        JOIN news_article_tickers nat ON na.id = nat.article_id
        LEFT JOIN news_sentiment ns ON na.id = ns.article_id AND ns.ticker = nat.ticker
        WHERE nat.ticker = %(ticker)s
          AND na.published_utc >= CURRENT_TIMESTAMP - make_interval(days => %(days_back)s)
    """

    agg_rows = execute_query(agg_query, {"ticker": ticker, "days_back": days_back})
    agg = agg_rows[0] if agg_rows else {
        "total_articles": 0,
        "positive_count": 0,
        "negative_count": 0,
        "neutral_count": 0,
    }

    total = agg["total_articles"] or 0
    positive = agg["positive_count"] or 0
    negative = agg["negative_count"] or 0
    neutral = agg["neutral_count"] or 0

    # Compute sentiment score: range [-1, 1]
    # (positive - negative) / total, or 0 if no articles
    sentiment_score = round((positive - negative) / total, 3) if total > 0 else 0.0

    if sentiment_score > 0.2:
        overall_sentiment = "bullish"
    elif sentiment_score < -0.2:
        overall_sentiment = "bearish"
    else:
        overall_sentiment = "neutral"

    # Freshness: the window is anchored to the wall clock, so when the feed
    # stalls it silently shrinks toward zero. Report the watermarks so a
    # consumer can see that, and warn when the whole feed is stale.
    as_of = datetime.now(timezone.utc)
    window_start = as_of - timedelta(days=days_back)
    latest_any = _as_utc(agg.get("latest_any_ticker"))
    latest_for_ticker = _as_utc(agg.get("latest_for_ticker"))
    latest_in_window = _as_utc(agg.get("latest_in_window"))
    data_age_days = (
        round((as_of - latest_any).total_seconds() / 86400, 1) if latest_any else None
    )
    effective_days_covered = (
        round(max((latest_in_window - window_start).total_seconds() / 86400, 0.0), 1)
        if latest_in_window
        else 0.0
    )

    warnings: list[str] = []
    if latest_any is None:
        warnings.append(
            "news_articles is empty: the daily job has never loaded news, so "
            "sentiment_summary is meaningless."
        )
    elif data_age_days is not None and data_age_days > NEWS_STALE_AFTER_DAYS:
        warnings.append(
            f"News data is stale: the newest article in the database was published "
            f"{latest_any.isoformat()} ({data_age_days} days ago). The {days_back}-day "
            f"window effectively covers {effective_days_covered} day(s), so "
            f"sentiment_summary may not reflect current news. Check that the daily "
            f"job is running (sawa doctor --job watchdog)."
        )

    return {
        "ticker": ticker,
        "days_back": days_back,
        "sentiment_summary": {
            "total_articles": total,
            "positive": positive,
            "negative": negative,
            "neutral": neutral,
            "sentiment_score": sentiment_score,
            "overall_sentiment": overall_sentiment,
        },
        "freshness": {
            "as_of_utc": _iso(as_of),
            "window_start_utc": _iso(window_start),
            "latest_article_utc": _iso(latest_for_ticker),
            "latest_article_in_window_utc": _iso(latest_in_window),
            "latest_article_any_ticker_utc": _iso(latest_any),
            "data_age_days": data_age_days,
            "effective_days_covered": effective_days_covered,
        },
        "articles_in_window": total,
        "articles_returned": len(articles),
        "articles": articles,
        "warnings": warnings,
    }
