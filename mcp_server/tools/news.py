"""News sentiment MCP tools."""

import logging
from typing import Any

from ..database import execute_query

logger = logging.getLogger(__name__)


def get_recent_news_sentiment(
    ticker: str,
    days_back: int = 14,
    max_articles: int = 10,
) -> dict[str, Any]:
    """
    Get recent news articles with sentiment analysis for a ticker.

    Args:
        ticker: Stock ticker symbol
        days_back: Number of days to look back (default: 14, max: 90)
        max_articles: Maximum articles to return (default: 10, max: 50)

    Returns:
        Dict with articles list and aggregated sentiment metrics
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

    # Fetch aggregated sentiment counts
    agg_query = """
        SELECT
            COUNT(*) as total_articles,
            COUNT(*) FILTER (WHERE ns.sentiment = 'positive') as positive_count,
            COUNT(*) FILTER (WHERE ns.sentiment = 'negative') as negative_count,
            COUNT(*) FILTER (WHERE ns.sentiment = 'neutral') as neutral_count
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
        "articles": articles,
    }
