"""Tests for news sentiment MCP tool."""

from unittest.mock import patch

from mcp_server.tools.news import get_recent_news_sentiment


def _make_article(
    title: str,
    sentiment: str | None,
    published_utc: str = "2026-02-15T10:00:00+00:00",
    reasoning: str | None = None,
) -> dict:
    return {
        "title": title,
        "published_utc": published_utc,
        "author": "Test Author",
        "publisher_name": "Test Publisher",
        "sentiment": sentiment,
        "sentiment_reasoning": reasoning,
    }


def _make_agg(total: int, positive: int, negative: int, neutral: int) -> list[dict]:
    return [{
        "total_articles": total,
        "positive_count": positive,
        "negative_count": negative,
        "neutral_count": neutral,
    }]


class TestGetRecentNewsSentiment:
    """Tests for get_recent_news_sentiment."""

    @patch("mcp_server.tools.news.execute_query")
    def test_basic_positive_sentiment(self, mock_execute):
        """Test with mostly positive articles."""
        articles = [
            _make_article("Stock surges on earnings beat", "positive"),
            _make_article("Analyst upgrades ticker", "positive"),
            _make_article("Market holds steady", "neutral"),
        ]
        agg = _make_agg(total=3, positive=2, negative=0, neutral=1)

        mock_execute.side_effect = [articles, agg]

        result = get_recent_news_sentiment("AAPL")

        assert result["ticker"] == "AAPL"
        assert result["days_back"] == 14
        assert len(result["articles"]) == 3
        summary = result["sentiment_summary"]
        assert summary["total_articles"] == 3
        assert summary["positive"] == 2
        assert summary["negative"] == 0
        assert summary["neutral"] == 1
        assert summary["sentiment_score"] > 0
        assert summary["overall_sentiment"] == "bullish"

    @patch("mcp_server.tools.news.execute_query")
    def test_basic_negative_sentiment(self, mock_execute):
        """Test with mostly negative articles."""
        articles = [
            _make_article("Stock drops on weak guidance", "negative"),
            _make_article("Downgrade from analyst", "negative"),
            _make_article("Concerns about revenue", "negative"),
            _make_article("Market flat today", "neutral"),
        ]
        agg = _make_agg(total=4, positive=0, negative=3, neutral=1)

        mock_execute.side_effect = [articles, agg]

        result = get_recent_news_sentiment("TSLA", days_back=7)

        assert result["ticker"] == "TSLA"
        assert result["days_back"] == 7
        summary = result["sentiment_summary"]
        assert summary["negative"] == 3
        assert summary["sentiment_score"] < 0
        assert summary["overall_sentiment"] == "bearish"

    @patch("mcp_server.tools.news.execute_query")
    def test_neutral_sentiment(self, mock_execute):
        """Test with balanced sentiment."""
        articles = [
            _make_article("Stock rises slightly", "positive"),
            _make_article("Stock dips slightly", "negative"),
        ]
        agg = _make_agg(total=2, positive=1, negative=1, neutral=0)

        mock_execute.side_effect = [articles, agg]

        result = get_recent_news_sentiment("MSFT")

        summary = result["sentiment_summary"]
        assert summary["sentiment_score"] == 0.0
        assert summary["overall_sentiment"] == "neutral"

    @patch("mcp_server.tools.news.execute_query")
    def test_no_articles(self, mock_execute):
        """Test when no articles exist."""
        mock_execute.side_effect = [[], _make_agg(0, 0, 0, 0)]

        result = get_recent_news_sentiment("XYZ")

        assert result["articles"] == []
        summary = result["sentiment_summary"]
        assert summary["total_articles"] == 0
        assert summary["sentiment_score"] == 0.0
        assert summary["overall_sentiment"] == "neutral"

    @patch("mcp_server.tools.news.execute_query")
    def test_ticker_normalized_to_uppercase(self, mock_execute):
        """Test that ticker is normalized to uppercase."""
        mock_execute.side_effect = [[], _make_agg(0, 0, 0, 0)]

        result = get_recent_news_sentiment("aapl")

        assert result["ticker"] == "AAPL"
        # Check both queries received uppercase ticker
        for call in mock_execute.call_args_list:
            params = call[0][1]
            assert params["ticker"] == "AAPL"

    @patch("mcp_server.tools.news.execute_query")
    def test_days_back_clamped(self, mock_execute):
        """Test that days_back is clamped to valid range."""
        mock_execute.side_effect = [[], _make_agg(0, 0, 0, 0)]

        result = get_recent_news_sentiment("AAPL", days_back=200)
        assert result["days_back"] == 90

        mock_execute.side_effect = [[], _make_agg(0, 0, 0, 0)]
        result = get_recent_news_sentiment("AAPL", days_back=0)
        assert result["days_back"] == 1

    @patch("mcp_server.tools.news.execute_query")
    def test_max_articles_clamped(self, mock_execute):
        """Test that max_articles is clamped to valid range."""
        mock_execute.side_effect = [[], _make_agg(0, 0, 0, 0)]

        get_recent_news_sentiment("AAPL", max_articles=100)
        # Check the LIMIT param passed to articles query
        params = mock_execute.call_args_list[0][0][1]
        assert params["max_articles"] == 50

    @patch("mcp_server.tools.news.execute_query")
    def test_articles_with_no_sentiment(self, mock_execute):
        """Test articles that have no sentiment record (LEFT JOIN null)."""
        articles = [
            _make_article("Some article without sentiment", None),
        ]
        agg = _make_agg(total=1, positive=0, negative=0, neutral=0)

        mock_execute.side_effect = [articles, agg]

        result = get_recent_news_sentiment("GOOG")

        assert len(result["articles"]) == 1
        assert result["articles"][0]["sentiment"] is None
        summary = result["sentiment_summary"]
        assert summary["total_articles"] == 1
        assert summary["sentiment_score"] == 0.0
        assert summary["overall_sentiment"] == "neutral"

    @patch("mcp_server.tools.news.execute_query")
    def test_sentiment_score_calculation(self, mock_execute):
        """Test sentiment score edge cases."""
        # All positive
        mock_execute.side_effect = [
            [_make_article("Good news", "positive")] * 5,
            _make_agg(total=5, positive=5, negative=0, neutral=0),
        ]
        result = get_recent_news_sentiment("AAPL")
        assert result["sentiment_summary"]["sentiment_score"] == 1.0
        assert result["sentiment_summary"]["overall_sentiment"] == "bullish"

        # All negative
        mock_execute.side_effect = [
            [_make_article("Bad news", "negative")] * 5,
            _make_agg(total=5, positive=0, negative=5, neutral=0),
        ]
        result = get_recent_news_sentiment("AAPL")
        assert result["sentiment_summary"]["sentiment_score"] == -1.0
        assert result["sentiment_summary"]["overall_sentiment"] == "bearish"

    @patch("mcp_server.tools.news.execute_query")
    def test_empty_agg_result(self, mock_execute):
        """Test when aggregation query returns empty (edge case)."""
        mock_execute.side_effect = [[], []]

        result = get_recent_news_sentiment("AAPL")

        summary = result["sentiment_summary"]
        assert summary["total_articles"] == 0
        assert summary["sentiment_score"] == 0.0
