-- ============================================
-- NEWS AND SENTIMENT TABLES
-- ============================================

-- News articles with sentiment analysis
CREATE TABLE news_articles (
    id VARCHAR(100) PRIMARY KEY,
    title TEXT NOT NULL,
    author VARCHAR(255),
    description TEXT,
    article_url TEXT NOT NULL,
    image_url TEXT,
    published_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    publisher_name VARCHAR(255),
    publisher_logo_url TEXT,
    publisher_homepage_url TEXT,
    keywords TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Junction table for articles and tickers
CREATE TABLE news_article_tickers (
    article_id VARCHAR(100) NOT NULL REFERENCES news_articles(id) ON DELETE CASCADE,
    ticker VARCHAR(10) NOT NULL,
    PRIMARY KEY (article_id, ticker)
);

-- Sentiment insights per article/ticker
CREATE TABLE news_sentiment (
    article_id VARCHAR(100) NOT NULL REFERENCES news_articles(id) ON DELETE CASCADE,
    ticker VARCHAR(10) NOT NULL,
    sentiment VARCHAR(20),  -- positive, negative, neutral
    sentiment_reasoning TEXT,
    PRIMARY KEY (article_id, ticker)
);

-- Indexes for efficient querying
CREATE INDEX idx_news_articles_published ON news_articles(published_utc DESC);
CREATE INDEX idx_news_article_tickers_ticker ON news_article_tickers(ticker);
CREATE INDEX idx_news_sentiment_ticker ON news_sentiment(ticker);
CREATE INDEX idx_news_sentiment_sentiment ON news_sentiment(sentiment);
