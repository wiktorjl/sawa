-- Technical indicators table
-- Stores daily calculated indicators for all stocks using ta-lib
-- Single wide table design optimized for screening queries (cross-indicator filters)

CREATE TABLE technical_indicators (
    ticker VARCHAR(10) NOT NULL REFERENCES companies(ticker) ON DELETE CASCADE,
    date DATE NOT NULL,
    
    -- Trend indicators (8)
    -- NULLs allowed for insufficient historical data (first ~50 days)
    sma_5 NUMERIC(12, 4),
    sma_10 NUMERIC(12, 4),
    sma_20 NUMERIC(12, 4),
    sma_50 NUMERIC(12, 4),
    ema_12 NUMERIC(12, 4),
    ema_26 NUMERIC(12, 4),
    ema_50 NUMERIC(12, 4),
    vwap NUMERIC(12, 4),
    
    -- Momentum indicators (5)
    rsi_14 NUMERIC(10, 6),
    rsi_21 NUMERIC(10, 6),
    macd_line NUMERIC(12, 4),
    macd_signal NUMERIC(12, 4),
    macd_histogram NUMERIC(12, 4),
    
    -- Volatility indicators (4)
    bb_upper NUMERIC(12, 4),
    bb_middle NUMERIC(12, 4),
    bb_lower NUMERIC(12, 4),
    atr_14 NUMERIC(12, 4),
    
    -- Volume indicators (3)
    -- BIGINT safe: max ~10B << 9.2 quintillion limit
    -- Assumes post-split volume reporting
    obv BIGINT,
    volume_sma_20 BIGINT,
    volume_ratio NUMERIC(10, 6),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, date)
);

-- Basic indexes for lookup
CREATE INDEX idx_ta_date ON technical_indicators(date);
CREATE INDEX idx_ta_ticker_date_desc ON technical_indicators(ticker, date DESC);

-- BRIN indexes for screening queries (optimal for time-series range scans)
-- BRIN is smaller and faster than B-tree for sequential date-ordered data
CREATE INDEX idx_ta_rsi_14 ON technical_indicators USING BRIN (rsi_14);
CREATE INDEX idx_ta_rsi_21 ON technical_indicators USING BRIN (rsi_21);
CREATE INDEX idx_ta_atr_14 ON technical_indicators USING BRIN (atr_14);
CREATE INDEX idx_ta_macd_line ON technical_indicators USING BRIN (macd_line);
CREATE INDEX idx_ta_volume_ratio ON technical_indicators USING BRIN (volume_ratio);


-- Metadata registry for dynamic query building and API responses
CREATE TABLE technical_indicator_metadata (
    indicator_name VARCHAR(50) PRIMARY KEY,
    column_name VARCHAR(50) NOT NULL,
    category VARCHAR(30) NOT NULL,  -- 'trend', 'momentum', 'volatility', 'volume'
    description TEXT,
    ta_lib_function VARCHAR(50),
    params JSONB,
    validation_min NUMERIC(10, 6),
    validation_max NUMERIC(10, 6),
    is_bounded BOOLEAN DEFAULT FALSE,  -- TRUE for bounded indicators (RSI 0-100)
    min_periods_required INTEGER,       -- Minimum data points needed for calculation
    display_name VARCHAR(100),          -- Human-readable name for UI
    unit VARCHAR(20),                   -- 'percent', 'dollars', 'ratio', 'count'
    sort_order INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Populate metadata for initial 20 indicators
INSERT INTO technical_indicator_metadata 
    (indicator_name, column_name, category, description, ta_lib_function, params,
     validation_min, validation_max, is_bounded, min_periods_required, 
     display_name, unit, sort_order)
VALUES
    -- Trend indicators
    ('sma_5', 'sma_5', 'trend', '5-day Simple Moving Average', 
     'SMA', '{"timeperiod": 5}', NULL, NULL, FALSE, 5, 
     '5-Day SMA', 'dollars', 1),
    ('sma_10', 'sma_10', 'trend', '10-day Simple Moving Average', 
     'SMA', '{"timeperiod": 10}', NULL, NULL, FALSE, 10, 
     '10-Day SMA', 'dollars', 2),
    ('sma_20', 'sma_20', 'trend', '20-day Simple Moving Average', 
     'SMA', '{"timeperiod": 20}', NULL, NULL, FALSE, 20, 
     '20-Day SMA', 'dollars', 3),
    ('sma_50', 'sma_50', 'trend', '50-day Simple Moving Average', 
     'SMA', '{"timeperiod": 50}', NULL, NULL, FALSE, 50, 
     '50-Day SMA', 'dollars', 4),
    ('ema_12', 'ema_12', 'trend', '12-day Exponential Moving Average', 
     'EMA', '{"timeperiod": 12}', NULL, NULL, FALSE, 12, 
     '12-Day EMA', 'dollars', 5),
    ('ema_26', 'ema_26', 'trend', '26-day Exponential Moving Average', 
     'EMA', '{"timeperiod": 26}', NULL, NULL, FALSE, 26, 
     '26-Day EMA', 'dollars', 6),
    ('ema_50', 'ema_50', 'trend', '50-day Exponential Moving Average', 
     'EMA', '{"timeperiod": 50}', NULL, NULL, FALSE, 50, 
     '50-Day EMA', 'dollars', 7),
    ('vwap', 'vwap', 'trend', 'Volume Weighted Average Price (cumulative)', 
     'custom', '{}', NULL, NULL, FALSE, 1, 
     'VWAP', 'dollars', 8),
    
    -- Momentum indicators
    ('rsi_14', 'rsi_14', 'momentum', '14-day Relative Strength Index', 
     'RSI', '{"timeperiod": 14}', 0, 100, TRUE, 14, 
     '14-Day RSI', 'percent', 9),
    ('rsi_21', 'rsi_21', 'momentum', '21-day Relative Strength Index', 
     'RSI', '{"timeperiod": 21}', 0, 100, TRUE, 21, 
     '21-Day RSI', 'percent', 10),
    ('macd_line', 'macd_line', 'momentum', 'MACD Line (12-26 EMA difference)', 
     'MACD', '{"fastperiod": 12, "slowperiod": 26, "signalperiod": 9}', NULL, NULL, FALSE, 26, 
     'MACD Line', 'dollars', 11),
    ('macd_signal', 'macd_signal', 'momentum', 'MACD Signal Line (9-day EMA of MACD)', 
     'MACD', '{"fastperiod": 12, "slowperiod": 26, "signalperiod": 9}', NULL, NULL, FALSE, 35, 
     'MACD Signal', 'dollars', 12),
    ('macd_histogram', 'macd_histogram', 'momentum', 'MACD Histogram (MACD minus Signal)', 
     'MACD', '{"fastperiod": 12, "slowperiod": 26, "signalperiod": 9}', NULL, NULL, FALSE, 35, 
     'MACD Histogram', 'dollars', 13),
    
    -- Volatility indicators
    ('bb_upper', 'bb_upper', 'volatility', 'Bollinger Band Upper (20-day SMA + 2 std)', 
     'BBANDS', '{"timeperiod": 20, "nbdevup": 2, "nbdevdn": 2}', NULL, NULL, FALSE, 20, 
     'BB Upper', 'dollars', 14),
    ('bb_middle', 'bb_middle', 'volatility', 'Bollinger Band Middle (20-day SMA)', 
     'BBANDS', '{"timeperiod": 20, "nbdevup": 2, "nbdevdn": 2}', NULL, NULL, FALSE, 20, 
     'BB Middle', 'dollars', 15),
    ('bb_lower', 'bb_lower', 'volatility', 'Bollinger Band Lower (20-day SMA - 2 std)', 
     'BBANDS', '{"timeperiod": 20, "nbdevup": 2, "nbdevdn": 2}', NULL, NULL, FALSE, 20, 
     'BB Lower', 'dollars', 16),
    ('atr_14', 'atr_14', 'volatility', '14-day Average True Range', 
     'ATR', '{"timeperiod": 14}', 0, NULL, FALSE, 14, 
     '14-Day ATR', 'dollars', 17),
    
    -- Volume indicators
    ('obv', 'obv', 'volume', 'On Balance Volume (cumulative, can be negative)', 
     'OBV', '{}', NULL, NULL, FALSE, 1, 
     'OBV', 'count', 18),
    ('volume_sma_20', 'volume_sma_20', 'volume', '20-day Volume Simple Moving Average', 
     'SMA', '{"timeperiod": 20}', 0, NULL, FALSE, 20, 
     '20-Day Volume SMA', 'count', 19),
    ('volume_ratio', 'volume_ratio', 'volume', 'Volume Ratio (today volume / 20-day avg)', 
     'custom', '{}', 0, NULL, FALSE, 20, 
     'Volume Ratio', 'ratio', 20);
