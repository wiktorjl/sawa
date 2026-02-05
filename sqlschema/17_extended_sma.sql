-- Extended SMA and EMA indicators
-- Adds SMA-100, SMA-150, SMA-200 and EMA-100, EMA-200
-- These are commonly used for longer-term trend analysis

-- Add new columns to technical_indicators table
ALTER TABLE technical_indicators
    ADD COLUMN IF NOT EXISTS sma_100 NUMERIC(12, 4),
    ADD COLUMN IF NOT EXISTS sma_150 NUMERIC(12, 4),
    ADD COLUMN IF NOT EXISTS sma_200 NUMERIC(12, 4),
    ADD COLUMN IF NOT EXISTS ema_100 NUMERIC(12, 4),
    ADD COLUMN IF NOT EXISTS ema_200 NUMERIC(12, 4);

-- Add BRIN indexes for screening queries on new columns
CREATE INDEX IF NOT EXISTS idx_ta_sma_100 ON technical_indicators USING BRIN (sma_100);
CREATE INDEX IF NOT EXISTS idx_ta_sma_150 ON technical_indicators USING BRIN (sma_150);
CREATE INDEX IF NOT EXISTS idx_ta_sma_200 ON technical_indicators USING BRIN (sma_200);

-- Add metadata for new indicators
INSERT INTO technical_indicator_metadata 
    (indicator_name, column_name, category, description, ta_lib_function, params,
     validation_min, validation_max, is_bounded, min_periods_required, 
     display_name, unit, sort_order)
VALUES
    ('sma_100', 'sma_100', 'trend', '100-day Simple Moving Average', 
     'SMA', '{"timeperiod": 100}', NULL, NULL, FALSE, 100, 
     '100-Day SMA', 'dollars', 21),
    ('sma_150', 'sma_150', 'trend', '150-day Simple Moving Average (6-month trend)', 
     'SMA', '{"timeperiod": 150}', NULL, NULL, FALSE, 150, 
     '150-Day SMA', 'dollars', 22),
    ('sma_200', 'sma_200', 'trend', '200-day Simple Moving Average (long-term trend)', 
     'SMA', '{"timeperiod": 200}', NULL, NULL, FALSE, 200, 
     '200-Day SMA', 'dollars', 23),
    ('ema_100', 'ema_100', 'trend', '100-day Exponential Moving Average', 
     'EMA', '{"timeperiod": 100}', NULL, NULL, FALSE, 100, 
     '100-Day EMA', 'dollars', 24),
    ('ema_200', 'ema_200', 'trend', '200-day Exponential Moving Average (long-term trend)', 
     'EMA', '{"timeperiod": 200}', NULL, NULL, FALSE, 200, 
     '200-Day EMA', 'dollars', 25)
ON CONFLICT (indicator_name) DO UPDATE SET
    description = EXCLUDED.description,
    min_periods_required = EXCLUDED.min_periods_required,
    display_name = EXCLUDED.display_name,
    sort_order = EXCLUDED.sort_order;
