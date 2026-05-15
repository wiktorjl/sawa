-- Add ADX, Bollinger Band width %, and dollar-volume SMA to technical_indicators.
--
-- Driver: EOS Swing Analyzer's SMA50 Reclaim scanner needs `adx_14 >= 20`
-- as a universal trend-strength gate. The two bundled columns are cheap
-- to compute alongside (no new TA-Lib calls or external inputs) and
-- pre-empt repeated derivation in scanner SQL.

ALTER TABLE technical_indicators
    ADD COLUMN IF NOT EXISTS adx_14 NUMERIC(10, 6),
    ADD COLUMN IF NOT EXISTS bb_width_pct NUMERIC(10, 6),
    ADD COLUMN IF NOT EXISTS dollar_volume_sma_20 NUMERIC(20, 2);

-- BRIN indexes for screening queries (matches existing pattern in sqlschema/11)
CREATE INDEX IF NOT EXISTS idx_ta_adx_14 ON technical_indicators USING BRIN (adx_14);
CREATE INDEX IF NOT EXISTS idx_ta_bb_width_pct ON technical_indicators USING BRIN (bb_width_pct);
CREATE INDEX IF NOT EXISTS idx_ta_dollar_volume_sma_20 ON technical_indicators USING BRIN (dollar_volume_sma_20);

-- Metadata rows for the dynamic-screener MCP tools
-- (list_technical_indicators / screen_technical_indicators / get_technical_indicators).
INSERT INTO technical_indicator_metadata
    (indicator_name, column_name, category, description, ta_lib_function, params,
     validation_min, validation_max, is_bounded, min_periods_required,
     display_name, unit, sort_order)
VALUES
    ('adx_14', 'adx_14', 'momentum',
     '14-day Average Directional Index (Wilder). Trend strength only — does not indicate direction.',
     'ADX', '{"timeperiod": 14}', 0, 100, TRUE, 27,
     '14-Day ADX', 'ratio', 26),
    ('bb_width_pct', 'bb_width_pct', 'volatility',
     'Bollinger Band width as a percentage of the middle band: (bb_upper - bb_lower) / bb_middle * 100. Squeeze input.',
     'custom', '{"timeperiod": 20, "nbdevup": 2, "nbdevdn": 2}', 0, NULL, FALSE, 20,
     'BB Width %', 'percent', 27),
    ('dollar_volume_sma_20', 'dollar_volume_sma_20', 'volume',
     '20-day average daily dollar volume: SMA(close * volume, 20). Liquidity filter.',
     'custom', '{"timeperiod": 20}', 0, NULL, FALSE, 20,
     '20-Day Dollar Volume SMA', 'dollars', 28)
ON CONFLICT (indicator_name) DO UPDATE SET
    description = EXCLUDED.description,
    min_periods_required = EXCLUDED.min_periods_required,
    display_name = EXCLUDED.display_name,
    sort_order = EXCLUDED.sort_order;
