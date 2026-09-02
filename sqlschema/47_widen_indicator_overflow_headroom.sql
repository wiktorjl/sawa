-- ============================================
-- Give indicator columns headroom for extreme reverse-split histories
-- ============================================
-- Two distinct overflows were rolling back the whole per-ticker technical
-- indicator write (see ta_load.load_technical_indicators):
--
--   * ADTX — NUMERIC(16,4) tops out just under 10^12, the same ceiling as the
--     stock_prices columns it is derived from. A Bollinger band or moving
--     average computed on a ticker whose split-adjusted history peaks near
--     that ceiling (ADTX: 966,556,800,000) exceeds it, because bands add a
--     standard deviation on top of the price. Widening to NUMERIC(20,4) keeps
--     the 4-decimal scale and puts the derived columns four orders of
--     magnitude above their inputs instead of level with them.
--
--   * SMX — bb_width_pct and volume_ratio are unbounded ratios, not the
--     0..100 readings the other NUMERIC(10,6) columns hold. A band width of
--     10053.034392% on a post-reverse-split gap overflowed the 4 integer
--     digits NUMERIC(10,6) allows. NUMERIC(16,6) keeps the 6-decimal scale
--     and allows 10 integer digits.
--
-- rsi_14, rsi_21, and adx_14 stay NUMERIC(10,6): those are bounded to 0..100
-- by construction, so extra integer digits would only hide a calculation bug.
--
-- Postgres widens NUMERIC in place: metadata-only, no table rewrite. Follows
-- the precedent set by 31_widen_indicator_precision.sql.

ALTER TABLE technical_indicators
    ALTER COLUMN sma_5          TYPE NUMERIC(20, 4),
    ALTER COLUMN sma_10         TYPE NUMERIC(20, 4),
    ALTER COLUMN sma_20         TYPE NUMERIC(20, 4),
    ALTER COLUMN sma_50         TYPE NUMERIC(20, 4),
    ALTER COLUMN sma_100        TYPE NUMERIC(20, 4),
    ALTER COLUMN sma_150        TYPE NUMERIC(20, 4),
    ALTER COLUMN sma_200        TYPE NUMERIC(20, 4),
    ALTER COLUMN ema_12         TYPE NUMERIC(20, 4),
    ALTER COLUMN ema_26         TYPE NUMERIC(20, 4),
    ALTER COLUMN ema_50         TYPE NUMERIC(20, 4),
    ALTER COLUMN ema_100        TYPE NUMERIC(20, 4),
    ALTER COLUMN ema_200        TYPE NUMERIC(20, 4),
    ALTER COLUMN vwap           TYPE NUMERIC(20, 4),
    ALTER COLUMN macd_line      TYPE NUMERIC(20, 4),
    ALTER COLUMN macd_signal    TYPE NUMERIC(20, 4),
    ALTER COLUMN macd_histogram TYPE NUMERIC(20, 4),
    ALTER COLUMN bb_upper       TYPE NUMERIC(20, 4),
    ALTER COLUMN bb_middle      TYPE NUMERIC(20, 4),
    ALTER COLUMN bb_lower       TYPE NUMERIC(20, 4),
    ALTER COLUMN atr_14         TYPE NUMERIC(20, 4),
    ALTER COLUMN bb_width_pct   TYPE NUMERIC(16, 6),
    ALTER COLUMN volume_ratio   TYPE NUMERIC(16, 6);
