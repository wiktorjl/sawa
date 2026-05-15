-- Widen NUMERIC(12,4) → NUMERIC(16,4) on price-shaped indicator columns
-- in technical_indicators, matching the precedent set by
-- 24_widen_price_precision.sql for stock_prices.
--
-- NUMERIC(12,4) tops out at $99,999,999.9999 (~$10^8). Any moving
-- average / band / ATR computed on a corrupted high-price input
-- (typically a microcap with multiple compounded reverse splits where
-- Polygon's adjusted-prices API mis-applies the cumulative ratio)
-- overflows the column and the row fails to upsert.
--
-- NUMERIC(16,4) supports up to 999,999,999,999.9999 (~$10^12) — the
-- same headroom the underlying stock_prices columns already have.
-- Postgres widens NUMERIC in place: metadata-only, no table rewrite.

ALTER TABLE technical_indicators
    ALTER COLUMN sma_5         TYPE NUMERIC(16, 4),
    ALTER COLUMN sma_10        TYPE NUMERIC(16, 4),
    ALTER COLUMN sma_20        TYPE NUMERIC(16, 4),
    ALTER COLUMN sma_50        TYPE NUMERIC(16, 4),
    ALTER COLUMN sma_100       TYPE NUMERIC(16, 4),
    ALTER COLUMN sma_150       TYPE NUMERIC(16, 4),
    ALTER COLUMN sma_200       TYPE NUMERIC(16, 4),
    ALTER COLUMN ema_12        TYPE NUMERIC(16, 4),
    ALTER COLUMN ema_26        TYPE NUMERIC(16, 4),
    ALTER COLUMN ema_50        TYPE NUMERIC(16, 4),
    ALTER COLUMN ema_100       TYPE NUMERIC(16, 4),
    ALTER COLUMN ema_200       TYPE NUMERIC(16, 4),
    ALTER COLUMN vwap          TYPE NUMERIC(16, 4),
    ALTER COLUMN macd_line     TYPE NUMERIC(16, 4),
    ALTER COLUMN macd_signal   TYPE NUMERIC(16, 4),
    ALTER COLUMN macd_histogram TYPE NUMERIC(16, 4),
    ALTER COLUMN bb_upper      TYPE NUMERIC(16, 4),
    ALTER COLUMN bb_middle     TYPE NUMERIC(16, 4),
    ALTER COLUMN bb_lower      TYPE NUMERIC(16, 4),
    ALTER COLUMN atr_14        TYPE NUMERIC(16, 4);
