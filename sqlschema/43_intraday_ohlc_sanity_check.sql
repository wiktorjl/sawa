-- ============================================
-- stock_prices_intraday OHLC sanity CHECK
-- ============================================
-- Migration 41 added stock_prices_ohlcv_sane to stock_prices as a DB-level
-- backstop against junk OHLCV (zero/negative price, inverted high<low, negative
-- volume), but stock_prices_intraday was left with only NOT NULL + PK + FK
-- constraints. The stock_prices_live view aggregates intraday bars into today's
-- (data_source='intraday') candle (open=first, high=MAX, low=MIN, close=last,
-- volume=SUM) with no value guard, and neither the websocket client nor
-- load_intraday_bars performs any OHLC sanity check — so an out-of-spec intraday
-- bar could propagate into the live daily candle served by get_live_price /
-- get_top_movers during market hours with no DB backstop.
--
-- Add the same CHECK to stock_prices_intraday, mirroring stock_prices_ohlcv_sane
-- exactly (open/high/low/close > 0, high >= low, volume >= 0).
--
-- Added NOT VALID (project pattern, cf. migration 33 FKs): the constraint is
-- enforced for all new/updated rows immediately but is not validated against
-- pre-existing rows, so the migration never fails on any legacy bars. The
-- intraday table is wiped between sessions (empty here), so this is a cheap,
-- forward-looking guard. Validate later with `ALTER TABLE stock_prices_intraday
-- VALIDATE CONSTRAINT stock_prices_intraday_ohlcv_sane;` once known clean.
--
-- Idempotent: the IF NOT EXISTS guard makes re-runs a no-op, and the file is
-- applied by both fresh coldstart and upgrades.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'stock_prices_intraday_ohlcv_sane'
    ) THEN
        ALTER TABLE stock_prices_intraday
            ADD CONSTRAINT stock_prices_intraday_ohlcv_sane
            CHECK (
                open > 0 AND high > 0 AND low > 0 AND close > 0
                AND high >= low
                AND volume >= 0
            )
            NOT VALID;
    END IF;
END $$;
