-- ============================================
-- EARNINGS TABLE UPDATE FOR YFINANCE DATA
-- ============================================
-- Adjust earnings table to work with yfinance data format

-- Add surprise_pct column
ALTER TABLE earnings ADD COLUMN IF NOT EXISTS surprise_pct NUMERIC(10, 4);

-- Change unique constraint from (ticker, fiscal_year, fiscal_quarter) to (ticker, report_date)
-- First drop the old constraint if it exists
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'earnings_ticker_fiscal_year_fiscal_quarter_key'
    ) THEN
        ALTER TABLE earnings DROP CONSTRAINT earnings_ticker_fiscal_year_fiscal_quarter_key;
    END IF;
END $$;

-- Add new unique constraint on (ticker, report_date)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'earnings_ticker_report_date_key'
    ) THEN
        ALTER TABLE earnings ADD CONSTRAINT earnings_ticker_report_date_key UNIQUE (ticker, report_date);
    END IF;
END $$;

-- Update comment
COMMENT ON TABLE earnings IS 'Earnings calendar and actuals from yfinance';
COMMENT ON COLUMN earnings.surprise_pct IS 'EPS surprise percentage ((actual - estimate) / estimate * 100)';
