-- ============================================
-- STOCK CHARACTER CLASSIFICATION SYSTEM
-- ============================================
-- Classifies each stock into one of three regimes (range_bound, trending, boom_bust)
-- using Hurst exponent analysis, then computes regime-specific baselines, flags,
-- and a final ranked scorecard.
--
-- Pipeline stages:
--   Stage 1 (classification) -> stock_character_classification
--   Stage 2 (baseline)       -> stock_character_baseline
--   Stage 3 (flags)          -> stock_character_flags
--   Stage 4 (scorecard)      -> stock_character_scorecard
--
-- All tables keyed on (ticker, run_date) for weekly batch runs.

-- ============================================
-- Stage 1: Classification
-- ============================================
-- One row per ticker per weekly run. Hurst exponents at multiple lookbacks
-- determine the character; regime-specific evidence stored alongside.

CREATE TABLE IF NOT EXISTS stock_character_classification (
    ticker TEXT NOT NULL,
    run_date DATE NOT NULL,
    character TEXT NOT NULL,
    confidence TEXT NOT NULL,
    hurst_3yr NUMERIC(6, 4),
    hurst_2yr NUMERIC(6, 4),
    hurst_1yr NUMERIC(6, 4),
    adx_avg NUMERIC(8, 4),           -- range_bound only
    regression_r2 NUMERIC(6, 4),     -- trending only
    vol_of_vol NUMERIC(6, 4),        -- boom_bust only
    survivorship_flag BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (ticker, run_date),

    CONSTRAINT chk_classification_character
        CHECK (character IN ('range_bound', 'trending', 'boom_bust')),
    CONSTRAINT chk_classification_confidence
        CHECK (confidence IN ('HIGH', 'MEDIUM'))
);

CREATE INDEX IF NOT EXISTS idx_scc_run_date_brin
    ON stock_character_classification USING BRIN (run_date);

CREATE INDEX IF NOT EXISTS idx_scc_ticker_run_date_desc
    ON stock_character_classification (ticker, run_date DESC);

CREATE INDEX IF NOT EXISTS idx_scc_character
    ON stock_character_classification (character);

-- ============================================
-- Stage 2: Baseline
-- ============================================
-- Regime-specific baseline statistics. Common fields (ATR, correlations, volume)
-- plus columns that are NULL when not applicable to the stock's character.

CREATE TABLE IF NOT EXISTS stock_character_baseline (
    ticker TEXT NOT NULL,
    run_date DATE NOT NULL,
    character TEXT NOT NULL,

    -- Common baseline
    atr_baseline NUMERIC(16, 4),
    atr_pct_baseline NUMERIC(8, 6),
    spy_corr_90d_mean NUMERIC(6, 4),
    spy_corr_90d_std NUMERIC(6, 4),
    gld_corr_90d_mean NUMERIC(6, 4),
    tlt_corr_90d_mean NUMERIC(6, 4),
    volume_sma20 BIGINT,

    -- Range-bound specific
    range_high NUMERIC(16, 4),
    range_low NUMERIC(16, 4),
    range_midpoint NUMERIC(16, 4),
    hvn_levels NUMERIC(16, 4)[],     -- high volume node price levels
    lvn_levels NUMERIC(16, 4)[],     -- low volume node price levels
    typical_cycle_days NUMERIC(8, 2),
    volume_profile_source TEXT DEFAULT 'daily_approximation',

    -- Trending specific
    regression_slope NUMERIC(16, 8),
    regression_intercept NUMERIC(16, 8),
    regression_r2 NUMERIC(6, 4),
    residuals_std NUMERIC(16, 4),
    residuals_2std NUMERIC(16, 4),
    expected_price_today NUMERIC(16, 4),

    -- SMA adherence
    sma_150_adherence_ratio NUMERIC(6, 4),   -- touch-and-bounce ratio for 150 SMA
    sma_200_adherence_ratio NUMERIC(6, 4),   -- touch-and-bounce ratio for 200 SMA

    created_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (ticker, run_date),

    CONSTRAINT chk_baseline_character
        CHECK (character IN ('range_bound', 'trending', 'boom_bust'))
);

CREATE INDEX IF NOT EXISTS idx_scb_run_date_brin
    ON stock_character_baseline USING BRIN (run_date);

CREATE INDEX IF NOT EXISTS idx_scb_ticker_run_date_desc
    ON stock_character_baseline (ticker, run_date DESC);

-- ============================================
-- Stage 3: Flags
-- ============================================
-- Individual flags per ticker per run. Each flag records the computed
-- value and the threshold it was compared against.

CREATE TABLE IF NOT EXISTS stock_character_flags (
    ticker TEXT NOT NULL,
    run_date DATE NOT NULL,
    flag TEXT NOT NULL,            -- e.g. 'EXTREMUM_HIGH', 'COMPRESSION', etc.
    value NUMERIC(12, 6),         -- the computed value that triggered the flag
    threshold NUMERIC(12, 6),     -- the threshold it was compared against
    created_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (ticker, run_date, flag)
);

CREATE INDEX IF NOT EXISTS idx_scf_run_date_brin
    ON stock_character_flags USING BRIN (run_date);

CREATE INDEX IF NOT EXISTS idx_scf_ticker_run_date_desc
    ON stock_character_flags (ticker, run_date DESC);

CREATE INDEX IF NOT EXISTS idx_scf_flag
    ON stock_character_flags (flag);

-- ============================================
-- Stage 4: Scorecard
-- ============================================
-- Final ranked alert output. Aggregates classification, baseline, and flags
-- into a single row for screening and prioritisation.

CREATE TABLE IF NOT EXISTS stock_character_scorecard (
    ticker TEXT NOT NULL,
    run_date DATE NOT NULL,
    character TEXT NOT NULL,
    confidence TEXT NOT NULL,
    current_price NUMERIC(10, 4),
    price_percentile NUMERIC(6, 2),   -- NULL for non-range stocks
    sigma_distance NUMERIC(6, 2),     -- NULL for non-trending stocks
    flag_count INTEGER NOT NULL DEFAULT 0,
    flags TEXT[],                      -- array of flag names
    atr_ratio NUMERIC(6, 4),
    spy_corr_recent NUMERIC(6, 4),
    spy_corr_baseline NUMERIC(6, 4),
    at_hvn BOOLEAN DEFAULT FALSE,
    in_lvn BOOLEAN DEFAULT FALSE,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (ticker, run_date),

    CONSTRAINT chk_scorecard_character
        CHECK (character IN ('range_bound', 'trending', 'boom_bust')),
    CONSTRAINT chk_scorecard_confidence
        CHECK (confidence IN ('HIGH', 'MEDIUM'))
);

CREATE INDEX IF NOT EXISTS idx_scs_run_date_brin
    ON stock_character_scorecard USING BRIN (run_date);

CREATE INDEX IF NOT EXISTS idx_scs_ticker_run_date_desc
    ON stock_character_scorecard (ticker, run_date DESC);

CREATE INDEX IF NOT EXISTS idx_scs_flag_count_confidence
    ON stock_character_scorecard (flag_count DESC, confidence);
