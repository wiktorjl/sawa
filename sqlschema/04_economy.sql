-- ============================================
-- ECONOMY DATA TABLES
-- ============================================

-- Treasury yields
CREATE TABLE treasury_yields (
    date DATE PRIMARY KEY,
    yield_1_month NUMERIC(8, 4),
    yield_3_month NUMERIC(8, 4),
    yield_6_month NUMERIC(8, 4),
    yield_1_year NUMERIC(8, 4),
    yield_2_year NUMERIC(8, 4),
    yield_3_year NUMERIC(8, 4),
    yield_5_year NUMERIC(8, 4),
    yield_7_year NUMERIC(8, 4),
    yield_10_year NUMERIC(8, 4),
    yield_20_year NUMERIC(8, 4),
    yield_30_year NUMERIC(8, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Inflation metrics
CREATE TABLE inflation (
    date DATE PRIMARY KEY,
    cpi NUMERIC(12, 3),
    cpi_core NUMERIC(12, 3),
    cpi_year_over_year NUMERIC(8, 6),
    pce NUMERIC(12, 3),
    pce_core NUMERIC(12, 3),
    pce_spending NUMERIC(20, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Inflation expectations
CREATE TABLE inflation_expectations (
    date DATE PRIMARY KEY,
    market_5_year NUMERIC(8, 4),
    market_10_year NUMERIC(8, 4),
    forward_years_5_to_10 NUMERIC(8, 4),
    model_1_year NUMERIC(8, 4),
    model_5_year NUMERIC(8, 4),
    model_10_year NUMERIC(8, 4),
    model_30_year NUMERIC(8, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Labor market indicators
CREATE TABLE labor_market (
    date DATE PRIMARY KEY,
    unemployment_rate NUMERIC(6, 4),
    labor_force_participation_rate NUMERIC(6, 4),
    avg_hourly_earnings NUMERIC(10, 4),
    job_openings NUMERIC(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
