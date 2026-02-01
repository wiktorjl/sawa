-- ============================================
-- CORE TABLES
-- ============================================

-- Companies/Overviews (central reference table)
CREATE TABLE companies (
    ticker VARCHAR(10) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    market VARCHAR(20),
    type VARCHAR(10),
    locale VARCHAR(10),
    currency_name VARCHAR(10),
    active BOOLEAN DEFAULT TRUE,
    list_date DATE,
    delisted_utc TIMESTAMP,
    primary_exchange VARCHAR(10),
    cik VARCHAR(10),
    composite_figi VARCHAR(20),
    share_class_figi VARCHAR(20),
    sic_code VARCHAR(4),
    sic_description VARCHAR(255),
    market_cap NUMERIC(20, 2),
    weighted_shares_outstanding BIGINT,
    share_class_shares_outstanding BIGINT,
    total_employees INTEGER,
    round_lot INTEGER DEFAULT 100,
    ticker_root VARCHAR(10),
    ticker_suffix VARCHAR(10),
    homepage_url VARCHAR(255),
    phone_number VARCHAR(20),
    address_address1 VARCHAR(255),
    address_city VARCHAR(100),
    address_state VARCHAR(10),
    address_postal_code VARCHAR(20),
    branding_logo_url TEXT,
    branding_icon_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Trigger to auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_companies_updated_at 
    BEFORE UPDATE ON companies 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();
