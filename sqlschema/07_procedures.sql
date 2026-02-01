-- ============================================
-- DATA LOADING PROCEDURES
-- ============================================

-- Procedure to load companies from CSV
-- Usage: COPY companies FROM '/path/to/overviews/OVERVIEWS.csv' WITH (FORMAT csv, HEADER true);
-- Or use this procedure with proper path

CREATE OR REPLACE PROCEDURE load_companies_from_csv(file_path TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
    CREATE TEMP TABLE temp_companies (LIKE companies INCLUDING ALL) ON COMMIT DROP;
    
    EXECUTE format('COPY temp_companies (
        ticker, name, description, market, type, locale, currency_name, active,
        list_date, delisted_utc, primary_exchange, cik, composite_figi, share_class_figi,
        sic_code, sic_description, market_cap, weighted_shares_outstanding,
        share_class_shares_outstanding, total_employees, round_lot, ticker_root,
        ticker_suffix, homepage_url, phone_number, address_address1, address_city,
        address_state, address_postal_code, branding_logo_url, branding_icon_url
    ) FROM %L WITH (FORMAT csv, HEADER true)', file_path);
    
    INSERT INTO companies
    SELECT * FROM temp_companies
    ON CONFLICT (ticker) DO UPDATE SET
        name = EXCLUDED.name,
        description = EXCLUDED.description,
        market = EXCLUDED.market,
        active = EXCLUDED.active,
        market_cap = EXCLUDED.market_cap,
        total_employees = EXCLUDED.total_employees,
        updated_at = CURRENT_TIMESTAMP;
    
    COMMIT;
END;
$$;

-- Procedure to load stock prices from CSV
CREATE OR REPLACE PROCEDURE load_stock_prices_from_csv(file_path TEXT, ticker_symbol TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
    CREATE TEMP TABLE temp_prices (
        date DATE,
        symbol VARCHAR(10),
        open NUMERIC(12, 4),
        high NUMERIC(12, 4),
        low NUMERIC(12, 4),
        close NUMERIC(12, 4),
        volume BIGINT
    ) ON COMMIT DROP;
    
    EXECUTE format('COPY temp_prices FROM %L WITH (FORMAT csv, HEADER true)', file_path);
    
    INSERT INTO stock_prices (ticker, date, open, high, low, close, volume)
    SELECT ticker_symbol, date, open, high, low, close, volume
    FROM temp_prices
    ON CONFLICT (ticker, date) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume;
    
    COMMIT;
END;
$$;

-- Procedure to load financial ratios from CSV
CREATE OR REPLACE PROCEDURE load_financial_ratios_from_csv(file_path TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
    CREATE TEMP TABLE temp_ratios (
        ticker VARCHAR(10),
        average_volume BIGINT,
        cash NUMERIC(10, 4),
        cik VARCHAR(10),
        current NUMERIC(10, 4),
        date DATE,
        debt_to_equity NUMERIC(10, 4),
        dividend_yield NUMERIC(10, 6),
        earnings_per_share NUMERIC(12, 4),
        enterprise_value NUMERIC(20, 2),
        ev_to_ebitda NUMERIC(10, 4),
        ev_to_sales NUMERIC(10, 4),
        free_cash_flow NUMERIC(20, 2),
        market_cap NUMERIC(20, 2),
        price NUMERIC(12, 4),
        price_to_book NUMERIC(10, 4),
        price_to_cash_flow NUMERIC(10, 4),
        price_to_earnings NUMERIC(10, 4),
        price_to_free_cash_flow NUMERIC(10, 4),
        price_to_sales NUMERIC(10, 4),
        quick NUMERIC(10, 4),
        return_on_assets NUMERIC(10, 6),
        return_on_equity NUMERIC(10, 6)
    ) ON COMMIT DROP;
    
    EXECUTE format('COPY temp_ratios FROM %L WITH (FORMAT csv, HEADER true)', file_path);
    
    INSERT INTO financial_ratios (
        ticker, date, average_volume, cash, current, debt_to_equity,
        dividend_yield, earnings_per_share, enterprise_value, ev_to_ebitda,
        ev_to_sales, free_cash_flow, market_cap, price, price_to_book,
        price_to_cash_flow, price_to_earnings, price_to_free_cash_flow,
        price_to_sales, quick, return_on_assets, return_on_equity
    )
    SELECT ticker, date, average_volume, cash, current, debt_to_equity,
        dividend_yield, earnings_per_share, enterprise_value, ev_to_ebitda,
        ev_to_sales, free_cash_flow, market_cap, price, price_to_book,
        price_to_cash_flow, price_to_earnings, price_to_free_cash_flow,
        price_to_sales, quick, return_on_assets, return_on_equity
    FROM temp_ratios
    ON CONFLICT (ticker, date) DO UPDATE SET
        average_volume = EXCLUDED.average_volume,
        cash = EXCLUDED.cash,
        current = EXCLUDED.current,
        debt_to_equity = EXCLUDED.debt_to_equity,
        dividend_yield = EXCLUDED.dividend_yield,
        earnings_per_share = EXCLUDED.earnings_per_share,
        enterprise_value = EXCLUDED.enterprise_value,
        ev_to_ebitda = EXCLUDED.ev_to_ebitda,
        ev_to_sales = EXCLUDED.ev_to_sales,
        free_cash_flow = EXCLUDED.free_cash_flow,
        market_cap = EXCLUDED.market_cap,
        price = EXCLUDED.price,
        price_to_book = EXCLUDED.price_to_book,
        price_to_cash_flow = EXCLUDED.price_to_cash_flow,
        price_to_earnings = EXCLUDED.price_to_earnings,
        price_to_free_cash_flow = EXCLUDED.price_to_free_cash_flow,
        price_to_sales = EXCLUDED.price_to_sales,
        quick = EXCLUDED.quick,
        return_on_assets = EXCLUDED.return_on_assets,
        return_on_equity = EXCLUDED.return_on_equity;
    
    COMMIT;
END;
$$;
