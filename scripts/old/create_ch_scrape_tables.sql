-- Create tables for Companies House web scraping data
-- These tables will store scraped data for companies that couldn't be matched in our existing database

-- Main scraping control table - tracks what needs to be scraped and status
CREATE TABLE IF NOT EXISTS ch_scrape_queue (
    id SERIAL PRIMARY KEY,
    search_name TEXT NOT NULL UNIQUE,  -- The company name we're searching for
    found_name TEXT,                   -- The name found on Companies House
    company_number TEXT,               -- Companies House number if found
    company_url TEXT,                  -- Full URL to company page
    search_status TEXT DEFAULT 'pending',  -- pending, searching, found, not_found, error
    search_timestamp TIMESTAMPTZ,
    search_error TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Table for company overview data
CREATE TABLE IF NOT EXISTS ch_scrape_overview (
    id SERIAL PRIMARY KEY,
    company_number TEXT NOT NULL UNIQUE,
    company_url TEXT NOT NULL,
    raw_html BYTEA,                    -- Compressed HTML content
    -- Parsed fields from overview page
    company_name TEXT,
    company_status TEXT,
    incorporation_date DATE,
    company_type TEXT,
    registered_office_address TEXT,
    sic_codes TEXT[],                  -- Array of SIC codes
    previous_names TEXT[],             -- Array of previous company names
    accounts_next_due DATE,
    confirmation_statement_next_due DATE,
    -- Scraping metadata
    scrape_status TEXT DEFAULT 'pending',  -- pending, scraped, parsed, error
    scrape_timestamp TIMESTAMPTZ,
    parse_timestamp TIMESTAMPTZ,
    scrape_error TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Table for officers data
CREATE TABLE IF NOT EXISTS ch_scrape_officers (
    id SERIAL PRIMARY KEY,
    company_number TEXT NOT NULL,
    officer_id TEXT,                   -- Unique ID for the officer appointment
    raw_html BYTEA,                    -- Compressed HTML for this officer
    -- Parsed fields
    officer_name TEXT,
    officer_role TEXT,
    appointed_date DATE,
    resigned_date DATE,
    nationality TEXT,
    country_of_residence TEXT,
    occupation TEXT,
    date_of_birth_year INTEGER,
    date_of_birth_month INTEGER,
    address TEXT,
    -- Scraping metadata
    scrape_status TEXT DEFAULT 'pending',
    scrape_timestamp TIMESTAMPTZ,
    parse_timestamp TIMESTAMPTZ,
    scrape_error TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(company_number, officer_id)
);

-- Table for charges/mortgages data
CREATE TABLE IF NOT EXISTS ch_scrape_charges (
    id SERIAL PRIMARY KEY,
    company_number TEXT NOT NULL,
    charge_id TEXT,
    raw_html BYTEA,                    -- Compressed HTML content
    -- Parsed fields
    charge_status TEXT,
    charge_type TEXT,
    delivered_date DATE,
    created_date DATE,
    satisfied_date DATE,
    amount TEXT,
    persons_entitled TEXT[],           -- Array of persons/entities entitled
    brief_description TEXT,
    -- Scraping metadata
    scrape_status TEXT DEFAULT 'pending',
    scrape_timestamp TIMESTAMPTZ,
    parse_timestamp TIMESTAMPTZ,
    scrape_error TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(company_number, charge_id)
);

-- Table for insolvency data
CREATE TABLE IF NOT EXISTS ch_scrape_insolvency (
    id SERIAL PRIMARY KEY,
    company_number TEXT NOT NULL UNIQUE,
    raw_html BYTEA,                    -- Compressed HTML content
    -- Parsed fields
    has_insolvency_history BOOLEAN DEFAULT FALSE,
    insolvency_cases JSONB,            -- JSON array of insolvency case details
    -- Scraping metadata
    scrape_status TEXT DEFAULT 'pending',
    scrape_timestamp TIMESTAMPTZ,
    parse_timestamp TIMESTAMPTZ,
    scrape_error TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_ch_scrape_queue_status ON ch_scrape_queue(search_status);
CREATE INDEX IF NOT EXISTS idx_ch_scrape_queue_company_number ON ch_scrape_queue(company_number);
CREATE INDEX IF NOT EXISTS idx_ch_scrape_overview_status ON ch_scrape_overview(scrape_status);
CREATE INDEX IF NOT EXISTS idx_ch_scrape_officers_company ON ch_scrape_officers(company_number);
CREATE INDEX IF NOT EXISTS idx_ch_scrape_charges_company ON ch_scrape_charges(company_number);

-- Trigger to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply the trigger to all tables
CREATE TRIGGER update_ch_scrape_queue_updated_at BEFORE UPDATE ON ch_scrape_queue
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_ch_scrape_overview_updated_at BEFORE UPDATE ON ch_scrape_overview
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_ch_scrape_officers_updated_at BEFORE UPDATE ON ch_scrape_officers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_ch_scrape_charges_updated_at BEFORE UPDATE ON ch_scrape_charges
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_ch_scrape_insolvency_updated_at BEFORE UPDATE ON ch_scrape_insolvency
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();