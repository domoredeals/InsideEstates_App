-- Land Registry PostgreSQL Schema
-- Optimized for CCOD (Commercial) and OCOD (Overseas) ownership data

-- Enable extensions if not already done
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Main properties table
CREATE TABLE IF NOT EXISTS properties (
    id BIGSERIAL PRIMARY KEY,
    title_number VARCHAR(20) UNIQUE NOT NULL,
    tenure VARCHAR(50),
    property_address TEXT,
    district VARCHAR(100),
    county VARCHAR(100),
    region VARCHAR(100),
    postcode VARCHAR(10),
    multiple_address_indicator CHAR(1),
    price_paid NUMERIC(12,2),
    date_added DATE,
    change_indicator CHAR(1),
    change_date DATE,
    dataset_type VARCHAR(10) NOT NULL, -- 'CCOD' or 'OCOD'
    file_month DATE NOT NULL, -- Track which monthly file this came from
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Proprietors table (owners)
CREATE TABLE IF NOT EXISTS proprietors (
    id BIGSERIAL PRIMARY KEY,
    property_id BIGINT NOT NULL REFERENCES properties(id) ON DELETE CASCADE,
    proprietor_number SMALLINT NOT NULL, -- 1-4 based on column position
    proprietor_name VARCHAR(500),
    company_registration_no VARCHAR(50),
    proprietorship_category VARCHAR(100),
    country_incorporated VARCHAR(100), -- Only for OCOD
    address_1 VARCHAR(500),
    address_2 VARCHAR(500),
    address_3 VARCHAR(500),
    date_proprietor_added DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_property_proprietor UNIQUE(property_id, proprietor_number)
);

-- Company lookup table (deduplicated companies)
CREATE TABLE IF NOT EXISTS companies (
    id BIGSERIAL PRIMARY KEY,
    company_registration_no VARCHAR(50) UNIQUE,
    company_name VARCHAR(500),
    proprietorship_category VARCHAR(100),
    country_incorporated VARCHAR(100),
    first_seen_date DATE,
    last_seen_date DATE,
    property_count INTEGER DEFAULT 0,
    total_value_owned NUMERIC(15,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Import tracking table
CREATE TABLE IF NOT EXISTS import_history (
    id SERIAL PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    dataset_type VARCHAR(10) NOT NULL,
    file_month DATE NOT NULL,
    import_started TIMESTAMP NOT NULL,
    import_completed TIMESTAMP,
    rows_processed INTEGER,
    rows_inserted INTEGER,
    rows_updated INTEGER,
    rows_failed INTEGER,
    status VARCHAR(20) NOT NULL, -- 'running', 'completed', 'failed'
    error_message TEXT,
    CONSTRAINT uk_filename UNIQUE(filename)
);

-- Create indexes for performance
CREATE INDEX idx_properties_title_number ON properties(title_number);
CREATE INDEX idx_properties_postcode ON properties(postcode);
CREATE INDEX idx_properties_district ON properties(district);
CREATE INDEX idx_properties_county ON properties(county);
CREATE INDEX idx_properties_dataset_type ON properties(dataset_type);
CREATE INDEX idx_properties_file_month ON properties(file_month);
CREATE INDEX idx_properties_price_paid ON properties(price_paid) WHERE price_paid IS NOT NULL;

CREATE INDEX idx_proprietors_property_id ON proprietors(property_id);
CREATE INDEX idx_proprietors_company_reg_no ON proprietors(company_registration_no) WHERE company_registration_no IS NOT NULL;
CREATE INDEX idx_proprietors_name ON proprietors(proprietor_name);
CREATE INDEX idx_proprietors_name_trgm ON proprietors USING gin(proprietor_name gin_trgm_ops);

CREATE INDEX idx_companies_registration_no ON companies(company_registration_no);
CREATE INDEX idx_companies_name ON companies(company_name);
CREATE INDEX idx_companies_name_trgm ON companies USING gin(company_name gin_trgm_ops);
CREATE INDEX idx_companies_property_count ON companies(property_count);
CREATE INDEX idx_companies_total_value ON companies(total_value_owned);

-- Function to update company statistics
CREATE OR REPLACE FUNCTION update_company_stats() RETURNS void AS $$
BEGIN
    -- Update company statistics from proprietors data
    WITH company_stats AS (
        SELECT 
            pr.company_registration_no,
            MAX(pr.proprietor_name) as company_name,
            MAX(pr.proprietorship_category) as category,
            MAX(pr.country_incorporated) as country,
            COUNT(DISTINCT p.id) as prop_count,
            SUM(DISTINCT p.price_paid) as total_value,
            MIN(pr.date_proprietor_added) as first_seen,
            MAX(pr.date_proprietor_added) as last_seen
        FROM proprietors pr
        JOIN properties p ON pr.property_id = p.id
        WHERE pr.company_registration_no IS NOT NULL
        GROUP BY pr.company_registration_no
    )
    INSERT INTO companies (
        company_registration_no, 
        company_name, 
        proprietorship_category,
        country_incorporated,
        property_count, 
        total_value_owned,
        first_seen_date,
        last_seen_date
    )
    SELECT 
        company_registration_no,
        company_name,
        category,
        country,
        prop_count,
        total_value,
        first_seen,
        last_seen
    FROM company_stats
    ON CONFLICT (company_registration_no) 
    DO UPDATE SET
        company_name = EXCLUDED.company_name,
        property_count = EXCLUDED.property_count,
        total_value_owned = EXCLUDED.total_value_owned,
        last_seen_date = EXCLUDED.last_seen_date,
        updated_at = CURRENT_TIMESTAMP;
END;
$$ LANGUAGE plpgsql;

-- Trigger to update timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_properties_updated_at BEFORE UPDATE ON properties
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_companies_updated_at BEFORE UPDATE ON companies
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();