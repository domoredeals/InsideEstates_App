-- Create optimized views for QlikView

-- Summary view for properties with calculated fields
CREATE OR REPLACE VIEW qv_property_summary AS
SELECT 
    p.title_number,
    p.tenure,
    p.property_address,
    p.postcode,
    LEFT(p.postcode, 
        CASE 
            WHEN LENGTH(p.postcode) - LENGTH(REPLACE(p.postcode, ' ', '')) > 0 
            THEN POSITION(' ' IN p.postcode) - 1
            ELSE LENGTH(p.postcode)
        END
    ) as postcode_area,
    p.district,
    p.county,
    p.region,
    p.price_paid,
    p.dataset_type,
    p.change_date,
    p.change_indicator,
    EXTRACT(YEAR FROM p.change_date) as change_year,
    EXTRACT(MONTH FROM p.change_date) as change_month,
    TO_CHAR(p.change_date, 'YYYY-MM') as year_month,
    CASE 
        WHEN p.price_paid IS NOT NULL THEN 
            CASE 
                WHEN p.price_paid < 250000 THEN '1. Under £250k'
                WHEN p.price_paid < 500000 THEN '2. £250k-£500k'
                WHEN p.price_paid < 1000000 THEN '3. £500k-£1m'
                WHEN p.price_paid < 5000000 THEN '4. £1m-£5m'
                ELSE '5. Over £5m'
            END
        ELSE '0. No Price'
    END as price_band,
    CASE 
        WHEN p.multiple_address_indicator = 'Y' THEN 'Multiple'
        ELSE 'Single'
    END as address_type
FROM properties p;

-- Monthly aggregates for performance
CREATE OR REPLACE VIEW qv_monthly_stats AS
SELECT 
    dataset_type,
    file_month,
    TO_CHAR(file_month, 'YYYY-MM') as year_month,
    EXTRACT(YEAR FROM file_month) as year,
    EXTRACT(MONTH FROM file_month) as month,
    COUNT(*) as property_count,
    COUNT(price_paid) as properties_with_price,
    ROUND(AVG(price_paid)::numeric, 0) as avg_price,
    SUM(price_paid) as total_value,
    COUNT(DISTINCT district) as unique_districts,
    COUNT(DISTINCT LEFT(postcode, POSITION(' ' IN postcode || ' ') - 1)) as unique_postcode_areas
FROM properties
GROUP BY dataset_type, file_month;

-- Geographic aggregates
CREATE OR REPLACE VIEW qv_geographic_stats AS
SELECT 
    dataset_type,
    region,
    county,
    district,
    COUNT(*) as property_count,
    COUNT(price_paid) as properties_with_price,
    ROUND(AVG(price_paid)::numeric, 0) as avg_price,
    MIN(price_paid) as min_price,
    MAX(price_paid) as max_price,
    COUNT(DISTINCT postcode) as unique_postcodes,
    COUNT(DISTINCT title_number) as unique_properties
FROM properties
WHERE region != '' AND county != ''
GROUP BY dataset_type, region, county, district;

-- Company statistics (once proprietor data is loaded)
CREATE OR REPLACE VIEW qv_company_stats AS
SELECT 
    c.company_name,
    c.company_registration_no,
    c.proprietorship_category,
    c.country_incorporated,
    c.property_count,
    c.total_value_owned,
    c.first_seen_date,
    c.last_seen_date,
    CASE 
        WHEN c.country_incorporated IS NOT NULL THEN 'Overseas'
        ELSE 'UK'
    END as company_type,
    CASE 
        WHEN c.property_count = 1 THEN '1. Single Property'
        WHEN c.property_count <= 5 THEN '2. 2-5 Properties'
        WHEN c.property_count <= 10 THEN '3. 6-10 Properties'
        WHEN c.property_count <= 50 THEN '4. 11-50 Properties'
        WHEN c.property_count <= 100 THEN '5. 51-100 Properties'
        ELSE '6. Over 100 Properties'
    END as portfolio_size
FROM companies c
WHERE c.property_count > 0;

-- Price trends by region and year
CREATE OR REPLACE VIEW qv_price_trends AS
SELECT 
    dataset_type,
    region,
    EXTRACT(YEAR FROM change_date) as year,
    COUNT(*) as transaction_count,
    COUNT(price_paid) as priced_transactions,
    ROUND(AVG(price_paid)::numeric, 0) as avg_price,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_paid)::numeric, 0) as median_price,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price_paid)::numeric, 0) as price_25th_percentile,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price_paid)::numeric, 0) as price_75th_percentile
FROM properties
WHERE price_paid IS NOT NULL 
    AND price_paid > 0 
    AND region != ''
    AND EXTRACT(YEAR FROM change_date) >= 2018
GROUP BY dataset_type, region, EXTRACT(YEAR FROM change_date);

-- Recent activity view
CREATE OR REPLACE VIEW qv_recent_activity AS
SELECT 
    dataset_type,
    change_date,
    change_indicator,
    COUNT(*) as property_count,
    COUNT(price_paid) as priced_properties,
    ROUND(AVG(price_paid)::numeric, 0) as avg_price,
    COUNT(DISTINCT county) as counties_affected,
    COUNT(DISTINCT district) as districts_affected
FROM properties
WHERE change_date >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY dataset_type, change_date, change_indicator
ORDER BY change_date DESC;

-- Create additional indexes for QlikView query performance
CREATE INDEX IF NOT EXISTS idx_properties_change_year ON properties(EXTRACT(YEAR FROM change_date));
CREATE INDEX IF NOT EXISTS idx_properties_region_county ON properties(region, county) WHERE region != '';

-- Grant permissions to potential read-only user
-- CREATE USER qlikview_reader WITH PASSWORD 'ReadOnly2024!';
-- GRANT CONNECT ON DATABASE insideestates_app TO qlikview_reader;
-- GRANT USAGE ON SCHEMA public TO qlikview_reader;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO qlikview_reader;
-- GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO qlikview_reader;