-- Create a suite of ownership views for different use cases

-- 1. View showing ONLY CURRENT ownership (properties as they are now)
CREATE OR REPLACE VIEW v_current_ownership AS
SELECT * FROM v_ownership_history_comprehensive
WHERE ownership_status = 'Current'
ORDER BY title_number, proprietor_sequence;

-- 2. View showing latest record per title/owner (like the original view)
CREATE OR REPLACE VIEW v_ownership_latest_per_owner AS
WITH latest_records AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY title_number, proprietor_name 
            ORDER BY file_month DESC, created_at DESC
        ) as rn
    FROM v_ownership_history_comprehensive
)
SELECT * FROM latest_records WHERE rn = 1
ORDER BY title_number, proprietor_sequence;

-- 3. View showing properties with ownership changes in the last 12 months
CREATE OR REPLACE VIEW v_recent_ownership_changes AS
SELECT * FROM v_ownership_history_comprehensive
WHERE change_indicator IN ('A', 'D', 'M')
  AND change_date >= CURRENT_DATE - INTERVAL '12 months'
ORDER BY change_date DESC, title_number;

-- 4. View showing only properties with high-quality Companies House matches
CREATE OR REPLACE VIEW v_ownership_with_good_ch_matches AS
SELECT * FROM v_ownership_history_comprehensive
WHERE ch_match_confidence >= 0.7
  AND ch_match_type IN ('Name+Number', 'Number')
ORDER BY title_number, file_month DESC;

-- 5. Simplified current ownership view (one row per property showing all current owners)
CREATE OR REPLACE VIEW v_current_ownership_simplified AS
SELECT 
    title_number,
    property_address,
    district,
    county,
    region,
    postcode,
    tenure,
    dataset_type,
    price_paid,
    MAX(file_month) as latest_file_month,
    COUNT(DISTINCT proprietor_name) as current_owner_count,
    STRING_AGG(
        DISTINCT proprietor_name, 
        '; ' 
        ORDER BY proprietor_name
    ) as current_owners,
    STRING_AGG(
        DISTINCT CASE 
            WHEN ch_company_name IS NOT NULL 
            THEN ch_company_name || ' (' || COALESCE(ch_matched_number, 'no number') || ')'
            ELSE NULL
        END, 
        '; '
    ) as matched_companies,
    MAX(ch_match_confidence) as best_match_confidence
FROM v_ownership_history_comprehensive
WHERE ownership_status = 'Current'
GROUP BY 
    title_number,
    property_address,
    district,
    county,
    region,
    postcode,
    tenure,
    dataset_type,
    price_paid;

-- 6. View for analyzing ownership patterns by company
CREATE OR REPLACE VIEW v_company_property_portfolio AS
SELECT 
    ch_company_name,
    ch_matched_number as company_number,
    ch_company_status as company_status,
    COUNT(DISTINCT CASE WHEN ownership_status = 'Current' THEN title_number END) as current_properties,
    COUNT(DISTINCT CASE WHEN ownership_status = 'Historical' THEN title_number END) as historical_properties,
    COUNT(DISTINCT title_number) as total_properties_ever_owned,
    STRING_AGG(
        DISTINCT CASE 
            WHEN ownership_status = 'Current' 
            THEN region 
        END, 
        ', '
    ) as current_regions,
    SUM(CASE WHEN ownership_status = 'Current' THEN price_paid ELSE 0 END) as total_current_value,
    MIN(date_proprietor_added) as earliest_ownership,
    MAX(CASE WHEN ownership_status = 'Current' THEN date_proprietor_added END) as latest_acquisition
FROM v_ownership_history_comprehensive
WHERE ch_company_name IS NOT NULL
GROUP BY 
    ch_company_name,
    ch_matched_number,
    ch_company_status
HAVING COUNT(DISTINCT CASE WHEN ownership_status = 'Current' THEN title_number END) > 0
ORDER BY current_properties DESC;

-- Add helpful comments
COMMENT ON VIEW v_current_ownership IS 'Shows only current property ownership records (latest snapshot)';
COMMENT ON VIEW v_ownership_latest_per_owner IS 'Shows only the latest record for each unique title/owner combination';
COMMENT ON VIEW v_recent_ownership_changes IS 'Shows properties with ownership changes in the last 12 months';
COMMENT ON VIEW v_ownership_with_good_ch_matches IS 'Shows only records with high-confidence Companies House matches';
COMMENT ON VIEW v_current_ownership_simplified IS 'Simplified view with one row per property showing all current owners';
COMMENT ON VIEW v_company_property_portfolio IS 'Analysis view showing property portfolios by company';

-- Sample queries to demonstrate usage:
/*
-- Get total number of properties with current ownership
SELECT COUNT(DISTINCT title_number) as properties_with_current_ownership
FROM v_current_ownership;

-- Find top property-owning companies
SELECT * FROM v_company_property_portfolio
LIMIT 20;

-- See recent ownership changes
SELECT 
    title_number,
    property_address,
    proprietor_name,
    change_description,
    change_date
FROM v_recent_ownership_changes
LIMIT 100;

-- Get simplified current ownership for a postcode
SELECT * FROM v_current_ownership_simplified
WHERE postcode LIKE 'SW1A%';
*/