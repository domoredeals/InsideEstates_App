-- Create a comprehensive view joining Land Registry data with matched Companies House data
-- This view provides easy access to property ownership with full company details

CREATE OR REPLACE VIEW v_properties_with_companies AS
WITH property_companies AS (
    SELECT 
        lr.id,
        lr.title_number,
        lr.tenure,
        lr.property_address,
        lr.district,
        lr.county,
        lr.region,
        lr.postcode,
        lr.price_paid,
        lr.date_proprietor_added,
        lr.dataset_type,
        lr.file_month,
        
        -- Proprietor 1
        lr.proprietor_1_name,
        lr.company_1_reg_no,
        lr.ch_matched_number_1,
        lr.ch_match_type_1,
        lr.ch_match_confidence_1,
        ch1.company_name as ch1_company_name,
        ch1.company_status as ch1_status,
        ch1.incorporation_date as ch1_incorporation_date,
        ch1.reg_address_postcode as ch1_postcode,
        
        -- Proprietor 2
        lr.proprietor_2_name,
        lr.company_2_reg_no,
        lr.ch_matched_number_2,
        lr.ch_match_type_2,
        lr.ch_match_confidence_2,
        ch2.company_name as ch2_company_name,
        ch2.company_status as ch2_status,
        ch2.incorporation_date as ch2_incorporation_date,
        ch2.reg_address_postcode as ch2_postcode,
        
        -- Proprietor 3
        lr.proprietor_3_name,
        lr.company_3_reg_no,
        lr.ch_matched_number_3,
        lr.ch_match_type_3,
        lr.ch_match_confidence_3,
        ch3.company_name as ch3_company_name,
        ch3.company_status as ch3_status,
        ch3.incorporation_date as ch3_incorporation_date,
        ch3.reg_address_postcode as ch3_postcode,
        
        -- Proprietor 4
        lr.proprietor_4_name,
        lr.company_4_reg_no,
        lr.ch_matched_number_4,
        lr.ch_match_type_4,
        lr.ch_match_confidence_4,
        ch4.company_name as ch4_company_name,
        ch4.company_status as ch4_status,
        ch4.incorporation_date as ch4_incorporation_date,
        ch4.reg_address_postcode as ch4_postcode,
        
        -- Match metadata
        lr.ch_match_date,
        
        -- Calculate total match confidence for the property
        COALESCE(lr.ch_match_confidence_1, 0) + 
        COALESCE(lr.ch_match_confidence_2, 0) + 
        COALESCE(lr.ch_match_confidence_3, 0) + 
        COALESCE(lr.ch_match_confidence_4, 0) as total_match_confidence,
        
        -- Count matched proprietors
        (CASE WHEN lr.ch_match_type_1 NOT IN ('No_Match', NULL) THEN 1 ELSE 0 END +
         CASE WHEN lr.ch_match_type_2 NOT IN ('No_Match', NULL) THEN 1 ELSE 0 END +
         CASE WHEN lr.ch_match_type_3 NOT IN ('No_Match', NULL) THEN 1 ELSE 0 END +
         CASE WHEN lr.ch_match_type_4 NOT IN ('No_Match', NULL) THEN 1 ELSE 0 END) as matched_proprietor_count,
         
        -- Count total proprietors
        (CASE WHEN lr.proprietor_1_name IS NOT NULL AND lr.proprietor_1_name != '' THEN 1 ELSE 0 END +
         CASE WHEN lr.proprietor_2_name IS NOT NULL AND lr.proprietor_2_name != '' THEN 1 ELSE 0 END +
         CASE WHEN lr.proprietor_3_name IS NOT NULL AND lr.proprietor_3_name != '' THEN 1 ELSE 0 END +
         CASE WHEN lr.proprietor_4_name IS NOT NULL AND lr.proprietor_4_name != '' THEN 1 ELSE 0 END) as total_proprietor_count
        
    FROM land_registry_data lr
    LEFT JOIN companies_house_data ch1 ON lr.ch_matched_number_1 = ch1.company_number
    LEFT JOIN companies_house_data ch2 ON lr.ch_matched_number_2 = ch2.company_number
    LEFT JOIN companies_house_data ch3 ON lr.ch_matched_number_3 = ch3.company_number
    LEFT JOIN companies_house_data ch4 ON lr.ch_matched_number_4 = ch4.company_number
)
SELECT * FROM property_companies;

-- Create indexes on the view's base tables for better performance
CREATE INDEX IF NOT EXISTS idx_lr_ch_lookup ON land_registry_data(ch_matched_number_1, ch_matched_number_2, ch_matched_number_3, ch_matched_number_4);

-- Create a simplified view for finding all properties owned by a specific company
CREATE OR REPLACE VIEW v_company_properties AS
SELECT 
    company_number,
    company_name,
    COUNT(DISTINCT title_number) as property_count,
    STRING_AGG(DISTINCT title_number, ', ' ORDER BY title_number) as title_numbers,
    STRING_AGG(DISTINCT postcode, ', ' ORDER BY postcode) as property_postcodes,
    SUM(price_paid) as total_price_paid,
    MIN(date_proprietor_added) as earliest_acquisition,
    MAX(date_proprietor_added) as latest_acquisition
FROM (
    -- Proprietor 1
    SELECT 
        lr.title_number,
        lr.postcode,
        lr.price_paid,
        lr.date_proprietor_added,
        COALESCE(lr.ch_matched_number_1, lr.company_1_reg_no) as company_number,
        COALESCE(ch.company_name, lr.proprietor_1_name) as company_name
    FROM land_registry_data lr
    LEFT JOIN companies_house_data ch ON lr.ch_matched_number_1 = ch.company_number
    WHERE lr.proprietor_1_name IS NOT NULL AND lr.proprietor_1_name != ''
    
    UNION ALL
    
    -- Proprietor 2
    SELECT 
        lr.title_number,
        lr.postcode,
        lr.price_paid,
        lr.date_proprietor_added,
        COALESCE(lr.ch_matched_number_2, lr.company_2_reg_no) as company_number,
        COALESCE(ch.company_name, lr.proprietor_2_name) as company_name
    FROM land_registry_data lr
    LEFT JOIN companies_house_data ch ON lr.ch_matched_number_2 = ch.company_number
    WHERE lr.proprietor_2_name IS NOT NULL AND lr.proprietor_2_name != ''
    
    UNION ALL
    
    -- Proprietor 3
    SELECT 
        lr.title_number,
        lr.postcode,
        lr.price_paid,
        lr.date_proprietor_added,
        COALESCE(lr.ch_matched_number_3, lr.company_3_reg_no) as company_number,
        COALESCE(ch.company_name, lr.proprietor_3_name) as company_name
    FROM land_registry_data lr
    LEFT JOIN companies_house_data ch ON lr.ch_matched_number_3 = ch.company_number
    WHERE lr.proprietor_3_name IS NOT NULL AND lr.proprietor_3_name != ''
    
    UNION ALL
    
    -- Proprietor 4
    SELECT 
        lr.title_number,
        lr.postcode,
        lr.price_paid,
        lr.date_proprietor_added,
        COALESCE(lr.ch_matched_number_4, lr.company_4_reg_no) as company_number,
        COALESCE(ch.company_name, lr.proprietor_4_name) as company_name
    FROM land_registry_data lr
    LEFT JOIN companies_house_data ch ON lr.ch_matched_number_4 = ch.company_number
    WHERE lr.proprietor_4_name IS NOT NULL AND lr.proprietor_4_name != ''
) all_proprietors
WHERE company_number IS NOT NULL
GROUP BY company_number, company_name;

-- Create a view showing match quality statistics by region
CREATE OR REPLACE VIEW v_match_quality_by_region AS
SELECT 
    region,
    COUNT(*) as total_properties,
    COUNT(CASE WHEN ch_match_type_1 = 'Name+Number' THEN 1 END) as tier1_matches,
    COUNT(CASE WHEN ch_match_type_1 = 'Number' THEN 1 END) as tier2_matches,
    COUNT(CASE WHEN ch_match_type_1 = 'Name' THEN 1 END) as tier3_matches,
    COUNT(CASE WHEN ch_match_type_1 = 'Previous_Name' THEN 1 END) as tier4_matches,
    COUNT(CASE WHEN ch_match_type_1 = 'No_Match' THEN 1 END) as no_matches,
    ROUND(100.0 * COUNT(CASE WHEN ch_match_type_1 != 'No_Match' AND ch_match_type_1 IS NOT NULL THEN 1 END) / 
          NULLIF(COUNT(CASE WHEN proprietor_1_name IS NOT NULL AND proprietor_1_name != '' THEN 1 END), 0), 2) as match_rate_pct
FROM land_registry_data
WHERE ch_match_date IS NOT NULL
GROUP BY region
ORDER BY total_properties DESC;

-- Sample queries using these views:

-- Find all properties owned by a specific company number:
-- SELECT * FROM v_company_properties WHERE company_number = '12345678';

-- Find properties with low confidence matches that might need manual review:
-- SELECT * FROM v_properties_with_companies 
-- WHERE total_match_confidence > 0 AND total_match_confidence < 2.0
-- ORDER BY total_match_confidence ASC;

-- Find companies with most properties:
-- SELECT * FROM v_company_properties 
-- ORDER BY property_count DESC 
-- LIMIT 100;

-- Analyze match quality by region:
-- SELECT * FROM v_match_quality_by_region;