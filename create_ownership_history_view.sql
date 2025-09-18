-- Create a comprehensive ownership history view for any title number
-- This view shows the complete ownership timeline with Companies House details
-- Includes matching confidence scores and handles cases where matches don't exist

CREATE OR REPLACE VIEW v_ownership_history AS
WITH ownership_timeline AS (
    SELECT 
        -- Property identification
        lr.title_number,
        lr.file_month,
        lr.dataset_type,
        lr.update_type,
        lr.change_indicator,
        lr.change_date,
        
        -- Property details
        lr.tenure,
        lr.property_address,
        lr.district,
        lr.county,
        lr.region,
        lr.postcode,
        lr.price_paid,
        lr.date_proprietor_added,
        lr.multiple_address_indicator,
        lr.additional_proprietor_indicator,
        
        -- Metadata
        lr.source_filename,
        lr.created_at,
        lr.ch_match_date,
        
        -- Proprietor details with sequence number
        1 as proprietor_sequence,
        lr.proprietor_1_name as proprietor_name,
        lr.company_1_reg_no as lr_company_reg_no,
        lr.proprietorship_1_category as proprietorship_category,
        lr.country_1_incorporated as country_incorporated,
        lr.proprietor_1_address_1 as proprietor_address_1,
        lr.proprietor_1_address_2 as proprietor_address_2,
        lr.proprietor_1_address_3 as proprietor_address_3,
        
        -- Companies House matching details
        lr.ch_matched_name_1 as ch_matched_name,
        lr.ch_matched_number_1 as ch_matched_number,
        lr.ch_match_type_1 as ch_match_type,
        lr.ch_match_confidence_1 as ch_match_confidence,
        
        -- Companies House company details
        ch.company_name as ch_company_name,
        ch.company_status as ch_company_status,
        ch.company_category as ch_company_category,
        ch.incorporation_date as ch_incorporation_date,
        ch.dissolution_date as ch_dissolution_date,
        ch.country_of_origin as ch_country_of_origin,
        ch.reg_address_line1 as ch_reg_address_line1,
        ch.reg_address_line2 as ch_reg_address_line2,
        ch.reg_address_post_town as ch_reg_address_post_town,
        ch.reg_address_county as ch_reg_address_county,
        ch.reg_address_country as ch_reg_address_country,
        ch.reg_address_postcode as ch_reg_address_postcode,
        ch.accounts_next_due_date as ch_accounts_next_due_date,
        ch.accounts_last_made_up_date as ch_accounts_last_made_up_date,
        ch.accounts_category as ch_accounts_category,
        ch.sic_code_1 as ch_sic_code_1,
        ch.sic_code_2 as ch_sic_code_2,
        ch.sic_code_3 as ch_sic_code_3,
        ch.sic_code_4 as ch_sic_code_4,
        
        -- Match quality indicators
        CASE 
            WHEN lr.ch_match_type_1 = 'Name+Number' THEN 'Excellent'
            WHEN lr.ch_match_type_1 = 'Number' THEN 'Very Good'
            WHEN lr.ch_match_type_1 = 'Name' THEN 'Good'
            WHEN lr.ch_match_type_1 = 'Previous_Name' THEN 'Fair'
            WHEN lr.ch_match_type_1 = 'No_Match' THEN 'No Match'
            ELSE 'Unknown'
        END as match_quality_description
        
    FROM land_registry_data lr
    LEFT JOIN companies_house_data ch ON lr.ch_matched_number_1 = ch.company_number
    WHERE lr.proprietor_1_name IS NOT NULL AND lr.proprietor_1_name != ''
    
    UNION ALL
    
    -- Proprietor 2
    SELECT 
        lr.title_number,
        lr.file_month,
        lr.dataset_type,
        lr.update_type,
        lr.change_indicator,
        lr.change_date,
        lr.tenure,
        lr.property_address,
        lr.district,
        lr.county,
        lr.region,
        lr.postcode,
        lr.price_paid,
        lr.date_proprietor_added,
        lr.multiple_address_indicator,
        lr.additional_proprietor_indicator,
        lr.source_filename,
        lr.created_at,
        lr.ch_match_date,
        
        2 as proprietor_sequence,
        lr.proprietor_2_name as proprietor_name,
        lr.company_2_reg_no as lr_company_reg_no,
        lr.proprietorship_2_category as proprietorship_category,
        lr.country_2_incorporated as country_incorporated,
        lr.proprietor_2_address_1 as proprietor_address_1,
        lr.proprietor_2_address_2 as proprietor_address_2,
        lr.proprietor_2_address_3 as proprietor_address_3,
        
        lr.ch_matched_name_2 as ch_matched_name,
        lr.ch_matched_number_2 as ch_matched_number,
        lr.ch_match_type_2 as ch_match_type,
        lr.ch_match_confidence_2 as ch_match_confidence,
        
        ch.company_name as ch_company_name,
        ch.company_status as ch_company_status,
        ch.company_category as ch_company_category,
        ch.incorporation_date as ch_incorporation_date,
        ch.dissolution_date as ch_dissolution_date,
        ch.country_of_origin as ch_country_of_origin,
        ch.reg_address_line1 as ch_reg_address_line1,
        ch.reg_address_line2 as ch_reg_address_line2,
        ch.reg_address_post_town as ch_reg_address_post_town,
        ch.reg_address_county as ch_reg_address_county,
        ch.reg_address_country as ch_reg_address_country,
        ch.reg_address_postcode as ch_reg_address_postcode,
        ch.accounts_next_due_date as ch_accounts_next_due_date,
        ch.accounts_last_made_up_date as ch_accounts_last_made_up_date,
        ch.accounts_category as ch_accounts_category,
        ch.sic_code_1 as ch_sic_code_1,
        ch.sic_code_2 as ch_sic_code_2,
        ch.sic_code_3 as ch_sic_code_3,
        ch.sic_code_4 as ch_sic_code_4,
        
        CASE 
            WHEN lr.ch_match_type_2 = 'Name+Number' THEN 'Excellent'
            WHEN lr.ch_match_type_2 = 'Number' THEN 'Very Good'
            WHEN lr.ch_match_type_2 = 'Name' THEN 'Good'
            WHEN lr.ch_match_type_2 = 'Previous_Name' THEN 'Fair'
            WHEN lr.ch_match_type_2 = 'No_Match' THEN 'No Match'
            ELSE 'Unknown'
        END as match_quality_description
        
    FROM land_registry_data lr
    LEFT JOIN companies_house_data ch ON lr.ch_matched_number_2 = ch.company_number
    WHERE lr.proprietor_2_name IS NOT NULL AND lr.proprietor_2_name != ''
    
    UNION ALL
    
    -- Proprietor 3
    SELECT 
        lr.title_number,
        lr.file_month,
        lr.dataset_type,
        lr.update_type,
        lr.change_indicator,
        lr.change_date,
        lr.tenure,
        lr.property_address,
        lr.district,
        lr.county,
        lr.region,
        lr.postcode,
        lr.price_paid,
        lr.date_proprietor_added,
        lr.multiple_address_indicator,
        lr.additional_proprietor_indicator,
        lr.source_filename,
        lr.created_at,
        lr.ch_match_date,
        
        3 as proprietor_sequence,
        lr.proprietor_3_name as proprietor_name,
        lr.company_3_reg_no as lr_company_reg_no,
        lr.proprietorship_3_category as proprietorship_category,
        lr.country_3_incorporated as country_incorporated,
        lr.proprietor_3_address_1 as proprietor_address_1,
        lr.proprietor_3_address_2 as proprietor_address_2,
        lr.proprietor_3_address_3 as proprietor_address_3,
        
        lr.ch_matched_name_3 as ch_matched_name,
        lr.ch_matched_number_3 as ch_matched_number,
        lr.ch_match_type_3 as ch_match_type,
        lr.ch_match_confidence_3 as ch_match_confidence,
        
        ch.company_name as ch_company_name,
        ch.company_status as ch_company_status,
        ch.company_category as ch_company_category,
        ch.incorporation_date as ch_incorporation_date,
        ch.dissolution_date as ch_dissolution_date,
        ch.country_of_origin as ch_country_of_origin,
        ch.reg_address_line1 as ch_reg_address_line1,
        ch.reg_address_line2 as ch_reg_address_line2,
        ch.reg_address_post_town as ch_reg_address_post_town,
        ch.reg_address_county as ch_reg_address_county,
        ch.reg_address_country as ch_reg_address_country,
        ch.reg_address_postcode as ch_reg_address_postcode,
        ch.accounts_next_due_date as ch_accounts_next_due_date,
        ch.accounts_last_made_up_date as ch_accounts_last_made_up_date,
        ch.accounts_category as ch_accounts_category,
        ch.sic_code_1 as ch_sic_code_1,
        ch.sic_code_2 as ch_sic_code_2,
        ch.sic_code_3 as ch_sic_code_3,
        ch.sic_code_4 as ch_sic_code_4,
        
        CASE 
            WHEN lr.ch_match_type_3 = 'Name+Number' THEN 'Excellent'
            WHEN lr.ch_match_type_3 = 'Number' THEN 'Very Good'
            WHEN lr.ch_match_type_3 = 'Name' THEN 'Good'
            WHEN lr.ch_match_type_3 = 'Previous_Name' THEN 'Fair'
            WHEN lr.ch_match_type_3 = 'No_Match' THEN 'No Match'
            ELSE 'Unknown'
        END as match_quality_description
        
    FROM land_registry_data lr
    LEFT JOIN companies_house_data ch ON lr.ch_matched_number_3 = ch.company_number
    WHERE lr.proprietor_3_name IS NOT NULL AND lr.proprietor_3_name != ''
    
    UNION ALL
    
    -- Proprietor 4
    SELECT 
        lr.title_number,
        lr.file_month,
        lr.dataset_type,
        lr.update_type,
        lr.change_indicator,
        lr.change_date,
        lr.tenure,
        lr.property_address,
        lr.district,
        lr.county,
        lr.region,
        lr.postcode,
        lr.price_paid,
        lr.date_proprietor_added,
        lr.multiple_address_indicator,
        lr.additional_proprietor_indicator,
        lr.source_filename,
        lr.created_at,
        lr.ch_match_date,
        
        4 as proprietor_sequence,
        lr.proprietor_4_name as proprietor_name,
        lr.company_4_reg_no as lr_company_reg_no,
        lr.proprietorship_4_category as proprietorship_category,
        lr.country_4_incorporated as country_incorporated,
        lr.proprietor_4_address_1 as proprietor_address_1,
        lr.proprietor_4_address_2 as proprietor_address_2,
        lr.proprietor_4_address_3 as proprietor_address_3,
        
        lr.ch_matched_name_4 as ch_matched_name,
        lr.ch_matched_number_4 as ch_matched_number,
        lr.ch_match_type_4 as ch_match_type,
        lr.ch_match_confidence_4 as ch_match_confidence,
        
        ch.company_name as ch_company_name,
        ch.company_status as ch_company_status,
        ch.company_category as ch_company_category,
        ch.incorporation_date as ch_incorporation_date,
        ch.dissolution_date as ch_dissolution_date,
        ch.country_of_origin as ch_country_of_origin,
        ch.reg_address_line1 as ch_reg_address_line1,
        ch.reg_address_line2 as ch_reg_address_line2,
        ch.reg_address_post_town as ch_reg_address_post_town,
        ch.reg_address_county as ch_reg_address_county,
        ch.reg_address_country as ch_reg_address_country,
        ch.reg_address_postcode as ch_reg_address_postcode,
        ch.accounts_next_due_date as ch_accounts_next_due_date,
        ch.accounts_last_made_up_date as ch_accounts_last_made_up_date,
        ch.accounts_category as ch_accounts_category,
        ch.sic_code_1 as ch_sic_code_1,
        ch.sic_code_2 as ch_sic_code_2,
        ch.sic_code_3 as ch_sic_code_3,
        ch.sic_code_4 as ch_sic_code_4,
        
        CASE 
            WHEN lr.ch_match_type_4 = 'Name+Number' THEN 'Excellent'
            WHEN lr.ch_match_type_4 = 'Number' THEN 'Very Good'
            WHEN lr.ch_match_type_4 = 'Name' THEN 'Good'
            WHEN lr.ch_match_type_4 = 'Previous_Name' THEN 'Fair'
            WHEN lr.ch_match_type_4 = 'No_Match' THEN 'No Match'
            ELSE 'Unknown'
        END as match_quality_description
        
    FROM land_registry_data lr
    LEFT JOIN companies_house_data ch ON lr.ch_matched_number_4 = ch.company_number
    WHERE lr.proprietor_4_name IS NOT NULL AND lr.proprietor_4_name != ''
)
SELECT 
    *,
    -- Additional computed fields for analysis
    CASE 
        WHEN change_indicator = 'A' THEN 'Added'
        WHEN change_indicator = 'D' THEN 'Deleted'
        WHEN change_indicator = 'M' THEN 'Modified'
        ELSE 'Unchanged'
    END as change_description,
    
    -- Create a ownership period indicator
    ROW_NUMBER() OVER (
        PARTITION BY title_number, proprietor_name 
        ORDER BY file_month, proprietor_sequence
    ) as ownership_sequence,
    
    -- Flag for current vs historical ownership
    CASE 
        WHEN file_month = (
            SELECT MAX(file_month) 
            FROM land_registry_data lr2 
            WHERE lr2.title_number = ownership_timeline.title_number
        ) THEN 'Current'
        ELSE 'Historical'
    END as ownership_status,
    
    -- Calculate ownership duration in months (approximate)
    EXTRACT(YEAR FROM AGE(
        COALESCE(change_date, CURRENT_DATE), 
        COALESCE(date_proprietor_added, file_month)
    )) * 12 + 
    EXTRACT(MONTH FROM AGE(
        COALESCE(change_date, CURRENT_DATE), 
        COALESCE(date_proprietor_added, file_month)
    )) as ownership_duration_months

FROM ownership_timeline
ORDER BY title_number, file_month DESC, proprietor_sequence;

-- Create indexes to improve query performance
CREATE INDEX IF NOT EXISTS idx_ownership_history_title ON land_registry_data(title_number);
CREATE INDEX IF NOT EXISTS idx_ownership_history_file_month ON land_registry_data(file_month);
CREATE INDEX IF NOT EXISTS idx_ownership_history_proprietor ON land_registry_data(proprietor_1_name, proprietor_2_name, proprietor_3_name, proprietor_4_name);

-- Create a summary view for quick ownership analysis
CREATE OR REPLACE VIEW v_ownership_summary AS
SELECT 
    title_number,
    property_address,
    postcode,
    tenure,
    dataset_type,
    COUNT(DISTINCT proprietor_name) as total_unique_owners,
    COUNT(DISTINCT file_month) as ownership_records,
    MIN(file_month) as earliest_record,
    MAX(file_month) as latest_record,
    STRING_AGG(
        DISTINCT CASE WHEN ownership_status = 'Current' THEN proprietor_name END, 
        ', ' 
        ORDER BY CASE WHEN ownership_status = 'Current' THEN proprietor_name END
    ) as current_owners,
    COUNT(CASE WHEN ch_match_confidence >= 0.9 THEN 1 END) as high_confidence_matches,
    COUNT(CASE WHEN ch_match_confidence >= 0.7 AND ch_match_confidence < 0.9 THEN 1 END) as medium_confidence_matches,
    COUNT(CASE WHEN ch_match_confidence > 0 AND ch_match_confidence < 0.7 THEN 1 END) as low_confidence_matches,
    COUNT(CASE WHEN ch_match_confidence = 0 OR ch_match_confidence IS NULL THEN 1 END) as no_matches
FROM v_ownership_history
GROUP BY title_number, property_address, postcode, tenure, dataset_type;

-- Add helpful comments
COMMENT ON VIEW v_ownership_history IS 'Comprehensive ownership history view showing all proprietors across time with Companies House details and matching confidence';
COMMENT ON VIEW v_ownership_summary IS 'Summary view showing ownership statistics and match quality for each property';

-- Sample queries to demonstrate usage:

/*
-- Get complete ownership history for a specific property:
SELECT * FROM v_ownership_history 
WHERE title_number = 'ABC123' 
ORDER BY file_month DESC, proprietor_sequence;

-- Find all properties owned by a specific company (using CH matched name):
SELECT DISTINCT title_number, property_address, ownership_status, ch_company_name
FROM v_ownership_history 
WHERE ch_company_name ILIKE '%EXAMPLE COMPANY%'
ORDER BY title_number;

-- Find properties with ownership changes (additions/deletions):
SELECT title_number, proprietor_name, change_indicator, change_date, file_month
FROM v_ownership_history 
WHERE change_indicator IN ('A', 'D')
ORDER BY change_date DESC;

-- Get ownership summary for properties in a specific postcode:
SELECT * FROM v_ownership_summary 
WHERE postcode LIKE 'SW1%'
ORDER BY total_unique_owners DESC;

-- Find properties with poor matching quality that might need manual review:
SELECT title_number, proprietor_name, ch_match_type, ch_match_confidence, match_quality_description
FROM v_ownership_history 
WHERE ch_match_confidence < 0.7 AND ch_match_confidence > 0
ORDER BY ch_match_confidence ASC;

-- Analyze ownership duration:
SELECT 
    title_number,
    proprietor_name,
    ownership_duration_months,
    CASE 
        WHEN ownership_duration_months < 12 THEN 'Short-term (<1 year)'
        WHEN ownership_duration_months < 36 THEN 'Medium-term (1-3 years)'
        WHEN ownership_duration_months < 120 THEN 'Long-term (3-10 years)'
        ELSE 'Very long-term (>10 years)'
    END as duration_category
FROM v_ownership_history
WHERE ownership_status = 'Current'
ORDER BY ownership_duration_months DESC;

-- Find companies with properties across multiple regions:
SELECT 
    ch_company_name,
    ch_matched_number,
    COUNT(DISTINCT region) as regions_count,
    COUNT(DISTINCT title_number) as properties_count,
    STRING_AGG(DISTINCT region, ', ') as regions
FROM v_ownership_history 
WHERE ch_company_name IS NOT NULL
GROUP BY ch_company_name, ch_matched_number
HAVING COUNT(DISTINCT region) > 1
ORDER BY regions_count DESC, properties_count DESC;
*/