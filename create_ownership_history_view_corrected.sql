-- Drop existing views
DROP VIEW IF EXISTS v_ownership_summary CASCADE;
DROP VIEW IF EXISTS v_ownership_history CASCADE;
DROP VIEW IF EXISTS v_current_ownership CASCADE;
DROP VIEW IF EXISTS v_ownership_latest_per_owner CASCADE;
DROP VIEW IF EXISTS v_recent_ownership_changes CASCADE;
DROP VIEW IF EXISTS v_ownership_with_good_ch_matches CASCADE;
DROP VIEW IF EXISTS v_current_ownership_simplified CASCADE;
DROP VIEW IF EXISTS v_company_property_portfolio CASCADE;
DROP VIEW IF EXISTS v_ownership_history_comprehensive CASCADE;

-- Create a corrected ownership history view that properly handles FULL + COU updates
-- This view correctly identifies current ownership by looking at the most recent
-- record for each title/proprietor combination, excluding deleted records
CREATE OR REPLACE VIEW v_ownership_history_comprehensive AS
WITH ownership_timeline AS (
    SELECT 
        -- Property identification
        lr.id,
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
        lrm.ch_match_date,
        
        -- Proprietor details with sequence number
        1 as proprietor_sequence,
        lr.proprietor_1_name as proprietor_name,
        lr.company_1_reg_no as lr_company_reg_no,
        lr.proprietorship_1_category as proprietorship_category,
        lr.country_1_incorporated as country_incorporated,
        lr.proprietor_1_address_1 as proprietor_address_1,
        lr.proprietor_1_address_2 as proprietor_address_2,
        lr.proprietor_1_address_3 as proprietor_address_3,
        
        -- Companies House matching details from separate table
        lrm.ch_matched_name_1 as ch_matched_name,
        lrm.ch_matched_number_1 as ch_matched_number,
        lrm.ch_match_type_1 as ch_match_type,
        lrm.ch_match_confidence_1 as ch_match_confidence,
        
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
            WHEN lrm.ch_match_type_1 = 'Name+Number' THEN 'Excellent'
            WHEN lrm.ch_match_type_1 = 'Number' THEN 'Very Good'
            WHEN lrm.ch_match_type_1 = 'Name' THEN 'Good'
            WHEN lrm.ch_match_type_1 = 'Previous_Name' THEN 'Fair'
            WHEN lrm.ch_match_type_1 = 'No_Match' OR lrm.ch_match_type_1 IS NULL THEN 'No Match'
            ELSE 'Unknown'
        END as match_quality_description,
        
        -- Ranking to find most recent record per title/proprietor
        ROW_NUMBER() OVER (
            PARTITION BY lr.title_number, lr.proprietor_1_name
            ORDER BY lr.file_month DESC, lr.created_at DESC
        ) as recency_rank
        
    FROM land_registry_data lr
    LEFT JOIN land_registry_ch_matches lrm ON lr.id = lrm.id
    LEFT JOIN companies_house_data ch ON lrm.ch_matched_number_1 = ch.company_number
    WHERE lr.proprietor_1_name IS NOT NULL AND lr.proprietor_1_name != ''
    
    UNION ALL
    
    -- Proprietor 2
    SELECT 
        lr.id,
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
        lrm.ch_match_date,
        
        2 as proprietor_sequence,
        lr.proprietor_2_name as proprietor_name,
        lr.company_2_reg_no as lr_company_reg_no,
        lr.proprietorship_2_category as proprietorship_category,
        lr.country_2_incorporated as country_incorporated,
        lr.proprietor_2_address_1 as proprietor_address_1,
        lr.proprietor_2_address_2 as proprietor_address_2,
        lr.proprietor_2_address_3 as proprietor_address_3,
        
        lrm.ch_matched_name_2 as ch_matched_name,
        lrm.ch_matched_number_2 as ch_matched_number,
        lrm.ch_match_type_2 as ch_match_type,
        lrm.ch_match_confidence_2 as ch_match_confidence,
        
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
            WHEN lrm.ch_match_type_2 = 'Name+Number' THEN 'Excellent'
            WHEN lrm.ch_match_type_2 = 'Number' THEN 'Very Good'
            WHEN lrm.ch_match_type_2 = 'Name' THEN 'Good'
            WHEN lrm.ch_match_type_2 = 'Previous_Name' THEN 'Fair'
            WHEN lrm.ch_match_type_2 = 'No_Match' OR lrm.ch_match_type_2 IS NULL THEN 'No Match'
            ELSE 'Unknown'
        END as match_quality_description,
        
        ROW_NUMBER() OVER (
            PARTITION BY lr.title_number, lr.proprietor_2_name
            ORDER BY lr.file_month DESC, lr.created_at DESC
        ) as recency_rank
        
    FROM land_registry_data lr
    LEFT JOIN land_registry_ch_matches lrm ON lr.id = lrm.id
    LEFT JOIN companies_house_data ch ON lrm.ch_matched_number_2 = ch.company_number
    WHERE lr.proprietor_2_name IS NOT NULL AND lr.proprietor_2_name != ''
    
    UNION ALL
    
    -- Proprietor 3
    SELECT 
        lr.id,
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
        lrm.ch_match_date,
        
        3 as proprietor_sequence,
        lr.proprietor_3_name as proprietor_name,
        lr.company_3_reg_no as lr_company_reg_no,
        lr.proprietorship_3_category as proprietorship_category,
        lr.country_3_incorporated as country_incorporated,
        lr.proprietor_3_address_1 as proprietor_address_1,
        lr.proprietor_3_address_2 as proprietor_address_2,
        lr.proprietor_3_address_3 as proprietor_address_3,
        
        lrm.ch_matched_name_3 as ch_matched_name,
        lrm.ch_matched_number_3 as ch_matched_number,
        lrm.ch_match_type_3 as ch_match_type,
        lrm.ch_match_confidence_3 as ch_match_confidence,
        
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
            WHEN lrm.ch_match_type_3 = 'Name+Number' THEN 'Excellent'
            WHEN lrm.ch_match_type_3 = 'Number' THEN 'Very Good'
            WHEN lrm.ch_match_type_3 = 'Name' THEN 'Good'
            WHEN lrm.ch_match_type_3 = 'Previous_Name' THEN 'Fair'
            WHEN lrm.ch_match_type_3 = 'No_Match' OR lrm.ch_match_type_3 IS NULL THEN 'No Match'
            ELSE 'Unknown'
        END as match_quality_description,
        
        ROW_NUMBER() OVER (
            PARTITION BY lr.title_number, lr.proprietor_3_name
            ORDER BY lr.file_month DESC, lr.created_at DESC
        ) as recency_rank
        
    FROM land_registry_data lr
    LEFT JOIN land_registry_ch_matches lrm ON lr.id = lrm.id
    LEFT JOIN companies_house_data ch ON lrm.ch_matched_number_3 = ch.company_number
    WHERE lr.proprietor_3_name IS NOT NULL AND lr.proprietor_3_name != ''
    
    UNION ALL
    
    -- Proprietor 4
    SELECT 
        lr.id,
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
        lrm.ch_match_date,
        
        4 as proprietor_sequence,
        lr.proprietor_4_name as proprietor_name,
        lr.company_4_reg_no as lr_company_reg_no,
        lr.proprietorship_4_category as proprietorship_category,
        lr.country_4_incorporated as country_incorporated,
        lr.proprietor_4_address_1 as proprietor_address_1,
        lr.proprietor_4_address_2 as proprietor_address_2,
        lr.proprietor_4_address_3 as proprietor_address_3,
        
        lrm.ch_matched_name_4 as ch_matched_name,
        lrm.ch_matched_number_4 as ch_matched_number,
        lrm.ch_match_type_4 as ch_match_type,
        lrm.ch_match_confidence_4 as ch_match_confidence,
        
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
            WHEN lrm.ch_match_type_4 = 'Name+Number' THEN 'Excellent'
            WHEN lrm.ch_match_type_4 = 'Number' THEN 'Very Good'
            WHEN lrm.ch_match_type_4 = 'Name' THEN 'Good'
            WHEN lrm.ch_match_type_4 = 'Previous_Name' THEN 'Fair'
            WHEN lrm.ch_match_type_4 = 'No_Match' OR lrm.ch_match_type_4 IS NULL THEN 'No Match'
            ELSE 'Unknown'
        END as match_quality_description,
        
        ROW_NUMBER() OVER (
            PARTITION BY lr.title_number, lr.proprietor_4_name
            ORDER BY lr.file_month DESC, lr.created_at DESC
        ) as recency_rank
        
    FROM land_registry_data lr
    LEFT JOIN land_registry_ch_matches lrm ON lr.id = lrm.id
    LEFT JOIN companies_house_data ch ON lrm.ch_matched_number_4 = ch.company_number
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
    
    -- CORRECTED: Proper ownership status for FULL + COU data model
    -- A record is Current if:
    -- 1. It's the most recent record for that title/proprietor (recency_rank = 1) AND
    -- 2. It's NOT marked as deleted (change_indicator != 'D')
    -- Everything else is Historical
    CASE 
        WHEN recency_rank = 1 AND (change_indicator IS NULL OR change_indicator != 'D') THEN 'Current'
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
CREATE INDEX IF NOT EXISTS idx_lr_ch_matches_id ON land_registry_ch_matches(id);
CREATE INDEX IF NOT EXISTS idx_lr_ch_matches_numbers ON land_registry_ch_matches(ch_matched_number_1, ch_matched_number_2, ch_matched_number_3, ch_matched_number_4);

-- Create the main v_ownership_history view pointing to comprehensive view
CREATE OR REPLACE VIEW v_ownership_history AS
SELECT * FROM v_ownership_history_comprehensive;

-- Create a materialized view for better performance on current ownership queries
-- This will need to be refreshed periodically after new data imports
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_current_ownership AS
SELECT * FROM v_ownership_history_comprehensive
WHERE ownership_status = 'Current';

-- Create index on materialized view
CREATE INDEX IF NOT EXISTS idx_mv_current_ownership_title ON mv_current_ownership(title_number);
CREATE INDEX IF NOT EXISTS idx_mv_current_ownership_company ON mv_current_ownership(ch_matched_number);
CREATE INDEX IF NOT EXISTS idx_mv_current_ownership_postcode ON mv_current_ownership(postcode);

-- Add helpful comments
COMMENT ON VIEW v_ownership_history_comprehensive IS 'Comprehensive ownership history view showing ALL proprietor records with correct Current/Historical status based on FULL + COU data model';
COMMENT ON VIEW v_ownership_history IS 'Main ownership history view - currently points to comprehensive view';
COMMENT ON MATERIALIZED VIEW mv_current_ownership IS 'Materialized view of current ownership for performance - refresh after data imports using: REFRESH MATERIALIZED VIEW mv_current_ownership';

-- Verification queries to show the correct results:
/*
-- Count Current vs Historical ownership records
SELECT 
    ownership_status,
    COUNT(*) as record_count,
    COUNT(DISTINCT title_number) as unique_properties
FROM v_ownership_history
WHERE ownership_status = 'Current'
GROUP BY ownership_status;

-- This should show approximately 3.6M properties with current ownership
-- based on the FULL snapshot from 2018 plus all subsequent changes
*/