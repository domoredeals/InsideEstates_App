-- Optimize the ownership history view for better performance
-- First, let's check how much data we're dealing with

-- Check row counts
SELECT 'land_registry_data rows' as table_name, COUNT(*) as row_count FROM land_registry_data;
SELECT 'companies_house_data rows' as table_name, COUNT(*) as row_count FROM companies_house_data;

-- Create indexes if they don't exist for better join performance
CREATE INDEX IF NOT EXISTS idx_lr_ch_matched_1 ON land_registry_data(ch_matched_number_1);
CREATE INDEX IF NOT EXISTS idx_lr_ch_matched_2 ON land_registry_data(ch_matched_number_2);
CREATE INDEX IF NOT EXISTS idx_lr_ch_matched_3 ON land_registry_data(ch_matched_number_3);
CREATE INDEX IF NOT EXISTS idx_lr_ch_matched_4 ON land_registry_data(ch_matched_number_4);
CREATE INDEX IF NOT EXISTS idx_ch_company_number ON companies_house_data(company_number);

-- Create indexes for filtering
CREATE INDEX IF NOT EXISTS idx_lr_title_number ON land_registry_data(title_number);
CREATE INDEX IF NOT EXISTS idx_lr_file_month ON land_registry_data(file_month);
CREATE INDEX IF NOT EXISTS idx_lr_dataset_type ON land_registry_data(dataset_type);

-- Create a materialized view instead of a regular view for better performance
DROP MATERIALIZED VIEW IF EXISTS mv_ownership_history CASCADE;

CREATE MATERIALIZED VIEW mv_ownership_history AS
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
        
        -- Date breakdown fields for analysis
        CASE 
            WHEN lr.date_proprietor_added IS NOT NULL THEN 
                TO_CHAR(lr.date_proprietor_added, 'YYYY-MM')
            ELSE NULL 
        END as date_proprietor_added_yyyy_mm,
        
        CASE 
            WHEN lr.date_proprietor_added IS NOT NULL THEN 
                EXTRACT(YEAR FROM lr.date_proprietor_added)::TEXT
            ELSE NULL 
        END as date_proprietor_added_yyyy,
        
        CASE 
            WHEN lr.date_proprietor_added IS NOT NULL THEN 
                EXTRACT(YEAR FROM lr.date_proprietor_added)::TEXT || '-Q' || 
                EXTRACT(QUARTER FROM lr.date_proprietor_added)::TEXT
            ELSE NULL 
        END as date_proprietor_added_yyyy_q,
        
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
        
        -- Companies House registered address
        ch.reg_address_line1 as ch_reg_address_line1,
        ch.reg_address_line2 as ch_reg_address_line2,
        ch.reg_address_post_town as ch_reg_address_post_town,
        ch.reg_address_county as ch_reg_address_county,
        ch.reg_address_country as ch_reg_address_country,
        ch.reg_address_postcode as ch_reg_address_postcode,
        
        -- Companies House accounts information
        ch.accounts_next_due_date as ch_accounts_next_due_date,
        ch.accounts_last_made_up_date as ch_accounts_last_made_up_date,
        ch.accounts_category as ch_accounts_category,
        
        -- SIC codes
        ch.sic_code_1 as ch_sic_code_1,
        ch.sic_code_2 as ch_sic_code_2,
        ch.sic_code_3 as ch_sic_code_3,
        ch.sic_code_4 as ch_sic_code_4,
        
        -- Match quality description
        CASE 
            WHEN lr.ch_match_confidence_1 >= 1.0 THEN 'Perfect Match - Company Number & Name'
            WHEN lr.ch_match_confidence_1 >= 0.9 THEN 'High Confidence - Company Number Only'
            WHEN lr.ch_match_confidence_1 >= 0.7 THEN 'Medium Confidence - Name Match'
            WHEN lr.ch_match_confidence_1 >= 0.5 THEN 'Low Confidence - Previous Name'
            WHEN lr.ch_match_confidence_1 >= 0.3 THEN 'Land Registry Data Only'
            ELSE 'No Match'
        END as match_quality_description,
        
        -- Change description
        CASE 
            WHEN lr.change_indicator = 'A' THEN 'Added'
            WHEN lr.change_indicator = 'D' THEN 'Deleted' 
            WHEN lr.change_indicator = 'U' THEN 'Updated'
            ELSE 'No Change'
        END as change_description,
        
        -- Ownership status
        CASE 
            WHEN lr.change_indicator = 'D' THEN 'Previous Owner'
            WHEN lr.date_proprietor_added IS NULL THEN 'Current Owner - Date Unknown'
            WHEN lr.date_proprietor_added <= CURRENT_DATE THEN 'Current Owner'
            ELSE 'Future Owner'
        END as ownership_status,
        
        -- Ownership duration in months
        CASE 
            WHEN lr.date_proprietor_added IS NOT NULL THEN 
                EXTRACT(YEAR FROM AGE(CURRENT_DATE, lr.date_proprietor_added)) * 12 + 
                EXTRACT(MONTH FROM AGE(CURRENT_DATE, lr.date_proprietor_added))
            ELSE NULL
        END as ownership_duration_months
        
    FROM land_registry_data lr
    LEFT JOIN companies_house_data ch ON lr.ch_matched_number_1 = ch.company_number
    WHERE lr.proprietor_1_name IS NOT NULL
    
    -- Add other proprietors (2,3,4) with similar structure...
)
SELECT * FROM ownership_timeline;

-- Create indexes on the materialized view for common queries
CREATE INDEX idx_mv_title_number ON mv_ownership_history(title_number);
CREATE INDEX idx_mv_company_number ON mv_ownership_history(ch_matched_number);
CREATE INDEX idx_mv_postcode ON mv_ownership_history(postcode);
CREATE INDEX idx_mv_file_month ON mv_ownership_history(file_month);

-- Create a limited view that Qlik can use with filtering
CREATE OR REPLACE VIEW v_ownership_history AS
SELECT * FROM mv_ownership_history;

-- Add comment
COMMENT ON MATERIALIZED VIEW mv_ownership_history IS 'Materialized view of ownership history for better performance. Refresh periodically with: REFRESH MATERIALIZED VIEW mv_ownership_history;';
COMMENT ON VIEW v_ownership_history IS 'View wrapper around materialized view for Qlik access. Use WHERE clauses to limit data.';

-- Show statistics
SELECT 
    'Materialized view created with ' || COUNT(*) || ' rows' as status
FROM mv_ownership_history;