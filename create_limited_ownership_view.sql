-- Create a limited version of the ownership view to avoid memory issues
-- This view requires filtering to work properly

DROP VIEW IF EXISTS v_ownership_history_limited CASCADE;

CREATE VIEW v_ownership_history_limited AS
WITH latest_month AS (
    SELECT MAX(file_month) as max_month
    FROM land_registry_data
),
ownership_timeline AS (
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
    CROSS JOIN latest_month lm
    LEFT JOIN companies_house_data ch ON lr.ch_matched_number_1 = ch.company_number
    WHERE lr.proprietor_1_name IS NOT NULL
    -- LIMIT TO RECENT DATA TO AVOID MEMORY ISSUES
    AND lr.file_month = lm.max_month
)
SELECT * FROM ownership_timeline;

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_lr_file_month_prop1 ON land_registry_data(file_month) WHERE proprietor_1_name IS NOT NULL;

-- Also create a function to get data for specific filters
CREATE OR REPLACE FUNCTION get_ownership_history(
    p_title_number TEXT DEFAULT NULL,
    p_company_number TEXT DEFAULT NULL,
    p_postcode TEXT DEFAULT NULL,
    p_file_month VARCHAR DEFAULT NULL
)
RETURNS TABLE (
    title_number VARCHAR,
    file_month VARCHAR,
    dataset_type VARCHAR,
    update_type VARCHAR,
    change_indicator VARCHAR,
    change_date DATE,
    tenure VARCHAR,
    property_address TEXT,
    district VARCHAR,
    county VARCHAR,
    region VARCHAR,
    postcode VARCHAR,
    price_paid NUMERIC,
    date_proprietor_added DATE,
    date_proprietor_added_yyyy_mm VARCHAR,
    date_proprietor_added_yyyy VARCHAR,
    date_proprietor_added_yyyy_q VARCHAR,
    multiple_address_indicator VARCHAR,
    additional_proprietor_indicator VARCHAR,
    source_filename VARCHAR,
    created_at TIMESTAMP,
    ch_match_date TIMESTAMP,
    proprietor_sequence INT,
    proprietor_name VARCHAR,
    lr_company_reg_no VARCHAR,
    proprietorship_category VARCHAR,
    country_incorporated VARCHAR,
    proprietor_address_1 VARCHAR,
    proprietor_address_2 VARCHAR,
    proprietor_address_3 VARCHAR,
    ch_matched_name VARCHAR,
    ch_matched_number VARCHAR,
    ch_match_type VARCHAR,
    ch_match_confidence NUMERIC,
    ch_company_name VARCHAR,
    ch_company_status VARCHAR,
    ch_company_category VARCHAR,
    ch_incorporation_date DATE,
    ch_dissolution_date DATE,
    ch_country_of_origin VARCHAR,
    ch_reg_address_line1 VARCHAR,
    ch_reg_address_line2 VARCHAR,
    ch_reg_address_post_town VARCHAR,
    ch_reg_address_county VARCHAR,
    ch_reg_address_country VARCHAR,
    ch_reg_address_postcode VARCHAR,
    ch_accounts_next_due_date DATE,
    ch_accounts_last_made_up_date DATE,
    ch_accounts_category VARCHAR,
    ch_sic_code_1 VARCHAR,
    ch_sic_code_2 VARCHAR,
    ch_sic_code_3 VARCHAR,
    ch_sic_code_4 VARCHAR,
    match_quality_description TEXT,
    change_description TEXT,
    ownership_status TEXT,
    ownership_duration_months INT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT * FROM v_ownership_history
    WHERE (p_title_number IS NULL OR v_ownership_history.title_number = p_title_number)
    AND (p_company_number IS NULL OR v_ownership_history.ch_matched_number = p_company_number)
    AND (p_postcode IS NULL OR v_ownership_history.postcode = p_postcode)
    AND (p_file_month IS NULL OR v_ownership_history.file_month = p_file_month);
END;
$$;

COMMENT ON VIEW v_ownership_history_limited IS 'Limited view showing only latest month data to avoid memory issues. Use filters for specific queries.';
COMMENT ON FUNCTION get_ownership_history IS 'Function to retrieve ownership history with specific filters to avoid memory issues.';