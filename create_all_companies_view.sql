-- Create a view showing ALL company data (no date restrictions)
-- But still filtered to only show Limited Companies/PLCs and LLPs

DROP VIEW IF EXISTS v_ownership_history CASCADE;

-- Create the view with company filtering but NO date restrictions
CREATE OR REPLACE VIEW v_ownership_history AS
WITH normalized AS (
    -- Proprietor 1 - ONLY Limited Companies/PLCs and LLPs
    SELECT 
        lr.id,
        lr.title_number,
        lr.tenure,
        lr.property_address,
        lr.district,
        lr.county,
        lr.region,
        lr.postcode,
        lr.multiple_address_indicator,
        lr.price_paid,
        lr.date_proprietor_added,
        TO_CHAR(lr.date_proprietor_added, 'YYYY-MM') as date_proprietor_added_YYYY_MM,
        TO_CHAR(lr.date_proprietor_added, 'YYYY') as date_proprietor_added_YYYY,
        TO_CHAR(lr.date_proprietor_added, 'YYYY-"Q"Q') as date_proprietor_added_YYYY_Q,
        lr.additional_proprietor_indicator,
        lr.dataset_type,
        lr.update_type,
        lr.file_month,
        lr.change_indicator,
        lr.change_date,
        lr.source_filename,
        lr.created_at,
        m.ch_match_date,
        1 as proprietor_sequence,
        lr.proprietor_1_name as proprietor_name,
        lr.company_1_reg_no as lr_company_reg_no,
        lr.proprietorship_1_category as proprietorship_category,
        lr.country_1_incorporated as country_incorporated,
        lr.proprietor_1_address_1 as proprietor_address_1,
        lr.proprietor_1_address_2 as proprietor_address_2,
        lr.proprietor_1_address_3 as proprietor_address_3,
        m.ch_matched_name_1 as ch_matched_name,
        m.ch_matched_number_1 as ch_matched_number,
        m.ch_match_type_1 as ch_match_type,
        m.ch_match_confidence_1 as ch_match_confidence,
        COALESCE(ch.company_name, m.ch_matched_name_1) as ch_company_name
    FROM land_registry_data lr
    LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
    LEFT JOIN companies_house_data ch ON m.ch_matched_number_1 = ch.company_number
    WHERE lr.proprietor_1_name IS NOT NULL
    -- FILTER: Only show Limited Companies/PLCs and LLPs
    AND lr.proprietorship_1_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership')
    AND COALESCE(m.ch_match_type_1, '') != 'Not_Company'
    -- FILTER: Exclude invalid future dates
    AND (lr.date_proprietor_added IS NULL OR lr.date_proprietor_added <= CURRENT_DATE)
    
    UNION ALL
    
    -- Proprietor 2 - ONLY Limited Companies/PLCs and LLPs
    SELECT 
        lr.id,
        lr.title_number,
        lr.tenure,
        lr.property_address,
        lr.district,
        lr.county,
        lr.region,
        lr.postcode,
        lr.multiple_address_indicator,
        lr.price_paid,
        lr.date_proprietor_added,
        TO_CHAR(lr.date_proprietor_added, 'YYYY-MM') as date_proprietor_added_YYYY_MM,
        TO_CHAR(lr.date_proprietor_added, 'YYYY') as date_proprietor_added_YYYY,
        TO_CHAR(lr.date_proprietor_added, 'YYYY-"Q"Q') as date_proprietor_added_YYYY_Q,
        lr.additional_proprietor_indicator,
        lr.dataset_type,
        lr.update_type,
        lr.file_month,
        lr.change_indicator,
        lr.change_date,
        lr.source_filename,
        lr.created_at,
        m.ch_match_date,
        2 as proprietor_sequence,
        lr.proprietor_2_name as proprietor_name,
        lr.company_2_reg_no as lr_company_reg_no,
        lr.proprietorship_2_category as proprietorship_category,
        lr.country_2_incorporated as country_incorporated,
        lr.proprietor_2_address_1 as proprietor_address_1,
        lr.proprietor_2_address_2 as proprietor_address_2,
        lr.proprietor_2_address_3 as proprietor_address_3,
        m.ch_matched_name_2 as ch_matched_name,
        m.ch_matched_number_2 as ch_matched_number,
        m.ch_match_type_2 as ch_match_type,
        m.ch_match_confidence_2 as ch_match_confidence,
        COALESCE(ch.company_name, m.ch_matched_name_2) as ch_company_name
    FROM land_registry_data lr
    LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
    LEFT JOIN companies_house_data ch ON m.ch_matched_number_2 = ch.company_number
    WHERE lr.proprietor_2_name IS NOT NULL
    -- FILTER: Only show Limited Companies/PLCs and LLPs
    AND lr.proprietorship_2_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership')
    AND COALESCE(m.ch_match_type_2, '') != 'Not_Company'
    -- FILTER: Exclude invalid future dates
    AND (lr.date_proprietor_added IS NULL OR lr.date_proprietor_added <= CURRENT_DATE)
    
    UNION ALL
    
    -- Proprietor 3 - ONLY Limited Companies/PLCs and LLPs
    SELECT 
        lr.id,
        lr.title_number,
        lr.tenure,
        lr.property_address,
        lr.district,
        lr.county,
        lr.region,
        lr.postcode,
        lr.multiple_address_indicator,
        lr.price_paid,
        lr.date_proprietor_added,
        TO_CHAR(lr.date_proprietor_added, 'YYYY-MM') as date_proprietor_added_YYYY_MM,
        TO_CHAR(lr.date_proprietor_added, 'YYYY') as date_proprietor_added_YYYY,
        TO_CHAR(lr.date_proprietor_added, 'YYYY-"Q"Q') as date_proprietor_added_YYYY_Q,
        lr.additional_proprietor_indicator,
        lr.dataset_type,
        lr.update_type,
        lr.file_month,
        lr.change_indicator,
        lr.change_date,
        lr.source_filename,
        lr.created_at,
        m.ch_match_date,
        3 as proprietor_sequence,
        lr.proprietor_3_name as proprietor_name,
        lr.company_3_reg_no as lr_company_reg_no,
        lr.proprietorship_3_category as proprietorship_category,
        lr.country_3_incorporated as country_incorporated,
        lr.proprietor_3_address_1 as proprietor_address_1,
        lr.proprietor_3_address_2 as proprietor_address_2,
        lr.proprietor_3_address_3 as proprietor_address_3,
        m.ch_matched_name_3 as ch_matched_name,
        m.ch_matched_number_3 as ch_matched_number,
        m.ch_match_type_3 as ch_match_type,
        m.ch_match_confidence_3 as ch_match_confidence,
        COALESCE(ch.company_name, m.ch_matched_name_3) as ch_company_name
    FROM land_registry_data lr
    LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
    LEFT JOIN companies_house_data ch ON m.ch_matched_number_3 = ch.company_number
    WHERE lr.proprietor_3_name IS NOT NULL
    -- FILTER: Only show Limited Companies/PLCs and LLPs
    AND lr.proprietorship_3_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership')
    AND COALESCE(m.ch_match_type_3, '') != 'Not_Company'
    -- FILTER: Exclude invalid future dates
    AND (lr.date_proprietor_added IS NULL OR lr.date_proprietor_added <= CURRENT_DATE)
    
    UNION ALL
    
    -- Proprietor 4 - ONLY Limited Companies/PLCs and LLPs
    SELECT 
        lr.id,
        lr.title_number,
        lr.tenure,
        lr.property_address,
        lr.district,
        lr.county,
        lr.region,
        lr.postcode,
        lr.multiple_address_indicator,
        lr.price_paid,
        lr.date_proprietor_added,
        TO_CHAR(lr.date_proprietor_added, 'YYYY-MM') as date_proprietor_added_YYYY_MM,
        TO_CHAR(lr.date_proprietor_added, 'YYYY') as date_proprietor_added_YYYY,
        TO_CHAR(lr.date_proprietor_added, 'YYYY-"Q"Q') as date_proprietor_added_YYYY_Q,
        lr.additional_proprietor_indicator,
        lr.dataset_type,
        lr.update_type,
        lr.file_month,
        lr.change_indicator,
        lr.change_date,
        lr.source_filename,
        lr.created_at,
        m.ch_match_date,
        4 as proprietor_sequence,
        lr.proprietor_4_name as proprietor_name,
        lr.company_4_reg_no as lr_company_reg_no,
        lr.proprietorship_4_category as proprietorship_category,
        lr.country_4_incorporated as country_incorporated,
        lr.proprietor_4_address_1 as proprietor_address_1,
        lr.proprietor_4_address_2 as proprietor_address_2,
        lr.proprietor_4_address_3 as proprietor_address_3,
        m.ch_matched_name_4 as ch_matched_name,
        m.ch_matched_number_4 as ch_matched_number,
        m.ch_match_type_4 as ch_match_type,
        m.ch_match_confidence_4 as ch_match_confidence,
        COALESCE(ch.company_name, m.ch_matched_name_4) as ch_company_name
    FROM land_registry_data lr
    LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
    LEFT JOIN companies_house_data ch ON m.ch_matched_number_4 = ch.company_number
    WHERE lr.proprietor_4_name IS NOT NULL
    -- FILTER: Only show Limited Companies/PLCs and LLPs
    AND lr.proprietorship_4_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership')
    AND COALESCE(m.ch_match_type_4, '') != 'Not_Company'
    -- FILTER: Exclude invalid future dates
    AND (lr.date_proprietor_added IS NULL OR lr.date_proprietor_added <= CURRENT_DATE)
)
SELECT 
    n.*,
    -- Add computed columns that frontend expects
    CASE 
        WHEN ch_match_confidence >= 0.9 THEN 'High'
        WHEN ch_match_confidence >= 0.7 THEN 'Good'
        WHEN ch_match_confidence >= 0.5 THEN 'Medium'
        WHEN ch_match_confidence >= 0.3 THEN 'Poor'
        ELSE 'No Match'
    END as match_quality_description,
    
    CASE 
        WHEN change_indicator = 'A' THEN 'Added'
        WHEN change_indicator = 'D' THEN 'Deleted'
        ELSE 'No Change'
    END as change_description,
    
    CASE 
        WHEN change_indicator = 'D' THEN 'Historical'
        WHEN file_month = (SELECT MAX(file_month) FROM land_registry_data) THEN 'Current'
        ELSE 'Historical'
    END as ownership_status,
    
    CASE 
        WHEN date_proprietor_added IS NOT NULL THEN
            EXTRACT(YEAR FROM AGE(
                CASE 
                    WHEN change_indicator = 'D' AND change_date IS NOT NULL THEN change_date
                    ELSE CURRENT_DATE
                END,
                date_proprietor_added
            )) * 12 +
            EXTRACT(MONTH FROM AGE(
                CASE 
                    WHEN change_indicator = 'D' AND change_date IS NOT NULL THEN change_date
                    ELSE CURRENT_DATE
                END,
                date_proprietor_added
            ))
        ELSE NULL
    END::INTEGER as ownership_duration_months,
    
    -- Add placeholders for missing CH fields that frontend expects
    NULL::VARCHAR(100) as ch_company_status,
    NULL::VARCHAR(100) as ch_company_category,
    NULL::DATE as ch_incorporation_date,
    NULL::DATE as ch_dissolution_date,
    NULL::VARCHAR(100) as ch_country_of_origin,
    NULL::TEXT as ch_reg_address_line1,
    NULL::TEXT as ch_reg_address_line2,
    NULL::TEXT as ch_reg_address_post_town,
    NULL::TEXT as ch_reg_address_county,
    NULL::TEXT as ch_reg_address_country,
    NULL::VARCHAR(20) as ch_reg_address_postcode,
    NULL::DATE as ch_accounts_next_due_date,
    NULL::DATE as ch_accounts_last_made_up_date,
    NULL::VARCHAR(50) as ch_accounts_category,
    NULL::VARCHAR(10) as ch_sic_code_1,
    NULL::VARCHAR(10) as ch_sic_code_2,
    NULL::VARCHAR(10) as ch_sic_code_3,
    NULL::VARCHAR(10) as ch_sic_code_4
FROM normalized n;

-- Create indexes if needed
CREATE INDEX IF NOT EXISTS idx_lr_proprietorship_categories ON land_registry_data(
    proprietorship_1_category, 
    proprietorship_2_category,
    proprietorship_3_category,
    proprietorship_4_category
);

-- Add comment
COMMENT ON VIEW v_ownership_history IS 'Companies-only view showing ALL Limited Companies/PLCs and LLPs across all dates. Excludes records with future date_proprietor_added. WARNING: Large dataset - use WHERE clauses to filter by title_number, ch_matched_number, postcode, or file_month to avoid memory issues.';