-- Create a view that only shows Limited Companies/PLCs and LLPs
-- This filters out individuals, charities, and other non-company proprietors

DROP VIEW IF EXISTS v_ownership_history CASCADE;

CREATE OR REPLACE VIEW v_ownership_history AS
WITH latest_month AS (
    SELECT MAX(file_month) as max_month FROM land_registry_data
),
limited_data AS (
    SELECT lr.id as lr_id,
           lr.title_number,
           lr.tenure,
           lr.property_address,
           lr.district,
           lr.county,
           lr.region,
           lr.postcode,
           lr.multiple_address_indicator,
           lr.price_paid,
           lr.proprietor_1_name,
           lr.company_1_reg_no,
           lr.proprietorship_1_category,
           lr.country_1_incorporated,
           lr.proprietor_1_address_1,
           lr.proprietor_1_address_2,
           lr.proprietor_1_address_3,
           lr.proprietor_2_name,
           lr.company_2_reg_no,
           lr.proprietorship_2_category,
           lr.country_2_incorporated,
           lr.proprietor_2_address_1,
           lr.proprietor_2_address_2,
           lr.proprietor_2_address_3,
           lr.proprietor_3_name,
           lr.company_3_reg_no,
           lr.proprietorship_3_category,
           lr.country_3_incorporated,
           lr.proprietor_3_address_1,
           lr.proprietor_3_address_2,
           lr.proprietor_3_address_3,
           lr.proprietor_4_name,
           lr.company_4_reg_no,
           lr.proprietorship_4_category,
           lr.country_4_incorporated,
           lr.proprietor_4_address_1,
           lr.proprietor_4_address_2,
           lr.proprietor_4_address_3,
           lr.date_proprietor_added,
           lr.additional_proprietor_indicator,
           lr.dataset_type,
           lr.update_type,
           lr.file_month,
           lr.change_indicator,
           lr.change_date,
           lr.source_filename,
           lr.created_at,
           lr.updated_at,
           m.ch_matched_name_1,
           m.ch_matched_number_1,
           m.ch_match_type_1,
           m.ch_match_confidence_1,
           m.ch_matched_name_2,
           m.ch_matched_number_2,
           m.ch_match_type_2,
           m.ch_match_confidence_2,
           m.ch_matched_name_3,
           m.ch_matched_number_3,
           m.ch_match_type_3,
           m.ch_match_confidence_3,
           m.ch_matched_name_4,
           m.ch_matched_number_4,
           m.ch_match_type_4,
           m.ch_match_confidence_4,
           m.ch_match_date,
           COALESCE(ch1.company_name, m.ch_matched_name_1) as ch_company_name_1,
           COALESCE(ch2.company_name, m.ch_matched_name_2) as ch_company_name_2,
           COALESCE(ch3.company_name, m.ch_matched_name_3) as ch_company_name_3,
           COALESCE(ch4.company_name, m.ch_matched_name_4) as ch_company_name_4
    FROM land_registry_data lr
    CROSS JOIN latest_month lm
    LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
    LEFT JOIN companies_house_data ch1 ON m.ch_matched_number_1 = ch1.company_number
    LEFT JOIN companies_house_data ch2 ON m.ch_matched_number_2 = ch2.company_number
    LEFT JOIN companies_house_data ch3 ON m.ch_matched_number_3 = ch3.company_number
    LEFT JOIN companies_house_data ch4 ON m.ch_matched_number_4 = ch4.company_number
    -- IMPORTANT: Limit to recent data by default
    WHERE lr.file_month >= lm.max_month - INTERVAL '3 months'
),
normalized AS (
    -- Proprietor 1 - ONLY Limited Companies/PLCs and LLPs
    SELECT 
        ld.lr_id as id,
        ld.title_number,
        ld.tenure,
        ld.property_address,
        ld.district,
        ld.county,
        ld.region,
        ld.postcode,
        ld.multiple_address_indicator,
        ld.price_paid,
        ld.date_proprietor_added,
        TO_CHAR(ld.date_proprietor_added, 'YYYY-MM') as date_proprietor_added_YYYY_MM,
        TO_CHAR(ld.date_proprietor_added, 'YYYY') as date_proprietor_added_YYYY,
        ld.additional_proprietor_indicator,
        ld.dataset_type,
        ld.update_type,
        ld.file_month,
        ld.change_indicator,
        ld.change_date,
        ld.source_filename,
        ld.created_at,
        ld.ch_match_date,
        1 as proprietor_sequence,
        ld.proprietor_1_name as proprietor_name,
        ld.company_1_reg_no as lr_company_reg_no,
        ld.proprietorship_1_category as proprietorship_category,
        ld.country_1_incorporated as country_incorporated,
        ld.proprietor_1_address_1 as proprietor_address_1,
        ld.proprietor_1_address_2 as proprietor_address_2,
        ld.proprietor_1_address_3 as proprietor_address_3,
        ld.ch_matched_name_1 as ch_matched_name,
        ld.ch_matched_number_1 as ch_matched_number,
        ld.ch_match_type_1 as ch_match_type,
        ld.ch_match_confidence_1 as ch_match_confidence,
        ld.ch_company_name_1 as ch_company_name
    FROM limited_data ld
    WHERE ld.proprietor_1_name IS NOT NULL
    -- FILTER: Only show Limited Companies/PLCs and LLPs
    AND ld.proprietorship_1_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership')
    AND ld.ch_match_type_1 != 'Not_Company'
    
    UNION ALL
    
    -- Proprietor 2 - ONLY Limited Companies/PLCs and LLPs
    SELECT 
        ld.lr_id as id,
        ld.title_number,
        ld.tenure,
        ld.property_address,
        ld.district,
        ld.county,
        ld.region,
        ld.postcode,
        ld.multiple_address_indicator,
        ld.price_paid,
        ld.date_proprietor_added,
        TO_CHAR(ld.date_proprietor_added, 'YYYY-MM') as date_proprietor_added_YYYY_MM,
        TO_CHAR(ld.date_proprietor_added, 'YYYY') as date_proprietor_added_YYYY,
        ld.additional_proprietor_indicator,
        ld.dataset_type,
        ld.update_type,
        ld.file_month,
        ld.change_indicator,
        ld.change_date,
        ld.source_filename,
        ld.created_at,
        ld.ch_match_date,
        2 as proprietor_sequence,
        ld.proprietor_2_name as proprietor_name,
        ld.company_2_reg_no as lr_company_reg_no,
        ld.proprietorship_2_category as proprietorship_category,
        ld.country_2_incorporated as country_incorporated,
        ld.proprietor_2_address_1 as proprietor_address_1,
        ld.proprietor_2_address_2 as proprietor_address_2,
        ld.proprietor_2_address_3 as proprietor_address_3,
        ld.ch_matched_name_2 as ch_matched_name,
        ld.ch_matched_number_2 as ch_matched_number,
        ld.ch_match_type_2 as ch_match_type,
        ld.ch_match_confidence_2 as ch_match_confidence,
        ld.ch_company_name_2 as ch_company_name
    FROM limited_data ld
    WHERE ld.proprietor_2_name IS NOT NULL
    -- FILTER: Only show Limited Companies/PLCs and LLPs
    AND ld.proprietorship_2_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership')
    AND ld.ch_match_type_2 != 'Not_Company'
    
    UNION ALL
    
    -- Proprietor 3 - ONLY Limited Companies/PLCs and LLPs
    SELECT 
        ld.lr_id as id,
        ld.title_number,
        ld.tenure,
        ld.property_address,
        ld.district,
        ld.county,
        ld.region,
        ld.postcode,
        ld.multiple_address_indicator,
        ld.price_paid,
        ld.date_proprietor_added,
        TO_CHAR(ld.date_proprietor_added, 'YYYY-MM') as date_proprietor_added_YYYY_MM,
        TO_CHAR(ld.date_proprietor_added, 'YYYY') as date_proprietor_added_YYYY,
        ld.additional_proprietor_indicator,
        ld.dataset_type,
        ld.update_type,
        ld.file_month,
        ld.change_indicator,
        ld.change_date,
        ld.source_filename,
        ld.created_at,
        ld.ch_match_date,
        3 as proprietor_sequence,
        ld.proprietor_3_name as proprietor_name,
        ld.company_3_reg_no as lr_company_reg_no,
        ld.proprietorship_3_category as proprietorship_category,
        ld.country_3_incorporated as country_incorporated,
        ld.proprietor_3_address_1 as proprietor_address_1,
        ld.proprietor_3_address_2 as proprietor_address_2,
        ld.proprietor_3_address_3 as proprietor_address_3,
        ld.ch_matched_name_3 as ch_matched_name,
        ld.ch_matched_number_3 as ch_matched_number,
        ld.ch_match_type_3 as ch_match_type,
        ld.ch_match_confidence_3 as ch_match_confidence,
        ld.ch_company_name_3 as ch_company_name
    FROM limited_data ld
    WHERE ld.proprietor_3_name IS NOT NULL
    -- FILTER: Only show Limited Companies/PLCs and LLPs
    AND ld.proprietorship_3_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership')
    AND ld.ch_match_type_3 != 'Not_Company'
    
    UNION ALL
    
    -- Proprietor 4 - ONLY Limited Companies/PLCs and LLPs
    SELECT 
        ld.lr_id as id,
        ld.title_number,
        ld.tenure,
        ld.property_address,
        ld.district,
        ld.county,
        ld.region,
        ld.postcode,
        ld.multiple_address_indicator,
        ld.price_paid,
        ld.date_proprietor_added,
        TO_CHAR(ld.date_proprietor_added, 'YYYY-MM') as date_proprietor_added_YYYY_MM,
        TO_CHAR(ld.date_proprietor_added, 'YYYY') as date_proprietor_added_YYYY,
        ld.additional_proprietor_indicator,
        ld.dataset_type,
        ld.update_type,
        ld.file_month,
        ld.change_indicator,
        ld.change_date,
        ld.source_filename,
        ld.created_at,
        ld.ch_match_date,
        4 as proprietor_sequence,
        ld.proprietor_4_name as proprietor_name,
        ld.company_4_reg_no as lr_company_reg_no,
        ld.proprietorship_4_category as proprietorship_category,
        ld.country_4_incorporated as country_incorporated,
        ld.proprietor_4_address_1 as proprietor_address_1,
        ld.proprietor_4_address_2 as proprietor_address_2,
        ld.proprietor_4_address_3 as proprietor_address_3,
        ld.ch_matched_name_4 as ch_matched_name,
        ld.ch_matched_number_4 as ch_matched_number,
        ld.ch_match_type_4 as ch_match_type,
        ld.ch_match_confidence_4 as ch_match_confidence,
        ld.ch_company_name_4 as ch_company_name
    FROM limited_data ld
    WHERE ld.proprietor_4_name IS NOT NULL
    -- FILTER: Only show Limited Companies/PLCs and LLPs
    AND ld.proprietorship_4_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership')
    AND ld.ch_match_type_4 != 'Not_Company'
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
COMMENT ON VIEW v_ownership_history IS 'Companies-only view showing Limited Companies/PLCs and LLPs. Excludes individuals, charities, and other non-company proprietors. Limited to last 3 months by default.';