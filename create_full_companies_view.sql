-- Create a materialized view for better performance with full data
-- This pre-computes and stores the results for all companies

DROP VIEW IF EXISTS v_ownership_history CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_ownership_history CASCADE;

-- First, create a materialized view with all company data
CREATE MATERIALIZED VIEW mv_ownership_history AS
WITH company_data AS (
    SELECT 
        lr.id as lr_id,
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
        lr.additional_proprietor_indicator,
        lr.dataset_type,
        lr.update_type,
        lr.file_month,
        lr.change_indicator,
        lr.change_date,
        lr.source_filename,
        lr.created_at,
        lr.updated_at,
        m.ch_match_date,
        -- Proprietor 1
        CASE WHEN lr.proprietorship_1_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership') 
             AND m.ch_match_type_1 != 'Not_Company' THEN 1 ELSE NULL END as seq_1,
        lr.proprietor_1_name,
        lr.company_1_reg_no,
        lr.proprietorship_1_category,
        lr.country_1_incorporated,
        lr.proprietor_1_address_1,
        lr.proprietor_1_address_2,
        lr.proprietor_1_address_3,
        m.ch_matched_name_1,
        m.ch_matched_number_1,
        m.ch_match_type_1,
        m.ch_match_confidence_1,
        COALESCE(ch1.company_name, m.ch_matched_name_1) as ch_company_name_1,
        -- Proprietor 2
        CASE WHEN lr.proprietorship_2_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership') 
             AND m.ch_match_type_2 != 'Not_Company' THEN 2 ELSE NULL END as seq_2,
        lr.proprietor_2_name,
        lr.company_2_reg_no,
        lr.proprietorship_2_category,
        lr.country_2_incorporated,
        lr.proprietor_2_address_1,
        lr.proprietor_2_address_2,
        lr.proprietor_2_address_3,
        m.ch_matched_name_2,
        m.ch_matched_number_2,
        m.ch_match_type_2,
        m.ch_match_confidence_2,
        COALESCE(ch2.company_name, m.ch_matched_name_2) as ch_company_name_2,
        -- Proprietor 3
        CASE WHEN lr.proprietorship_3_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership') 
             AND m.ch_match_type_3 != 'Not_Company' THEN 3 ELSE NULL END as seq_3,
        lr.proprietor_3_name,
        lr.company_3_reg_no,
        lr.proprietorship_3_category,
        lr.country_3_incorporated,
        lr.proprietor_3_address_1,
        lr.proprietor_3_address_2,
        lr.proprietor_3_address_3,
        m.ch_matched_name_3,
        m.ch_matched_number_3,
        m.ch_match_type_3,
        m.ch_match_confidence_3,
        COALESCE(ch3.company_name, m.ch_matched_name_3) as ch_company_name_3,
        -- Proprietor 4
        CASE WHEN lr.proprietorship_4_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership') 
             AND m.ch_match_type_4 != 'Not_Company' THEN 4 ELSE NULL END as seq_4,
        lr.proprietor_4_name,
        lr.company_4_reg_no,
        lr.proprietorship_4_category,
        lr.country_4_incorporated,
        lr.proprietor_4_address_1,
        lr.proprietor_4_address_2,
        lr.proprietor_4_address_3,
        m.ch_matched_name_4,
        m.ch_matched_number_4,
        m.ch_match_type_4,
        m.ch_match_confidence_4,
        COALESCE(ch4.company_name, m.ch_matched_name_4) as ch_company_name_4
    FROM land_registry_data lr
    LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
    LEFT JOIN companies_house_data ch1 ON m.ch_matched_number_1 = ch1.company_number
    LEFT JOIN companies_house_data ch2 ON m.ch_matched_number_2 = ch2.company_number
    LEFT JOIN companies_house_data ch3 ON m.ch_matched_number_3 = ch3.company_number
    LEFT JOIN companies_house_data ch4 ON m.ch_matched_number_4 = ch4.company_number
    -- Only include records that have at least one company proprietor
    WHERE 
        (lr.proprietorship_1_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership') AND m.ch_match_type_1 != 'Not_Company') OR
        (lr.proprietorship_2_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership') AND m.ch_match_type_2 != 'Not_Company') OR
        (lr.proprietorship_3_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership') AND m.ch_match_type_3 != 'Not_Company') OR
        (lr.proprietorship_4_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership') AND m.ch_match_type_4 != 'Not_Company')
)
-- Normalize the data
SELECT 
    lr_id as id,
    title_number,
    tenure,
    property_address,
    district,
    county,
    region,
    postcode,
    multiple_address_indicator,
    price_paid,
    date_proprietor_added,
    TO_CHAR(date_proprietor_added, 'YYYY-MM') as date_proprietor_added_YYYY_MM,
    TO_CHAR(date_proprietor_added, 'YYYY') as date_proprietor_added_YYYY,
    additional_proprietor_indicator,
    dataset_type,
    update_type,
    file_month,
    change_indicator,
    change_date,
    source_filename,
    created_at,
    ch_match_date,
    proprietor_sequence,
    proprietor_name,
    lr_company_reg_no,
    proprietorship_category,
    country_incorporated,
    proprietor_address_1,
    proprietor_address_2,
    proprietor_address_3,
    ch_matched_name,
    ch_matched_number,
    ch_match_type,
    ch_match_confidence,
    ch_company_name,
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
    -- Placeholder CH fields
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
FROM (
    -- Proprietor 1
    SELECT *, seq_1 as proprietor_sequence, 
           proprietor_1_name as proprietor_name,
           company_1_reg_no as lr_company_reg_no,
           proprietorship_1_category as proprietorship_category,
           country_1_incorporated as country_incorporated,
           proprietor_1_address_1 as proprietor_address_1,
           proprietor_1_address_2 as proprietor_address_2,
           proprietor_1_address_3 as proprietor_address_3,
           ch_matched_name_1 as ch_matched_name,
           ch_matched_number_1 as ch_matched_number,
           ch_match_type_1 as ch_match_type,
           ch_match_confidence_1 as ch_match_confidence,
           ch_company_name_1 as ch_company_name
    FROM company_data
    WHERE seq_1 IS NOT NULL
    
    UNION ALL
    
    -- Proprietor 2
    SELECT *, seq_2 as proprietor_sequence,
           proprietor_2_name as proprietor_name,
           company_2_reg_no as lr_company_reg_no,
           proprietorship_2_category as proprietorship_category,
           country_2_incorporated as country_incorporated,
           proprietor_2_address_1 as proprietor_address_1,
           proprietor_2_address_2 as proprietor_address_2,
           proprietor_2_address_3 as proprietor_address_3,
           ch_matched_name_2 as ch_matched_name,
           ch_matched_number_2 as ch_matched_number,
           ch_match_type_2 as ch_match_type,
           ch_match_confidence_2 as ch_match_confidence,
           ch_company_name_2 as ch_company_name
    FROM company_data
    WHERE seq_2 IS NOT NULL
    
    UNION ALL
    
    -- Proprietor 3
    SELECT *, seq_3 as proprietor_sequence,
           proprietor_3_name as proprietor_name,
           company_3_reg_no as lr_company_reg_no,
           proprietorship_3_category as proprietorship_category,
           country_3_incorporated as country_incorporated,
           proprietor_3_address_1 as proprietor_address_1,
           proprietor_3_address_2 as proprietor_address_2,
           proprietor_3_address_3 as proprietor_address_3,
           ch_matched_name_3 as ch_matched_name,
           ch_matched_number_3 as ch_matched_number,
           ch_match_type_3 as ch_match_type,
           ch_match_confidence_3 as ch_match_confidence,
           ch_company_name_3 as ch_company_name
    FROM company_data
    WHERE seq_3 IS NOT NULL
    
    UNION ALL
    
    -- Proprietor 4
    SELECT *, seq_4 as proprietor_sequence,
           proprietor_4_name as proprietor_name,
           company_4_reg_no as lr_company_reg_no,
           proprietorship_4_category as proprietorship_category,
           country_4_incorporated as country_incorporated,
           proprietor_4_address_1 as proprietor_address_1,
           proprietor_4_address_2 as proprietor_address_2,
           proprietor_4_address_3 as proprietor_address_3,
           ch_matched_name_4 as ch_matched_name,
           ch_matched_number_4 as ch_matched_number,
           ch_match_type_4 as ch_match_type,
           ch_match_confidence_4 as ch_match_confidence,
           ch_company_name_4 as ch_company_name
    FROM company_data
    WHERE seq_4 IS NOT NULL
) normalized;

-- Create indexes on the materialized view
CREATE INDEX idx_mv_ownership_title ON mv_ownership_history(title_number);
CREATE INDEX idx_mv_ownership_company ON mv_ownership_history(ch_matched_number);
CREATE INDEX idx_mv_ownership_postcode ON mv_ownership_history(postcode);
CREATE INDEX idx_mv_ownership_file_month ON mv_ownership_history(file_month);
CREATE INDEX idx_mv_ownership_match_type ON mv_ownership_history(ch_match_type);

-- Create a regular view that queries the materialized view
CREATE VIEW v_ownership_history AS
SELECT * FROM mv_ownership_history;

-- Add comments
COMMENT ON MATERIALIZED VIEW mv_ownership_history IS 'Pre-computed view of all Limited Companies/PLCs and LLPs property ownership. Refresh periodically with: REFRESH MATERIALIZED VIEW mv_ownership_history;';
COMMENT ON VIEW v_ownership_history IS 'View of company property ownership. Shows only Limited Companies/PLCs and LLPs. Queries the materialized view for performance.';

-- Function to refresh the materialized view
CREATE OR REPLACE FUNCTION refresh_ownership_history()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_ownership_history;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION refresh_ownership_history IS 'Refreshes the ownership history materialized view. Run after data updates.';