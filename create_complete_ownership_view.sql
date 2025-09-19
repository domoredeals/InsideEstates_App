-- Create complete ownership history view with all required fields
-- This view includes all fields for Qlik integration

DROP VIEW IF EXISTS v_ownership_history CASCADE;

CREATE VIEW v_ownership_history AS
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

    UNION ALL

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
        
        -- Date breakdown fields
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
        
        -- Proprietor 2
        2 as proprietor_sequence,
        lr.proprietor_2_name as proprietor_name,
        lr.company_2_reg_no as lr_company_reg_no,
        lr.proprietorship_2_category as proprietorship_category,
        lr.country_2_incorporated as country_incorporated,
        lr.proprietor_2_address_1 as proprietor_address_1,
        lr.proprietor_2_address_2 as proprietor_address_2,
        lr.proprietor_2_address_3 as proprietor_address_3,
        
        -- Companies House matching details
        lr.ch_matched_name_2 as ch_matched_name,
        lr.ch_matched_number_2 as ch_matched_number,
        lr.ch_match_type_2 as ch_match_type,
        lr.ch_match_confidence_2 as ch_match_confidence,
        
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
            WHEN lr.ch_match_confidence_2 >= 1.0 THEN 'Perfect Match - Company Number & Name'
            WHEN lr.ch_match_confidence_2 >= 0.9 THEN 'High Confidence - Company Number Only'
            WHEN lr.ch_match_confidence_2 >= 0.7 THEN 'Medium Confidence - Name Match'
            WHEN lr.ch_match_confidence_2 >= 0.5 THEN 'Low Confidence - Previous Name'
            WHEN lr.ch_match_confidence_2 >= 0.3 THEN 'Land Registry Data Only'
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
    LEFT JOIN companies_house_data ch ON lr.ch_matched_number_2 = ch.company_number
    WHERE lr.proprietor_2_name IS NOT NULL

    UNION ALL

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
        
        -- Date breakdown fields
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
        
        -- Proprietor 3
        3 as proprietor_sequence,
        lr.proprietor_3_name as proprietor_name,
        lr.company_3_reg_no as lr_company_reg_no,
        lr.proprietorship_3_category as proprietorship_category,
        lr.country_3_incorporated as country_incorporated,
        lr.proprietor_3_address_1 as proprietor_address_1,
        lr.proprietor_3_address_2 as proprietor_address_2,
        lr.proprietor_3_address_3 as proprietor_address_3,
        
        -- Companies House matching details
        lr.ch_matched_name_3 as ch_matched_name,
        lr.ch_matched_number_3 as ch_matched_number,
        lr.ch_match_type_3 as ch_match_type,
        lr.ch_match_confidence_3 as ch_match_confidence,
        
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
            WHEN lr.ch_match_confidence_3 >= 1.0 THEN 'Perfect Match - Company Number & Name'
            WHEN lr.ch_match_confidence_3 >= 0.9 THEN 'High Confidence - Company Number Only'
            WHEN lr.ch_match_confidence_3 >= 0.7 THEN 'Medium Confidence - Name Match'
            WHEN lr.ch_match_confidence_3 >= 0.5 THEN 'Low Confidence - Previous Name'
            WHEN lr.ch_match_confidence_3 >= 0.3 THEN 'Land Registry Data Only'
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
    LEFT JOIN companies_house_data ch ON lr.ch_matched_number_3 = ch.company_number
    WHERE lr.proprietor_3_name IS NOT NULL

    UNION ALL

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
        
        -- Date breakdown fields
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
        
        -- Proprietor 4
        4 as proprietor_sequence,
        lr.proprietor_4_name as proprietor_name,
        lr.company_4_reg_no as lr_company_reg_no,
        lr.proprietorship_4_category as proprietorship_category,
        lr.country_4_incorporated as country_incorporated,
        lr.proprietor_4_address_1 as proprietor_address_1,
        lr.proprietor_4_address_2 as proprietor_address_2,
        lr.proprietor_4_address_3 as proprietor_address_3,
        
        -- Companies House matching details
        lr.ch_matched_name_4 as ch_matched_name,
        lr.ch_matched_number_4 as ch_matched_number,
        lr.ch_match_type_4 as ch_match_type,
        lr.ch_match_confidence_4 as ch_match_confidence,
        
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
            WHEN lr.ch_match_confidence_4 >= 1.0 THEN 'Perfect Match - Company Number & Name'
            WHEN lr.ch_match_confidence_4 >= 0.9 THEN 'High Confidence - Company Number Only'
            WHEN lr.ch_match_confidence_4 >= 0.7 THEN 'Medium Confidence - Name Match'
            WHEN lr.ch_match_confidence_4 >= 0.5 THEN 'Low Confidence - Previous Name'
            WHEN lr.ch_match_confidence_4 >= 0.3 THEN 'Land Registry Data Only'
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
    LEFT JOIN companies_house_data ch ON lr.ch_matched_number_4 = ch.company_number
    WHERE lr.proprietor_4_name IS NOT NULL
)
SELECT * FROM ownership_timeline;

-- Add comment
COMMENT ON VIEW v_ownership_history IS 'Complete ownership history view with all fields for Qlik integration. Includes calculated fields for match quality, ownership status, and duration.';