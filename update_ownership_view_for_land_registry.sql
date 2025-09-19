-- Update ownership history view to use ch_matched_name when ch_company_name is NULL
-- This ensures frontend always has a company name to display

-- Drop existing view
DROP VIEW IF EXISTS v_ownership_history CASCADE;

-- Create improved view that uses ch_matched_name as fallback
CREATE OR REPLACE VIEW v_ownership_history AS
WITH ownership_data AS (
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
        lr.additional_proprietor_indicator,
        lr.dataset_type,
        lr.update_type,
        lr.file_month,
        lr.change_indicator,
        lr.change_date,
        lr.created_at,
        lr.updated_at,
        
        -- Proprietor 1
        1 as proprietor_sequence,
        lr.proprietor_1_name as proprietor_name,
        lr.company_1_reg_no as company_reg_no,
        lr.proprietorship_1_category as proprietorship_category,
        lr.country_1_incorporated as country_incorporated,
        lr.proprietor_1_address_1 as proprietor_address_1,
        lr.proprietor_1_address_2 as proprietor_address_2,
        lr.proprietor_1_address_3 as proprietor_address_3,
        lr.proprietor_1_postcode as proprietor_postcode,
        
        -- CH Match data
        m.ch_matched_name_1 as ch_matched_name,
        m.ch_matched_number_1 as ch_matched_number,
        m.ch_match_type_1 as ch_match_type,
        m.ch_match_confidence_1 as ch_match_confidence,
        
        -- Companies House company details (use matched name as fallback)
        COALESCE(ch.company_name, m.ch_matched_name_1) as ch_company_name,
        ch.company_status as ch_company_status,
        ch.company_category as ch_company_category,
        ch.incorporation_date as ch_incorporation_date,
        ch.dissolution_date as ch_dissolution_date,
        ch.country_of_origin as ch_country_of_origin,
        ch.reg_address_line1 as ch_reg_address_line1,
        ch.reg_address_line2 as ch_reg_address_line2,
        ch.reg_address_county as ch_reg_address_county,
        ch.reg_address_country as ch_reg_address_country,
        ch.reg_address_post_town as ch_reg_address_post_town,
        ch.reg_address_postcode as ch_reg_address_postcode,
        ch.sic_code_1 as ch_sic_code_1,
        ch.sic_code_2 as ch_sic_code_2,
        ch.sic_code_3 as ch_sic_code_3,
        ch.sic_code_4 as ch_sic_code_4,
        
        -- Calculate ownership status
        CASE 
            WHEN lr.change_indicator = 'D' THEN 'Historical'
            WHEN lr.file_month = (SELECT MAX(file_month) FROM land_registry_data) THEN 'Current'
            ELSE 'Historical'
        END as ownership_status,
        
        -- Calculate ownership duration (months since date_proprietor_added)
        CASE 
            WHEN lr.date_proprietor_added IS NOT NULL THEN
                EXTRACT(YEAR FROM AGE(
                    CASE 
                        WHEN lr.change_indicator = 'D' THEN lr.change_date
                        ELSE CURRENT_DATE
                    END,
                    lr.date_proprietor_added
                )) * 12 +
                EXTRACT(MONTH FROM AGE(
                    CASE 
                        WHEN lr.change_indicator = 'D' THEN lr.change_date
                        ELSE CURRENT_DATE
                    END,
                    lr.date_proprietor_added
                ))
            ELSE NULL
        END as ownership_duration_months
        
    FROM land_registry_data lr
    LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
    LEFT JOIN companies_house_data ch ON m.ch_matched_number_1 = ch.company_number
    WHERE lr.proprietor_1_name IS NOT NULL
    
    UNION ALL
    
    -- Proprietor 2
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
        lr.additional_proprietor_indicator,
        lr.dataset_type,
        lr.update_type,
        lr.file_month,
        lr.change_indicator,
        lr.change_date,
        lr.created_at,
        lr.updated_at,
        
        2 as proprietor_sequence,
        lr.proprietor_2_name as proprietor_name,
        lr.company_2_reg_no as company_reg_no,
        lr.proprietorship_2_category as proprietorship_category,
        lr.country_2_incorporated as country_incorporated,
        lr.proprietor_2_address_1 as proprietor_address_1,
        lr.proprietor_2_address_2 as proprietor_address_2,
        lr.proprietor_2_address_3 as proprietor_address_3,
        lr.proprietor_2_postcode as proprietor_postcode,
        
        m.ch_matched_name_2 as ch_matched_name,
        m.ch_matched_number_2 as ch_matched_number,
        m.ch_match_type_2 as ch_match_type,
        m.ch_match_confidence_2 as ch_match_confidence,
        
        COALESCE(ch.company_name, m.ch_matched_name_2) as ch_company_name,
        ch.company_status as ch_company_status,
        ch.company_category as ch_company_category,
        ch.incorporation_date as ch_incorporation_date,
        ch.dissolution_date as ch_dissolution_date,
        ch.country_of_origin as ch_country_of_origin,
        ch.reg_address_line1 as ch_reg_address_line1,
        ch.reg_address_line2 as ch_reg_address_line2,
        ch.reg_address_county as ch_reg_address_county,
        ch.reg_address_country as ch_reg_address_country,
        ch.reg_address_post_town as ch_reg_address_post_town,
        ch.reg_address_postcode as ch_reg_address_postcode,
        ch.sic_code_1 as ch_sic_code_1,
        ch.sic_code_2 as ch_sic_code_2,
        ch.sic_code_3 as ch_sic_code_3,
        ch.sic_code_4 as ch_sic_code_4,
        
        CASE 
            WHEN lr.change_indicator = 'D' THEN 'Historical'
            WHEN lr.file_month = (SELECT MAX(file_month) FROM land_registry_data) THEN 'Current'
            ELSE 'Historical'
        END as ownership_status,
        
        CASE 
            WHEN lr.date_proprietor_added IS NOT NULL THEN
                EXTRACT(YEAR FROM AGE(
                    CASE 
                        WHEN lr.change_indicator = 'D' THEN lr.change_date
                        ELSE CURRENT_DATE
                    END,
                    lr.date_proprietor_added
                )) * 12 +
                EXTRACT(MONTH FROM AGE(
                    CASE 
                        WHEN lr.change_indicator = 'D' THEN lr.change_date
                        ELSE CURRENT_DATE
                    END,
                    lr.date_proprietor_added
                ))
            ELSE NULL
        END as ownership_duration_months
        
    FROM land_registry_data lr
    LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
    LEFT JOIN companies_house_data ch ON m.ch_matched_number_2 = ch.company_number
    WHERE lr.proprietor_2_name IS NOT NULL
    
    UNION ALL
    
    -- Proprietor 3
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
        lr.additional_proprietor_indicator,
        lr.dataset_type,
        lr.update_type,
        lr.file_month,
        lr.change_indicator,
        lr.change_date,
        lr.created_at,
        lr.updated_at,
        
        3 as proprietor_sequence,
        lr.proprietor_3_name as proprietor_name,
        lr.company_3_reg_no as company_reg_no,
        lr.proprietorship_3_category as proprietorship_category,
        lr.country_3_incorporated as country_incorporated,
        lr.proprietor_3_address_1 as proprietor_address_1,
        lr.proprietor_3_address_2 as proprietor_address_2,
        lr.proprietor_3_address_3 as proprietor_address_3,
        lr.proprietor_3_postcode as proprietor_postcode,
        
        m.ch_matched_name_3 as ch_matched_name,
        m.ch_matched_number_3 as ch_matched_number,
        m.ch_match_type_3 as ch_match_type,
        m.ch_match_confidence_3 as ch_match_confidence,
        
        COALESCE(ch.company_name, m.ch_matched_name_3) as ch_company_name,
        ch.company_status as ch_company_status,
        ch.company_category as ch_company_category,
        ch.incorporation_date as ch_incorporation_date,
        ch.dissolution_date as ch_dissolution_date,
        ch.country_of_origin as ch_country_of_origin,
        ch.reg_address_line1 as ch_reg_address_line1,
        ch.reg_address_line2 as ch_reg_address_line2,
        ch.reg_address_county as ch_reg_address_county,
        ch.reg_address_country as ch_reg_address_country,
        ch.reg_address_post_town as ch_reg_address_post_town,
        ch.reg_address_postcode as ch_reg_address_postcode,
        ch.sic_code_1 as ch_sic_code_1,
        ch.sic_code_2 as ch_sic_code_2,
        ch.sic_code_3 as ch_sic_code_3,
        ch.sic_code_4 as ch_sic_code_4,
        
        CASE 
            WHEN lr.change_indicator = 'D' THEN 'Historical'
            WHEN lr.file_month = (SELECT MAX(file_month) FROM land_registry_data) THEN 'Current'
            ELSE 'Historical'
        END as ownership_status,
        
        CASE 
            WHEN lr.date_proprietor_added IS NOT NULL THEN
                EXTRACT(YEAR FROM AGE(
                    CASE 
                        WHEN lr.change_indicator = 'D' THEN lr.change_date
                        ELSE CURRENT_DATE
                    END,
                    lr.date_proprietor_added
                )) * 12 +
                EXTRACT(MONTH FROM AGE(
                    CASE 
                        WHEN lr.change_indicator = 'D' THEN lr.change_date
                        ELSE CURRENT_DATE
                    END,
                    lr.date_proprietor_added
                ))
            ELSE NULL
        END as ownership_duration_months
        
    FROM land_registry_data lr
    LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
    LEFT JOIN companies_house_data ch ON m.ch_matched_number_3 = ch.company_number
    WHERE lr.proprietor_3_name IS NOT NULL
    
    UNION ALL
    
    -- Proprietor 4
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
        lr.additional_proprietor_indicator,
        lr.dataset_type,
        lr.update_type,
        lr.file_month,
        lr.change_indicator,
        lr.change_date,
        lr.created_at,
        lr.updated_at,
        
        4 as proprietor_sequence,
        lr.proprietor_4_name as proprietor_name,
        lr.company_4_reg_no as company_reg_no,
        lr.proprietorship_4_category as proprietorship_category,
        lr.country_4_incorporated as country_incorporated,
        lr.proprietor_4_address_1 as proprietor_address_1,
        lr.proprietor_4_address_2 as proprietor_address_2,
        lr.proprietor_4_address_3 as proprietor_address_3,
        lr.proprietor_4_postcode as proprietor_postcode,
        
        m.ch_matched_name_4 as ch_matched_name,
        m.ch_matched_number_4 as ch_matched_number,
        m.ch_match_type_4 as ch_match_type,
        m.ch_match_confidence_4 as ch_match_confidence,
        
        COALESCE(ch.company_name, m.ch_matched_name_4) as ch_company_name,
        ch.company_status as ch_company_status,
        ch.company_category as ch_company_category,
        ch.incorporation_date as ch_incorporation_date,
        ch.dissolution_date as ch_dissolution_date,
        ch.country_of_origin as ch_country_of_origin,
        ch.reg_address_line1 as ch_reg_address_line1,
        ch.reg_address_line2 as ch_reg_address_line2,
        ch.reg_address_county as ch_reg_address_county,
        ch.reg_address_country as ch_reg_address_country,
        ch.reg_address_post_town as ch_reg_address_post_town,
        ch.reg_address_postcode as ch_reg_address_postcode,
        ch.sic_code_1 as ch_sic_code_1,
        ch.sic_code_2 as ch_sic_code_2,
        ch.sic_code_3 as ch_sic_code_3,
        ch.sic_code_4 as ch_sic_code_4,
        
        CASE 
            WHEN lr.change_indicator = 'D' THEN 'Historical'
            WHEN lr.file_month = (SELECT MAX(file_month) FROM land_registry_data) THEN 'Current'
            ELSE 'Historical'
        END as ownership_status,
        
        CASE 
            WHEN lr.date_proprietor_added IS NOT NULL THEN
                EXTRACT(YEAR FROM AGE(
                    CASE 
                        WHEN lr.change_indicator = 'D' THEN lr.change_date
                        ELSE CURRENT_DATE
                    END,
                    lr.date_proprietor_added
                )) * 12 +
                EXTRACT(MONTH FROM AGE(
                    CASE 
                        WHEN lr.change_indicator = 'D' THEN lr.change_date
                        ELSE CURRENT_DATE
                    END,
                    lr.date_proprietor_added
                ))
            ELSE NULL
        END as ownership_duration_months
        
    FROM land_registry_data lr
    LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
    LEFT JOIN companies_house_data ch ON m.ch_matched_number_4 = ch.company_number
    WHERE lr.proprietor_4_name IS NOT NULL
)
SELECT * FROM ownership_data;

-- Create index on commonly used columns
CREATE INDEX IF NOT EXISTS idx_ownership_history_title_number ON land_registry_data(title_number);
CREATE INDEX IF NOT EXISTS idx_ownership_history_ch_company_name ON companies_house_data(company_name);
CREATE INDEX IF NOT EXISTS idx_ownership_history_postcode ON land_registry_data(postcode);

-- Test the fix
SELECT 
    COUNT(*) as total_land_registry_matches,
    COUNT(CASE WHEN ch_company_name IS NOT NULL THEN 1 END) as with_company_name,
    COUNT(CASE WHEN ch_company_name IS NULL THEN 1 END) as without_company_name
FROM v_ownership_history
WHERE ch_match_type = 'Land_Registry';