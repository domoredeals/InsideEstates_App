-- Drop existing views
DROP VIEW IF EXISTS v_ownership_summary CASCADE;
DROP VIEW IF EXISTS v_ownership_history CASCADE;

-- Create a comprehensive ownership history view showing only the latest record per title/owner combination
CREATE OR REPLACE VIEW v_ownership_history AS
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
        
        -- Create a ranking for latest record per title/owner
        ROW_NUMBER() OVER (
            PARTITION BY lr.title_number, lr.proprietor_1_name 
            ORDER BY lr.file_month DESC, lr.created_at DESC
        ) as rn
        
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
        ) as rn
        
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
        ) as rn
        
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
        ) as rn
        
    FROM land_registry_data lr
    LEFT JOIN land_registry_ch_matches lrm ON lr.id = lrm.id
    LEFT JOIN companies_house_data ch ON lrm.ch_matched_number_4 = ch.company_number
    WHERE lr.proprietor_4_name IS NOT NULL AND lr.proprietor_4_name != ''
),
latest_ownership AS (
    SELECT * FROM ownership_timeline WHERE rn = 1
)
SELECT 
    id,
    title_number,
    file_month,
    dataset_type,
    update_type,
    change_indicator,
    change_date,
    tenure,
    property_address,
    district,
    county,
    region,
    postcode,
    price_paid,
    date_proprietor_added,
    multiple_address_indicator,
    additional_proprietor_indicator,
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
    ch_company_status,
    ch_company_category,
    ch_incorporation_date,
    ch_dissolution_date,
    ch_country_of_origin,
    ch_reg_address_line1,
    ch_reg_address_line2,
    ch_reg_address_post_town,
    ch_reg_address_county,
    ch_reg_address_country,
    ch_reg_address_postcode,
    ch_accounts_next_due_date,
    ch_accounts_last_made_up_date,
    ch_accounts_category,
    ch_sic_code_1,
    ch_sic_code_2,
    ch_sic_code_3,
    ch_sic_code_4,
    match_quality_description,
    
    -- Additional computed fields for analysis
    CASE 
        WHEN change_indicator = 'A' THEN 'Added'
        WHEN change_indicator = 'D' THEN 'Deleted'
        WHEN change_indicator = 'M' THEN 'Modified'
        ELSE 'Unchanged'
    END as change_description,
    
    -- Flag for current vs historical ownership
    CASE 
        WHEN change_indicator = 'D' THEN 'Historical'
        WHEN file_month = (
            SELECT MAX(file_month) 
            FROM land_registry_data lr2 
            WHERE lr2.title_number = latest_ownership.title_number
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

FROM latest_ownership
ORDER BY title_number, proprietor_sequence;

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_lr_ch_matches_id ON land_registry_ch_matches(id);
CREATE INDEX IF NOT EXISTS idx_lr_ch_matches_numbers ON land_registry_ch_matches(ch_matched_number_1, ch_matched_number_2, ch_matched_number_3, ch_matched_number_4);

-- Create summary view
CREATE OR REPLACE VIEW v_ownership_summary AS
SELECT 
    title_number,
    property_address,
    postcode,
    tenure,
    dataset_type,
    COUNT(DISTINCT proprietor_name) as total_unique_owners,
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
COMMENT ON VIEW v_ownership_history IS 'Ownership history view showing only the latest record per title/owner combination';
COMMENT ON VIEW v_ownership_summary IS 'Summary view showing ownership statistics and match quality for each property';