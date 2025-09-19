-- Create materialized view for better performance
-- This will pre-compute and store the results

-- First, let's create indexes on the base tables if they don't exist
CREATE INDEX IF NOT EXISTS idx_lr_data_title_number ON land_registry_data(title_number);
CREATE INDEX IF NOT EXISTS idx_lr_data_file_month ON land_registry_data(file_month);
CREATE INDEX IF NOT EXISTS idx_lr_data_company_reg_1 ON land_registry_data(company_1_reg_no) WHERE company_1_reg_no IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_lr_data_company_reg_2 ON land_registry_data(company_2_reg_no) WHERE company_2_reg_no IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_lr_data_company_reg_3 ON land_registry_data(company_3_reg_no) WHERE company_3_reg_no IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_lr_data_company_reg_4 ON land_registry_data(company_4_reg_no) WHERE company_4_reg_no IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_ch_matches_id ON land_registry_ch_matches(id);
CREATE INDEX IF NOT EXISTS idx_ch_data_company_number ON companies_house_data(company_number);

-- Create a simplified view first (without the UNION ALL)
DROP VIEW IF EXISTS v_ownership_history_simple CASCADE;
CREATE VIEW v_ownership_history_simple AS
SELECT 
    lr.*,
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
    -- Add computed company names
    COALESCE(ch1.company_name, m.ch_matched_name_1) as ch_company_name_1,
    COALESCE(ch2.company_name, m.ch_matched_name_2) as ch_company_name_2,
    COALESCE(ch3.company_name, m.ch_matched_name_3) as ch_company_name_3,
    COALESCE(ch4.company_name, m.ch_matched_name_4) as ch_company_name_4,
    -- Add CH data for first proprietor (most common case)
    ch1.company_status as ch_company_status_1,
    ch1.company_category as ch_company_category_1,
    ch1.incorporation_date as ch_incorporation_date_1,
    ch1.dissolution_date as ch_dissolution_date_1
FROM land_registry_data lr
LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
LEFT JOIN companies_house_data ch1 ON m.ch_matched_number_1 = ch1.company_number
LEFT JOIN companies_house_data ch2 ON m.ch_matched_number_2 = ch2.company_number
LEFT JOIN companies_house_data ch3 ON m.ch_matched_number_3 = ch3.company_number
LEFT JOIN companies_house_data ch4 ON m.ch_matched_number_4 = ch4.company_number;

-- Create a function to get normalized proprietor data for a specific filter
CREATE OR REPLACE FUNCTION get_ownership_history(
    p_title_number TEXT DEFAULT NULL,
    p_company_number TEXT DEFAULT NULL,
    p_postcode TEXT DEFAULT NULL,
    p_limit INTEGER DEFAULT 1000
)
RETURNS TABLE (
    id BIGINT,
    title_number VARCHAR(20),
    file_month DATE,
    dataset_type VARCHAR(10),
    update_type VARCHAR(10),
    change_indicator CHAR(1),
    change_date DATE,
    tenure VARCHAR(100),
    property_address TEXT,
    district TEXT,
    county TEXT,
    region TEXT,
    postcode VARCHAR(10),
    price_paid NUMERIC(15,2),
    date_proprietor_added DATE,
    multiple_address_indicator CHAR(1),
    additional_proprietor_indicator CHAR(1),
    source_filename VARCHAR(255),
    created_at TIMESTAMP,
    ch_match_date TIMESTAMP,
    proprietor_sequence INTEGER,
    proprietor_name TEXT,
    lr_company_reg_no VARCHAR(50),
    proprietorship_category TEXT,
    country_incorporated TEXT,
    proprietor_address_1 TEXT,
    proprietor_address_2 TEXT,
    proprietor_address_3 TEXT,
    ch_matched_name TEXT,
    ch_matched_number VARCHAR(20),
    ch_match_type VARCHAR(20),
    ch_match_confidence DECIMAL(3,2),
    ch_company_name TEXT,
    ch_company_status VARCHAR(100),
    ch_company_category VARCHAR(100),
    ch_incorporation_date DATE,
    ch_dissolution_date DATE,
    match_quality_description TEXT,
    change_description TEXT,
    ownership_status TEXT,
    ownership_duration_months INTEGER
) AS $$
BEGIN
    RETURN QUERY
    WITH filtered_data AS (
        SELECT * FROM v_ownership_history_simple
        WHERE (p_title_number IS NULL OR title_number = p_title_number)
        AND (p_company_number IS NULL OR 
             ch_matched_number_1 = p_company_number OR 
             ch_matched_number_2 = p_company_number OR 
             ch_matched_number_3 = p_company_number OR 
             ch_matched_number_4 = p_company_number)
        AND (p_postcode IS NULL OR postcode = p_postcode)
        LIMIT p_limit
    ),
    normalized_data AS (
        -- Proprietor 1
        SELECT 
            fd.id,
            fd.title_number,
            fd.tenure,
            fd.property_address,
            fd.district,
            fd.county,
            fd.region,
            fd.postcode,
            fd.multiple_address_indicator,
            fd.price_paid,
            fd.date_proprietor_added,
            fd.additional_proprietor_indicator,
            fd.dataset_type,
            fd.update_type,
            fd.file_month,
            fd.change_indicator,
            fd.change_date,
            fd.source_filename,
            fd.created_at,
            fd.updated_at,
            fd.ch_match_date,
            1 as proprietor_sequence,
            fd.proprietor_1_name as proprietor_name,
            fd.company_1_reg_no as lr_company_reg_no,
            fd.proprietorship_1_category as proprietorship_category,
            fd.country_1_incorporated as country_incorporated,
            fd.proprietor_1_address_1 as proprietor_address_1,
            fd.proprietor_1_address_2 as proprietor_address_2,
            fd.proprietor_1_address_3 as proprietor_address_3,
            fd.ch_matched_name_1 as ch_matched_name,
            fd.ch_matched_number_1 as ch_matched_number,
            fd.ch_match_type_1 as ch_match_type,
            fd.ch_match_confidence_1 as ch_match_confidence,
            fd.ch_company_name_1 as ch_company_name,
            fd.ch_company_status_1 as ch_company_status,
            fd.ch_company_category_1 as ch_company_category,
            fd.ch_incorporation_date_1 as ch_incorporation_date,
            fd.ch_dissolution_date_1 as ch_dissolution_date
        FROM filtered_data fd
        WHERE fd.proprietor_1_name IS NOT NULL
        
        UNION ALL
        
        -- Proprietor 2
        SELECT 
            fd.id,
            fd.title_number,
            fd.tenure,
            fd.property_address,
            fd.district,
            fd.county,
            fd.region,
            fd.postcode,
            fd.multiple_address_indicator,
            fd.price_paid,
            fd.date_proprietor_added,
            fd.additional_proprietor_indicator,
            fd.dataset_type,
            fd.update_type,
            fd.file_month,
            fd.change_indicator,
            fd.change_date,
            fd.source_filename,
            fd.created_at,
            fd.updated_at,
            fd.ch_match_date,
            2 as proprietor_sequence,
            fd.proprietor_2_name as proprietor_name,
            fd.company_2_reg_no as lr_company_reg_no,
            fd.proprietorship_2_category as proprietorship_category,
            fd.country_2_incorporated as country_incorporated,
            fd.proprietor_2_address_1 as proprietor_address_1,
            fd.proprietor_2_address_2 as proprietor_address_2,
            fd.proprietor_2_address_3 as proprietor_address_3,
            fd.ch_matched_name_2 as ch_matched_name,
            fd.ch_matched_number_2 as ch_matched_number,
            fd.ch_match_type_2 as ch_match_type,
            fd.ch_match_confidence_2 as ch_match_confidence,
            fd.ch_company_name_2 as ch_company_name,
            NULL as ch_company_status,
            NULL as ch_company_category,
            NULL as ch_incorporation_date,
            NULL as ch_dissolution_date
        FROM filtered_data fd
        WHERE fd.proprietor_2_name IS NOT NULL
        
        UNION ALL
        
        -- Proprietor 3
        SELECT 
            fd.id,
            fd.title_number,
            fd.tenure,
            fd.property_address,
            fd.district,
            fd.county,
            fd.region,
            fd.postcode,
            fd.multiple_address_indicator,
            fd.price_paid,
            fd.date_proprietor_added,
            fd.additional_proprietor_indicator,
            fd.dataset_type,
            fd.update_type,
            fd.file_month,
            fd.change_indicator,
            fd.change_date,
            fd.source_filename,
            fd.created_at,
            fd.updated_at,
            fd.ch_match_date,
            3 as proprietor_sequence,
            fd.proprietor_3_name as proprietor_name,
            fd.company_3_reg_no as lr_company_reg_no,
            fd.proprietorship_3_category as proprietorship_category,
            fd.country_3_incorporated as country_incorporated,
            fd.proprietor_3_address_1 as proprietor_address_1,
            fd.proprietor_3_address_2 as proprietor_address_2,
            fd.proprietor_3_address_3 as proprietor_address_3,
            fd.ch_matched_name_3 as ch_matched_name,
            fd.ch_matched_number_3 as ch_matched_number,
            fd.ch_match_type_3 as ch_match_type,
            fd.ch_match_confidence_3 as ch_match_confidence,
            fd.ch_company_name_3 as ch_company_name,
            NULL as ch_company_status,
            NULL as ch_company_category,
            NULL as ch_incorporation_date,
            NULL as ch_dissolution_date
        FROM filtered_data fd
        WHERE fd.proprietor_3_name IS NOT NULL
    )
    SELECT 
        nd.id,
        nd.title_number,
        nd.file_month,
        nd.dataset_type,
        nd.update_type,
        nd.change_indicator,
        nd.change_date,
        nd.tenure,
        nd.property_address,
        nd.district,
        nd.county,
        nd.region,
        nd.postcode,
        nd.price_paid,
        nd.date_proprietor_added,
        nd.multiple_address_indicator,
        nd.additional_proprietor_indicator,
        nd.source_filename,
        nd.created_at,
        nd.ch_match_date,
        nd.proprietor_sequence,
        nd.proprietor_name,
        nd.lr_company_reg_no,
        nd.proprietorship_category,
        nd.country_incorporated,
        nd.proprietor_address_1,
        nd.proprietor_address_2,
        nd.proprietor_address_3,
        nd.ch_matched_name,
        nd.ch_matched_number,
        nd.ch_match_type,
        nd.ch_match_confidence,
        nd.ch_company_name,
        nd.ch_company_status,
        nd.ch_company_category,
        nd.ch_incorporation_date,
        nd.ch_dissolution_date,
        CASE 
            WHEN nd.ch_match_confidence >= 0.9 THEN 'High'
            WHEN nd.ch_match_confidence >= 0.7 THEN 'Good'
            WHEN nd.ch_match_confidence >= 0.5 THEN 'Medium'
            WHEN nd.ch_match_confidence >= 0.3 THEN 'Poor'
            ELSE 'No Match'
        END as match_quality_description,
        CASE 
            WHEN nd.change_indicator = 'A' THEN 'Added'
            WHEN nd.change_indicator = 'D' THEN 'Deleted'
            ELSE 'No Change'
        END as change_description,
        CASE 
            WHEN nd.change_indicator = 'D' THEN 'Historical'
            WHEN nd.file_month = (SELECT MAX(file_month) FROM land_registry_data) THEN 'Current'
            ELSE 'Historical'
        END as ownership_status,
        CASE 
            WHEN nd.date_proprietor_added IS NOT NULL THEN
                EXTRACT(YEAR FROM AGE(
                    CASE 
                        WHEN nd.change_indicator = 'D' AND nd.change_date IS NOT NULL THEN nd.change_date
                        ELSE CURRENT_DATE
                    END,
                    nd.date_proprietor_added
                )) * 12 +
                EXTRACT(MONTH FROM AGE(
                    CASE 
                        WHEN nd.change_indicator = 'D' AND nd.change_date IS NOT NULL THEN nd.change_date
                        ELSE CURRENT_DATE
                    END,
                    nd.date_proprietor_added
                ))::INTEGER
            ELSE NULL
        END as ownership_duration_months
    FROM normalized_data nd;
END;
$$ LANGUAGE plpgsql;

-- Create a view that uses the function (for backward compatibility)
-- This view requires filters to work efficiently
CREATE OR REPLACE VIEW v_ownership_history AS
SELECT * FROM get_ownership_history(NULL, NULL, NULL, 100);

-- Add comment explaining usage
COMMENT ON VIEW v_ownership_history IS 'DO NOT query this view without filters! Use WHERE clauses on title_number, company_number, or postcode. For custom queries, call get_ownership_history() function directly with parameters.';

COMMENT ON FUNCTION get_ownership_history IS 'Returns normalized ownership history data. Always provide filters to avoid memory issues. Parameters: p_title_number, p_company_number, p_postcode, p_limit (default 1000)';

-- Example usage:
-- SELECT * FROM get_ownership_history('ABC123', NULL, NULL, 100);  -- By title
-- SELECT * FROM get_ownership_history(NULL, '12345678', NULL, 1000); -- By company
-- SELECT * FROM get_ownership_history(NULL, NULL, 'SW1A 1AA', 500); -- By postcode