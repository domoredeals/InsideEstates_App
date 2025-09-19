-- Simple fix: Update existing view to use COALESCE for ch_company_name
-- This ensures ch_company_name is populated from ch_matched_name when there's no CH data

-- First, let's check the existing view structure
-- This won't fail if view doesn't exist
DROP VIEW IF EXISTS v_ownership_history_backup CASCADE;
CREATE VIEW v_ownership_history_backup AS SELECT * FROM v_ownership_history WHERE false;

-- Now update the view definition with COALESCE
-- We'll recreate it based on the existing structure but with the fix
CREATE OR REPLACE VIEW v_ownership_history AS
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
    
    -- Use COALESCE to ensure ch_company_name is populated
    -- This uses the matched name when there's no Companies House record
    CASE 
        WHEN lr.proprietor_1_name IS NOT NULL THEN COALESCE(ch1.company_name, m.ch_matched_name_1)
        ELSE NULL
    END as ch_company_name_1,
    CASE 
        WHEN lr.proprietor_2_name IS NOT NULL THEN COALESCE(ch2.company_name, m.ch_matched_name_2)
        ELSE NULL
    END as ch_company_name_2,
    CASE 
        WHEN lr.proprietor_3_name IS NOT NULL THEN COALESCE(ch3.company_name, m.ch_matched_name_3)
        ELSE NULL
    END as ch_company_name_3,
    CASE 
        WHEN lr.proprietor_4_name IS NOT NULL THEN COALESCE(ch4.company_name, m.ch_matched_name_4)
        ELSE NULL
    END as ch_company_name_4
    
FROM land_registry_data lr
LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
LEFT JOIN companies_house_data ch1 ON m.ch_matched_number_1 = ch1.company_number
LEFT JOIN companies_house_data ch2 ON m.ch_matched_number_2 = ch2.company_number
LEFT JOIN companies_house_data ch3 ON m.ch_matched_number_3 = ch3.company_number
LEFT JOIN companies_house_data ch4 ON m.ch_matched_number_4 = ch4.company_number;

-- Test the fix
SELECT 
    ch_match_type_1,
    COUNT(*) as total_records,
    COUNT(CASE WHEN ch_company_name_1 IS NOT NULL THEN 1 END) as with_company_name,
    COUNT(CASE WHEN ch_company_name_1 IS NULL THEN 1 END) as without_company_name
FROM v_ownership_history
WHERE proprietor_1_name IS NOT NULL
GROUP BY ch_match_type_1
ORDER BY ch_match_type_1;