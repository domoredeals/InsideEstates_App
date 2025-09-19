-- Update Land Registry matches with scraped Companies House data

-- First, let's see what will be updated (DRY RUN)
WITH matches_to_update AS (
    -- Direct name matches
    SELECT 
        lr.id,
        lr.proprietor_1_name as search_name,
        ch.company_number,
        ch.company_name as current_name,
        ch.company_status,
        CASE 
            WHEN ch.company_status = 'Dissolved' THEN 'Dissolved_Company'
            WHEN lr.proprietor_1_name != ch.company_name THEN 'Name_Changed'
            ELSE 'Exact_Match'
        END as new_match_type,
        'Direct match on search_name' as match_reason
    FROM land_registry_data lr
    JOIN ch_scrape_overview ch ON lr.proprietor_1_name = ch.search_name
    LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
    WHERE lr.proprietorship_1_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership')
    AND (m.ch_match_type_1 = 'No_Match' OR m.ch_match_type_1 IS NULL)
    
    UNION
    
    -- Previous name matches (like CGU INSURANCE PLC)
    SELECT 
        lr.id,
        lr.proprietor_1_name as search_name,
        ch.company_number,
        ch.company_name as current_name,
        ch.company_status,
        'Previous_Name_Match' as new_match_type,
        'Matched on previous company name' as match_reason
    FROM land_registry_data lr
    JOIN ch_scrape_overview ch ON lr.proprietor_1_name = ANY(ch.previous_names)
    LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
    WHERE lr.proprietorship_1_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership')
    AND (m.ch_match_type_1 = 'No_Match' OR m.ch_match_type_1 IS NULL)
)
SELECT 
    search_name as "Land Registry Name",
    company_number as "CH Number",
    current_name as "Current CH Name",
    company_status as "Status",
    new_match_type as "Match Type",
    match_reason as "How Matched",
    COUNT(*) as "Properties"
FROM matches_to_update
GROUP BY search_name, company_number, current_name, company_status, new_match_type, match_reason
ORDER BY search_name;

-- To actually update the matches table, uncomment and run this:
/*
UPDATE land_registry_ch_matches m
SET 
    ch_matched_name_1 = ch.company_name,
    ch_matched_number_1 = ch.company_number,
    ch_match_type_1 = CASE 
        WHEN ch.company_status = 'Dissolved' THEN 'Dissolved_Company'
        WHEN lr.proprietor_1_name != ch.company_name THEN 'Name_Changed'
        WHEN lr.proprietor_1_name = ANY(ch.previous_names) THEN 'Previous_Name_Match'
        ELSE 'Exact_Match'
    END,
    ch_match_confidence_1 = 1.0,
    ch_match_date = NOW()
FROM land_registry_data lr
JOIN ch_scrape_overview ch ON (
    lr.proprietor_1_name = ch.search_name 
    OR lr.proprietor_1_name = ANY(ch.previous_names)
)
WHERE m.id = lr.id
AND lr.proprietorship_1_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership')
AND (m.ch_match_type_1 = 'No_Match' OR m.ch_match_type_1 IS NULL);
*/