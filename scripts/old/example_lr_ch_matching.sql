-- Example queries for matching Land Registry data with scraped Companies House data

-- 1. Find Land Registry companies that were found on Companies House
-- This matches on the search_name (what was in Land Registry)
SELECT DISTINCT
    lr.id as lr_id,
    lr.proprietor_1_name as lr_company_name,
    ch.company_number as ch_number,
    ch.company_name as ch_current_name,
    ch.company_status,
    ch.previous_names
FROM land_registry_data lr
JOIN ch_scrape_overview ch ON lr.proprietor_1_name = ch.search_name
WHERE lr.proprietorship_1_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership')
LIMIT 10;

-- 2. Find companies that changed names (search name != current name)
SELECT 
    search_name as "Searched For (from Land Registry)",
    company_name as "Current Name on Companies House",
    company_number,
    company_status,
    previous_names
FROM ch_scrape_overview
WHERE search_name != company_name
ORDER BY search_name;

-- 3. Find dissolved companies in Land Registry data
SELECT DISTINCT
    lr.proprietor_1_name,
    ch.company_number,
    ch.company_status,
    ch.incorporation_date,
    COUNT(DISTINCT lr.title_number) as property_count
FROM land_registry_data lr
JOIN ch_scrape_overview ch ON lr.proprietor_1_name = ch.search_name
WHERE ch.company_status = 'Dissolved'
GROUP BY lr.proprietor_1_name, ch.company_number, ch.company_status, ch.incorporation_date
ORDER BY property_count DESC;

-- 4. Update Land Registry matches table with newly found companies
-- This would update your matching table with the scraped data
UPDATE land_registry_ch_matches m
SET 
    ch_matched_name_1 = ch.company_name,
    ch_matched_number_1 = ch.company_number,
    ch_match_type_1 = CASE 
        WHEN ch.company_status = 'Dissolved' THEN 'Dissolved_Company'
        WHEN ch.search_name != ch.company_name THEN 'Name_Changed'
        ELSE 'Exact_Match'
    END,
    ch_match_confidence_1 = 1.0,
    ch_match_date = NOW()
FROM land_registry_data lr
JOIN ch_scrape_overview ch ON lr.proprietor_1_name = ch.search_name
WHERE m.id = lr.id
AND lr.proprietorship_1_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership')
AND m.ch_match_type_1 = 'No_Match';

-- 5. Find companies that match on previous names
-- This is powerful - finds companies even when they've changed names
SELECT DISTINCT
    lr.proprietor_1_name as "Land Registry Name",
    ch.company_name as "Current Companies House Name",
    ch.company_number,
    ch.company_status,
    ch.previous_names
FROM land_registry_data lr
JOIN ch_scrape_overview ch ON lr.proprietor_1_name = ANY(ch.previous_names)
WHERE lr.proprietorship_1_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership')
LIMIT 10;