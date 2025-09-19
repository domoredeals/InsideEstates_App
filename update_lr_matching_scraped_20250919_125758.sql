
-- Update Land Registry matching with scraped Companies House data
-- Generated automatically by import_scraped_overview_results.py

-- Update exact name matches
UPDATE land_registry_data lr
SET 
    company_1_reg_no = o.company_number,
    company_1_status = o.company_status,
    company_1_matched_name = o.company_name,
    updated_at = NOW()
FROM ch_scrape_queue q
JOIN ch_scrape_overview o ON q.company_number = o.company_number
WHERE q.search_status = 'found'
  AND lr.proprietor_1_name = q.search_name
  AND lr.company_1_reg_no IS NULL  -- Only update unmatched records
  AND o.company_number IS NOT NULL;

-- Update previous name matches
UPDATE land_registry_data lr
SET 
    company_1_reg_no = o.company_number,
    company_1_status = o.company_status,
    company_1_matched_name = o.company_name,
    company_1_match_type = 'previous_name',
    updated_at = NOW()
FROM ch_scrape_overview o
WHERE lr.proprietor_1_name = ANY(o.previous_names)
  AND lr.company_1_reg_no IS NULL  -- Only update unmatched records
  AND o.company_number IS NOT NULL;

-- Generate summary of updates
SELECT 
    'Updates Applied' as status,
    COUNT(*) as total_updates,
    COUNT(DISTINCT company_1_reg_no) as unique_companies
FROM land_registry_data 
WHERE company_1_matched_name IS NOT NULL 
  AND updated_at > NOW() - INTERVAL '1 hour';
