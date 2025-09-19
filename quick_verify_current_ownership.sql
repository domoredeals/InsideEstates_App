-- Quick verification of the corrected ownership logic
-- This query directly implements the logic without the full view

-- First, let's check how many title/proprietor combinations have
-- their most recent record as non-deleted (which should be Current)

WITH latest_records AS (
    SELECT 
        title_number,
        proprietor_1_name,
        file_month,
        change_indicator,
        ROW_NUMBER() OVER (
            PARTITION BY title_number, proprietor_1_name
            ORDER BY file_month DESC
        ) as rn
    FROM land_registry_data
    WHERE proprietor_1_name IS NOT NULL AND proprietor_1_name != ''
)
SELECT 
    CASE 
        WHEN rn = 1 AND (change_indicator IS NULL OR change_indicator != 'D') THEN 'Current'
        ELSE 'Historical'
    END as ownership_status,
    COUNT(*) as record_count,
    COUNT(DISTINCT title_number) as unique_properties
FROM latest_records
WHERE rn <= 10  -- Only look at top 10 records per title/owner for quick results
GROUP BY 1;

-- Expected result: Should show millions of Current ownership records
-- since we're looking at the most recent record for each title/proprietor combo