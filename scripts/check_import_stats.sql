-- Check Land Registry Import Statistics

-- Overall statistics
SELECT 
    'Total Properties' as metric,
    COUNT(*) as count
FROM properties
UNION ALL
SELECT 
    'Total Proprietors' as metric,
    COUNT(*) as count
FROM proprietors
UNION ALL
SELECT 
    'Total Companies' as metric,
    COUNT(*) as count
FROM companies;

-- Properties by dataset type
SELECT 
    dataset_type,
    COUNT(*) as property_count,
    COUNT(DISTINCT file_month) as months_loaded
FROM properties
GROUP BY dataset_type
ORDER BY dataset_type;

-- Import history summary
SELECT 
    dataset_type,
    COUNT(*) as files_imported,
    SUM(rows_processed) as total_rows,
    SUM(rows_inserted) as total_inserted,
    SUM(rows_updated) as total_updated,
    SUM(rows_failed) as total_failed
FROM import_history
WHERE status = 'completed'
GROUP BY dataset_type
ORDER BY dataset_type;

-- Top companies by property count
SELECT 
    company_name,
    company_registration_no,
    property_count,
    total_value_owned,
    CASE 
        WHEN country_incorporated IS NOT NULL THEN country_incorporated 
        ELSE 'UK' 
    END as country
FROM companies
WHERE property_count > 0
ORDER BY property_count DESC
LIMIT 20;

-- Properties with price data
SELECT 
    COUNT(*) as properties_with_price,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM properties) as percentage
FROM properties
WHERE price_paid IS NOT NULL;

-- Date range of data
SELECT 
    MIN(file_month) as earliest_month,
    MAX(file_month) as latest_month,
    COUNT(DISTINCT file_month) as total_months
FROM properties;

-- Check for any failed imports
SELECT 
    filename,
    status,
    rows_processed,
    rows_failed,
    error_message
FROM import_history
WHERE status != 'completed'
ORDER BY import_started DESC;