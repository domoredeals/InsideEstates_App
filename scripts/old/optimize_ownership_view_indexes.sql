-- Optimize indexes for ownership history view performance
-- This script creates targeted indexes for the v_ownership_history and v_ownership_summary views

-- =====================================================
-- CRITICAL INDEXES FOR VIEW JOINS
-- =====================================================

-- Indexes on CH matched numbers for efficient JOINs (these are the most important!)
CREATE INDEX IF NOT EXISTS idx_lr_ch_matched_number_1 ON land_registry_data(ch_matched_number_1) 
WHERE ch_matched_number_1 IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_lr_ch_matched_number_2 ON land_registry_data(ch_matched_number_2) 
WHERE ch_matched_number_2 IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_lr_ch_matched_number_3 ON land_registry_data(ch_matched_number_3) 
WHERE ch_matched_number_3 IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_lr_ch_matched_number_4 ON land_registry_data(ch_matched_number_4) 
WHERE ch_matched_number_4 IS NOT NULL;

-- =====================================================
-- COMPOSITE INDEXES FOR COMMON QUERY PATTERNS
-- =====================================================

-- For queries filtering by title_number and ordering by file_month
CREATE INDEX IF NOT EXISTS idx_lr_title_file_month_desc 
ON land_registry_data(title_number, file_month DESC);

-- For change tracking queries
CREATE INDEX IF NOT EXISTS idx_lr_change_indicator_date 
ON land_registry_data(change_indicator, change_date DESC) 
WHERE change_indicator IN ('A', 'D', 'M');

-- For postcode-based searches with ownership data
CREATE INDEX IF NOT EXISTS idx_lr_postcode_title 
ON land_registry_data(postcode, title_number) 
WHERE postcode IS NOT NULL;

-- =====================================================
-- INDEXES FOR MATCH QUALITY ANALYSIS
-- =====================================================

-- For filtering by match confidence levels
CREATE INDEX IF NOT EXISTS idx_lr_ch_confidence_1 
ON land_registry_data(ch_match_confidence_1) 
WHERE ch_match_confidence_1 IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_lr_ch_confidence_2 
ON land_registry_data(ch_match_confidence_2) 
WHERE ch_match_confidence_2 IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_lr_ch_confidence_3 
ON land_registry_data(ch_match_confidence_3) 
WHERE ch_match_confidence_3 IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_lr_ch_confidence_4 
ON land_registry_data(ch_match_confidence_4) 
WHERE ch_match_confidence_4 IS NOT NULL;

-- Composite index for match type analysis
CREATE INDEX IF NOT EXISTS idx_lr_match_types 
ON land_registry_data(ch_match_type_1, ch_match_type_2, ch_match_type_3, ch_match_type_4);

-- =====================================================
-- COMPANIES HOUSE OPTIMIZATION
-- =====================================================

-- For name searches (case-insensitive)
CREATE INDEX IF NOT EXISTS idx_ch_company_name_lower 
ON companies_house_data(LOWER(company_name));

-- For active companies (most queries)
CREATE INDEX IF NOT EXISTS idx_ch_active_companies 
ON companies_house_data(company_number) 
WHERE company_status = 'Active';

-- =====================================================
-- SPECIALIZED INDEXES FOR PERFORMANCE
-- =====================================================

-- For finding latest records per title (used in ownership_status calculation)
CREATE INDEX IF NOT EXISTS idx_lr_title_file_month_max 
ON land_registry_data(title_number, file_month DESC NULLS LAST);

-- For proprietor name searches across all 4 columns
CREATE INDEX IF NOT EXISTS idx_lr_all_proprietor_names 
ON land_registry_data USING gin(
    to_tsvector('simple', 
        COALESCE(proprietor_1_name, '') || ' ' || 
        COALESCE(proprietor_2_name, '') || ' ' || 
        COALESCE(proprietor_3_name, '') || ' ' || 
        COALESCE(proprietor_4_name, '')
    )
);

-- =====================================================
-- COVERING INDEXES FOR COMMON QUERIES
-- =====================================================

-- For ownership summary view aggregations
CREATE INDEX IF NOT EXISTS idx_lr_summary_covering 
ON land_registry_data(title_number, file_month) 
INCLUDE (
    property_address, postcode, tenure, dataset_type,
    proprietor_1_name, proprietor_2_name, proprietor_3_name, proprietor_4_name,
    ch_match_confidence_1, ch_match_confidence_2, ch_match_confidence_3, ch_match_confidence_4
);

-- =====================================================
-- MAINTENANCE COMMANDS
-- =====================================================

-- Update statistics for query planner
ANALYZE land_registry_data;
ANALYZE companies_house_data;

-- Show index sizes
SELECT 
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_stat_user_indexes
WHERE tablename IN ('land_registry_data', 'companies_house_data')
ORDER BY pg_relation_size(indexrelid) DESC;