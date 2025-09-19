-- Cleanup script to remove old multi-table schema
-- This will drop all the old tables and related objects

-- Drop the old tables
DROP TABLE IF EXISTS proprietors CASCADE;
DROP TABLE IF EXISTS properties CASCADE;
DROP TABLE IF EXISTS companies CASCADE;
DROP TABLE IF EXISTS import_history CASCADE;

-- Drop the function that was used for company stats
DROP FUNCTION IF EXISTS update_company_stats() CASCADE;

-- Drop any triggers related to old tables
DROP TRIGGER IF EXISTS update_properties_updated_at ON properties CASCADE;
DROP TRIGGER IF EXISTS update_companies_updated_at ON companies CASCADE;

-- Keep only the new single table: land_registry_data

-- Verify what's left
\dt