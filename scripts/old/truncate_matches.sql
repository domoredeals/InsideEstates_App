-- Truncate the matches table to remove incorrect matches
-- This will reset the table for a fresh run with the fixed script

TRUNCATE TABLE land_registry_ch_matches;

-- Reset the sequence if there is one
-- ALTER SEQUENCE land_registry_ch_matches_id_seq RESTART WITH 1;

-- Verify it's empty
SELECT COUNT(*) as record_count FROM land_registry_ch_matches;