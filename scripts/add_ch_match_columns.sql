-- Add Companies House matching columns to land_registry_data table
-- This script adds columns to store matched Companies House data for each proprietor

-- Add matched Companies House fields for Proprietor 1
ALTER TABLE land_registry_data
ADD COLUMN IF NOT EXISTS ch_matched_name_1 TEXT,
ADD COLUMN IF NOT EXISTS ch_matched_number_1 VARCHAR(20),
ADD COLUMN IF NOT EXISTS ch_match_type_1 VARCHAR(20),
ADD COLUMN IF NOT EXISTS ch_match_confidence_1 DECIMAL(3,2);

-- Add matched Companies House fields for Proprietor 2
ALTER TABLE land_registry_data
ADD COLUMN IF NOT EXISTS ch_matched_name_2 TEXT,
ADD COLUMN IF NOT EXISTS ch_matched_number_2 VARCHAR(20),
ADD COLUMN IF NOT EXISTS ch_match_type_2 VARCHAR(20),
ADD COLUMN IF NOT EXISTS ch_match_confidence_2 DECIMAL(3,2);

-- Add matched Companies House fields for Proprietor 3
ALTER TABLE land_registry_data
ADD COLUMN IF NOT EXISTS ch_matched_name_3 TEXT,
ADD COLUMN IF NOT EXISTS ch_matched_number_3 VARCHAR(20),
ADD COLUMN IF NOT EXISTS ch_match_type_3 VARCHAR(20),
ADD COLUMN IF NOT EXISTS ch_match_confidence_3 DECIMAL(3,2);

-- Add matched Companies House fields for Proprietor 4
ALTER TABLE land_registry_data
ADD COLUMN IF NOT EXISTS ch_matched_name_4 TEXT,
ADD COLUMN IF NOT EXISTS ch_matched_number_4 VARCHAR(20),
ADD COLUMN IF NOT EXISTS ch_match_type_4 VARCHAR(20),
ADD COLUMN IF NOT EXISTS ch_match_confidence_4 DECIMAL(3,2);

-- Add a timestamp for when the matching was performed
ALTER TABLE land_registry_data
ADD COLUMN IF NOT EXISTS ch_match_date TIMESTAMP;

-- Add indexes on the matched company numbers for efficient lookups
CREATE INDEX IF NOT EXISTS idx_lr_ch_matched_number_1 ON land_registry_data(ch_matched_number_1) 
WHERE ch_matched_number_1 IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_lr_ch_matched_number_2 ON land_registry_data(ch_matched_number_2) 
WHERE ch_matched_number_2 IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_lr_ch_matched_number_3 ON land_registry_data(ch_matched_number_3) 
WHERE ch_matched_number_3 IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_lr_ch_matched_number_4 ON land_registry_data(ch_matched_number_4) 
WHERE ch_matched_number_4 IS NOT NULL;

-- Create a composite index for match type analysis
CREATE INDEX IF NOT EXISTS idx_lr_ch_match_types ON land_registry_data(ch_match_type_1, ch_match_type_2, ch_match_type_3, ch_match_type_4);

-- Add comments to explain the columns
COMMENT ON COLUMN land_registry_data.ch_matched_name_1 IS 'Official Companies House name matched to proprietor 1';
COMMENT ON COLUMN land_registry_data.ch_matched_number_1 IS 'Companies House registration number matched to proprietor 1';
COMMENT ON COLUMN land_registry_data.ch_match_type_1 IS 'Type of match: Name+Number, Number, Name, Previous_Name, or No_Match';
COMMENT ON COLUMN land_registry_data.ch_match_confidence_1 IS 'Confidence score: 1.0 (Name+Number), 0.9 (Number), 0.7 (Name), 0.5 (Previous_Name), 0.0 (No_Match)';

COMMENT ON COLUMN land_registry_data.ch_matched_name_2 IS 'Official Companies House name matched to proprietor 2';
COMMENT ON COLUMN land_registry_data.ch_matched_number_2 IS 'Companies House registration number matched to proprietor 2';
COMMENT ON COLUMN land_registry_data.ch_match_type_2 IS 'Type of match: Name+Number, Number, Name, Previous_Name, or No_Match';
COMMENT ON COLUMN land_registry_data.ch_match_confidence_2 IS 'Confidence score: 1.0 (Name+Number), 0.9 (Number), 0.7 (Name), 0.5 (Previous_Name), 0.0 (No_Match)';

COMMENT ON COLUMN land_registry_data.ch_matched_name_3 IS 'Official Companies House name matched to proprietor 3';
COMMENT ON COLUMN land_registry_data.ch_matched_number_3 IS 'Companies House registration number matched to proprietor 3';
COMMENT ON COLUMN land_registry_data.ch_match_type_3 IS 'Type of match: Name+Number, Number, Name, Previous_Name, or No_Match';
COMMENT ON COLUMN land_registry_data.ch_match_confidence_3 IS 'Confidence score: 1.0 (Name+Number), 0.9 (Number), 0.7 (Name), 0.5 (Previous_Name), 0.0 (No_Match)';

COMMENT ON COLUMN land_registry_data.ch_matched_name_4 IS 'Official Companies House name matched to proprietor 4';
COMMENT ON COLUMN land_registry_data.ch_matched_number_4 IS 'Companies House registration number matched to proprietor 4';
COMMENT ON COLUMN land_registry_data.ch_match_type_4 IS 'Type of match: Name+Number, Number, Name, Previous_Name, or No_Match';
COMMENT ON COLUMN land_registry_data.ch_match_confidence_4 IS 'Confidence score: 1.0 (Name+Number), 0.9 (Number), 0.7 (Name), 0.5 (Previous_Name), 0.0 (No_Match)';

COMMENT ON COLUMN land_registry_data.ch_match_date IS 'Timestamp when the Companies House matching was performed';

-- Create a summary view showing match statistics
CREATE OR REPLACE VIEW v_ch_match_summary AS
SELECT 
    COUNT(*) as total_properties,
    COUNT(DISTINCT CASE WHEN ch_match_type_1 != 'No_Match' OR ch_match_type_2 != 'No_Match' 
                            OR ch_match_type_3 != 'No_Match' OR ch_match_type_4 != 'No_Match' 
                       THEN title_number END) as properties_with_matches,
    -- Proprietor 1 stats
    COUNT(CASE WHEN ch_match_type_1 = 'Name+Number' THEN 1 END) as prop1_name_number_matches,
    COUNT(CASE WHEN ch_match_type_1 = 'Number' THEN 1 END) as prop1_number_matches,
    COUNT(CASE WHEN ch_match_type_1 = 'Name' THEN 1 END) as prop1_name_matches,
    COUNT(CASE WHEN ch_match_type_1 = 'Previous_Name' THEN 1 END) as prop1_previous_name_matches,
    COUNT(CASE WHEN ch_match_type_1 = 'No_Match' THEN 1 END) as prop1_no_matches,
    -- Total match counts across all proprietors
    COUNT(CASE WHEN ch_match_type_1 != 'No_Match' THEN 1 END) +
    COUNT(CASE WHEN ch_match_type_2 != 'No_Match' THEN 1 END) +
    COUNT(CASE WHEN ch_match_type_3 != 'No_Match' THEN 1 END) +
    COUNT(CASE WHEN ch_match_type_4 != 'No_Match' THEN 1 END) as total_proprietor_matches
FROM land_registry_data
WHERE ch_match_date IS NOT NULL;

-- Sample query to find properties by matched company
-- SELECT * FROM land_registry_data 
-- WHERE ch_matched_number_1 = '12345678' 
--    OR ch_matched_number_2 = '12345678'
--    OR ch_matched_number_3 = '12345678' 
--    OR ch_matched_number_4 = '12345678';