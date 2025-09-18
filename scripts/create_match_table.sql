-- Create a separate table for CH matches instead of altering the main table
-- This is MUCH faster than adding columns to a large table

-- Drop if exists
DROP TABLE IF EXISTS land_registry_ch_matches CASCADE;

-- Create matches table
CREATE TABLE land_registry_ch_matches (
    id BIGINT PRIMARY KEY REFERENCES land_registry_data(id) ON DELETE CASCADE,
    
    -- Matched data for proprietor 1
    ch_matched_name_1 TEXT,
    ch_matched_number_1 VARCHAR(20),
    ch_match_type_1 VARCHAR(20),
    ch_match_confidence_1 DECIMAL(3,2),
    
    -- Matched data for proprietor 2
    ch_matched_name_2 TEXT,
    ch_matched_number_2 VARCHAR(20),
    ch_match_type_2 VARCHAR(20),
    ch_match_confidence_2 DECIMAL(3,2),
    
    -- Matched data for proprietor 3
    ch_matched_name_3 TEXT,
    ch_matched_number_3 VARCHAR(20),
    ch_match_type_3 VARCHAR(20),
    ch_match_confidence_3 DECIMAL(3,2),
    
    -- Matched data for proprietor 4
    ch_matched_name_4 TEXT,
    ch_matched_number_4 VARCHAR(20),
    ch_match_type_4 VARCHAR(20),
    ch_match_confidence_4 DECIMAL(3,2),
    
    -- Match metadata
    ch_match_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX idx_ch_matches_numbers ON land_registry_ch_matches(
    ch_matched_number_1, ch_matched_number_2, ch_matched_number_3, ch_matched_number_4
);

CREATE INDEX idx_ch_matches_types ON land_registry_ch_matches(
    ch_match_type_1, ch_match_type_2, ch_match_type_3, ch_match_type_4
);

CREATE INDEX idx_ch_matches_date ON land_registry_ch_matches(ch_match_date);

-- Create a view that joins the two tables
CREATE OR REPLACE VIEW v_land_registry_with_ch AS
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
    m.ch_match_date
FROM land_registry_data lr
LEFT JOIN land_registry_ch_matches m ON lr.id = m.id;

-- Grant permissions if needed
-- GRANT SELECT, INSERT, UPDATE, DELETE ON land_registry_ch_matches TO insideestates_user;
-- GRANT SELECT ON v_land_registry_with_ch TO insideestates_user;