-- Create ownership_history table exactly as in SQLite database
-- This table tracks property ownership changes over time with buyers/sellers

-- Drop table if exists (be careful in production!)
DROP TABLE IF EXISTS ownership_history;

-- Create the table with exact structure from SQLite
CREATE TABLE ownership_history (
    title_number TEXT,
    property_address TEXT,
    ownership_start_date TEXT,
    ownership_end_date TEXT,
    owner_1 TEXT,
    owner_2 TEXT,
    owner_3 TEXT,
    owner_4 TEXT,
    seller_1 TEXT,
    seller_2 TEXT,
    seller_3 TEXT,
    seller_4 TEXT,
    buyer_1 TEXT,
    buyer_2 TEXT,
    buyer_3 TEXT,
    buyer_4 TEXT,
    price_at_acquisition REAL,
    price_at_disposal REAL,
    ownership_status TEXT,
    ownership_duration_days INTEGER,
    source TEXT,
    ownership_type TEXT,
    inferred_disposal_flag INTEGER,
    disposal_from_company INTEGER
);

-- Create all indexes exactly as in SQLite (with IF NOT EXISTS)
CREATE INDEX IF NOT EXISTS idx_ownership_title ON ownership_history(title_number);
CREATE INDEX IF NOT EXISTS idx_ownership_status ON ownership_history(ownership_status);
CREATE INDEX IF NOT EXISTS idx_ownership_hist_status_owner ON ownership_history(ownership_status, owner_1);
CREATE INDEX IF NOT EXISTS idx_ownership_history_owner_status ON ownership_history(owner_1, ownership_status);
CREATE INDEX IF NOT EXISTS idx_ownership_history_title ON ownership_history(title_number);

-- PostgreSQL equivalent of SQLite's COLLATE NOCASE
-- Using lower() function index for case-insensitive search
CREATE INDEX IF NOT EXISTS idx_ownership_history_search ON ownership_history(LOWER(owner_1));

-- Additional PostgreSQL-specific optimizations (optional)
-- These won't affect the table structure but will improve performance
CREATE INDEX IF NOT EXISTS idx_ownership_dates ON ownership_history(ownership_start_date, ownership_end_date);
CREATE INDEX IF NOT EXISTS idx_ownership_type ON ownership_history(ownership_type);

-- Add table comment
COMMENT ON TABLE ownership_history IS 'Transactional ownership history tracking buyers, sellers, and ownership periods. Migrated from SQLite lr_v3_memory_efficient.db';

-- Add column comments for clarity
COMMENT ON COLUMN ownership_history.ownership_status IS 'Current or Previous ownership';
COMMENT ON COLUMN ownership_history.inferred_disposal_flag IS '1 if disposal was inferred (property disappeared from snapshots)';
COMMENT ON COLUMN ownership_history.disposal_from_company IS '1 if property no longer appears in company ownership';
COMMENT ON COLUMN ownership_history.ownership_type IS 'UK COMPANY, OVERSEAS COMPANY, or OTHER';
COMMENT ON COLUMN ownership_history.source IS 'Source filename (e.g., CCOD_FULL_2018_05.csv)';