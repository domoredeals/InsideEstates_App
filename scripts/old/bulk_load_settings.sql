-- PostgreSQL Bulk Loading Optimization Settings
-- Use these settings before large data imports

-- Disable synchronous commits (faster writes, slight durability risk)
SET synchronous_commit = OFF;

-- Increase checkpoint segments for less frequent checkpoints
SET checkpoint_segments = 100;  -- For older versions
-- For newer versions:
SET max_wal_size = '16GB';
SET checkpoint_timeout = '30min';
SET checkpoint_completion_target = 0.9;

-- Maximize work memory for this session
SET work_mem = '1GB';
SET maintenance_work_mem = '16GB';  -- For index creation

-- Disable autovacuum during bulk load (re-enable after!)
SET session_replication_role = replica;  -- This disables triggers and FK checks

-- For COPY operations
SET wal_level = minimal;  -- Requires restart, reduces WAL logging

-- Parallel workers for index creation
SET max_parallel_maintenance_workers = 16;
SET max_parallel_workers_per_gather = 16;

-- After bulk load, run:
-- RESET session_replication_role;
-- VACUUM ANALYZE;
-- REINDEX CONCURRENTLY;