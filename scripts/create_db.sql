-- Run this as postgres superuser
-- sudo -u postgres psql -f create_db.sql

-- Create user
CREATE USER insideestates_user WITH PASSWORD 'InsideEstates2024!';

-- Create database
CREATE DATABASE insideestates_app OWNER insideestates_user;

-- Grant all privileges
GRANT ALL PRIVILEGES ON DATABASE insideestates_app TO insideestates_user;

-- Connect to the new database
\c insideestates_app

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Grant schema permissions to user  
GRANT ALL ON SCHEMA public TO insideestates_user;

-- Show what was created
\du insideestates_user
\l insideestates_app