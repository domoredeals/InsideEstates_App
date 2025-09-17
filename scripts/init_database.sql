-- Create the InsideEstates database and user
-- Run this as PostgreSQL superuser (postgres)

-- Create database
CREATE DATABASE insideestates_app;

-- Create user
CREATE USER insideestates_user WITH PASSWORD 'your_secure_password_here';

-- Grant all privileges on database
GRANT ALL PRIVILEGES ON DATABASE insideestates_app TO insideestates_user;

-- Connect to the database
\c insideestates_app;

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm"; -- For fuzzy text search

-- Grant schema permissions
GRANT ALL ON SCHEMA public TO insideestates_user;