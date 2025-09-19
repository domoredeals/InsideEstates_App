#!/bin/bash

# PostgreSQL Initial Setup for InsideEstates App
echo "==================================="
echo "PostgreSQL Setup for InsideEstates"
echo "==================================="

# Set credentials
DB_NAME="insideestates_app"
DB_USER="insideestates_user"
DB_PASS="InsideEstates2024!"  # Change this if you want

echo "Creating database and user..."

# Create user and database as postgres superuser
sudo -u postgres psql << EOF
-- Create user
CREATE USER ${DB_USER} WITH PASSWORD '${DB_PASS}';

-- Create database
CREATE DATABASE ${DB_NAME} OWNER ${DB_USER};

-- Grant all privileges
GRANT ALL PRIVILEGES ON DATABASE ${DB_NAME} TO ${DB_USER};

-- Connect to the new database
\c ${DB_NAME}

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Grant schema permissions to user
GRANT ALL ON SCHEMA public TO ${DB_USER};

-- Show created objects
\du ${DB_USER}
\l ${DB_NAME}
EOF

echo ""
echo "Creating .env file with credentials..."

# Create .env file with the credentials
cat > /home/adc/Projects/InsideEstates_App/.env << EOF
# PostgreSQL Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=${DB_NAME}
DB_USER=${DB_USER}
DB_PASSWORD=${DB_PASS}

# Flask Configuration
FLASK_ENV=development
FLASK_DEBUG=True
SECRET_KEY=dev-secret-key-change-in-production

# API Keys (if needed)
COMPANIES_HOUSE_API_KEY=your_api_key_here
EOF

echo ""
echo "Testing connection..."
PGPASSWORD=${DB_PASS} psql -h localhost -U ${DB_USER} -d ${DB_NAME} -c "SELECT version();"

echo ""
echo "==================================="
echo "Setup Complete!"
echo "==================================="
echo ""
echo "Database Name: ${DB_NAME}"
echo "Username: ${DB_USER}"
echo "Password: ${DB_PASS}"
echo ""
echo "These credentials have been saved to:"
echo "/home/adc/Projects/InsideEstates_App/.env"
echo ""
echo "Next steps:"
echo "1. cd /home/adc/Projects/InsideEstates_App"
echo "2. python scripts/setup_database.py"
echo "3. python scripts/optimize_postgresql.py"