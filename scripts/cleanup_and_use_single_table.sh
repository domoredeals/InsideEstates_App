#!/bin/bash

# Script to clean up old multi-table schema and switch to single table import

echo "=== InsideEstates Single Table Migration ==="
echo ""
echo "This script will:"
echo "1. Drop the old multi-table schema (properties, proprietors, companies, import_history)"
echo "2. Keep only the new single 'land_registry_data' table"
echo ""
read -p "Are you sure you want to proceed? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Cancelled."
    exit 1
fi

# Activate virtual environment
cd /home/adc/Projects/InsideEstates_App
source venv/bin/activate

echo ""
echo "Step 1: Dropping old tables..."
psql -U insideestates_user -d insideestates_app -f scripts/cleanup_old_tables.sql

echo ""
echo "Step 2: Creating new single table schema..."
psql -U insideestates_user -d insideestates_app -f scripts/create_single_table.sql

echo ""
echo "Done! Your database now has only the single 'land_registry_data' table."
echo ""
echo "To import data, run:"
echo "  python scripts/import_to_single_table.py"
echo ""
echo "For more info, see: scripts/SINGLE_TABLE_IMPORT.md"