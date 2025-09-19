#!/usr/bin/env python3
import psycopg2
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent))
from config.postgresql_config import POSTGRESQL_CONFIG

# Connect to database
conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

# Read SQL file
with open('update_ownership_view_for_land_registry.sql', 'r') as f:
    sql = f.read()

# Execute
cursor.execute(sql)
conn.commit()

print("View updated successfully!")

# Test the results
cursor.execute("""
    SELECT 
        COUNT(*) as total_land_registry_matches,
        COUNT(CASE WHEN ch_company_name IS NOT NULL THEN 1 END) as with_company_name,
        COUNT(CASE WHEN ch_company_name IS NULL THEN 1 END) as without_company_name
    FROM v_ownership_history
    WHERE ch_match_type = 'Land_Registry'
""")

result = cursor.fetchone()
print(f"\nLand_Registry matches: {result[0]:,}")
print(f"With company name: {result[1]:,}")
print(f"Without company name: {result[2]:,}")

cursor.close()
conn.close()