#!/usr/bin/env python3
"""
Check if 854344 is a typo for 845344
"""

import psycopg2
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

print("=== Checking registration numbers ===\n")

# Check both numbers
cursor.execute("""
    SELECT company_name, company_number, company_status, incorporation_date
    FROM companies_house_data
    WHERE company_number IN ('00854344', '00845344')
    ORDER BY company_number
""")

results = cursor.fetchall()
for name, number, status, inc_date in results:
    print(f"{number}: {name} - {status} (Inc: {inc_date})")

# Check how many LR records have each number
print("\n=== Land Registry records ===")
cursor.execute("""
    SELECT 
        company_1_reg_no as reg_no,
        COUNT(*) as count
    FROM land_registry_data
    WHERE proprietor_1_name = 'S NOTARO LIMITED'
    AND company_1_reg_no IN ('854344', '845344')
    GROUP BY company_1_reg_no
    ORDER BY count DESC
""")

for reg_no, count in cursor.fetchall():
    print(f"S NOTARO LIMITED with reg no '{reg_no}': {count} records")

cursor.close()
conn.close()