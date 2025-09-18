#!/usr/bin/env python3
"""
Check why S NOTARO LIMITED isn't found in our CH database
"""

import psycopg2
import re
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

def normalize_company_name_fixed(name):
    """Fixed normalization that REMOVES suffixes"""
    if not name:
        return None
        
    name = str(name).upper().strip()
    name = name.replace(' AND ', ' ')
    name = name.replace(' & ', ' ')
    
    # REMOVE company type suffixes
    suffix_pattern = r'\s*(LIMITED LIABILITY PARTNERSHIP|LIMITED|COMPANY|LTD\.|LLP|LTD|PLC|CO\.|CO).*$'
    name = re.sub(suffix_pattern, '', name)
    
    # Remove special characters but keep alphanumeric
    name = ''.join(char for char in name if char.isalnum())
    
    return name

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

print("=== Searching for S NOTARO LIMITED ===\n")

# Check by exact company number
company_number = '00845344'
print(f"1. Searching by company number: {company_number}")
cursor.execute("""
    SELECT company_name, company_number, company_status, incorporation_date
    FROM companies_house_data
    WHERE company_number = %s
""", (company_number,))

result = cursor.fetchone()
if result:
    print(f"   ✅ FOUND: {result[0]} ({result[1]}) - {result[2]} - Inc: {result[3]}")
else:
    print(f"   ❌ NOT FOUND with number {company_number}")

# Check variations of the name
print("\n2. Searching by name variations:")
name_variations = [
    'S. NOTARO LIMITED',
    'S NOTARO LIMITED',
    'S.NOTARO LIMITED',
    'S NOTARO LTD',
    'S. NOTARO LTD'
]

for name in name_variations:
    cursor.execute("""
        SELECT company_name, company_number, company_status
        FROM companies_house_data
        WHERE company_name = %s
    """, (name,))
    
    result = cursor.fetchone()
    if result:
        print(f"   ✅ '{name}' → {result[0]} ({result[1]}) - {result[2]}")
    else:
        print(f"   ❌ '{name}' → Not found")

# Check with normalized name
print("\n3. Searching with normalized name:")
norm_name = normalize_company_name_fixed('S NOTARO LIMITED')
print(f"   Normalized: 'S NOTARO LIMITED' → '{norm_name}'")

# Search for anything starting with NOTARO
print("\n4. Searching for any company with 'NOTARO' in the name:")
cursor.execute("""
    SELECT company_name, company_number, company_status, incorporation_date
    FROM companies_house_data
    WHERE company_name LIKE '%NOTARO%'
    ORDER BY company_name
    LIMIT 20
""")

notaro_companies = cursor.fetchall()
if notaro_companies:
    print(f"   Found {len(notaro_companies)} companies with NOTARO:")
    for name, number, status, inc_date in notaro_companies:
        print(f"   - {name} ({number}) - {status} - Inc: {inc_date}")
else:
    print("   ❌ No companies found with NOTARO in the name")

# Check if it might be in the data but with dots
print("\n5. Checking if periods/dots might be the issue:")
cursor.execute("""
    SELECT COUNT(*) FROM companies_house_data WHERE company_name LIKE '%.%'
""")
dot_count = cursor.fetchone()[0]
print(f"   Companies with dots in name: {dot_count}")

# Check the Land Registry side
print("\n\n=== Land Registry Records for S NOTARO LIMITED ===")
cursor.execute("""
    SELECT DISTINCT
        proprietor_1_name,
        company_1_reg_no,
        COUNT(*) as property_count
    FROM land_registry_data
    WHERE proprietor_1_name LIKE '%NOTARO%'
    GROUP BY proprietor_1_name, company_1_reg_no
    ORDER BY property_count DESC
""")

lr_records = cursor.fetchall()
if lr_records:
    print(f"Found {len(lr_records)} variations in Land Registry:")
    for name, reg_no, count in lr_records:
        norm = normalize_company_name_fixed(name)
        print(f"   '{name}' ({reg_no or 'No RegNo'}) - {count} properties")
        print(f"     → Normalized: '{norm}'")
        
        # For records with a reg number, check if we can match by number
        if reg_no == '00845344':
            print(f"     → This SHOULD match by Number (Tier 2) at minimum!")

cursor.close()
conn.close()