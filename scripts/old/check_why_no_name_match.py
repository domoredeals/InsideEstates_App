#!/usr/bin/env python3
"""
Check why ST181927 didn't get a Name match even though names should match
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
    if not name or name.strip() == '':
        return ""
    
    name = str(name).upper().strip()
    name = name.replace(' AND ', ' ').replace(' & ', ' ')
    
    # Pre-compiled regex for suffixes - REMOVE THEM
    suffix_pattern = r'\s*(LIMITED LIABILITY PARTNERSHIP|LIMITED|COMPANY|LTD\.|LLP|LTD|PLC|CO\.|CO).*$'
    name = re.sub(suffix_pattern, '', name)
    
    # Keep only alphanumeric
    name = ''.join(char for char in name if char.isalnum())
    
    return name

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

print("=== Why ST181927 didn't get a Name match ===\n")

# Check the LR record
lr_name = "S NOTARO LIMITED"
norm_lr_name = normalize_company_name_fixed(lr_name)

print(f"Land Registry name: '{lr_name}'")
print(f"Normalized to: '{norm_lr_name}'")

# Check if this normalized name exists in CH
print(f"\nChecking Companies House for companies with normalized name '{norm_lr_name}'...")

# Get all companies that would normalize to SNOTARO
cursor.execute("""
    SELECT company_name, company_number, company_status
    FROM companies_house_data
    WHERE company_name LIKE '%NOTARO%LIMITED%'
    LIMIT 20
""")

print("\nCompanies House entries containing NOTARO:")
matches_found = False

for ch_name, ch_number, ch_status in cursor.fetchall():
    norm_ch_name = normalize_company_name_fixed(ch_name)
    if norm_ch_name == norm_lr_name:
        matches_found = True
        print(f"✅ '{ch_name}' → '{norm_ch_name}' ({ch_number}) - {ch_status}")
    else:
        print(f"   '{ch_name}' → '{norm_ch_name}' ({ch_number})")

if matches_found:
    print(f"\n🤔 Found companies that normalize to '{norm_lr_name}'")
    print("So ST181927 SHOULD have gotten a Tier 3 (Name) match!")
    
    # Check when this record was last processed
    cursor.execute("""
        SELECT 
            m.ch_match_type_1,
            m.updated_at
        FROM land_registry_ch_matches m
        JOIN land_registry_data lr ON m.id = lr.id
        WHERE lr.title_number = 'ST181927'
    """)
    
    result = cursor.fetchone()
    if result:
        match_type, updated = result
        print(f"\nCurrent status: {match_type}")
        print(f"Last updated: {updated}")
        
        if updated and '2025-09-18' in str(updated):
            print("\n✅ This WAS processed today with the fixed normalization")
            print("❌ But still didn't match - there may be an issue with the matching logic")
        else:
            print("\n❌ This was NOT processed today - it was processed before the fix")

cursor.close()
conn.close()