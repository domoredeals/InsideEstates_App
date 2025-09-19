#!/usr/bin/env python3
"""
Test why certain records aren't being matched
"""

import psycopg2
import re
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

# Copy the exact normalization functions from the production script
def normalize_company_name_fixed(name):
    """Use the PROVEN normalization from the original script that REMOVES suffixes"""
    if not name:
        return None
        
    name = str(name).upper().strip()
    
    # Replace common separators
    name = name.replace(' AND ', ' ')
    name = name.replace(' & ', ' ')
    
    # REMOVE company type suffixes (proven to increase matches)
    suffix_pattern = r'\s*(LIMITED LIABILITY PARTNERSHIP|LIMITED|COMPANY|LTD\.|LLP|LTD|PLC|CO\.|CO).*$'
    name = re.sub(suffix_pattern, '', name)
    
    # Remove special characters but keep alphanumeric
    name = ''.join(char for char in name if char.isalnum())
    
    return name

def normalize_company_number(number):
    """Normalize company number - pad with zeros to 8 digits"""
    if not number:
        return None
    
    # Remove any non-numeric characters
    number = re.sub(r'[^0-9]', '', str(number))
    
    # Pad with leading zeros to make it 8 digits
    if number and len(number) < 8:
        number = number.zfill(8)
    
    return number if number else None

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

# Test a few No_Match records that should match
test_cases = [
    ('ROWANFIELD OAK LTD', '15483533'),
    ('ABC LIMITED', None),  # Generic test
    ('XYZ LTD', None)       # Generic test
]

print("=== Testing No_Match records that should match ===\n")

# Get some actual No_Match records with registration numbers
cursor.execute("""
    SELECT 
        lr.id,
        lr.proprietor_1_name,
        lr.company_1_reg_no
    FROM land_registry_ch_matches m
    JOIN land_registry_data lr ON m.id = lr.id
    WHERE m.ch_match_type_1 = 'No_Match'
    AND lr.company_1_reg_no IS NOT NULL
    AND lr.company_1_reg_no != ''
    LIMIT 10
""")

no_match_with_numbers = cursor.fetchall()

print(f"Testing {len(no_match_with_numbers)} No_Match records that have registration numbers:\n")

matches_found = 0
for lr_id, name, reg_no in no_match_with_numbers:
    norm_name = normalize_company_name_fixed(name)
    norm_number = normalize_company_number(reg_no)
    
    print(f"LR: '{name}' ({reg_no})")
    print(f"  Normalized name: '{norm_name}'")
    print(f"  Normalized number: '{norm_number}'")
    
    if norm_number:
        # Try to find by number
        cursor.execute("""
            SELECT company_name, company_number, company_status
            FROM companies_house_data
            WHERE company_number = %s
        """, (norm_number,))
        
        ch_result = cursor.fetchone()
        if ch_result:
            ch_name, ch_num, ch_status = ch_result
            ch_norm_name = normalize_company_name_fixed(ch_name)
            matches_found += 1
            print(f"  ✅ FOUND in CH: '{ch_name}' ({ch_num}) - {ch_status}")
            print(f"  CH normalized name: '{ch_norm_name}'")
            print(f"  Names match: {norm_name == ch_norm_name}")
            if norm_name == ch_norm_name:
                print(f"  → Should be Tier 1 (Name+Number) match!")
            else:
                print(f"  → Should be Tier 2 (Number only) match!")
        else:
            print(f"  ❌ NOT FOUND in CH with number {norm_number}")
    
    print()

print(f"\nSummary: {matches_found}/{len(no_match_with_numbers)} records with numbers should have matched")

# Check if the matching script is checking the right columns
print("\n=== Checking script logic ===")
print("The production script should be checking all 4 proprietor columns:")
print("- proprietor_1_name, company_1_reg_no")
print("- proprietor_2_name, company_2_reg_no") 
print("- proprietor_3_name, company_3_reg_no")
print("- proprietor_4_name, company_4_reg_no")

cursor.close()
conn.close()