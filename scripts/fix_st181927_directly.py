#!/usr/bin/env python3
"""
Directly fix ST181927 and verify the normalization works
"""

import psycopg2
import re
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

def normalize_company_name(name):
    """Fixed normalization that REMOVES suffixes"""
    if not name or name.strip() == '':
        return ""
    
    name = str(name).upper().strip()
    name = name.replace(' AND ', ' ').replace(' & ', ' ')
    
    # Remove suffixes and anything after
    suffix_pattern = r'\s*(LIMITED LIABILITY PARTNERSHIP|LIMITED|COMPANY|LTD\.|LLP|LTD|PLC|CO\.|CO).*$'
    name = re.sub(suffix_pattern, '', name)
    
    # Keep only alphanumeric
    name = ''.join(char for char in name if char.isalnum())
    
    return name

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

print("=== Directly fixing ST181927 ===\n")

# Get the record
cursor.execute("""
    SELECT 
        lr.id,
        lr.title_number,
        lr.proprietor_1_name,
        lr.company_1_reg_no
    FROM land_registry_data lr
    WHERE lr.title_number = 'ST181927'
""")

record = cursor.fetchone()
if not record:
    print("ST181927 not found!")
    sys.exit(1)

record_id, title, lr_name, lr_reg_no = record
print(f"Found: {title}")
print(f"ID: {record_id}")
print(f"Company: {lr_name}")
print(f"Reg No: {lr_reg_no}")

# Normalize the name
norm_name = normalize_company_name(lr_name)
print(f"\nNormalized name: '{norm_name}'")

# Find matching companies
print(f"\nSearching Companies House for normalized name '{norm_name}'...")
cursor.execute("""
    SELECT company_name, company_number, company_status
    FROM companies_house_data
    WHERE company_name LIKE '%NOTARO%'
    LIMIT 50
""")

matches = []
for ch_name, ch_number, ch_status in cursor.fetchall():
    ch_norm = normalize_company_name(ch_name)
    if ch_norm == norm_name:
        matches.append((ch_name, ch_number, ch_status))
        print(f"✅ Match found: {ch_name} ({ch_number}) - {ch_status}")

if matches:
    # Use the first active company or the first one
    best_match = next((m for m in matches if m[2] == 'Active'), matches[0])
    ch_name, ch_number, ch_status = best_match
    
    print(f"\nUpdating ST181927 with match: {ch_name} ({ch_number})")
    
    # Update the match record
    cursor.execute("""
        UPDATE land_registry_ch_matches
        SET 
            ch_matched_name_1 = %s,
            ch_matched_number_1 = %s,
            ch_match_type_1 = 'Name',
            ch_match_confidence_1 = 0.7,
            updated_at = NOW()
        WHERE id = %s
    """, (ch_name, ch_number, record_id))
    
    conn.commit()
    print("✅ Updated successfully!")
    
    # Verify the update
    cursor.execute("""
        SELECT ch_match_type_1, ch_matched_name_1, ch_matched_number_1
        FROM land_registry_ch_matches
        WHERE id = %s
    """, (record_id,))
    
    match_type, matched_name, matched_number = cursor.fetchone()
    print(f"\nVerification:")
    print(f"  Match Type: {match_type}")
    print(f"  Matched Company: {matched_name}")
    print(f"  Matched Number: {matched_number}")
else:
    print("❌ No matches found - this shouldn't happen!")

cursor.close()
conn.close()