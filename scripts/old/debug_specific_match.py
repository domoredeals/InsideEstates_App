#!/usr/bin/env python3
"""
Debug why specific records like ROWANFIELD OAK LTD aren't matching
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
    
    return number

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

# Get specific ROWANFIELD OAK LTD record
print("=== Checking specific ROWANFIELD OAK LTD record ===")
cursor.execute("""
    SELECT 
        lr.id,
        lr.proprietor_1_name,
        lr.company_1_reg_no,
        lr.title_number,
        m.ch_match_type_1,
        m.ch_matched_name_1,
        m.ch_matched_number_1
    FROM land_registry_data lr
    LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
    WHERE lr.proprietor_1_name = 'ROWANFIELD OAK LTD'
    AND lr.company_1_reg_no = '15483533'
    LIMIT 1
""")

result = cursor.fetchone()
if result:
    lr_id, prop_name, reg_no, title, match_type, ch_name, ch_no = result
    print(f"\nLR Record ID: {lr_id}")
    print(f"Title: {title}")
    print(f"LR Name: '{prop_name}' → Normalized: '{normalize_company_name_fixed(prop_name)}'")
    print(f"LR Number: '{reg_no}' → Normalized: '{normalize_company_number(reg_no)}'")
    print(f"Current Match Status: {match_type}")
    
    # Check if this ID was processed
    print(f"\n=== Checking if record {lr_id} was processed ===")
    
    # Check what mode was used
    cursor.execute("""
        SELECT MIN(id), MAX(id), COUNT(*) 
        FROM land_registry_ch_matches 
        WHERE ch_match_type_1 = 'No_Match'
    """)
    min_id, max_id, count = cursor.fetchone()
    print(f"No_Match records range: ID {min_id} to {max_id} (count: {count})")
    print(f"Was this record in range? {min_id <= lr_id <= max_id}")
    
    # Check if it's in the match table at all
    cursor.execute("""
        SELECT COUNT(*) FROM land_registry_ch_matches WHERE id = %s
    """, (lr_id,))
    in_match_table = cursor.fetchone()[0]
    print(f"Is record in match table? {'Yes' if in_match_table else 'No'}")
    
    # Try to manually match it
    print("\n=== Manual matching test ===")
    norm_number = normalize_company_number(reg_no)
    cursor.execute("""
        SELECT company_name, company_number, company_status
        FROM companies_house_data
        WHERE company_number = %s
    """, (norm_number,))
    
    ch_result = cursor.fetchone()
    if ch_result:
        ch_name, ch_num, ch_status = ch_result
        print(f"Found in CH: '{ch_name}' ({ch_num}) - Status: {ch_status}")
        print(f"CH Name normalized: '{normalize_company_name_fixed(ch_name)}'")
        print(f"Names match? {normalize_company_name_fixed(prop_name) == normalize_company_name_fixed(ch_name)}")
        print(f"Numbers match? {norm_number == ch_num}")
        print(f"Should be Tier 1 match (Name+Number)!")
    else:
        print(f"NOT FOUND in CH with number {norm_number}")

cursor.close()
conn.close()