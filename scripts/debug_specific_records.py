#!/usr/bin/env python3
"""
Debug why specific S NOTARO LIMITED records with 845344 didn't match
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

def normalize_company_number_fixed(number):
    """Normalize company number for matching"""
    if not number or str(number).strip() == '':
        return ""
    
    number = str(number).strip().upper()
    number = re.sub(r'[^A-Z0-9]', '', number)
    
    if number.startswith('SC'):
        return number
    
    if number.isdigit():
        return number.zfill(8)
    
    return number

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

# Check specific records from the screenshot
test_title_numbers = ['ST74065', 'CL98579', 'ST118064', 'ST118064']

print("=== Debugging S NOTARO LIMITED records with 845344 ===\n")

# Check a few specific records
cursor.execute("""
    SELECT 
        lr.id,
        lr.title_number,
        lr.proprietor_1_name,
        lr.company_1_reg_no,
        m.ch_match_type_1,
        m.ch_matched_name_1,
        m.ch_matched_number_1,
        m.updated_at
    FROM land_registry_data lr
    LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
    WHERE lr.proprietor_1_name = 'S NOTARO LIMITED'
    AND lr.company_1_reg_no = '845344'
    LIMIT 10
""")

records = cursor.fetchall()

print(f"Found {len(records)} S NOTARO LIMITED records with reg no '845344'\n")

for record in records:
    lr_id, title, prop_name, reg_no, match_type, ch_name, ch_no, updated = record
    
    print(f"Record ID: {lr_id} | Title: {title}")
    print(f"  LR Name: '{prop_name}' → Normalized: '{normalize_company_name_fixed(prop_name)}'")
    print(f"  LR Reg No: '{reg_no}' → Normalized: '{normalize_company_number_fixed(reg_no)}'")
    print(f"  Match Status: {match_type or 'NOT IN MATCH TABLE'}")
    if ch_name:
        print(f"  Matched to: {ch_name} ({ch_no})")
    print(f"  Last Updated: {updated}")
    
    # Check if this specific record should match
    norm_number = normalize_company_number_fixed(reg_no)
    print(f"\n  Testing match with normalized number '{norm_number}':")
    
    # Check if this number exists in CH
    cursor.execute("""
        SELECT company_name, company_number, company_status
        FROM companies_house_data
        WHERE company_number = %s
    """, (norm_number,))
    
    ch_result = cursor.fetchone()
    if ch_result:
        ch_name_found, ch_num_found, ch_status = ch_result
        print(f"  ✅ Found in CH: {ch_name_found} ({ch_num_found}) - {ch_status}")
        
        # Check if names would match after normalization
        norm_lr_name = normalize_company_name_fixed(prop_name)
        norm_ch_name = normalize_company_name_fixed(ch_name_found)
        print(f"  LR normalized name: '{norm_lr_name}'")
        print(f"  CH normalized name: '{norm_ch_name}'")
        print(f"  Names match: {norm_lr_name == norm_ch_name}")
        
        if norm_lr_name == norm_ch_name:
            print(f"  → This SHOULD be a Tier 1 (Name+Number) match!")
        else:
            print(f"  → This SHOULD be a Tier 2 (Number only) match!")
    else:
        print(f"  ❌ NOT found in CH with number {norm_number}")
    
    print("-" * 80)

# Check if these records were in the last processing batch
print("\n\n=== Checking if these were processed in recent runs ===")
cursor.execute("""
    SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN updated_at > '2025-09-18 12:00:00' THEN 1 END) as updated_today
    FROM land_registry_ch_matches m
    JOIN land_registry_data lr ON m.id = lr.id
    WHERE lr.proprietor_1_name = 'S NOTARO LIMITED'
    AND lr.company_1_reg_no = '845344'
""")

total, updated_today = cursor.fetchone()
print(f"Total S NOTARO LIMITED records with '845344': {total}")
print(f"Updated today after 12:00: {updated_today}")

cursor.close()
conn.close()