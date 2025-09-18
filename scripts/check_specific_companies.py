#!/usr/bin/env python3
"""
Check specific companies that should match
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
    
    return number if number else None

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

# Companies to check
test_companies = [
    "AL RAYAN BANK PLC",
    "S NOTARO LIMITED",
    "HNE FOODS LTD",
    "T&D HOLDINGS LIMITED",
    "MANNING PROPERTY RENTALS LIMITED"
]

print("=== Checking Specific Companies ===\n")

for company_name in test_companies:
    print(f"\n{'='*80}")
    print(f"Checking: {company_name}")
    print(f"{'='*80}")
    
    # Check in Land Registry data
    cursor.execute("""
        SELECT DISTINCT
            lr.proprietor_1_name,
            lr.company_1_reg_no,
            lr.title_number,
            m.ch_match_type_1,
            m.ch_matched_name_1,
            m.ch_matched_number_1
        FROM land_registry_data lr
        LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
        WHERE lr.proprietor_1_name = %s
        LIMIT 5
    """, (company_name,))
    
    lr_records = cursor.fetchall()
    
    if lr_records:
        print(f"\n✅ Found in Land Registry: {len(lr_records)} records")
        for record in lr_records:
            lr_name, lr_reg_no, title, match_type, ch_name, ch_number = record
            print(f"\n  Title: {title}")
            print(f"  LR Name: {lr_name}")
            print(f"  LR Reg No: {lr_reg_no or 'None'}")
            
            if match_type and match_type != 'No_Match':
                print(f"  ✅ MATCHED to: {ch_name} ({ch_number})")
                print(f"  Match Type: {match_type}")
            else:
                print(f"  ❌ NO MATCH in current results")
    else:
        print(f"\n❌ Not found in Land Registry data")
    
    # Check in Companies House data
    norm_name = normalize_company_name_fixed(company_name)
    print(f"\n🔍 Searching Companies House...")
    print(f"  Normalized name: '{norm_name}'")
    
    # Try exact name match first
    cursor.execute("""
        SELECT company_name, company_number, company_status, incorporation_date
        FROM companies_house_data
        WHERE company_name = %s
        LIMIT 5
    """, (company_name,))
    
    ch_exact = cursor.fetchall()
    
    if ch_exact:
        print(f"\n✅ Found EXACT match in Companies House:")
        for ch_name, ch_number, ch_status, inc_date in ch_exact:
            print(f"  {ch_name} ({ch_number}) - {ch_status} - Inc: {inc_date}")
    
    # Try partial match
    cursor.execute("""
        SELECT company_name, company_number, company_status, incorporation_date
        FROM companies_house_data
        WHERE company_name LIKE %s
        LIMIT 5
    """, (f"%{company_name.replace(' LIMITED', '').replace(' LTD', '').replace(' PLC', '')}%",))
    
    ch_partial = cursor.fetchall()
    
    if ch_partial and not ch_exact:
        print(f"\n🔍 Found PARTIAL matches in Companies House:")
        for ch_name, ch_number, ch_status, inc_date in ch_partial:
            ch_norm = normalize_company_name_fixed(ch_name)
            match_quality = "✅ Names normalize to same" if ch_norm == norm_name else "❌ Different after normalization"
            print(f"  {ch_name} ({ch_number}) - {ch_status}")
            print(f"    Normalized: '{ch_norm}' - {match_quality}")
    
    # Check if normalization would help
    if lr_records and not any(r[3] != 'No_Match' for r in lr_records):
        print(f"\n💡 Normalization Analysis:")
        print(f"  Original: '{company_name}'")
        print(f"  Normalized: '{norm_name}'")
        
        # Check if there's a company with the same normalized name
        cursor.execute("""
            SELECT company_name, company_number
            FROM companies_house_data
            WHERE company_name LIKE %s
            LIMIT 10
        """, (f"%{norm_name[:10]}%",))
        
        potential_matches = cursor.fetchall()
        found_match = False
        
        for pot_name, pot_number in potential_matches:
            pot_norm = normalize_company_name_fixed(pot_name)
            if pot_norm == norm_name:
                print(f"  🎯 SHOULD MATCH: {pot_name} ({pot_number})")
                found_match = True
                break
        
        if not found_match:
            print(f"  ❓ No Companies House entry found with matching normalized name")

cursor.close()
conn.close()