#!/usr/bin/env python3
"""
Debug why specific companies aren't matching
"""

import psycopg2
import re
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

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

# Check specific examples
test_cases = [
    ("AL RAYAN BANK PLC", "004483430"),  # LR has leading zeros
    ("HNE FOODS LTD", "16424988"),
    ("T&D HOLDINGS LIMITED", "13503412"),
]

print("=== Debugging Why Companies Don't Match ===\n")

for company_name, lr_reg_no in test_cases:
    print(f"\n{'='*60}")
    print(f"Company: {company_name}")
    print(f"LR Reg No: {lr_reg_no}")
    norm_number = normalize_company_number(lr_reg_no)
    print(f"Normalized: {norm_number}")
    
    # Check if this exact combination exists in LR
    cursor.execute("""
        SELECT 
            lr.id,
            lr.proprietor_1_name,
            lr.company_1_reg_no,
            m.ch_match_type_1
        FROM land_registry_data lr
        LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
        WHERE lr.proprietor_1_name = %s
        AND lr.company_1_reg_no = %s
        LIMIT 1
    """, (company_name, lr_reg_no))
    
    lr_result = cursor.fetchone()
    
    if lr_result:
        lr_id, prop_name, reg_no, match_type = lr_result
        print(f"\n✅ Found in LR with ID: {lr_id}")
        print(f"   Current match status: {match_type or 'Not in match table'}")
        
        # Check if normalized number exists in CH
        cursor.execute("""
            SELECT company_name, company_number, company_status
            FROM companies_house_data
            WHERE company_number = %s
        """, (norm_number,))
        
        ch_result = cursor.fetchone()
        
        if ch_result:
            ch_name, ch_number, ch_status = ch_result
            print(f"\n✅ Found in CH:")
            print(f"   {ch_name} ({ch_number}) - {ch_status}")
            print(f"   Names match: {company_name == ch_name}")
            
            # Check if this record was processed in the last run
            cursor.execute("""
                SELECT 
                    ch_match_type_1,
                    updated_at
                FROM land_registry_ch_matches
                WHERE id = %s
            """, (lr_id,))
            
            match_result = cursor.fetchone()
            if match_result:
                match_type, updated = match_result
                print(f"\n📋 Match table info:")
                print(f"   Match type: {match_type}")
                print(f"   Last updated: {updated}")
                print(f"   Was it in no_match_only range? {'Yes' if match_type == 'No_Match' else 'No'}")
        else:
            print(f"\n❌ NOT found in CH with number {norm_number}")
    else:
        print(f"\n❌ This exact combination not found in LR")

# Check AL RAYAN BANK special case - multiple number formats
print(f"\n\n{'='*60}")
print("Special case: AL RAYAN BANK PLC number variations")
print(f"{'='*60}")

cursor.execute("""
    SELECT DISTINCT company_1_reg_no, COUNT(*) as count
    FROM land_registry_data
    WHERE proprietor_1_name = 'AL RAYAN BANK PLC'
    AND company_1_reg_no IS NOT NULL
    GROUP BY company_1_reg_no
    ORDER BY count DESC
""")

variations = cursor.fetchall()
print(f"\nFound {len(variations)} different registration numbers for AL RAYAN BANK PLC:")
for reg_no, count in variations:
    norm = normalize_company_number(reg_no)
    print(f"  '{reg_no}' → '{norm}' ({count} records)")

# The correct number in CH
correct_no = '04483430'
print(f"\nCorrect number in CH: {correct_no}")
print("All variations should normalize to this number")

cursor.close()
conn.close()