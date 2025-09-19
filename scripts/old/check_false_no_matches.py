#!/usr/bin/env python3
"""
Check for false No_Match records - cases where companies have registration numbers
but were marked as No_Match even though they exist in Companies House data
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from config.postgresql_config import POSTGRESQL_CONFIG

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

# Count No_Match records that have company registration numbers
cursor.execute("""
    SELECT COUNT(*) 
    FROM land_registry_data lr
    JOIN land_registry_ch_matches lrm ON lr.id = lrm.id
    WHERE lrm.ch_match_type_1 = 'No_Match' 
    AND lr.company_1_reg_no IS NOT NULL 
    AND lr.company_1_reg_no != ''
""")

no_match_with_numbers = cursor.fetchone()[0]
print(f'No_Match records (proprietor 1) that have company numbers: {no_match_with_numbers:,}')

# Sample some to check if they exist in CH
cursor.execute("""
    SELECT DISTINCT
        lr.id,
        lr.proprietor_1_name,
        lr.company_1_reg_no
    FROM land_registry_data lr
    JOIN land_registry_ch_matches lrm ON lr.id = lrm.id
    WHERE lrm.ch_match_type_1 = 'No_Match' 
    AND lr.company_1_reg_no IS NOT NULL 
    AND lr.company_1_reg_no != ''
    AND LENGTH(lr.company_1_reg_no) >= 6
    LIMIT 20
""")

print('\nChecking if these No_Match records exist in Companies House:')
print('=' * 80)

false_no_matches = 0
records_to_check = cursor.fetchall()

for record_id, lr_name, lr_number in records_to_check:
    # Normalize the number
    normalized_number = lr_number.strip().upper()
    if normalized_number.isdigit():
        normalized_number = normalized_number.zfill(8)
    
    # Check if this company exists in CH
    cursor.execute("""
        SELECT company_name, company_status, company_number
        FROM companies_house_data
        WHERE company_number = %s
    """, (normalized_number,))
    
    ch_result = cursor.fetchone()
    
    print(f'\nLR: {lr_name} ({lr_number})')
    if ch_result:
        print(f'  ✅ EXISTS in CH: {ch_result[0]} ({ch_result[2]}) - Status: {ch_result[1]}')
        print(f'  ⚠️  This is a FALSE NO_MATCH!')
        false_no_matches += 1
    else:
        # Try exact match without normalization
        cursor.execute("""
            SELECT company_name, company_status, company_number
            FROM companies_house_data
            WHERE company_number = %s
        """, (lr_number,))
        
        ch_result2 = cursor.fetchone()
        if ch_result2:
            print(f'  ✅ EXISTS in CH: {ch_result2[0]} ({ch_result2[2]}) - Status: {ch_result2[1]}')
            print(f'  ⚠️  This is a FALSE NO_MATCH!')
            false_no_matches += 1
        else:
            print(f'  ❌ NOT found in CH data')

print('\n' + '=' * 80)
print(f'Summary: Found {false_no_matches} false No_Match records out of {len(records_to_check)} checked')
print(f'This suggests approximately {(false_no_matches/len(records_to_check)*100):.1f}% of No_Match records with company numbers are incorrect')

cursor.close()
conn.close()