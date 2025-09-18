#!/usr/bin/env python3
"""
Debug why ST181927 didn't match even after reprocessing
"""

import psycopg2
import re
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

def normalize_company_number_fixed(number):
    if not number or str(number).strip() == '':
        return ''
    
    number = str(number).strip().upper()
    number = re.sub(r'[^A-Z0-9]', '', number)
    
    if number.startswith('SC'):
        return number
    
    if number.isdigit():
        return number.zfill(8)
    
    return number

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

print("=== Why ST181927 (S NOTARO LIMITED with 854344) didn't match ===\n")

# The LR record
lr_number = "854344"
norm_lr_number = normalize_company_number_fixed(lr_number)

print(f"Land Registry number: {lr_number}")
print(f"Normalized to: {norm_lr_number}")

# Check if this normalized number exists in CH
print(f"\nChecking Companies House for {norm_lr_number}...")
cursor.execute("""
    SELECT company_name, company_number, company_status
    FROM companies_house_data
    WHERE company_number = %s
""", (norm_lr_number,))

result = cursor.fetchone()
if result:
    print(f"✅ Found: {result[0]} ({result[1]}) - {result[2]}")
else:
    print(f"❌ No company found with number {norm_lr_number}")
    
    # Check similar numbers
    print(f"\nChecking similar numbers...")
    cursor.execute("""
        SELECT company_number, company_name
        FROM companies_house_data
        WHERE company_number LIKE '008%344'
        ORDER BY company_number
        LIMIT 10
    """)
    
    similar = cursor.fetchall()
    if similar:
        print("Similar company numbers found:")
        for num, name in similar:
            print(f"  {num}: {name}")
    
    # The correct number
    correct_number = "00845344"
    print(f"\nThe correct number is probably: {correct_number}")
    cursor.execute("""
        SELECT company_name, company_number, company_status
        FROM companies_house_data
        WHERE company_number = %s
    """, (correct_number,))
    
    correct_result = cursor.fetchone()
    if correct_result:
        print(f"✅ Which is: {correct_result[0]} ({correct_result[1]}) - {correct_result[2]}")
    
    print(f"\nConclusion: 854344 is a TYPO in the Land Registry data")
    print(f"The digit '5' is in the wrong position: 854344 vs 845344")
    print(f"Normalization can pad numbers but can't fix transposed digits")

cursor.close()
conn.close()