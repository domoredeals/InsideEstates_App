#!/usr/bin/env python3
"""
Check all ownership records for a specific title number
"""

import psycopg2
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

title_number = 'MX418571'

print(f"=== Ownership History for Title Number: {title_number} ===\n")

# Get all records for this title using the view
cursor.execute("""
    SELECT 
        file_month,
        date_proprietor_added,
        proprietor_sequence,
        proprietor_name,
        lr_company_reg_no,
        ch_matched_name,
        ch_matched_number,
        ch_match_type,
        ch_company_name,
        ch_company_status,
        property_address,
        tenure,
        price_paid,
        ownership_status
    FROM v_ownership_history
    WHERE title_number = %s
    ORDER BY file_month DESC, date_proprietor_added DESC, proprietor_sequence
""", (title_number,))

records = cursor.fetchall()

if not records:
    print(f"No records found for title number {title_number}")
else:
    print(f"Found {len(records)} ownership records\n")
    
    current_period = None
    for record in records:
        file_month = record[0]
        date_added = record[1]
        prop_seq = record[2]
        prop_name = record[3]
        lr_reg_no = record[4]
        ch_matched_name = record[5]
        ch_matched_number = record[6]
        ch_match_type = record[7]
        ch_company_name = record[8]
        ch_company_status = record[9]
        property_address = record[10]
        tenure = record[11]
        price_paid = record[12]
        ownership_status = record[13]
        
        # Print header for each unique ownership period
        if current_period != (file_month, date_added):
            current_period = (file_month, date_added)
            print(f"\n{'='*80}")
            print(f"File Month: {file_month} | Date Proprietor Added: {date_added}")
            print(f"Property: {property_address}")
            print(f"Tenure: {tenure} | Price Paid: £{price_paid:,.2f}" if price_paid else f"Tenure: {tenure}")
            print(f"Ownership Status: {ownership_status}")
            print(f"{'='*80}")
        
        # Print proprietor info
        print(f"\nProprietor {prop_seq}:")
        print(f"  LR Name: {prop_name}")
        print(f"  LR Reg No: {lr_reg_no or 'None'}")
        
        if ch_match_type and ch_match_type != 'No_Match':
            print(f"  ✅ Matched to: {ch_matched_name} ({ch_matched_number})")
            print(f"  Match Type: {ch_match_type}")
            print(f"  CH Company: {ch_company_name}")
            print(f"  CH Status: {ch_company_status}")
        else:
            print(f"  ❌ No match found in Companies House")

# Also get a summary of distinct owners
print(f"\n\n{'='*80}")
print("=== SUMMARY OF DISTINCT PROPRIETORS ===")
print(f"{'='*80}")

cursor.execute("""
    SELECT DISTINCT
        proprietor_name,
        lr_company_reg_no,
        ch_matched_name,
        ch_match_type,
        ch_company_status
    FROM v_ownership_history
    WHERE title_number = %s
    AND proprietor_name IS NOT NULL
    ORDER BY proprietor_name
""", (title_number,))

distinct_owners = cursor.fetchall()

for owner in distinct_owners:
    name, reg_no, ch_name, match_type, ch_status = owner
    print(f"\n{name}")
    print(f"  Reg No: {reg_no or 'None'}")
    if match_type and match_type != 'No_Match':
        print(f"  ✅ Matched: {ch_name} ({match_type})")
        print(f"  CH Status: {ch_status}")
    else:
        print(f"  ❌ No CH match")

cursor.close()
conn.close()