#!/usr/bin/env python3
"""
Check specific example - ROWANFIELD OAK LTD
"""

import psycopg2
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

# Check if ROWANFIELD OAK exists in Companies House
print("=== Checking Companies House for ROWANFIELD OAK ===")
cursor.execute("""
    SELECT company_name, company_number, company_status, incorporation_date
    FROM companies_house_data
    WHERE company_name LIKE '%ROWANFIELD%OAK%'
    ORDER BY company_name
""")

ch_results = cursor.fetchall()
print(f"Found {len(ch_results)} matching companies in CH:")
for name, number, status, inc_date in ch_results:
    print(f"  - {name} ({number}) - Status: {status}, Inc: {inc_date}")

# Check Land Registry records
print("\n=== Checking Land Registry for ROWANFIELD OAK ===")
cursor.execute("""
    SELECT DISTINCT 
        lr.proprietor_1_name, 
        lr.company_1_reg_no,
        lr.title_number,
        lr.file_month,
        m.ch_match_type_1,
        m.ch_matched_name_1,
        m.ch_matched_number_1
    FROM land_registry_data lr
    LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
    WHERE lr.proprietor_1_name LIKE '%ROWANFIELD%OAK%'
    ORDER BY lr.file_month DESC
    LIMIT 10
""")

lr_results = cursor.fetchall()
print(f"Found {len(lr_results)} LR records:")
for row in lr_results:
    prop_name, reg_no, title, month, match_type, ch_name, ch_no = row
    print(f"\nLR Name: '{prop_name}'")
    print(f"LR Reg No: '{reg_no}'")
    print(f"Title: {title}, Month: {month}")
    print(f"Match Status: {match_type}")
    if ch_name:
        print(f"Matched to: '{ch_name}' ({ch_no})")

# Check if the company number exists in CH
if lr_results and lr_results[0][1]:  # If we have a reg number
    reg_no = lr_results[0][1]
    print(f"\n=== Checking CH for registration number {reg_no} ===")
    
    # Try with padding
    padded_no = reg_no.zfill(8)
    cursor.execute("""
        SELECT company_name, company_number, company_status
        FROM companies_house_data
        WHERE company_number = %s OR company_number = %s
    """, (reg_no, padded_no))
    
    ch_by_number = cursor.fetchall()
    if ch_by_number:
        print(f"Found company by number:")
        for name, number, status in ch_by_number:
            print(f"  - {name} ({number}) - Status: {status}")
    else:
        print(f"No company found with number {reg_no} or {padded_no}")

cursor.close()
conn.close()