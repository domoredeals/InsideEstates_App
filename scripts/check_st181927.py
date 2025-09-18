#!/usr/bin/env python3
"""
Check the status of title ST181927
"""

import psycopg2
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

print("=== Checking title ST181927 ===\n")

# Check the current status
cursor.execute("""
    SELECT 
        lr.id,
        lr.title_number,
        lr.proprietor_1_name,
        lr.company_1_reg_no,
        lr.proprietor_2_name,
        lr.company_2_reg_no,
        lr.proprietor_3_name,
        lr.company_3_reg_no,
        lr.proprietor_4_name,
        lr.company_4_reg_no,
        m.ch_match_type_1,
        m.ch_matched_name_1,
        m.ch_matched_number_1,
        m.updated_at
    FROM land_registry_data lr
    LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
    WHERE lr.title_number = 'ST181927'
""")

result = cursor.fetchone()

if result:
    (lr_id, title, prop1_name, prop1_no, prop2_name, prop2_no, 
     prop3_name, prop3_no, prop4_name, prop4_no,
     match_type, ch_name, ch_number, updated) = result
    
    print(f"Title: {title}")
    print(f"Record ID: {lr_id}")
    print(f"Last Updated: {updated}")
    print(f"\nProprietors:")
    
    proprietors = [
        (prop1_name, prop1_no, match_type, ch_name, ch_number),
        (prop2_name, prop2_no, None, None, None),
        (prop3_name, prop3_no, None, None, None),
        (prop4_name, prop4_no, None, None, None)
    ]
    
    for i, (name, reg_no, m_type, m_name, m_number) in enumerate(proprietors, 1):
        if name:
            print(f"\nProprietor {i}:")
            print(f"  Name: {name}")
            print(f"  Reg No: {reg_no or 'None'}")
            if i == 1 and m_type:
                print(f"  Match Status: {m_type}")
                if m_name:
                    print(f"  Matched to: {m_name} ({m_number})")
else:
    print("Title ST181927 not found")

cursor.close()
conn.close()