#!/usr/bin/env python3
"""
Debug why ST181927 was skipped in the no_match_only run
"""

import psycopg2
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

print("=== Debugging why ST181927 wasn't processed ===\n")

# Check the full record details
cursor.execute("""
    SELECT 
        lr.id,
        lr.title_number,
        lr.proprietor_1_name,
        lr.proprietor_2_name,
        lr.proprietor_3_name,
        lr.proprietor_4_name,
        m.ch_match_type_1,
        m.ch_match_type_2,
        m.ch_match_type_3,
        m.ch_match_type_4,
        m.updated_at
    FROM land_registry_data lr
    JOIN land_registry_ch_matches m ON lr.id = m.id
    WHERE lr.title_number = 'ST181927'
""")

result = cursor.fetchone()
if result:
    (record_id, title, p1_name, p2_name, p3_name, p4_name, 
     mt1, mt2, mt3, mt4, updated) = result
    
    print(f"Record ID: {record_id}")
    print(f"Title: {title}")
    print(f"\nProprietors and match types:")
    print(f"  1. {p1_name} -> {mt1}")
    print(f"  2. {p2_name} -> {mt2}")
    print(f"  3. {p3_name} -> {mt3}")
    print(f"  4. {p4_name} -> {mt4}")
    print(f"\nLast updated: {updated}")
    
    # Check if it would be selected by no_match_only query
    would_be_selected = (
        mt1 == 'No_Match' or 
        mt2 == 'No_Match' or 
        mt3 == 'No_Match' or 
        mt4 == 'No_Match'
    )
    print(f"\nWould be selected by no_match_only query? {would_be_selected}")
    
    # Now check the exact query used by the script
    print("\n=== Testing the exact query from the script ===")
    
    # This is the query from get_processing_query() for no_match_only mode
    cursor.execute("""
        SELECT 
            lr.id,
            lr.proprietor_1_name, lr.company_1_reg_no,
            lr.proprietor_2_name, lr.company_2_reg_no,
            lr.proprietor_3_name, lr.company_3_reg_no,
            lr.proprietor_4_name, lr.company_4_reg_no
        FROM land_registry_data lr
        JOIN land_registry_ch_matches m ON lr.id = m.id
        WHERE lr.id = %s
        AND (
            m.ch_match_type_1 = 'No_Match' OR
            m.ch_match_type_2 = 'No_Match' OR  
            m.ch_match_type_3 = 'No_Match' OR
            m.ch_match_type_4 = 'No_Match'
        )
    """, (record_id,))
    
    query_result = cursor.fetchone()
    if query_result:
        print("✅ Record WOULD be selected by the script's query")
    else:
        print("❌ Record would NOT be selected by the script's query")
        
        # Check why
        print("\nChecking match types more carefully...")
        cursor.execute("""
            SELECT 
                ch_match_type_1,
                ch_match_type_2,
                ch_match_type_3,
                ch_match_type_4
            FROM land_registry_ch_matches
            WHERE id = %s
        """, (record_id,))
        
        types = cursor.fetchone()
        if types:
            print(f"Match types in DB: {types}")
            print(f"Are any NULL? {any(t is None for t in types)}")

cursor.close()
conn.close()