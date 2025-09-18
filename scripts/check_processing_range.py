#!/usr/bin/env python3
"""
Check if ST181927 was in the no_match_only processing range
"""

import psycopg2
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

# Get the record ID for ST181927
cursor.execute("""
    SELECT 
        lr.id,
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
    record_id, mt1, mt2, mt3, mt4, updated = result
    print(f"ST181927 record ID: {record_id}")
    print(f"Match types: 1={mt1}, 2={mt2}, 3={mt3}, 4={mt4}")
    print(f"Last updated: {updated}")
    
    # Check if this would be selected by no_match_only query
    would_be_selected = (
        mt1 == 'No_Match' or 
        mt2 == 'No_Match' or 
        mt3 == 'No_Match' or 
        mt4 == 'No_Match'
    )
    print(f"\nWould be selected by no_match_only: {would_be_selected}")
    
    # Check the range of IDs processed today
    cursor.execute("""
        SELECT 
            MIN(id) as min_id,
            MAX(id) as max_id,
            COUNT(*) as count
        FROM land_registry_ch_matches
        WHERE updated_at > '2025-09-18 12:00:00'
    """)
    
    min_id, max_id, count = cursor.fetchone()
    if min_id and max_id:
        print(f"\nRecords processed today:")
        print(f"  ID range: {min_id:,} to {max_id:,}")
        print(f"  Total count: {count:,}")
        print(f"  Was {record_id} in range? {min_id <= record_id <= max_id}")
    
    # Check if there are other No_Match records that weren't processed
    cursor.execute("""
        SELECT COUNT(*)
        FROM land_registry_ch_matches
        WHERE ch_match_type_1 = 'No_Match'
        AND updated_at < '2025-09-18 12:00:00'
    """)
    
    unprocessed = cursor.fetchone()[0]
    print(f"\nNo_Match records NOT processed today: {unprocessed:,}")

cursor.close()
conn.close()