#!/usr/bin/env python3
"""
Check why the script stops at ID 11067187
"""

import psycopg2
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

print("=== Investigating why processing stops at ID 11067187 ===\n")

# Check what's at that ID
cursor.execute("""
    SELECT 
        lr.id,
        lr.title_number,
        lr.proprietor_1_name,
        m.ch_match_type_1,
        m.updated_at
    FROM land_registry_data lr
    LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
    WHERE lr.id >= 11067187
    ORDER BY lr.id
    LIMIT 5
""")

print("Records around ID 11067187:")
for record_id, title, name, match_type, updated in cursor.fetchall():
    print(f"ID: {record_id} | Title: {title} | Name: {name} | Match: {match_type} | Updated: {updated}")

# Check the distribution of No_Match records
print("\n=== Distribution of No_Match records by ID ===")
cursor.execute("""
    SELECT 
        CASE 
            WHEN id < 5000000 THEN '0-5M'
            WHEN id < 10000000 THEN '5-10M'
            WHEN id < 15000000 THEN '10-15M'
            WHEN id < 20000000 THEN '15-20M'
            ELSE '20M+'
        END as id_range,
        COUNT(*) as no_match_count
    FROM land_registry_ch_matches
    WHERE ch_match_type_1 = 'No_Match'
    GROUP BY id_range
    ORDER BY id_range
""")

for id_range, count in cursor.fetchall():
    print(f"{id_range}: {count:,} No_Match records")

# Check total No_Match records after ID 11067187
cursor.execute("""
    SELECT COUNT(*)
    FROM land_registry_ch_matches
    WHERE ch_match_type_1 = 'No_Match'
    AND id > 11067187
""")
remaining = cursor.fetchone()[0]
print(f"\nNo_Match records after ID 11067187: {remaining:,}")

# Check if ST181927 is after this ID
cursor.execute("""
    SELECT id FROM land_registry_data WHERE title_number = 'ST181927'
""")
st_id = cursor.fetchone()[0]
print(f"\nST181927 ID: {st_id:,}")
print(f"Is it after 11067187? {'Yes' if st_id > 11067187 else 'No'}")

# Check what the no_match_only query would actually return
print("\n=== Testing the no_match_only query ===")
cursor.execute("""
    SELECT COUNT(*)
    FROM land_registry_data lr
    JOIN land_registry_ch_matches m ON lr.id = m.id
    WHERE (
        m.ch_match_type_1 = 'No_Match' OR
        m.ch_match_type_2 = 'No_Match' OR  
        m.ch_match_type_3 = 'No_Match' OR
        m.ch_match_type_4 = 'No_Match'
    )
""")
total_no_match = cursor.fetchone()[0]
print(f"Total records matching no_match_only criteria: {total_no_match:,}")

cursor.close()
conn.close()