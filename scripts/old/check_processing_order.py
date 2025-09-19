#!/usr/bin/env python3
"""
Check the processing order issue
"""

import psycopg2
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

print("=== Understanding the processing order issue ===\n")

# The script said it saved checkpoint at ID 11067187
print("Script checkpoint: ID 11067187")
print("ST181927 ID: 15971641\n")

# Check No_Match records around the checkpoint
print("=== No_Match records around checkpoint ID ===")
cursor.execute("""
    SELECT 
        lr.id,
        lr.title_number,
        lr.proprietor_1_name,
        m.ch_match_type_1,
        m.updated_at
    FROM land_registry_data lr
    JOIN land_registry_ch_matches m ON lr.id = m.id
    WHERE m.ch_match_type_1 = 'No_Match'
    AND lr.id BETWEEN 11067180 AND 11067195
    ORDER BY lr.id
""")

for row in cursor.fetchall():
    print(f"ID: {row[0]} | Title: {row[1]} | Name: {row[2]} | Updated: {row[4]}")

# Check if the script processes in ID order
print("\n=== Checking if script processes in strict ID order ===")
print("The script uses: WHERE lr.id > %s ... ORDER BY lr.id LIMIT %s")
print("This means it processes in ID order and stops after 3.715M records")
print(f"\nStarting from ID 0, after 3,715,000 records, it would reach approximately:")
print(f"Estimated max ID processed: ~11,067,187 (matches the checkpoint)")

# Count No_Match records by ID ranges
print("\n=== No_Match records by ID range ===")
cursor.execute("""
    SELECT 
        CASE 
            WHEN id <= 11067187 THEN '0 to 11,067,187'
            ELSE 'Above 11,067,187'
        END as id_range,
        COUNT(*) as count
    FROM land_registry_ch_matches
    WHERE ch_match_type_1 = 'No_Match'
    OR ch_match_type_2 = 'No_Match'
    OR ch_match_type_3 = 'No_Match'
    OR ch_match_type_4 = 'No_Match'
    GROUP BY id_range
    ORDER BY id_range
""")

for range_name, count in cursor.fetchall():
    print(f"{range_name}: {count:,} No_Match records")

print("\n💡 CONCLUSION:")
print("The script processes records in ID order, not by No_Match status.")
print("It processed the first 3.715M records (IDs 0-11,067,187) that had No_Match status.")
print("ST181927 (ID 15,971,641) is beyond this range, so it wasn't processed!")

cursor.close()
conn.close()