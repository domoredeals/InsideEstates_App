#!/usr/bin/env python3
"""
Check what's actually in the matches table
"""

import psycopg2
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

print("=== Checking actual match table contents ===\n")

# Count total records
cursor.execute("SELECT COUNT(*) FROM land_registry_ch_matches")
total = cursor.fetchone()[0]
print(f"Total records in match table: {total:,}")

# Check ID range
cursor.execute("SELECT MIN(id), MAX(id) FROM land_registry_ch_matches")
min_id, max_id = cursor.fetchone()
print(f"ID range: {min_id:,} to {max_id:,}")

# Check when they were updated
cursor.execute("""
    SELECT 
        DATE(updated_at) as update_date,
        COUNT(*) as count
    FROM land_registry_ch_matches
    GROUP BY DATE(updated_at)
    ORDER BY update_date DESC
    LIMIT 5
""")

print("\nRecords by update date:")
for date, count in cursor.fetchall():
    print(f"  {date}: {count:,} records")

# Check match type distribution
cursor.execute("""
    SELECT 
        ch_match_type_1,
        COUNT(*) as count
    FROM land_registry_ch_matches
    GROUP BY ch_match_type_1
    ORDER BY count DESC
""")

print("\nMatch type distribution:")
for match_type, count in cursor.fetchall():
    print(f"  {match_type}: {count:,}")

# Check ST181927 specifically
cursor.execute("""
    SELECT 
        lr.title_number,
        m.ch_match_type_1,
        m.ch_matched_name_1,
        m.updated_at
    FROM land_registry_data lr
    LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
    WHERE lr.title_number = 'ST181927'
""")

result = cursor.fetchone()
if result:
    title, match_type, matched_name, updated = result
    print(f"\nST181927 status:")
    print(f"  Match type: {match_type}")
    print(f"  Matched to: {matched_name}")
    print(f"  Updated: {updated}")

cursor.close()
conn.close()