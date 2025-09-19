#!/usr/bin/env python3
"""
Check the improvements from the matching runs
"""

import psycopg2
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

print("=== MATCHING IMPROVEMENTS SUMMARY ===\n")

# Overall statistics
cursor.execute("""
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN ch_match_type_1 != 'No_Match' OR 
                       ch_match_type_2 != 'No_Match' OR 
                       ch_match_type_3 != 'No_Match' OR 
                       ch_match_type_4 != 'No_Match' THEN 1 END) as matched_records,
        COUNT(CASE WHEN ch_match_type_1 = 'No_Match' AND
                       (ch_match_type_2 IS NULL OR ch_match_type_2 = 'No_Match') AND
                       (ch_match_type_3 IS NULL OR ch_match_type_3 = 'No_Match') AND
                       (ch_match_type_4 IS NULL OR ch_match_type_4 = 'No_Match') THEN 1 END) as no_match_records
    FROM land_registry_ch_matches
""")

total, matched, no_match = cursor.fetchone()
match_rate = (matched / total * 100) if total > 0 else 0

print(f"Total records: {total:,}")
print(f"Matched records: {matched:,} ({match_rate:.2f}%)")
print(f"No match records: {no_match:,} ({no_match/total*100:.2f}%)")

# Breakdown by match type
print("\n=== MATCH TYPE BREAKDOWN ===")
cursor.execute("""
    SELECT 
        ch_match_type_1 as match_type,
        COUNT(*) as count
    FROM land_registry_ch_matches
    GROUP BY ch_match_type_1
    ORDER BY 
        CASE ch_match_type_1
            WHEN 'Name+Number' THEN 1
            WHEN 'Number' THEN 2
            WHEN 'Name' THEN 3
            WHEN 'Previous_Name' THEN 4
            WHEN 'No_Match' THEN 5
            ELSE 6
        END
""")

for match_type, count in cursor.fetchall():
    pct = (count / total * 100) if total > 0 else 0
    print(f"{match_type}: {count:,} ({pct:.2f}%)")

# Check improvements over time
print("\n=== RECENT IMPROVEMENTS ===")
cursor.execute("""
    SELECT 
        DATE(updated_at) as update_date,
        COUNT(*) as records_updated,
        COUNT(CASE WHEN ch_match_type_1 != 'No_Match' THEN 1 END) as new_matches
    FROM land_registry_ch_matches
    WHERE updated_at >= CURRENT_DATE - INTERVAL '2 days'
    GROUP BY DATE(updated_at)
    ORDER BY update_date DESC
""")

for update_date, records_updated, new_matches in cursor.fetchall():
    print(f"{update_date}: {records_updated:,} records updated, {new_matches:,} new matches")

# Check S NOTARO specifically
print("\n=== S NOTARO LIMITED STATUS ===")
cursor.execute("""
    SELECT 
        ch_match_type_1,
        COUNT(*) as count
    FROM land_registry_ch_matches m
    JOIN land_registry_data lr ON m.id = lr.id
    WHERE lr.proprietor_1_name LIKE '%NOTARO LIMITED'
    GROUP BY ch_match_type_1
    ORDER BY count DESC
""")

for match_type, count in cursor.fetchall():
    print(f"{match_type}: {count}")

cursor.close()
conn.close()