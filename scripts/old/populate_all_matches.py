#!/usr/bin/env python3
"""
Ensure all Land Registry records are in the match table
"""

import psycopg2
from psycopg2.extras import execute_values
import sys
from pathlib import Path
from tqdm import tqdm

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

print("=== Populating match table with all Land Registry records ===\n")

# First check what we have
cursor.execute("SELECT COUNT(*) FROM land_registry_data")
lr_total = cursor.fetchone()[0]
print(f"Total Land Registry records: {lr_total:,}")

cursor.execute("SELECT COUNT(*) FROM land_registry_ch_matches")
match_total = cursor.fetchone()[0]
print(f"Current match table records: {match_total:,}")

if match_total >= lr_total:
    print("\n✅ Match table already has all records")
    sys.exit(0)

print(f"\nNeed to add {lr_total - match_total:,} missing records")

# Get missing records
print("\nFinding missing records...")
cursor.execute("""
    SELECT lr.id
    FROM land_registry_data lr
    LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
    WHERE m.id IS NULL
    ORDER BY lr.id
""")

missing_ids = [row[0] for row in cursor.fetchall()]
print(f"Found {len(missing_ids):,} missing records")

if missing_ids:
    # Check if ST181927 is in the missing list
    cursor.execute("SELECT id FROM land_registry_data WHERE title_number = 'ST181927'")
    st_id = cursor.fetchone()[0]
    if st_id in missing_ids:
        print(f"\n⚠️  ST181927 (ID {st_id}) is among the missing records!")
    
    # Insert missing records with No_Match status
    print("\nInserting missing records...")
    batch_size = 10000
    
    insert_query = """
        INSERT INTO land_registry_ch_matches (
            id,
            ch_matched_name_1, ch_matched_number_1, ch_match_type_1, ch_match_confidence_1,
            ch_matched_name_2, ch_matched_number_2, ch_match_type_2, ch_match_confidence_2,
            ch_matched_name_3, ch_matched_number_3, ch_match_type_3, ch_match_confidence_3,
            ch_matched_name_4, ch_matched_number_4, ch_match_type_4, ch_match_confidence_4
        ) VALUES %s
        ON CONFLICT (id) DO NOTHING
    """
    
    with tqdm(total=len(missing_ids), desc="Inserting") as pbar:
        for i in range(0, len(missing_ids), batch_size):
            batch_ids = missing_ids[i:i + batch_size]
            
            # Create values for this batch - all with No_Match status
            values = []
            for record_id in batch_ids:
                values.append((
                    record_id,
                    None, None, 'No_Match', 0.0,
                    None, None, None, None,
                    None, None, None, None,
                    None, None, None, None
                ))
            
            execute_values(cursor, insert_query, values)
            conn.commit()
            pbar.update(len(batch_ids))
    
    print("\n✅ All missing records added to match table")

# Final verification
cursor.execute("SELECT COUNT(*) FROM land_registry_ch_matches")
final_count = cursor.fetchone()[0]
print(f"\nFinal match table count: {final_count:,}")

# Check ST181927
cursor.execute("""
    SELECT m.ch_match_type_1
    FROM land_registry_data lr
    JOIN land_registry_ch_matches m ON lr.id = m.id
    WHERE lr.title_number = 'ST181927'
""")

result = cursor.fetchone()
if result:
    print(f"\nST181927 is now in match table with status: {result[0]}")

cursor.close()
conn.close()

print("\nNow you can run the matching script in no_match_only mode to process all records properly.")