#!/usr/bin/env python3
"""
Manually update a specific match to test if updates are working
"""

import psycopg2
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

# Update ROWANFIELD OAK LTD manually
lr_id = 17619079
print(f"Manually updating record {lr_id}...")

try:
    cursor.execute("""
        UPDATE land_registry_ch_matches
        SET 
            ch_matched_name_1 = 'ROWANFIELD OAK LTD',
            ch_matched_number_1 = '15483533',
            ch_match_type_1 = 'Name+Number',
            ch_match_confidence_1 = 1.0,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (lr_id,))
    
    rows_updated = cursor.rowcount
    print(f"Rows updated: {rows_updated}")
    
    if rows_updated > 0:
        conn.commit()
        print("✅ Update committed successfully")
        
        # Verify the update
        cursor.execute("""
            SELECT 
                ch_match_type_1,
                ch_matched_name_1,
                ch_matched_number_1
            FROM land_registry_ch_matches
            WHERE id = %s
        """, (lr_id,))
        
        result = cursor.fetchone()
        if result:
            match_type, name, number = result
            print(f"Verified - Match Type: {match_type}, Name: {name}, Number: {number}")
    else:
        print("❌ No rows were updated")
        conn.rollback()
        
except Exception as e:
    print(f"❌ Error: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()