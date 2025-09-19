#!/usr/bin/env python3
"""
Update FULL file records to have change_indicator = 'A' and change_date = date_proprietor_added
Process in batches to avoid timeout
"""
import psycopg2
import sys
import os
from tqdm import tqdm

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

def update_full_files():
    conn = psycopg2.connect(**POSTGRESQL_CONFIG)
    cursor = conn.cursor()
    
    # First get count of records to update
    cursor.execute("""
        SELECT COUNT(*) 
        FROM land_registry_data 
        WHERE update_type = 'FULL'
          AND (change_indicator IS NULL OR change_indicator = '');
    """)
    total_records = cursor.fetchone()[0]
    print(f"Found {total_records} FULL file records to update")
    
    if total_records == 0:
        print("No records to update")
        return
    
    # Update in batches of 100,000
    batch_size = 100000
    updated_total = 0
    
    with tqdm(total=total_records, desc="Updating FULL file records") as pbar:
        while updated_total < total_records:
            cursor.execute("""
                UPDATE land_registry_data 
                SET change_indicator = 'A',
                    change_date = date_proprietor_added
                WHERE id IN (
                    SELECT id 
                    FROM land_registry_data 
                    WHERE update_type = 'FULL'
                      AND (change_indicator IS NULL OR change_indicator = '')
                    LIMIT %s
                );
            """, (batch_size,))
            
            batch_updated = cursor.rowcount
            updated_total += batch_updated
            conn.commit()
            pbar.update(batch_updated)
            
            if batch_updated == 0:
                break
    
    print(f"\nCompleted! Updated {updated_total} records.")
    
    # Verify the update
    cursor.execute("""
        SELECT COUNT(*) as updated_count
        FROM land_registry_data 
        WHERE update_type = 'FULL'
          AND change_indicator = 'A';
    """)
    verified_count = cursor.fetchone()[0]
    print(f"Verified: {verified_count} FULL records now have change_indicator = 'A'")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    update_full_files()