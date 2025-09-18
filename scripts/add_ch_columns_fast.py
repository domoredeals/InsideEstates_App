#!/usr/bin/env python3
"""
Add CH columns with minimal locking using PostgreSQL 11+ features
"""

import sys
import os
import psycopg2
import time

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

def add_columns_fast():
    """Add columns with less aggressive locking"""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor()
        
        # Set statement timeout to avoid hanging
        cursor.execute("SET statement_timeout = '5min'")
        
        # Check PostgreSQL version
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        print(f"PostgreSQL version: {version}")
        
        # First check what columns already exist
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'land_registry_data' 
            AND column_name LIKE 'ch_%'
        """)
        existing_columns = {row[0] for row in cursor.fetchall()}
        
        if existing_columns:
            print(f"\nFound {len(existing_columns)} existing CH columns")
            for col in sorted(existing_columns):
                print(f"  ✓ {col}")
        
        # List of columns to add
        columns_to_add = [
            ("ch_matched_name_1", "TEXT"),
            ("ch_matched_number_1", "VARCHAR(20)"),
            ("ch_match_type_1", "VARCHAR(20)"),
            ("ch_match_confidence_1", "DECIMAL(3,2)"),
            ("ch_matched_name_2", "TEXT"),
            ("ch_matched_number_2", "VARCHAR(20)"),
            ("ch_match_type_2", "VARCHAR(20)"),
            ("ch_match_confidence_2", "DECIMAL(3,2)"),
            ("ch_matched_name_3", "TEXT"),
            ("ch_matched_number_3", "VARCHAR(20)"),
            ("ch_match_type_3", "VARCHAR(20)"),
            ("ch_match_confidence_3", "DECIMAL(3,2)"),
            ("ch_matched_name_4", "TEXT"),
            ("ch_matched_number_4", "VARCHAR(20)"),
            ("ch_match_type_4", "VARCHAR(20)"),
            ("ch_match_confidence_4", "DECIMAL(3,2)"),
            ("ch_match_date", "TIMESTAMP")
        ]
        
        # Add only missing columns
        print("\nAdding missing columns...")
        added_count = 0
        
        for col_name, col_type in columns_to_add:
            if col_name not in existing_columns:
                try:
                    print(f"  Adding {col_name}...", end='', flush=True)
                    start_time = time.time()
                    
                    # Use a shorter lock timeout for the ALTER
                    cursor.execute("SET lock_timeout = '30s'")
                    cursor.execute(f"ALTER TABLE land_registry_data ADD COLUMN {col_name} {col_type}")
                    conn.commit()
                    
                    elapsed = time.time() - start_time
                    print(f" ✓ Done in {elapsed:.1f}s")
                    added_count += 1
                    
                except psycopg2.errors.LockNotAvailable:
                    print(" ✗ Table locked, skipping")
                    conn.rollback()
                except psycopg2.errors.DuplicateColumn:
                    print(" ✓ Already exists")
                    conn.rollback()
                except Exception as e:
                    print(f" ✗ Error: {e}")
                    conn.rollback()
        
        if added_count == 0:
            print("\n✅ All columns already exist!")
        else:
            print(f"\n✅ Added {added_count} columns successfully!")
        
        # Check final state
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'land_registry_data' 
            AND column_name LIKE 'ch_%'
            ORDER BY column_name
        """)
        final_columns = cursor.fetchall()
        
        print(f"\nTotal CH columns now: {len(final_columns)}")
        
        cursor.close()
        conn.close()
        
        print("\nNext steps:")
        print("1. Run: python scripts/match_lr_to_ch.py --test 1000")
        print("2. If successful, run: python scripts/match_lr_to_ch.py")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    add_columns_fast()