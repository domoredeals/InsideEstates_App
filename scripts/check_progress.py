#!/usr/bin/env python3
"""
Check progress of database operations
"""

import sys
import os
import psycopg2

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

def check_progress():
    """Check what's happening in PostgreSQL"""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor()
        
        # Check active queries
        print("=== Active Queries ===")
        cursor.execute("""
            SELECT pid, now() - pg_stat_activity.query_start AS duration, 
                   state, query
            FROM pg_stat_activity
            WHERE state != 'idle' 
            AND query NOT ILIKE '%pg_stat_activity%'
            ORDER BY duration DESC
            LIMIT 5
        """)
        
        active_queries = cursor.fetchall()
        for pid, duration, state, query in active_queries:
            print(f"\nPID: {pid}")
            print(f"Duration: {duration}")
            print(f"State: {state}")
            print(f"Query: {query[:100]}...")
        
        # Check if columns already exist
        print("\n=== Checking CH Columns ===")
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'land_registry_data' 
            AND column_name LIKE 'ch_%'
            ORDER BY column_name
        """)
        
        columns = cursor.fetchall()
        if columns:
            print(f"Found {len(columns)} CH columns already:")
            for col in columns:
                print(f"  - {col[0]}")
        else:
            print("No CH columns found yet")
        
        # Check table size
        cursor.execute("""
            SELECT 
                pg_size_pretty(pg_total_relation_size('land_registry_data')) as total_size,
                COUNT(*) as row_count
            FROM land_registry_data
        """)
        size, count = cursor.fetchone()
        print(f"\nTable size: {size}, Row count: {count:,}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    check_progress()