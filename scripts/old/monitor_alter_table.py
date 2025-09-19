#!/usr/bin/env python3
"""
Monitor ALTER TABLE progress in PostgreSQL
"""

import sys
import os
import psycopg2
import time

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

def monitor_progress():
    """Monitor database activity"""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor()
        
        print("Monitoring database activity... (Press Ctrl+C to stop)\n")
        
        while True:
            # Clear screen
            print("\033[2J\033[H")  # ANSI escape codes to clear screen
            
            print("=== PostgreSQL Activity Monitor ===")
            print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Check active queries
            cursor.execute("""
                SELECT 
                    pid,
                    now() - pg_stat_activity.query_start AS duration,
                    state,
                    wait_event,
                    wait_event_type,
                    LEFT(query, 100) as query_preview
                FROM pg_stat_activity
                WHERE state != 'idle' 
                AND query NOT ILIKE '%pg_stat_activity%'
                ORDER BY duration DESC
                LIMIT 5
            """)
            
            active_queries = cursor.fetchall()
            print(f"\n📊 Active Queries ({len(active_queries)}):")
            for pid, duration, state, wait_event, wait_type, query in active_queries:
                print(f"\n  PID: {pid} | Duration: {duration}")
                print(f"  State: {state} | Wait: {wait_event} ({wait_type})")
                print(f"  Query: {query}...")
            
            # Check table locks
            cursor.execute("""
                SELECT 
                    l.pid,
                    l.mode,
                    l.granted,
                    a.query,
                    now() - a.query_start as duration
                FROM pg_locks l
                JOIN pg_stat_activity a ON l.pid = a.pid
                WHERE l.relation = 'land_registry_data'::regclass
                AND a.state != 'idle'
            """)
            
            locks = cursor.fetchall()
            if locks:
                print(f"\n🔒 Table Locks on land_registry_data:")
                for pid, mode, granted, query, duration in locks:
                    status = "✓ Granted" if granted else "⏳ Waiting"
                    print(f"  PID: {pid} | Mode: {mode} | {status}")
                    print(f"  Duration: {duration}")
            
            # Check existing CH columns
            cursor.execute("""
                SELECT COUNT(*) 
                FROM information_schema.columns 
                WHERE table_name = 'land_registry_data' 
                AND column_name LIKE 'ch_matched_%'
            """)
            ch_column_count = cursor.fetchone()[0]
            
            # Check table statistics
            cursor.execute("""
                SELECT 
                    pg_size_pretty(pg_total_relation_size('land_registry_data')) as size,
                    n_live_tup as row_estimate
                FROM pg_stat_user_tables
                WHERE relname = 'land_registry_data'
            """)
            result = cursor.fetchone()
            if result:
                size, rows = result
                print(f"\n📈 Table Statistics:")
                print(f"  Size: {size}")
                print(f"  Estimated rows: {rows:,}")
                print(f"  CH match columns added: {ch_column_count}/17")
            
            # Check if ALTER TABLE is running
            cursor.execute("""
                SELECT COUNT(*) 
                FROM pg_stat_activity 
                WHERE state = 'active' 
                AND query LIKE 'ALTER TABLE land_registry_data%'
            """)
            alter_count = cursor.fetchone()[0]
            
            if alter_count > 0:
                print(f"\n⚠️  ALTER TABLE operation in progress...")
                print("   This can take several minutes for large tables.")
                print("   The table has millions of rows, so please be patient.")
            
            time.sleep(5)  # Update every 5 seconds
            
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    monitor_progress()