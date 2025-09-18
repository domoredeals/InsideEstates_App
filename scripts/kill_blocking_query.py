#!/usr/bin/env python3
"""
Kill blocking queries safely
"""

import sys
import os
import psycopg2

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

def kill_blocking_queries():
    """Terminate blocking queries"""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        conn.autocommit = True  # Important for pg_terminate_backend
        cursor = conn.cursor()
        
        # The main blocking query
        blocking_pid = 308297
        
        print(f"Attempting to terminate blocking query (PID: {blocking_pid})...")
        
        try:
            cursor.execute("SELECT pg_terminate_backend(%s)", (blocking_pid,))
            result = cursor.fetchone()[0]
            if result:
                print(f"✓ Successfully terminated PID {blocking_pid}")
            else:
                print(f"✗ Could not terminate PID {blocking_pid} (may have already ended)")
        except Exception as e:
            print(f"Error terminating PID {blocking_pid}: {e}")
        
        # Also terminate the stuck ALTER TABLE commands
        stuck_pids = [326014, 326101, 326287]
        
        print("\nCleaning up stuck ALTER TABLE operations...")
        for pid in stuck_pids:
            try:
                cursor.execute("SELECT pg_terminate_backend(%s)", (pid,))
                result = cursor.fetchone()[0]
                if result:
                    print(f"✓ Terminated PID {pid}")
            except:
                pass
        
        print("\n✅ Cleanup complete!")
        print("\nNext steps:")
        print("1. Use the separate table approach instead:")
        print("   python scripts/run_sql_script.py scripts/create_match_table.sql")
        print("2. Then run the modified matching script")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    kill_blocking_queries()