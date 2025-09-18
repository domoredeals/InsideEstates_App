#!/usr/bin/env python3
"""
Fix database transaction state and check for issues
"""

import psycopg2
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

def fix_database_state():
    """Reset database state and check for issues"""
    
    conn = None
    try:
        # Create new connection with autocommit to avoid transaction issues
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        
        print("=== CHECKING DATABASE STATE ===")
        
        # Check for locks
        cursor.execute("""
            SELECT 
                pid,
                usename,
                application_name,
                client_addr,
                wait_event_type,
                wait_event,
                state,
                query
            FROM pg_stat_activity
            WHERE datname = 'insideestates_app'
            AND state != 'idle'
            AND pid != pg_backend_pid()
        """)
        
        active_queries = cursor.fetchall()
        if active_queries:
            print(f"\n⚠️  Found {len(active_queries)} active queries:")
            for row in active_queries:
                print(f"  PID: {row[0]}, User: {row[1]}, State: {row[6]}")
                if row[7]:
                    print(f"  Query: {row[7][:100]}...")
        else:
            print("✅ No blocking queries found")
        
        # Check for locks on the match table
        cursor.execute("""
            SELECT 
                locktype,
                relation::regclass,
                mode,
                granted
            FROM pg_locks 
            WHERE relation = 'land_registry_ch_matches'::regclass
        """)
        
        locks = cursor.fetchall()
        if locks:
            print(f"\n⚠️  Found {len(locks)} locks on land_registry_ch_matches table")
            for lock in locks:
                print(f"  Lock type: {lock[0]}, Mode: {lock[2]}, Granted: {lock[3]}")
        else:
            print("✅ No locks on match table")
        
        # Check match table statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(CASE WHEN ch_match_type_1 = 'No_Match' THEN 1 END) as no_match_count,
                COUNT(CASE WHEN ch_match_type_1 != 'No_Match' OR 
                               ch_match_type_2 != 'No_Match' OR 
                               ch_match_type_3 != 'No_Match' OR 
                               ch_match_type_4 != 'No_Match' THEN 1 END) as matched_count
            FROM land_registry_ch_matches
        """)
        
        stats = cursor.fetchone()
        total, no_match, matched = stats
        
        print(f"\n=== MATCH TABLE STATISTICS ===")
        print(f"Total records: {total:,}")
        print(f"No_Match records: {no_match:,}")
        print(f"Matched records: {matched:,}")
        print(f"Match rate: {matched/total*100:.1f}%")
        
        # Kill any blocking queries if requested
        if active_queries and input("\n❓ Kill blocking queries? (y/n): ").lower() == 'y':
            for row in active_queries:
                pid = row[0]
                if pid != cursor.connection.get_backend_pid():
                    print(f"Killing PID {pid}...")
                    cursor.execute("SELECT pg_terminate_backend(%s)", (pid,))
            print("✅ Blocking queries terminated")
        
        # Test the match query
        print("\n=== TESTING MATCH QUERY ===")
        cursor.execute("""
            SELECT id, ch_match_type_1 
            FROM land_registry_ch_matches 
            WHERE ch_match_type_1 = 'No_Match' 
            LIMIT 5
        """)
        
        test_results = cursor.fetchall()
        print(f"Successfully queried {len(test_results)} No_Match records")
        
        print("\n✅ Database state appears healthy")
        print("\nYou can now re-run the matching script:")
        print("python scripts/03_match_lr_to_ch_production.py --mode no_match_only")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    fix_database_state()