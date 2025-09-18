#!/usr/bin/env python3
"""
Check and resolve table locks
"""

import sys
import os
import psycopg2

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

def check_locks():
    """Check what's locking the table"""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor()
        
        print("=== Checking Table Locks ===\n")
        
        # Find blocking queries
        cursor.execute("""
            SELECT 
                blocked_locks.pid AS blocked_pid,
                blocked_activity.usename AS blocked_user,
                blocking_locks.pid AS blocking_pid,
                blocking_activity.usename AS blocking_user,
                blocked_activity.query AS blocked_query,
                blocking_activity.query AS blocking_query,
                blocked_activity.state AS blocked_state,
                blocking_activity.state AS blocking_state,
                now() - blocking_activity.query_start AS blocking_duration
            FROM pg_catalog.pg_locks blocked_locks
            JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
            JOIN pg_catalog.pg_locks blocking_locks 
                ON blocking_locks.locktype = blocked_locks.locktype
                AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
                AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
                AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
                AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
                AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
                AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
                AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
                AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
                AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
                AND blocking_locks.pid != blocked_locks.pid
            JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
            WHERE NOT blocked_locks.granted;
        """)
        
        blocks = cursor.fetchall()
        if blocks:
            print("Found blocking queries:")
            for row in blocks:
                print(f"\nBlocked PID: {row[0]} ({row[1]})")
                print(f"Blocked by PID: {row[2]} ({row[3]})")
                print(f"Blocking duration: {row[8]}")
                print(f"Blocking query: {row[5][:100]}...")
        else:
            print("No blocking queries found.")
        
        # Check specific locks on land_registry_data
        cursor.execute("""
            SELECT 
                l.pid,
                l.mode,
                l.granted,
                a.usename,
                a.application_name,
                a.state,
                now() - a.query_start as duration,
                LEFT(a.query, 150) as query_preview
            FROM pg_locks l
            JOIN pg_stat_activity a ON l.pid = a.pid
            WHERE l.relation = 'land_registry_data'::regclass
            ORDER BY l.granted, duration DESC
        """)
        
        locks = cursor.fetchall()
        if locks:
            print(f"\n=== Locks on land_registry_data ({len(locks)} total) ===")
            for pid, mode, granted, user, app, state, duration, query in locks:
                status = "✓ GRANTED" if granted else "⏳ WAITING"
                print(f"\nPID {pid}: {mode} lock {status}")
                print(f"User: {user} | App: {app} | State: {state}")
                print(f"Duration: {duration}")
                print(f"Query: {query}")
                
                # If it's a SELECT that's been running for a long time
                if granted and state == 'active' and 'SELECT' in query and duration:
                    minutes = duration.total_seconds() / 60
                    if minutes > 5:
                        print(f"⚠️  This SELECT has been running for {minutes:.1f} minutes!")
        
        # Check if there are any long-running transactions
        cursor.execute("""
            SELECT 
                pid,
                usename,
                state,
                now() - xact_start as transaction_duration,
                now() - query_start as query_duration,
                query
            FROM pg_stat_activity
            WHERE state != 'idle' 
            AND xact_start < now() - interval '5 minutes'
            ORDER BY xact_start
        """)
        
        long_transactions = cursor.fetchall()
        if long_transactions:
            print("\n=== Long-Running Transactions ===")
            for pid, user, state, xact_dur, query_dur, query in long_transactions:
                print(f"\nPID {pid} ({user}): Transaction running for {xact_dur}")
                print(f"Current state: {state}")
                print(f"Current query duration: {query_dur}")
                print(f"Query: {query[:100]}...")
        
        cursor.close()
        conn.close()
        
        print("\n=== Recommendations ===")
        print("The table appears to be locked by another process.")
        print("We should use the separate table approach instead:")
        print("\n1. Cancel any stuck ALTER TABLE operations")
        print("2. Run: python scripts/run_sql_script.py scripts/create_match_table.sql")
        print("3. Use the modified matching script that writes to the separate table")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    check_locks()