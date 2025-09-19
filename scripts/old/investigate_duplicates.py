#!/usr/bin/env python3
"""
Investigate why some duplicates are still appearing.
"""

import os
import sys
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables
load_dotenv()

def investigate_duplicates():
    """Investigate why some duplicates still exist."""
    
    # Database connection parameters
    conn_params = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'insideestates_app'),
        'user': os.getenv('DB_USER', 'insideestates_user'),
        'password': os.getenv('DB_PASSWORD', 'InsideEstates2024!')
    }
    
    try:
        # Connect to database
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        
        # Let's check one of the duplicate cases
        print("Investigating duplicate: Title NGL896367, Owner YASHAR HOLDINGS LIMITED")
        print("-" * 80)
        
        cur.execute("""
            SELECT 
                id,
                title_number,
                proprietor_name,
                proprietor_sequence,
                file_month,
                created_at,
                update_type,
                change_indicator,
                ownership_status
            FROM v_ownership_history
            WHERE title_number = 'NGL896367' 
            AND proprietor_name = 'YASHAR HOLDINGS LIMITED'
            ORDER BY file_month DESC, proprietor_sequence
        """)
        
        results = cur.fetchall()
        
        if results:
            print(f"Found {len(results)} records:")
            for row in results:
                print(f"\n  ID: {row[0]}")
                print(f"  Proprietor Sequence: {row[3]}")
                print(f"  File Month: {row[4]}")
                print(f"  Created At: {row[5]}")
                print(f"  Update Type: {row[6]}")
                print(f"  Change: {row[7]}")
                print(f"  Status: {row[8]}")
        
        # Check the raw data for this property
        print("\n\nChecking raw data for this property:")
        print("-" * 80)
        
        cur.execute("""
            SELECT 
                id,
                file_month,
                proprietor_1_name,
                proprietor_2_name,
                proprietor_3_name,
                proprietor_4_name,
                created_at
            FROM land_registry_data
            WHERE title_number = 'NGL896367'
            AND (
                proprietor_1_name = 'YASHAR HOLDINGS LIMITED' OR
                proprietor_2_name = 'YASHAR HOLDINGS LIMITED' OR
                proprietor_3_name = 'YASHAR HOLDINGS LIMITED' OR
                proprietor_4_name = 'YASHAR HOLDINGS LIMITED'
            )
            ORDER BY file_month DESC, created_at DESC
        """)
        
        raw_results = cur.fetchall()
        
        for row in raw_results:
            print(f"\nID: {row[0]}, Month: {row[1]}, Created: {row[6]}")
            print(f"  P1: {row[2]}")
            print(f"  P2: {row[3]}")
            print(f"  P3: {row[4]}")
            print(f"  P4: {row[5]}")
        
        # Close connection
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    investigate_duplicates()