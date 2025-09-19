#!/usr/bin/env python3
"""
Check that the updated view correctly shows only latest records per title/owner.
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

def check_deduplication():
    """Check that the view correctly shows only latest records."""
    
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
        
        # Check the specific title numbers from the screenshot
        test_titles = ['SGL328942']
        
        for title in test_titles:
            print(f"\nChecking title: {title}")
            print("-" * 80)
            
            # Get all records for this title
            cur.execute("""
                SELECT 
                    title_number,
                    proprietor_name,
                    file_month,
                    update_type,
                    change_indicator,
                    ownership_status,
                    source_filename
                FROM v_ownership_history
                WHERE title_number = %s
                ORDER BY proprietor_name, file_month DESC
            """, (title,))
            
            results = cur.fetchall()
            
            if results:
                print(f"Found {len(results)} records in the view:")
                for row in results:
                    print(f"  Owner: {row[1]}")
                    print(f"    Month: {row[2]}, Update: {row[3]}, Change: {row[4]}, Status: {row[5]}")
                    print(f"    Source: {row[6]}")
            else:
                print("No records found for this title")
        
        # Check for any duplicate title/owner combinations
        print("\n\nChecking for any remaining duplicates...")
        print("-" * 80)
        
        cur.execute("""
            SELECT 
                title_number,
                proprietor_name,
                COUNT(*) as record_count
            FROM v_ownership_history
            GROUP BY title_number, proprietor_name
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC
            LIMIT 10
        """)
        
        duplicates = cur.fetchall()
        
        if duplicates:
            print(f"WARNING: Found {len(duplicates)} duplicate title/owner combinations:")
            for dup in duplicates:
                print(f"  Title: {dup[0]}, Owner: {dup[1]}, Count: {dup[2]}")
        else:
            print("SUCCESS: No duplicate title/owner combinations found!")
        
        # Show statistics
        print("\n\nView Statistics:")
        print("-" * 80)
        
        cur.execute("""
            SELECT 
                COUNT(DISTINCT title_number) as unique_titles,
                COUNT(DISTINCT proprietor_name) as unique_owners,
                COUNT(*) as total_records,
                COUNT(CASE WHEN ownership_status = 'Current' THEN 1 END) as current_records,
                COUNT(CASE WHEN ownership_status = 'Historical' THEN 1 END) as historical_records
            FROM v_ownership_history
        """)
        
        stats = cur.fetchone()
        print(f"Unique titles: {stats[0]:,}")
        print(f"Unique owners: {stats[1]:,}")
        print(f"Total records: {stats[2]:,}")
        print(f"Current records: {stats[3]:,}")
        print(f"Historical records: {stats[4]:,}")
        
        # Close connection
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    check_deduplication()