#!/usr/bin/env python3
"""
Fix FULL file records by setting default Change Date and Change Indicator
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from config.postgresql_config import POSTGRESQL_CONFIG
from datetime import datetime

def fix_full_files():
    conn = psycopg2.connect(**POSTGRESQL_CONFIG)
    cursor = conn.cursor()
    
    try:
        print("Fixing FULL file records...")
        
        # Update CCOD_FULL records
        print("\nUpdating CCOD_FULL records...")
        cursor.execute("""
            UPDATE properties 
            SET change_date = '2018-01-01'::date,
                change_indicator = 'A'
            WHERE dataset_type = 'CCOD' 
            AND file_month = '2018-01-01'::date
            AND (change_date IS NULL OR change_indicator IS NULL OR change_indicator = '')
        """)
        ccod_updated = cursor.rowcount
        print(f"Updated {ccod_updated:,} CCOD_FULL records")
        
        # Update OCOD_FULL records  
        print("\nUpdating OCOD_FULL records...")
        cursor.execute("""
            UPDATE properties 
            SET change_date = '2018-01-01'::date,
                change_indicator = 'A'
            WHERE dataset_type = 'OCOD' 
            AND file_month = '2018-01-01'::date
            AND (change_date IS NULL OR change_indicator IS NULL OR change_indicator = '')
        """)
        ocod_updated = cursor.rowcount
        print(f"Updated {ocod_updated:,} OCOD_FULL records")
        
        # Verify the fix
        print("\nVerifying fixes...")
        cursor.execute("""
            SELECT 
                dataset_type,
                file_month,
                COUNT(*) as total_records,
                COUNT(change_date) as has_change_date,
                COUNT(change_indicator) as has_change_indicator
            FROM properties
            WHERE file_month = '2018-01-01'::date
            GROUP BY dataset_type, file_month
            ORDER BY dataset_type
        """)
        
        results = cursor.fetchall()
        print("\nVerification Results:")
        print("Dataset | File Month  | Total | Has Change Date | Has Change Indicator")
        print("-" * 70)
        for row in results:
            print(f"{row[0]:<7} | {row[1]} | {row[2]:>5} | {row[3]:>14} | {row[4]:>19}")
        
        # Check for any remaining NULL values
        cursor.execute("""
            SELECT COUNT(*) 
            FROM properties 
            WHERE (change_date IS NULL OR change_indicator IS NULL OR change_indicator = '')
        """)
        remaining_nulls = cursor.fetchone()[0]
        
        if remaining_nulls > 0:
            print(f"\nWarning: {remaining_nulls:,} records still have NULL change_date or change_indicator")
            
            # Show sample of remaining NULLs
            cursor.execute("""
                SELECT dataset_type, file_month, COUNT(*)
                FROM properties 
                WHERE (change_date IS NULL OR change_indicator IS NULL OR change_indicator = '')
                GROUP BY dataset_type, file_month
                ORDER BY COUNT(*) DESC
                LIMIT 10
            """)
            print("\nRemaining NULL records by file:")
            for row in cursor.fetchall():
                print(f"  {row[0]} - {row[1]}: {row[2]:,} records")
        
        conn.commit()
        print("\nFix completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    fix_full_files()