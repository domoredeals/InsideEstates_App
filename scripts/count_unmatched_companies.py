#!/usr/bin/env python3
"""
Count how many company names don't match
"""

import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

def count_unmatched():
    """Count unmatched company names"""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("=== COUNTING UNMATCHED COMPANY NAMES ===\n")
        
        # Count total unmatched proprietors
        cursor.execute("""
            SELECT COUNT(*) as total_unmatched
            FROM land_registry_ch_matches
            WHERE ch_match_type_1 = 'No_Match'
        """)
        
        total_unmatched = cursor.fetchone()['total_unmatched']
        print(f"Total unmatched proprietors: {total_unmatched:,}")
        
        # Count unique unmatched company names
        cursor.execute("""
            SELECT COUNT(DISTINCT lr.proprietor_1_name) as unique_unmatched_names
            FROM land_registry_data lr
            JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE m.ch_match_type_1 = 'No_Match'
            AND lr.proprietor_1_name IS NOT NULL
        """)
        
        unique_names = cursor.fetchone()['unique_unmatched_names']
        print(f"Unique unmatched company names: {unique_names:,}")
        
        # Get top unmatched names by frequency
        cursor.execute("""
            SELECT 
                lr.proprietor_1_name,
                COUNT(*) as property_count,
                MAX(lr.proprietorship_1_category) as category
            FROM land_registry_data lr
            JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE m.ch_match_type_1 = 'No_Match'
            AND lr.proprietor_1_name IS NOT NULL
            GROUP BY lr.proprietor_1_name
            ORDER BY COUNT(*) DESC
            LIMIT 20
        """)
        
        print("\nTop 20 unmatched company names by property count:")
        print("-" * 100)
        for row in cursor.fetchall():
            print(f"{row['property_count']:5} properties | {row['proprietor_1_name'][:60]:<60} | {row['category'] or 'No category'}")
        
        # Break down by whether they have registration numbers
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN lr.company_1_reg_no IS NOT NULL AND lr.company_1_reg_no != '' THEN 'With Reg Number'
                    ELSE 'Without Reg Number'
                END as has_reg_no,
                COUNT(DISTINCT lr.proprietor_1_name) as unique_names,
                COUNT(*) as total_records
            FROM land_registry_data lr
            JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE m.ch_match_type_1 = 'No_Match'
            AND lr.proprietor_1_name IS NOT NULL
            GROUP BY has_reg_no
        """)
        
        print("\n\nBreakdown by registration number:")
        print("-" * 60)
        for row in cursor.fetchall():
            print(f"{row['has_reg_no']:<20} | {row['unique_names']:>10,} unique names | {row['total_records']:>10,} records")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    count_unmatched()