#!/usr/bin/env python3
"""
Check the results of the Companies House matching
"""

import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

def check_results():
    """Check matching results"""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("=== Companies House Matching Results ===\n")
        
        # Check how many records were matched
        cursor.execute("""
            SELECT COUNT(*) as total_matched_records
            FROM land_registry_ch_matches
        """)
        result = cursor.fetchone()
        print(f"Total records with matches: {result['total_matched_records']:,}")
        
        # Check match statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN ch_match_type_1 = 'Name+Number' THEN 1 END) as tier1,
                COUNT(CASE WHEN ch_match_type_1 = 'Number' THEN 1 END) as tier2,
                COUNT(CASE WHEN ch_match_type_1 = 'Name' THEN 1 END) as tier3,
                COUNT(CASE WHEN ch_match_type_1 = 'Previous_Name' THEN 1 END) as tier4,
                COUNT(CASE WHEN ch_match_type_1 = 'No_Match' THEN 1 END) as no_match
            FROM land_registry_ch_matches
        """)
        stats = cursor.fetchone()
        
        print(f"\nProprietor 1 Match Statistics:")
        print(f"  Tier 1 (Name+Number): {stats['tier1']:,}")
        print(f"  Tier 2 (Number only): {stats['tier2']:,}")
        print(f"  Tier 3 (Name only): {stats['tier3']:,}")
        print(f"  Tier 4 (Previous name): {stats['tier4']:,}")
        print(f"  No matches: {stats['no_match']:,}")
        
        # Show sample matches
        print("\n=== Sample Matches ===")
        
        # Show a Name+Number match
        cursor.execute("""
            SELECT 
                lr.proprietor_1_name,
                lr.company_1_reg_no,
                m.ch_matched_name_1,
                m.ch_matched_number_1,
                m.ch_match_type_1,
                m.ch_match_confidence_1
            FROM land_registry_ch_matches m
            JOIN land_registry_data lr ON m.id = lr.id
            WHERE m.ch_match_type_1 = 'Name+Number'
            LIMIT 3
        """)
        
        print("\n1. Name+Number Matches (Highest Confidence):")
        for row in cursor.fetchall():
            print(f"   LR Name: {row['proprietor_1_name']}")
            print(f"   LR Number: {row['company_1_reg_no']}")
            print(f"   CH Name: {row['ch_matched_name_1']}")
            print(f"   CH Number: {row['ch_matched_number_1']}")
            print(f"   Confidence: {row['ch_match_confidence_1']}")
            print()
        
        # Show a Previous Name match
        cursor.execute("""
            SELECT 
                lr.proprietor_1_name,
                lr.company_1_reg_no,
                m.ch_matched_name_1,
                m.ch_matched_number_1,
                m.ch_match_type_1,
                m.ch_match_confidence_1
            FROM land_registry_ch_matches m
            JOIN land_registry_data lr ON m.id = lr.id
            WHERE m.ch_match_type_1 = 'Previous_Name'
            LIMIT 2
        """)
        
        print("2. Previous Name Matches (Company name changed):")
        for row in cursor.fetchall():
            print(f"   LR Name: {row['proprietor_1_name']}")
            print(f"   LR Number: {row['company_1_reg_no'] or 'None'}")
            print(f"   CH Current Name: {row['ch_matched_name_1']}")
            print(f"   CH Number: {row['ch_matched_number_1']}")
            print(f"   Confidence: {row['ch_match_confidence_1']}")
            print()
        
        # Check companies with multiple properties
        cursor.execute("""
            WITH company_properties AS (
                SELECT ch_matched_number_1 as company_number, ch_matched_name_1 as company_name
                FROM land_registry_ch_matches
                WHERE ch_matched_number_1 IS NOT NULL
                UNION ALL
                SELECT ch_matched_number_2, ch_matched_name_2
                FROM land_registry_ch_matches
                WHERE ch_matched_number_2 IS NOT NULL
                UNION ALL
                SELECT ch_matched_number_3, ch_matched_name_3
                FROM land_registry_ch_matches
                WHERE ch_matched_number_3 IS NOT NULL
                UNION ALL
                SELECT ch_matched_number_4, ch_matched_name_4
                FROM land_registry_ch_matches
                WHERE ch_matched_number_4 IS NOT NULL
            )
            SELECT 
                company_number,
                MAX(company_name) as company_name,
                COUNT(*) as property_count
            FROM company_properties
            GROUP BY company_number
            HAVING COUNT(*) > 1
            ORDER BY property_count DESC
            LIMIT 5
        """)
        
        print("3. Companies with Multiple Properties:")
        for row in cursor.fetchall():
            print(f"   {row['company_name']} ({row['company_number']}): {row['property_count']} properties")
        
        # Test the view
        cursor.execute("""
            SELECT COUNT(*) as view_count
            FROM v_land_registry_with_ch
            WHERE ch_matched_number_1 IS NOT NULL
        """)
        view_result = cursor.fetchone()
        print(f"\n✓ View v_land_registry_with_ch is working")
        print(f"  Records with CH matches: {view_result['view_count']:,}")
        
        cursor.close()
        conn.close()
        
        print("\n=== Next Steps ===")
        print("1. Run the full matching (this was just a 1,000 record test):")
        print("   python scripts/match_lr_to_ch_separate_table.py")
        print("\n2. Query matched data:")
        print("   SELECT * FROM v_land_registry_with_ch WHERE ch_matched_number_1 = 'YOUR_COMPANY_NUMBER'")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    check_results()