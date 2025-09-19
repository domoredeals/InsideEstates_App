#!/usr/bin/env python3
"""
Test script to verify the Land Registry to Companies House matching
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

def test_matching():
    """Run tests to verify matching functionality"""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("=== Land Registry to Companies House Matching Test ===\n")
        
        # Test 1: Check if CH columns exist
        print("1. Checking if CH match columns exist...")
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'land_registry_data' 
            AND column_name LIKE 'ch_match%'
            ORDER BY column_name
        """)
        columns = cursor.fetchall()
        if columns:
            print(f"✓ Found {len(columns)} CH match columns")
            for col in columns[:5]:  # Show first 5
                print(f"  - {col['column_name']}")
        else:
            print("✗ CH match columns not found. Run add_ch_match_columns.sql first.")
            return
        
        # Test 2: Check sample of unmatched records
        print("\n2. Checking sample of records to match...")
        cursor.execute("""
            SELECT 
                proprietor_1_name,
                company_1_reg_no,
                proprietor_2_name,
                company_2_reg_no
            FROM land_registry_data
            WHERE ch_match_date IS NULL
            AND (proprietor_1_name IS NOT NULL OR proprietor_2_name IS NOT NULL)
            LIMIT 5
        """)
        unmatched = cursor.fetchall()
        print(f"✓ Found {len(unmatched)} unmatched records to test")
        for i, record in enumerate(unmatched, 1):
            print(f"  {i}. {record['proprietor_1_name']} ({record['company_1_reg_no']})")
        
        # Test 3: Check Companies House data
        print("\n3. Checking Companies House data...")
        cursor.execute("SELECT COUNT(*) as count FROM companies_house_data")
        ch_count = cursor.fetchone()
        print(f"✓ Companies House table has {ch_count['count']:,} records")
        
        # Test 4: Show sample of CH data
        cursor.execute("""
            SELECT company_number, company_name, company_status
            FROM companies_house_data
            LIMIT 5
        """)
        ch_sample = cursor.fetchall()
        print("  Sample companies:")
        for company in ch_sample:
            print(f"  - {company['company_number']}: {company['company_name']} ({company['company_status']})")
        
        # Test 5: Run matching on small sample
        print("\n4. Running test match on 100 records...")
        print("   Execute: python match_lr_to_ch.py --test 100")
        
        # Test 6: Check if any matches already exist
        cursor.execute("""
            SELECT 
                COUNT(*) as total_matched,
                COUNT(CASE WHEN ch_match_type_1 = 'Name+Number' THEN 1 END) as tier1,
                COUNT(CASE WHEN ch_match_type_1 = 'Number' THEN 1 END) as tier2,
                COUNT(CASE WHEN ch_match_type_1 = 'Name' THEN 1 END) as tier3,
                COUNT(CASE WHEN ch_match_type_1 = 'Previous_Name' THEN 1 END) as tier4,
                COUNT(CASE WHEN ch_match_type_1 = 'No_Match' THEN 1 END) as no_match
            FROM land_registry_data
            WHERE ch_match_date IS NOT NULL
        """)
        stats = cursor.fetchone()
        
        if stats['total_matched'] > 0:
            print(f"\n5. Existing match statistics:")
            print(f"   Total matched records: {stats['total_matched']:,}")
            print(f"   - Tier 1 (Name+Number): {stats['tier1']:,}")
            print(f"   - Tier 2 (Number only): {stats['tier2']:,}")
            print(f"   - Tier 3 (Name only): {stats['tier3']:,}")
            print(f"   - Tier 4 (Previous name): {stats['tier4']:,}")
            print(f"   - No matches: {stats['no_match']:,}")
            
            # Show sample of matched records
            cursor.execute("""
                SELECT 
                    proprietor_1_name,
                    company_1_reg_no,
                    ch_matched_name_1,
                    ch_matched_number_1,
                    ch_match_type_1,
                    ch_match_confidence_1
                FROM land_registry_data
                WHERE ch_match_type_1 IS NOT NULL
                AND ch_match_type_1 != 'No_Match'
                LIMIT 3
            """)
            matches = cursor.fetchall()
            print("\n   Sample matches:")
            for match in matches:
                print(f"   - LR: {match['proprietor_1_name']} ({match['company_1_reg_no']})")
                print(f"     CH: {match['ch_matched_name_1']} ({match['ch_matched_number_1']})")
                print(f"     Type: {match['ch_match_type_1']} (confidence: {match['ch_match_confidence_1']})")
        
        print("\n=== Test Complete ===")
        print("\nNext steps:")
        print("1. Run SQL to add columns: psql -f add_ch_match_columns.sql")
        print("2. Test matching: python match_lr_to_ch.py --test 1000")
        print("3. Run full matching: python match_lr_to_ch.py")
        print("4. Create views: psql -f create_ch_matched_view.sql")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    test_matching()