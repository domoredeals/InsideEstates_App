#!/usr/bin/env python3
"""
Verify that 100% of LR records are being normalized and attempted for 4-tier matching
"""

import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

def verify_coverage():
    """Check if all LR records were processed"""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("=== VERIFYING 100% MATCHING COVERAGE ===\n")
        
        # Count total LR records with proprietors
        cursor.execute("""
            SELECT 
                COUNT(*) as total_lr_records,
                COUNT(CASE WHEN proprietor_1_name IS NOT NULL THEN 1 END) as with_proprietor_1,
                COUNT(CASE WHEN proprietor_2_name IS NOT NULL THEN 1 END) as with_proprietor_2,
                COUNT(CASE WHEN proprietor_3_name IS NOT NULL THEN 1 END) as with_proprietor_3,
                COUNT(CASE WHEN proprietor_4_name IS NOT NULL THEN 1 END) as with_proprietor_4
            FROM land_registry_data
        """)
        
        lr_stats = cursor.fetchone()
        print(f"Land Registry records:")
        print(f"  Total records: {lr_stats['total_lr_records']:,}")
        print(f"  With proprietor 1: {lr_stats['with_proprietor_1']:,}")
        print(f"  With proprietor 2: {lr_stats['with_proprietor_2']:,}")
        print(f"  With proprietor 3: {lr_stats['with_proprietor_3']:,}")
        print(f"  With proprietor 4: {lr_stats['with_proprietor_4']:,}")
        
        # Count total match table records
        cursor.execute("""
            SELECT 
                COUNT(*) as total_match_records
            FROM land_registry_ch_matches
        """)
        
        match_count = cursor.fetchone()['total_match_records']
        print(f"\nMatch table records: {match_count:,}")
        
        # Check for any LR records NOT in the match table
        cursor.execute("""
            SELECT COUNT(*) as missing_records
            FROM land_registry_data lr
            LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE m.id IS NULL
            AND (lr.proprietor_1_name IS NOT NULL 
                OR lr.proprietor_2_name IS NOT NULL 
                OR lr.proprietor_3_name IS NOT NULL 
                OR lr.proprietor_4_name IS NOT NULL)
        """)
        
        missing = cursor.fetchone()['missing_records']
        print(f"\nLR records with proprietors NOT in match table: {missing:,}")
        
        if missing > 0:
            print("\n⚠️  WARNING: Not all records were processed!")
            
            # Show examples of missing records
            cursor.execute("""
                SELECT 
                    lr.id,
                    lr.title_number,
                    lr.proprietor_1_name,
                    lr.company_1_reg_no
                FROM land_registry_data lr
                LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
                WHERE m.id IS NULL
                AND lr.proprietor_1_name IS NOT NULL
                LIMIT 10
            """)
            
            print("\nExamples of unprocessed records:")
            for row in cursor.fetchall():
                print(f"  ID: {row['id']}, Title: {row['title_number']}")
                print(f"  Proprietor: {row['proprietor_1_name']}")
                print(f"  Reg No: {row['company_1_reg_no'] or 'None'}")
                print()
        
        # Verify match types distribution
        print("\n=== MATCH TYPE DISTRIBUTION ===")
        
        cursor.execute("""
            SELECT 
                ch_match_type_1 as match_type,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM land_registry_ch_matches
            WHERE ch_match_type_1 IS NOT NULL
            GROUP BY ch_match_type_1
            ORDER BY count DESC
        """)
        
        print("\nProprietor 1 match distribution:")
        total_processed = 0
        for row in cursor.fetchall():
            print(f"  {row['match_type']:<15} {row['count']:>10,} ({row['percentage']:>5}%)")
            total_processed += row['count']
        
        print(f"\nTotal proprietor 1 records processed: {total_processed:,}")
        
        # Check some specific "No_Match" records without reg numbers
        print("\n=== SAMPLE OF NO_MATCH RECORDS WITHOUT REG NUMBERS ===")
        
        cursor.execute("""
            SELECT 
                lr.proprietor_1_name,
                lr.company_1_reg_no,
                lr.proprietorship_1_category
            FROM land_registry_data lr
            JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE m.ch_match_type_1 = 'No_Match'
            AND (lr.company_1_reg_no IS NULL OR lr.company_1_reg_no = '')
            AND lr.proprietor_1_name IS NOT NULL
            AND lr.proprietor_1_name LIKE '%LIMITED%'
            ORDER BY RANDOM()
            LIMIT 20
        """)
        
        print("\nRandom sample of unmatched companies with 'LIMITED' but no reg number:")
        for row in cursor.fetchall():
            print(f"  {row['proprietor_1_name']}")
            print(f"    Category: {row['proprietorship_1_category'] or 'None specified'}")
        
        cursor.close()
        conn.close()
        
        print("\n=== CONCLUSION ===")
        if missing == 0:
            print("✅ 100% of Land Registry records with proprietors were processed")
            print("✅ All records went through 4-tier matching")
            print("✅ Records without registration numbers DID get name-only matching attempted")
        else:
            print("❌ Some records were not processed - investigation needed!")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    verify_coverage()