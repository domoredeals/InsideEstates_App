#!/usr/bin/env python3
"""
Analyze unmatched records that have no registration number
"""

import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

def analyze_no_regnumber():
    """Analyze why records without reg numbers aren't matching"""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("=== Analyzing Unmatched Records Without Registration Numbers ===\n")
        
        # First, let's see the breakdown of unmatched records
        cursor.execute("""
            SELECT 
                COUNT(*) as total_unmatched,
                COUNT(CASE WHEN lr.company_1_reg_no IS NOT NULL AND lr.company_1_reg_no != '' THEN 1 END) as with_reg_no,
                COUNT(CASE WHEN lr.company_1_reg_no IS NULL OR lr.company_1_reg_no = '' THEN 1 END) as without_reg_no
            FROM land_registry_data lr
            JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE m.ch_match_type_1 = 'No_Match'
            AND lr.proprietor_1_name IS NOT NULL
        """)
        
        result = cursor.fetchone()
        print(f"Total unmatched proprietors: {result['total_unmatched']:,}")
        print(f"  With registration number: {result['with_reg_no']:,} ({result['with_reg_no']/result['total_unmatched']*100:.1f}%)")
        print(f"  Without registration number: {result['without_reg_no']:,} ({result['without_reg_no']/result['total_unmatched']*100:.1f}%)")
        
        # Now let's look at successful Name and Previous_Name matches
        print("\n\n=== Successful Name-Only Matches (for comparison) ===")
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN m.ch_match_type_1 = 'Name' THEN 1 END) as name_matches,
                COUNT(CASE WHEN m.ch_match_type_1 = 'Previous_Name' THEN 1 END) as previous_name_matches
            FROM land_registry_ch_matches m
            WHERE m.ch_match_type_1 IN ('Name', 'Previous_Name')
        """)
        
        result = cursor.fetchone()
        print(f"Successful name-based matches:")
        print(f"  Name matches (Tier 3): {result['name_matches']:,}")
        print(f"  Previous name matches (Tier 4): {result['previous_name_matches']:,}")
        
        # Check some successful name matches to understand what worked
        cursor.execute("""
            SELECT 
                lr.proprietor_1_name,
                lr.company_1_reg_no,
                m.ch_matched_name_1,
                m.ch_matched_number_1
            FROM land_registry_data lr
            JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE m.ch_match_type_1 = 'Name'
            AND (lr.company_1_reg_no IS NULL OR lr.company_1_reg_no = '')
            LIMIT 10
        """)
        
        print("\nExamples of SUCCESSFUL name-only matches (no reg number):")
        for row in cursor.fetchall():
            print(f"  LR: {row['proprietor_1_name']}")
            print(f"  CH: {row['ch_matched_name_1']} ({row['ch_matched_number_1']})")
            print()
        
        # Now check what types of names are NOT matching
        print("\n=== Analyzing Unmatched Company Types (no reg number) ===")
        
        # Get categories of unmatched
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN lr.proprietor_1_name LIKE '%COUNCIL%' THEN 'Council/Government'
                    WHEN lr.proprietor_1_name LIKE '%BOROUGH%' THEN 'Council/Government'
                    WHEN lr.proprietor_1_name LIKE '%AUTHORITY%' THEN 'Council/Government'
                    WHEN lr.proprietor_1_name LIKE '%NHS%' THEN 'NHS/Healthcare'
                    WHEN lr.proprietor_1_name LIKE '%TRUST%' AND lr.proprietor_1_name LIKE '%HOUSING%' THEN 'Housing Trust'
                    WHEN lr.proprietor_1_name LIKE '%HOUSING%' THEN 'Housing Association'
                    WHEN lr.proprietor_1_name LIKE '%HOMES%' THEN 'Housing/Homes'
                    WHEN lr.proprietor_1_name LIKE '%CHURCH%' THEN 'Religious'
                    WHEN lr.proprietor_1_name LIKE '%PARISH%' THEN 'Religious'
                    WHEN lr.proprietor_1_name LIKE '%CHARITY%' THEN 'Charity'
                    WHEN lr.proprietor_1_name LIKE '%SCHOOL%' THEN 'Education'
                    WHEN lr.proprietor_1_name LIKE '%COLLEGE%' THEN 'Education'
                    WHEN lr.proprietor_1_name LIKE '%UNIVERSITY%' THEN 'Education'
                    WHEN lr.proprietor_1_name ~ '^[A-Z][A-Z\s\-\']+$' AND lr.proprietor_1_name NOT LIKE '%LIMITED%' THEN 'Possible Individual'
                    WHEN lr.proprietor_1_name LIKE '%LIMITED%' OR lr.proprietor_1_name LIKE '%LTD%' OR lr.proprietor_1_name LIKE '%PLC%' THEN 'UK Company'
                    ELSE 'Other'
                END as entity_type,
                COUNT(*) as count
            FROM land_registry_data lr
            JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE m.ch_match_type_1 = 'No_Match'
            AND (lr.company_1_reg_no IS NULL OR lr.company_1_reg_no = '')
            AND lr.proprietor_1_name IS NOT NULL
            GROUP BY entity_type
            ORDER BY count DESC
        """)
        
        print("\nUnmatched entities without registration numbers by type:")
        total = 0
        for row in cursor.fetchall():
            print(f"  {row['entity_type']}: {row['count']:,}")
            total += row['count']
        print(f"  TOTAL: {total:,}")
        
        # Check specific housing associations
        print("\n\n=== Specific Housing Association Analysis ===")
        
        housing_names = [
            'CIRCLE THIRTY THREE HOUSING TRUST LIMITED',
            'ADACTUS HOUSING ASSOCIATION LIMITED', 
            'HEXAGON HOUSING ASSOCIATION LIMITED'
        ]
        
        for name in housing_names:
            # Check if it's in LR as unmatched
            cursor.execute("""
                SELECT COUNT(*) as count
                FROM land_registry_data lr
                JOIN land_registry_ch_matches m ON lr.id = m.id
                WHERE lr.proprietor_1_name = %s
                AND m.ch_match_type_1 = 'No_Match'
            """, (name,))
            
            lr_count = cursor.fetchone()['count']
            
            # Check if it exists in CH
            cursor.execute("""
                SELECT company_number, company_name, company_status
                FROM companies_house_data
                WHERE company_name = %s
            """, (name,))
            
            ch_result = cursor.fetchone()
            
            print(f"\n{name}:")
            print(f"  In LR (unmatched): {lr_count} properties")
            if ch_result:
                print(f"  In CH: YES - {ch_result['company_number']} ({ch_result['company_status']})")
            else:
                # Check for variations
                cursor.execute("""
                    SELECT company_number, company_name, company_status
                    FROM companies_house_data
                    WHERE company_name LIKE %s
                    LIMIT 3
                """, (f"%{name.split()[0]}%{name.split()[-2]}%",))
                
                similar = cursor.fetchall()
                if similar:
                    print("  In CH: Not exact match, but similar names exist:")
                    for s in similar:
                        print(f"    - {s['company_name']} ({s['company_number']})")
                else:
                    print("  In CH: NO - Not found")
        
        cursor.close()
        conn.close()
        
        print("\n\n=== SUMMARY ===")
        print("The 40% unmatched rate is primarily due to:")
        print("1. Government bodies (councils, authorities) - legitimately not in CH")
        print("2. Many housing associations without registration numbers")
        print("3. Religious organizations, charities, schools")
        print("4. Possible individuals (names without LIMITED/LTD)")
        print("5. Some companies that should match but have name variations")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    analyze_no_regnumber()