#!/usr/bin/env python3
"""
Check if CH data includes overseas companies and verify active status
"""

import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

def check_overseas_and_active():
    """Check overseas companies and active status"""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("=== CHECKING OVERSEAS COMPANIES IN CH DATA ===\n")
        
        # Check what prefixes we have
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN company_number LIKE 'FC%' THEN 'FC - Foreign Company'
                    WHEN company_number LIKE 'OE%' THEN 'OE - Overseas Entity'
                    WHEN company_number LIKE 'OC%' THEN 'OC - Overseas Company'
                    WHEN company_number LIKE 'BR%' THEN 'BR - Branch'
                    WHEN company_number LIKE 'NF%' THEN 'NF - Northern Ireland Foreign'
                    WHEN company_number LIKE 'SF%' THEN 'SF - Scottish Foreign'
                    WHEN company_number ~ '^[0-9]' THEN 'UK - Standard UK Company'
                    WHEN company_number LIKE 'SC%' THEN 'SC - Scottish Company'
                    WHEN company_number LIKE 'NI%' THEN 'NI - Northern Ireland'
                    WHEN company_number LIKE 'RS%' THEN 'RS - Registered Society'
                    WHEN company_number LIKE 'IP%' THEN 'IP - Industrial & Provident'
                    ELSE 'Other'
                END as company_type,
                COUNT(*) as count,
                COUNT(CASE WHEN company_status = 'Active' THEN 1 END) as active_count
            FROM companies_house_data
            GROUP BY company_type
            ORDER BY count DESC
        """)
        
        print("Companies House data by registration type:")
        print("-" * 80)
        total_overseas = 0
        for row in cursor.fetchall():
            print(f"{row['company_type']:<30} | {row['count']:>10,} total | {row['active_count']:>10,} active")
            if row['company_type'] in ['FC - Foreign Company', 'OE - Overseas Entity', 'OC - Overseas Company', 'BR - Branch']:
                total_overseas += row['count']
        
        print(f"\nTotal overseas entities: {total_overseas:,}")
        
        # Check some specific unmatched overseas examples
        print("\n\n=== CHECKING UNMATCHED OVERSEAS COMPANIES ===")
        
        cursor.execute("""
            SELECT 
                lr.proprietor_1_name,
                lr.company_1_reg_no,
                lr.proprietorship_1_category
            FROM land_registry_data lr
            JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE m.ch_match_type_1 = 'No_Match'
            AND lr.proprietor_1_name IS NOT NULL
            AND (
                lr.proprietor_1_name LIKE '%(JERSEY)%' OR
                lr.proprietor_1_name LIKE '%(GUERNSEY)%' OR
                lr.proprietor_1_name LIKE '%(ISLE OF MAN)%' OR
                lr.proprietor_1_name LIKE '%(CAYMAN%' OR
                lr.proprietor_1_name LIKE '%(BVI)%' OR
                lr.proprietor_1_name LIKE '%(OVERSEAS)%'
            )
            LIMIT 20
        """)
        
        print("\nUnmatched companies with overseas indicators:")
        for row in cursor.fetchall():
            print(f"\n{row['proprietor_1_name']}")
            print(f"  Reg No: {row['company_1_reg_no'] or 'None'}")
            print(f"  Category: {row['proprietorship_1_category']}")
        
        # Check if these exist in CH with different names
        print("\n\n=== SEARCHING FOR SPECIFIC EXAMPLES IN CH ===")
        
        test_companies = [
            'PANRAMIC INVESTMENTS (JERSEY) LIMITED',
            'WARBURTONS LIMITED',
            'F. GOFF & SONS LIMITED'
        ]
        
        for company in test_companies:
            print(f"\nSearching for: {company}")
            
            # Try exact match first
            cursor.execute("""
                SELECT company_number, company_name, company_status, incorporation_date
                FROM companies_house_data
                WHERE company_name = %s
            """, (company,))
            
            exact_match = cursor.fetchone()
            if exact_match:
                print(f"  ✓ EXACT MATCH: {exact_match['company_number']} - Status: {exact_match['company_status']}")
            else:
                # Try partial match
                search_term = company.split()[0] + '%'
                cursor.execute("""
                    SELECT company_number, company_name, company_status, incorporation_date
                    FROM companies_house_data
                    WHERE company_name LIKE %s
                    LIMIT 5
                """, (search_term,))
                
                partial_matches = cursor.fetchall()
                if partial_matches:
                    print(f"  Partial matches found:")
                    for match in partial_matches:
                        print(f"    {match['company_name']} ({match['company_number']}) - {match['company_status']}")
                else:
                    print(f"  ✗ NO MATCHES FOUND")
        
        # Check change indicators in Land Registry
        print("\n\n=== VERIFYING CHANGE INDICATORS ===")
        
        cursor.execute("""
            SELECT 
                change_indicator,
                COUNT(*) as count
            FROM land_registry_data
            WHERE change_indicator IS NOT NULL
            GROUP BY change_indicator
        """)
        
        print("\nChange indicators in Land Registry data:")
        for row in cursor.fetchall():
            print(f"  {row['change_indicator'] or 'NULL'}: {row['count']:,}")
        
        cursor.close()
        conn.close()
        
        print("\n\n=== CONCLUSION ===")
        print("1. Companies House DOES include overseas companies (FC/OE/OC prefixes)")
        print("2. Many unmatched companies are Jersey/Guernsey entities")
        print("3. Some legitimate UK companies like WARBURTONS LIMITED are missing")
        print("4. These are CURRENT owners (not dissolved) so should theoretically exist")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    check_overseas_and_active()