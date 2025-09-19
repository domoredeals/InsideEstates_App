#!/usr/bin/env python3
"""
Test the matching logic to understand why some names aren't matching
"""

import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import re

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

def normalize_company_name(name):
    """Normalize company name for matching - same as in matching script"""
    if not name or name.strip() == '':
        return ""
    
    name = str(name).upper().strip()
    
    # Replace common variations
    name = name.replace(' AND ', ' ')
    name = name.replace(' & ', ' ')
    name = name.replace('.', '')
    name = name.replace(',', '')
    
    # Remove common suffixes
    suffix_pattern = r'\s*(LIMITED LIABILITY PARTNERSHIP|LIMITED|COMPANY|LTD\.|LLP|LTD|PLC|CO\.|CO|LP|L\.P\.)$'
    name = re.sub(suffix_pattern, '', name)
    
    # Remove extra spaces and keep only alphanumeric
    name = ' '.join(name.split())
    name = ''.join(char for char in name if char.isalnum())
    
    return name

def test_matching_logic():
    """Test specific cases to understand matching failures"""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("=== Testing Matching Logic ===\n")
        
        # Test cases we know should match
        test_cases = [
            {
                'lr_name': 'TENSATOR LIMITED',
                'lr_number': '',
                'ch_number': '04046724'
            },
            {
                'lr_name': 'CIRCLE THIRTY THREE HOUSING TRUST LIMITED',
                'lr_number': '',
                'ch_number': None  # Unknown
            },
            {
                'lr_name': 'MARCHES HOUSING ASSOCIATION LIMITED',  # Fixed typo
                'lr_number': '',
                'ch_number': None
            }
        ]
        
        print("1. Testing normalization on known cases...")
        for case in test_cases:
            lr_name = case['lr_name']
            normalized = normalize_company_name(lr_name)
            
            print(f"\nLR Name: {lr_name}")
            print(f"Normalized: {normalized}")
            
            # Check if this normalized name exists in CH
            cursor.execute("""
                SELECT company_number, company_name, company_status
                FROM companies_house_data
                WHERE REGEXP_REPLACE(UPPER(company_name), '[^A-Z0-9]', '', 'g') = %s
            """, (normalized,))
            
            exact_match = cursor.fetchone()
            if exact_match:
                print(f"✓ EXACT MATCH FOUND: {exact_match['company_name']} ({exact_match['company_number']})")
            else:
                print("✗ No exact match")
                
                # Try to find similar names
                cursor.execute("""
                    SELECT company_number, company_name, company_status,
                           REGEXP_REPLACE(UPPER(company_name), '[^A-Z0-9]', '', 'g') as normalized_ch_name
                    FROM companies_house_data
                    WHERE company_name ILIKE %s
                    LIMIT 5
                """, (f'%{lr_name.split()[0]}%',))
                
                similar = cursor.fetchall()
                if similar:
                    print("  Similar names in CH:")
                    for s in similar:
                        print(f"    - {s['company_name']} ({s['company_number']})")
                        print(f"      CH Normalized: {s['normalized_ch_name']}")
        
        # Check if the issue is with our normalization
        print("\n\n2. Checking Companies House normalization...")
        
        # Get some CH companies with 'LIMITED' to see how they normalize
        cursor.execute("""
            SELECT 
                company_name,
                company_number,
                REGEXP_REPLACE(UPPER(company_name), '[^A-Z0-9]', '', 'g') as normalized
            FROM companies_house_data
            WHERE company_name LIKE 'TENSATOR%'
            LIMIT 10
        """)
        
        print("\nTENSATOR companies in CH:")
        for row in cursor.fetchall():
            print(f"  Original: {row['company_name']}")
            print(f"  Normalized: {row['normalized']}")
            print(f"  Our normalize: {normalize_company_name(row['company_name'])}")
            print()
        
        # Check housing associations
        print("\n3. Checking major housing associations...")
        
        housing_names = [
            'CIRCLE THIRTY THREE HOUSING TRUST LIMITED',
            'CIRCLE ANGLIA LIMITED',
            'CIRCLE HOUSING',
            'CIRCLE 33 HOUSING TRUST',
            'ADACTUS HOUSING',
            'HEXAGON HOUSING ASSOCIATION'
        ]
        
        for name in housing_names:
            cursor.execute("""
                SELECT company_number, company_name, company_status
                FROM companies_house_data
                WHERE company_name ILIKE %s
                LIMIT 3
            """, (f'%{name}%',))
            
            results = cursor.fetchall()
            print(f"\nSearching for '{name}':")
            if results:
                for r in results:
                    print(f"  Found: {r['company_name']} ({r['company_number']}) - {r['company_status']}")
            else:
                print("  Not found")
        
        # Check if these are registered societies
        print("\n\n4. Checking if housing associations might be Registered Societies...")
        
        # Look for common housing association names with RS numbers
        cursor.execute("""
            SELECT 
                company_number,
                company_name,
                company_status
            FROM companies_house_data
            WHERE company_number LIKE 'RS%'
            AND (
                company_name LIKE '%HOUSING%' OR
                company_name LIKE '%HOMES%' OR
                company_name LIKE '%TRUST%'
            )
            LIMIT 20
        """)
        
        print("\nSample Registered Societies (RS numbers) in housing sector:")
        for row in cursor.fetchall():
            print(f"  {row['company_name']} ({row['company_number']})")
        
        # Check Industrial & Provident Societies
        cursor.execute("""
            SELECT 
                company_number,
                company_name,
                company_status
            FROM companies_house_data
            WHERE company_number LIKE 'IP%'
            AND (
                company_name LIKE '%HOUSING%' OR
                company_name LIKE '%HOMES%'
            )
            LIMIT 10
        """)
        
        print("\nSample Industrial & Provident Societies (IP numbers):")
        for row in cursor.fetchall():
            print(f"  {row['company_name']} ({row['company_number']})")
        
        # Test if the issue is the "CO" removal in normalization
        print("\n\n5. Testing if 'CO' removal is causing issues...")
        
        test_names = [
            'PATHTOP PROPERTY CO LIMITED',
            'PATHTOP PROPERTY COMPANY LIMITED',
            'HARRY TAYLOR & CO. LIMITED',
            'HARRY TAYLOR & COMPANY LIMITED'
        ]
        
        for name in test_names:
            normalized = normalize_company_name(name)
            print(f"\nOriginal: {name}")
            print(f"Normalized: {normalized}")
            
            # Check if removing CO causes collisions
            cursor.execute("""
                SELECT COUNT(*) as count
                FROM companies_house_data
                WHERE REGEXP_REPLACE(UPPER(company_name), '[^A-Z0-9]', '', 'g') = %s
            """, (normalized,))
            
            count = cursor.fetchone()['count']
            print(f"Matches in CH: {count}")
        
        cursor.close()
        conn.close()
        
        print("\n\n=== KEY INSIGHTS ===")
        print("1. Some companies exist in CH but aren't matching due to normalization issues")
        print("2. Many housing associations might be Registered Societies (RS) or Industrial & Provident (IP)")
        print("3. The 'CO' vs 'COMPANY' normalization might be too aggressive")
        print("4. Some organizations in LR might use trading names rather than registered names")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_matching_logic()