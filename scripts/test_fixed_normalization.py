#!/usr/bin/env python3
"""
Test a fixed normalization that matches how Companies House normalizes names
"""

import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import re

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

def normalize_company_name_original(name):
    """Original normalization (BROKEN - removes suffixes)"""
    if not name or name.strip() == '':
        return ""
    
    name = str(name).upper().strip()
    name = name.replace(' AND ', ' ')
    name = name.replace(' & ', ' ')
    name = name.replace('.', '')
    name = name.replace(',', '')
    
    # PROBLEM: This removes LIMITED, LTD, etc!
    suffix_pattern = r'\s*(LIMITED LIABILITY PARTNERSHIP|LIMITED|COMPANY|LTD\.|LLP|LTD|PLC|CO\.|CO|LP|L\.P\.)$'
    name = re.sub(suffix_pattern, '', name)
    
    name = ' '.join(name.split())
    name = ''.join(char for char in name if char.isalnum())
    
    return name

def normalize_company_name_fixed(name):
    """Fixed normalization that matches CH approach"""
    if not name or name.strip() == '':
        return ""
    
    # Convert to uppercase and strip
    name = str(name).upper().strip()
    
    # Standardize common variations but DON'T remove them
    name = name.replace(' AND ', ' ')
    name = name.replace(' & ', ' ')
    name = name.replace('.', '')
    name = name.replace(',', '')
    name = name.replace('-', '')
    name = name.replace('(', '')
    name = name.replace(')', '')
    
    # Standardize LTD variations to LIMITED (but keep it!)
    name = re.sub(r'\bLTD\b', 'LIMITED', name)
    name = re.sub(r'\bCO\b', 'COMPANY', name)
    
    # Remove extra spaces
    name = ' '.join(name.split())
    
    # Remove all non-alphanumeric (spaces become nothing)
    name = ''.join(char for char in name if char.isalnum())
    
    return name

def test_fixed_normalization():
    """Test the fixed normalization"""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("=== Testing Fixed Normalization ===\n")
        
        # Test cases that should match
        test_cases = [
            'TENSATOR LIMITED',
            'TENSATOR LTD',
            'PATHTOP PROPERTY CO LIMITED',
            'HARRY TAYLOR & CO. LIMITED',
            'CIRCLE THIRTY THREE HOUSING TRUST LIMITED',
            'ADACTUS HOUSING ASSOCIATION LIMITED',
            'MARCHES HOUSING ASSOCIATION LIMITED'
        ]
        
        print("Comparing original vs fixed normalization:\n")
        
        for name in test_cases:
            original_norm = normalize_company_name_original(name)
            fixed_norm = normalize_company_name_fixed(name)
            
            print(f"LR Name: {name}")
            print(f"  Original norm: {original_norm}")
            print(f"  Fixed norm: {fixed_norm}")
            
            # Check if fixed normalization finds matches
            cursor.execute("""
                SELECT company_number, company_name, company_status
                FROM companies_house_data
                WHERE REGEXP_REPLACE(UPPER(company_name), '[^A-Z0-9]', '', 'g') = %s
                LIMIT 1
            """, (fixed_norm,))
            
            match = cursor.fetchone()
            if match:
                print(f"  ✓ MATCH FOUND: {match['company_name']} ({match['company_number']})")
            else:
                # Try with the original normalization for comparison
                cursor.execute("""
                    SELECT company_number, company_name, company_status
                    FROM companies_house_data
                    WHERE REGEXP_REPLACE(UPPER(company_name), '[^A-Z0-9]', '', 'g') = %s
                    LIMIT 1
                """, (original_norm,))
                
                original_match = cursor.fetchone()
                if original_match:
                    print(f"  ✗ Only matched with original norm (shouldn't happen)")
                else:
                    print(f"  ✗ No match with either normalization")
            
            print()
        
        # Test how many more matches we'd get with fixed normalization
        print("\n=== Estimating Impact of Fix ===\n")
        
        # Get a sample of unmatched companies
        cursor.execute("""
            SELECT DISTINCT lr.proprietor_1_name
            FROM land_registry_data lr
            JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE m.ch_match_type_1 = 'No_Match'
            AND lr.proprietor_1_name IS NOT NULL
            AND (
                lr.proprietor_1_name LIKE '%LIMITED%' OR
                lr.proprietor_1_name LIKE '%LTD%' OR
                lr.proprietor_1_name LIKE '%PLC%'
            )
            LIMIT 100
        """)
        
        unmatched_companies = [row['proprietor_1_name'] for row in cursor.fetchall()]
        
        matches_with_original = 0
        matches_with_fixed = 0
        new_matches = []
        
        for company in unmatched_companies:
            original_norm = normalize_company_name_original(company)
            fixed_norm = normalize_company_name_fixed(company)
            
            # Check original
            cursor.execute("""
                SELECT company_number
                FROM companies_house_data
                WHERE REGEXP_REPLACE(UPPER(company_name), '[^A-Z0-9]', '', 'g') = %s
                LIMIT 1
            """, (original_norm,))
            
            if cursor.fetchone():
                matches_with_original += 1
            
            # Check fixed
            cursor.execute("""
                SELECT company_number, company_name
                FROM companies_house_data
                WHERE REGEXP_REPLACE(UPPER(company_name), '[^A-Z0-9]', '', 'g') = %s
                LIMIT 1
            """, (fixed_norm,))
            
            result = cursor.fetchone()
            if result:
                matches_with_fixed += 1
                if len(new_matches) < 10:
                    new_matches.append((company, result['company_name'], result['company_number']))
        
        print(f"Sample of 100 unmatched companies:")
        print(f"  Matches with original normalization: {matches_with_original}")
        print(f"  Matches with fixed normalization: {matches_with_fixed}")
        print(f"  Additional matches gained: {matches_with_fixed - matches_with_original}")
        
        if new_matches:
            print("\nExamples of new matches with fixed normalization:")
            for lr_name, ch_name, ch_number in new_matches[:5]:
                print(f"  LR: {lr_name}")
                print(f"  CH: {ch_name} ({ch_number})")
                print()
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_fixed_normalization()