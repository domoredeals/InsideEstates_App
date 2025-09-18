#!/usr/bin/env python3
"""
Debug why name matching isn't working when normalization is applied to both sides
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
    """Exact same normalization as in matching script"""
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

def debug_matching():
    """Debug specific unmatched cases"""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("=== Debugging Name Matching Logic ===\n")
        
        # Get specific unmatched companies
        test_companies = [
            'TENSATOR LIMITED',
            'CIRCLE THIRTY THREE HOUSING TRUST LIMITED',
            'ADACTUS HOUSING ASSOCIATION LIMITED',
            'HEXAGON HOUSING ASSOCIATION LIMITED'
        ]
        
        for lr_name in test_companies:
            print(f"\nTesting: {lr_name}")
            lr_normalized = normalize_company_name(lr_name)
            print(f"LR Normalized: '{lr_normalized}'")
            
            # Check if any CH company normalizes to the same value
            cursor.execute("""
                SELECT 
                    company_name,
                    company_number,
                    company_status
                FROM companies_house_data
                WHERE company_name ILIKE %s
            """, (f'%{lr_name.split()[0]}%',))
            
            candidates = cursor.fetchall()
            print(f"Found {len(candidates)} companies starting with '{lr_name.split()[0]}'")
            
            # Check normalization of each candidate
            for candidate in candidates[:5]:
                ch_name = candidate['company_name']
                ch_normalized = normalize_company_name(ch_name)
                
                print(f"  CH: {ch_name}")
                print(f"  CH Normalized: '{ch_normalized}'")
                print(f"  Match: {lr_normalized == ch_normalized}")
                
                if lr_normalized == ch_normalized:
                    print(f"  ✓ SHOULD HAVE MATCHED! Company number: {candidate['company_number']}")
        
        # Check what's in our matching dictionaries
        print("\n\n=== Checking Name-Only Dictionary ===")
        
        # Build a small lookup to test
        name_lookup = {}
        
        cursor.execute("""
            SELECT company_name, company_number
            FROM companies_house_data
            WHERE company_name LIKE 'TENSATOR%'
            OR company_name LIKE 'CIRCLE%HOUSING%'
            OR company_name LIKE 'ADACTUS%'
            OR company_name LIKE 'HEXAGON%'
            LIMIT 20
        """)
        
        for row in cursor.fetchall():
            normalized = normalize_company_name(row['company_name'])
            if normalized and normalized not in name_lookup:
                name_lookup[normalized] = {
                    'company_name': row['company_name'],
                    'company_number': row['company_number']
                }
        
        print(f"\nBuilt lookup with {len(name_lookup)} entries")
        print("Sample entries:")
        for key, value in list(name_lookup.items())[:5]:
            print(f"  Key: '{key}' -> {value['company_name']}")
        
        # Test lookup
        for lr_name in test_companies:
            lr_normalized = normalize_company_name(lr_name)
            if lr_normalized in name_lookup:
                print(f"\n✓ {lr_name} -> Found in lookup!")
            else:
                print(f"\n✗ {lr_name} -> NOT in lookup (key: '{lr_normalized}')")
        
        # Check if these are actually registered societies
        print("\n\n=== Checking if these might be special entity types ===")
        
        for name in ['CIRCLE THIRTY THREE', 'ADACTUS', 'HEXAGON']:
            cursor.execute("""
                SELECT company_number, company_name, company_status
                FROM companies_house_data
                WHERE company_name ILIKE %s
                AND (company_number LIKE 'RS%' OR company_number LIKE 'IP%')
                LIMIT 5
            """, (f'%{name}%',))
            
            special_entities = cursor.fetchall()
            if special_entities:
                print(f"\nSpecial entities matching '{name}':")
                for entity in special_entities:
                    print(f"  {entity['company_name']} ({entity['company_number']})")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    debug_matching()