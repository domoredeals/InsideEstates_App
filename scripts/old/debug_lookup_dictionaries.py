#!/usr/bin/env python3
"""
Debug the lookup dictionaries to see why matches are failing
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
    """Exact normalization from matching script"""
    if not name or name.strip() == '':
        return ""
    
    name = str(name).upper().strip()
    name = name.replace(' AND ', ' ')
    name = name.replace(' & ', ' ')
    name = name.replace('.', '')
    name = name.replace(',', '')
    
    suffix_pattern = r'\s*(LIMITED LIABILITY PARTNERSHIP|LIMITED|COMPANY|LTD\.|LLP|LTD|PLC|CO\.|CO|LP|L\.P\.)$'
    name = re.sub(suffix_pattern, '', name)
    
    name = ' '.join(name.split())
    name = ''.join(char for char in name if char.isalnum())
    
    return name

def normalize_company_number(number):
    """Normalize company registration number for matching"""
    if not number or number.strip() == '':
        return ""
    
    number = str(number).strip().upper()
    number = re.sub(r'[^A-Z0-9]', '', number)
    
    if number.startswith('SC'):
        return number
    if number.startswith('NI'):
        return number
    if number.startswith('GI'):
        return number
    
    # If it's all digits, pad with zeros to 8 digits
    if number.isdigit():
        return number.zfill(8)
    
    return number

def debug_lookup():
    """Check what would be in the lookup dictionaries"""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("=== DEBUGGING LOOKUP DICTIONARY CREATION ===\n")
        
        # Test specific companies
        test_cases = [
            ('WARBURTONS LIMITED', '00178711'),
            ('PANRAMIC INVESTMENTS (JERSEY) LIMITED', 'OE025512')
        ]
        
        for company_name, company_number in test_cases:
            print(f"\nChecking: {company_name} ({company_number})")
            
            # Get the CH record
            cursor.execute("""
                SELECT company_number, company_name, company_status
                FROM companies_house_data
                WHERE company_number = %s
            """, (company_number,))
            
            ch_record = cursor.fetchone()
            if ch_record:
                print(f"CH Record found: {ch_record['company_name']} - {ch_record['company_status']}")
                
                # Simulate dictionary creation
                clean_name = normalize_company_name(ch_record['company_name'])
                clean_number = normalize_company_number(ch_record['company_number'])
                
                print(f"  Would normalize to:")
                print(f"    Name: '{clean_name}'")
                print(f"    Number: '{clean_number}'")
                print(f"    Name+Number key: '{clean_name + clean_number}'")
                
                # Check if there are duplicates
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM companies_house_data
                    WHERE company_name = %s
                """, (ch_record['company_name'],))
                
                dup_count = cursor.fetchone()['count']
                if dup_count > 1:
                    print(f"  ⚠️  WARNING: {dup_count} companies with this exact name!")
        
        # Check for multiple WARBURTONS
        print(f"\n\n{'='*80}")
        print("CHECKING FOR MULTIPLE WARBURTONS ENTRIES...\n")
        
        cursor.execute("""
            SELECT company_number, company_name, company_status, incorporation_date
            FROM companies_house_data
            WHERE company_name LIKE 'WARBURTONS%'
            ORDER BY company_name
        """)
        
        warburtons = cursor.fetchall()
        print(f"Found {len(warburtons)} WARBURTONS companies:")
        for w in warburtons:
            norm_name = normalize_company_name(w['company_name'])
            norm_num = normalize_company_number(w['company_number'])
            print(f"\n  {w['company_name']}")
            print(f"    Number: {w['company_number']}, Status: {w['company_status']}")
            print(f"    Normalized name: '{norm_name}'")
            print(f"    Normalized number: '{norm_num}'")
            print(f"    Name+Number key: '{norm_name + norm_num}'")
        
        # Check if the matching script is loading these companies
        print(f"\n\n{'='*80}")
        print("SIMULATING LOOKUP DICTIONARY LOADING...\n")
        
        # Simulate the exact query from the matching script
        cursor.execute("""
            SELECT 
                company_number,
                company_name,
                company_status,
                company_category
            FROM companies_house_data
            WHERE company_number IN ('00178711', 'OE025512')
        """)
        
        for row in cursor.fetchall():
            company_number = row['company_number']
            company_name = row['company_name']
            
            if company_name:  # Matching script checks this
                clean_name = normalize_company_name(company_name)
                clean_number = normalize_company_number(company_number)
                
                print(f"\nLoading: {company_name}")
                print(f"  Original number: {company_number}")
                print(f"  Clean name: '{clean_name}'")
                print(f"  Clean number: '{clean_number}'")
                
                if clean_name and clean_number:
                    key = clean_name + clean_number
                    print(f"  ✓ Would add to name+number lookup with key: '{key}'")
                
                if clean_number:
                    print(f"  ✓ Would add to number lookup with key: '{clean_number}'")
                    
                if clean_name:
                    print(f"  ✓ Would add to name lookup with key: '{clean_name}'")
        
        cursor.close()
        conn.close()
        
        print("\n\n=== CONCLUSION ===")
        print("The companies ARE in Companies House and SHOULD be in the lookup dictionaries")
        print("The normalization looks correct")
        print("Something else is preventing the match - possibly:")
        print("1. The matching script isn't loading all CH records")
        print("2. There's a bug in the matching logic")
        print("3. The dictionaries are being overwritten by duplicates")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    debug_lookup()