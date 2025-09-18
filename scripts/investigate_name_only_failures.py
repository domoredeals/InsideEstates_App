#!/usr/bin/env python3
"""
Investigate why records without registration numbers aren't matching on name
"""

import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import re
from collections import Counter

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

def investigate_name_failures():
    """Investigate why name-only matching isn't working"""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("=== Investigating Name-Only Match Failures ===\n")
        
        # Get unmatched records WITHOUT registration numbers but WITH company-like names
        print("1. Getting unmatched records with company names but no registration numbers...")
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
            AND (
                lr.proprietor_1_name LIKE '%LIMITED%' OR
                lr.proprietor_1_name LIKE '%LTD%' OR
                lr.proprietor_1_name LIKE '%PLC%' OR
                lr.proprietor_1_name LIKE '%LLP%'
            )
            LIMIT 1000
        """)
        
        unmatched_companies = cursor.fetchall()
        print(f"Found {len(unmatched_companies)} company names without registration numbers that didn't match\n")
        
        # Sample some names
        print("Sample unmatched company names:")
        for i, record in enumerate(unmatched_companies[:10]):
            print(f"{i+1}. {record['proprietor_1_name']}")
            normalized = normalize_company_name(record['proprietor_1_name'])
            print(f"   Normalized: {normalized}")
        
        # Let's check if these normalized names exist in CH
        print("\n\n2. Checking if these names exist in Companies House...")
        
        for record in unmatched_companies[:5]:
            lr_name = record['proprietor_1_name']
            normalized = normalize_company_name(lr_name)
            
            print(f"\nLR Name: {lr_name}")
            print(f"Normalized: {normalized}")
            
            # Try exact match on normalized name
            cursor.execute("""
                SELECT company_number, company_name, company_status
                FROM companies_house_data
                WHERE REPLACE(REPLACE(REPLACE(REPLACE(UPPER(company_name), ' AND ', ' '), ' & ', ' '), '.', ''), ',', '') 
                      SIMILAR TO %s || '%%'
                LIMIT 3
            """, (normalized[:20],))  # Use first 20 chars for partial match
            
            ch_results = cursor.fetchall()
            if ch_results:
                print("  Potential CH matches:")
                for ch in ch_results:
                    print(f"    - {ch['company_name']} ({ch['company_number']}) - {ch['company_status']}")
            else:
                print("  No similar names found in CH")
        
        # Analyze name patterns
        print("\n\n3. Analyzing naming patterns in unmatched records...")
        
        name_endings = Counter()
        name_patterns = {
            'housing_related': 0,
            'property_related': 0,
            'management_related': 0,
            'trust_related': 0,
            'group_related': 0,
            'holding_related': 0,
            'special_chars': 0,
            'very_long': 0,
            'inactive_words': 0
        }
        
        for record in unmatched_companies:
            name = record['proprietor_1_name'].upper()
            
            # Count endings
            if 'LIMITED' in name:
                name_endings['LIMITED'] += 1
            elif 'LTD' in name:
                name_endings['LTD'] += 1
            elif 'PLC' in name:
                name_endings['PLC'] += 1
            elif 'LLP' in name:
                name_endings['LLP'] += 1
            
            # Check patterns
            if any(word in name for word in ['HOUSING', 'HOMES', 'HOUSE']):
                name_patterns['housing_related'] += 1
            if any(word in name for word in ['PROPERTY', 'PROPERTIES', 'ESTATES']):
                name_patterns['property_related'] += 1
            if any(word in name for word in ['MANAGEMENT', 'MANAGING']):
                name_patterns['management_related'] += 1
            if any(word in name for word in ['TRUST', 'TRUSTEES']):
                name_patterns['trust_related'] += 1
            if any(word in name for word in ['GROUP', 'HOLDINGS']):
                name_patterns['group_related'] += 1
            if any(word in name for word in ['HOLDING', 'PARENT']):
                name_patterns['holding_related'] += 1
            if any(char in name for char in ['(', ')', '/', '-', '&']):
                name_patterns['special_chars'] += 1
            if len(name) > 60:
                name_patterns['very_long'] += 1
            if any(word in name for word in ['DISSOLVED', 'INACTIVE', 'FORMER', 'OLD']):
                name_patterns['inactive_words'] += 1
        
        print("\nName patterns in unmatched companies:")
        for pattern, count in sorted(name_patterns.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / len(unmatched_companies)) * 100
            print(f"  {pattern}: {count} ({percentage:.1f}%)")
        
        # Check specific housing associations
        print("\n\n4. Special focus on Housing Associations...")
        cursor.execute("""
            SELECT 
                lr.proprietor_1_name,
                COUNT(*) as property_count
            FROM land_registry_data lr
            JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE m.ch_match_type_1 = 'No_Match'
            AND (lr.company_1_reg_no IS NULL OR lr.company_1_reg_no = '')
            AND lr.proprietor_1_name LIKE '%HOUSING%'
            GROUP BY lr.proprietor_1_name
            ORDER BY property_count DESC
            LIMIT 10
        """)
        
        print("\nTop unmatched Housing organizations:")
        for row in cursor.fetchall():
            print(f"  {row['proprietor_1_name']}: {row['property_count']} properties")
        
        # Check typos or slight variations
        print("\n\n5. Checking for common variations that might prevent matching...")
        
        test_cases = [
            ('PEABODY', 'PEABODY TRUST'),
            ('NOTTING HILL', 'NOTTING HILL HOUSING'),
            ('SANCTUARY', 'SANCTUARY HOUSING'),
            ('PLACES FOR PEOPLE', 'PLACES FOR PEOPLE HOMES'),
        ]
        
        for base_name, full_name in test_cases:
            cursor.execute("""
                SELECT COUNT(*)
                FROM land_registry_data lr
                JOIN land_registry_ch_matches m ON lr.id = m.id
                WHERE m.ch_match_type_1 = 'No_Match'
                AND lr.proprietor_1_name LIKE %s
            """, (f'%{base_name}%',))
            
            unmatched_count = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT company_number, company_name
                FROM companies_house_data
                WHERE company_name LIKE %s
                LIMIT 5
            """, (f'%{base_name}%',))
            
            ch_matches = cursor.fetchall()
            
            print(f"\n'{base_name}':")
            print(f"  Unmatched in LR: {unmatched_count}")
            print(f"  In Companies House:")
            for ch in ch_matches:
                print(f"    - {ch['company_name']} ({ch['company_number']})")
        
        cursor.close()
        conn.close()
        
        print("\n\n=== KEY FINDINGS ===")
        print("Many unmatched 'companies' appear to be:")
        print("1. Housing Associations with complex/variable names")
        print("2. Names with special legal structures not in standard CH data")
        print("3. Trading names rather than registered company names")
        print("4. Historical company names no longer in use")
        print("5. Subsidiary or group company variations")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    investigate_name_failures()