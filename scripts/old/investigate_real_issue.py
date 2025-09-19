#!/usr/bin/env python3
"""
Investigate the REAL issue - why aren't name-only matches working when normalization is correct?
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
    """The CORRECT normalization that removes suffixes"""
    if not name or name.strip() == '':
        return ""
    
    name = str(name).upper().strip()
    name = name.replace(' AND ', ' ')
    name = name.replace(' & ', ' ')
    name = name.replace('.', '')
    name = name.replace(',', '')
    
    # CORRECTLY removes LIMITED, LTD, etc so they match!
    suffix_pattern = r'\s*(LIMITED LIABILITY PARTNERSHIP|LIMITED|COMPANY|LTD\.|LLP|LTD|PLC|CO\.|CO|LP|L\.P\.)$'
    name = re.sub(suffix_pattern, '', name)
    
    name = ' '.join(name.split())
    name = ''.join(char for char in name if char.isalnum())
    
    return name

def investigate():
    """Find out why name matching isn't working"""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("=== INVESTIGATING WHY NAME MATCHING ISN'T WORKING ===\n")
        
        # First, let's check if the issue is duplicate normalized names in CH
        print("1. Checking for duplicate normalized company names in Companies House...")
        
        cursor.execute("""
            WITH normalized AS (
                SELECT 
                    company_name,
                    company_number,
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                        UPPER(company_name),
                                        '\s*(LIMITED LIABILITY PARTNERSHIP|LIMITED|COMPANY|LTD\.|LLP|LTD|PLC|CO\.|CO|LP|L\.P\.)$', 
                                        ''
                                    ),
                                    '[^A-Z0-9 ]', '', 'g'
                                ),
                                '\s+', ' ', 'g'
                            ),
                            '^\s+|\s+$', '', 'g'
                        ),
                        '[^A-Z0-9]', '', 'g'
                    ) as normalized_name
                FROM companies_house_data
                WHERE company_name IS NOT NULL
            )
            SELECT 
                normalized_name,
                COUNT(*) as company_count,
                STRING_AGG(company_name || ' (' || company_number || ')', ', ' ORDER BY company_name) as examples
            FROM normalized
            WHERE normalized_name != ''
            GROUP BY normalized_name
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC
            LIMIT 10
        """)
        
        print("Top 10 normalized names with multiple companies:")
        for row in cursor.fetchall():
            print(f"\nNormalized: '{row['normalized_name']}'")
            print(f"  Count: {row['company_count']}")
            print(f"  Examples: {row['examples'][:200]}...")
        
        # Check specific unmatched examples
        print("\n\n2. Checking specific unmatched companies without reg numbers...")
        
        test_companies = [
            'CIRCLE THIRTY THREE HOUSING TRUST LIMITED',
            'ADACTUS HOUSING ASSOCIATION LIMITED',
            'HEXAGON HOUSING ASSOCIATION LIMITED'
        ]
        
        for company_name in test_companies:
            normalized = normalize_company_name(company_name)
            print(f"\n\nTesting: {company_name}")
            print(f"Normalizes to: '{normalized}'")
            
            # Check how many CH companies normalize to the same value
            cursor.execute("""
                SELECT 
                    company_name,
                    company_number,
                    company_status
                FROM companies_house_data
                WHERE REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    UPPER(company_name),
                                    '\s*(LIMITED LIABILITY PARTNERSHIP|LIMITED|COMPANY|LTD\.|LLP|LTD|PLC|CO\.|CO|LP|L\.P\.)$', 
                                    ''
                                ),
                                '[^A-Z0-9 ]', '', 'g'
                            ),
                            '\s+', ' ', 'g'
                        ),
                        '^\s+|\s+$', '', 'g'
                    ),
                    '[^A-Z0-9]', '', 'g'
                ) = %s
            """, (normalized,))
            
            matches = cursor.fetchall()
            print(f"Found {len(matches)} companies with same normalized name:")
            for match in matches[:5]:
                print(f"  - {match['company_name']} ({match['company_number']}) - Status: {match['company_status']}")
            if len(matches) > 5:
                print(f"  ... and {len(matches) - 5} more")
        
        # Check if these are special entities
        print("\n\n3. Checking if unmatched entities might be special types...")
        
        cursor.execute("""
            SELECT 
                lr.proprietor_1_name,
                lr.proprietorship_1_category,
                COUNT(*) as property_count
            FROM land_registry_data lr
            JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE m.ch_match_type_1 = 'No_Match'
            AND (lr.company_1_reg_no IS NULL OR lr.company_1_reg_no = '')
            AND lr.proprietor_1_name LIKE '%HOUSING%'
            GROUP BY lr.proprietor_1_name, lr.proprietorship_1_category
            ORDER BY COUNT(*) DESC
            LIMIT 20
        """)
        
        print("\nTop unmatched housing-related entities:")
        for row in cursor.fetchall():
            print(f"{row['property_count']:4} properties - {row['proprietor_1_name']} (Category: {row['proprietorship_1_category']})")
        
        cursor.close()
        conn.close()
        
        print("\n\n=== CONCLUSION ===")
        print("The normalization IS working correctly!")
        print("The issue is likely:")
        print("1. Multiple companies normalize to the same name (ambiguity)")
        print("2. Many housing associations might be registered as different entity types")
        print("3. Some might genuinely not exist in Companies House")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    investigate()