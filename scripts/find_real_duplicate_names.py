#!/usr/bin/env python3
"""
Find REAL examples of multiple companies with the same normalized name
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

try:
    conn = psycopg2.connect(**POSTGRESQL_CONFIG)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    print("=== REAL EXAMPLES OF DUPLICATE NORMALIZED NAMES ===\n")
    
    # Find companies that normalize to the same value
    cursor.execute("""
        WITH normalized_names AS (
            SELECT 
                company_name,
                company_number,
                company_status,
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
                ) as normalized
            FROM companies_house_data
            WHERE company_name IS NOT NULL
            AND company_name LIKE '%PROPERTIES%'
            AND company_status = 'Active'
        )
        SELECT 
            normalized,
            COUNT(*) as company_count,
            ARRAY_AGG(company_name || ' (' || company_number || ')' ORDER BY company_name) as companies
        FROM normalized_names
        WHERE normalized != ''
        AND LENGTH(normalized) > 10
        GROUP BY normalized
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC
        LIMIT 10
    """)
    
    print("Top normalized names with multiple active companies:\n")
    for row in cursor.fetchall():
        print(f"Normalized to: '{row['normalized']}'")
        print(f"Number of companies: {row['company_count']}")
        print("Companies:")
        for company in row['companies'][:5]:  # Show first 5
            print(f"  - {company}")
        if len(row['companies']) > 5:
            print(f"  ... and {len(row['companies']) - 5} more")
        print()
    
    # Check a specific common case
    print("\n" + "="*80)
    print("SPECIFIC EXAMPLE: Companies normalizing to 'PROPERTIES'\n")
    
    cursor.execute("""
        SELECT company_name, company_number, company_status
        FROM companies_house_data
        WHERE company_name IN ('PROPERTIES LIMITED', 'PROPERTIES LTD', 'PROPERTIES')
        ORDER BY company_name
    """)
    
    properties_companies = cursor.fetchall()
    if properties_companies:
        print(f"Found {len(properties_companies)} companies:")
        for comp in properties_companies:
            normalized = normalize_company_name(comp['company_name'])
            print(f"\n{comp['company_name']} ({comp['company_number']})")
            print(f"  Status: {comp['company_status']}")
            print(f"  Normalizes to: '{normalized}'")
    
    # Show impact on Land Registry
    print("\n" + "="*80)
    print("IMPACT ON LAND REGISTRY MATCHING\n")
    
    # Pick one of the duplicate sets
    cursor.execute("""
        SELECT DISTINCT lr.proprietor_1_name, lr.company_1_reg_no
        FROM land_registry_data lr
        JOIN land_registry_ch_matches m ON lr.id = m.id
        WHERE m.ch_match_type_1 = 'No_Match'
        AND lr.proprietor_1_name IN (
            'A & K PROPERTIES LIMITED',
            'A. K. PROPERTIES LIMITED', 
            'AK PROPERTIES LIMITED',
            'AK&PROPERTIES LIMITED'
        )
        AND (lr.company_1_reg_no IS NULL OR lr.company_1_reg_no = '')
        LIMIT 10
    """)
    
    unmatched = cursor.fetchall()
    if unmatched:
        print("Examples of unmatched Land Registry records due to ambiguous names:")
        for rec in unmatched:
            print(f"\nLR Name: {rec['proprietor_1_name']}")
            print(f"LR Reg No: {rec['company_1_reg_no'] or 'None'}")
            print("Cannot match because multiple companies normalize to same value!")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()