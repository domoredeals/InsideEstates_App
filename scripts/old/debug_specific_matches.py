#!/usr/bin/env python3
"""
Debug why specific companies that DO exist in CH didn't match
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

def debug_specific_matches():
    """Debug specific companies that should have matched"""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("=== DEBUGGING SPECIFIC MATCH FAILURES ===\n")
        
        # Companies we KNOW exist in CH
        test_cases = [
            ('WARBURTONS LIMITED', '00178711'),
            ('PANRAMIC INVESTMENTS (JERSEY) LIMITED', 'OE025512')
        ]
        
        for lr_name, ch_number in test_cases:
            print(f"\n{'='*80}")
            print(f"Testing: {lr_name}")
            print(f"Known CH number: {ch_number}")
            
            # Check if this company is in LR as unmatched
            cursor.execute("""
                SELECT 
                    lr.id,
                    lr.title_number,
                    lr.proprietor_1_name,
                    lr.company_1_reg_no,
                    m.ch_match_type_1,
                    m.ch_matched_number_1,
                    m.ch_matched_name_1
                FROM land_registry_data lr
                JOIN land_registry_ch_matches m ON lr.id = m.id
                WHERE lr.proprietor_1_name = %s
                LIMIT 5
            """, (lr_name,))
            
            lr_records = cursor.fetchall()
            print(f"\nFound {len(lr_records)} Land Registry records for this company")
            
            for rec in lr_records[:3]:
                print(f"\n  LR Record ID: {rec['id']}")
                print(f"  Title: {rec['title_number']}")
                print(f"  LR Reg No: {rec['company_1_reg_no'] or 'None'}")
                print(f"  Match Type: {rec['ch_match_type_1']}")
                print(f"  Matched to: {rec['ch_matched_name_1']} ({rec['ch_matched_number_1']})" if rec['ch_matched_name_1'] else "  No match")
            
            # Check the CH side
            print(f"\n  Companies House side:")
            cursor.execute("""
                SELECT company_number, company_name, company_status
                FROM companies_house_data
                WHERE company_number = %s
            """, (ch_number,))
            
            ch_record = cursor.fetchone()
            if ch_record:
                print(f"  CH Name: {ch_record['company_name']}")
                print(f"  CH Number: {ch_record['company_number']}")
                print(f"  Status: {ch_record['company_status']}")
            
            # Test normalization
            print(f"\n  Normalization test:")
            lr_normalized = normalize_company_name(lr_name)
            ch_normalized = normalize_company_name(ch_record['company_name']) if ch_record else 'N/A'
            
            print(f"  LR normalized: '{lr_normalized}'")
            print(f"  CH normalized: '{ch_normalized}'")
            print(f"  Match? {lr_normalized == ch_normalized}")
        
        # Check for more examples of should-have-matched
        print(f"\n\n{'='*80}")
        print("CHECKING MORE UNMATCHED THAT SHOULD EXIST...\n")
        
        cursor.execute("""
            WITH unmatched AS (
                SELECT DISTINCT
                    lr.proprietor_1_name,
                    lr.company_1_reg_no
                FROM land_registry_data lr
                JOIN land_registry_ch_matches m ON lr.id = m.id
                WHERE m.ch_match_type_1 = 'No_Match'
                AND lr.proprietor_1_name LIKE '%LIMITED'
                AND (lr.company_1_reg_no IS NULL OR lr.company_1_reg_no = '')
                ORDER BY RANDOM()
                LIMIT 10
            )
            SELECT 
                u.proprietor_1_name,
                ch.company_number,
                ch.company_name,
                ch.company_status
            FROM unmatched u
            LEFT JOIN companies_house_data ch 
                ON UPPER(ch.company_name) = u.proprietor_1_name
        """)
        
        print("Random unmatched companies - checking if they exist with exact name match:")
        for row in cursor.fetchall():
            print(f"\nLR: {row['proprietor_1_name']}")
            if row['company_number']:
                print(f"  ✓ EXISTS IN CH: {row['company_name']} ({row['company_number']}) - {row['company_status']}")
                print("  ⚠️  THIS SHOULD HAVE MATCHED!")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    debug_specific_matches()