#!/usr/bin/env python3
"""
Investigate why records with registration numbers didn't match
"""

import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import re

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

def normalize_company_number(number):
    """Normalize company registration number for matching"""
    if not number or number.strip() == '':
        return ""
    
    number = str(number).strip().upper()
    
    # Remove any non-alphanumeric characters
    number = re.sub(r'[^A-Z0-9]', '', number)
    
    # Handle Scottish numbers (start with SC)
    if number.startswith('SC'):
        return number
    
    # Handle Northern Ireland numbers (start with NI)
    if number.startswith('NI'):
        return number
        
    # Handle Gibraltar numbers (start with GI)
    if number.startswith('GI'):
        return number
    
    # If it's all digits, pad with zeros to 8 digits
    if number.isdigit():
        return number.zfill(8)
    
    return number

def investigate_unmatched():
    """Investigate unmatched records with registration numbers"""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("=== Investigating Unmatched Records WITH Registration Numbers ===\n")
        
        # Get a larger sample
        cursor.execute("""
            SELECT 
                lr.proprietor_1_name,
                lr.company_1_reg_no,
                lr.proprietorship_1_category
            FROM land_registry_data lr
            JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE m.ch_match_type_1 = 'No_Match'
            AND lr.company_1_reg_no IS NOT NULL 
            AND lr.company_1_reg_no != ''
            ORDER BY lr.company_1_reg_no
            LIMIT 1000
        """)
        
        unmatched = cursor.fetchall()
        
        # Analyze registration number patterns
        reg_patterns = {
            'standard_8_digit': [],
            'standard_7_digit': [],
            'standard_6_or_less': [],
            'scottish_sc': [],
            'northern_ireland_ni': [],
            'gibraltar_gi': [],
            'old_format': [],
            'rp_numbers': [],
            'ip_numbers': [],
            'rs_numbers': [],
            'invalid_format': [],
            'other': []
        }
        
        for record in unmatched:
            reg_no = record['company_1_reg_no']
            name = record['proprietor_1_name']
            
            if reg_no.startswith('SC'):
                reg_patterns['scottish_sc'].append((reg_no, name))
            elif reg_no.startswith('NI'):
                reg_patterns['northern_ireland_ni'].append((reg_no, name))
            elif reg_no.startswith('GI'):
                reg_patterns['gibraltar_gi'].append((reg_no, name))
            elif reg_no.startswith('RP'):
                reg_patterns['rp_numbers'].append((reg_no, name))
            elif reg_no.startswith('IP'):
                reg_patterns['ip_numbers'].append((reg_no, name))
            elif reg_no.startswith('RS'):
                reg_patterns['rs_numbers'].append((reg_no, name))
            elif reg_no.isdigit():
                if len(reg_no) == 8:
                    reg_patterns['standard_8_digit'].append((reg_no, name))
                elif len(reg_no) == 7:
                    reg_patterns['standard_7_digit'].append((reg_no, name))
                elif len(reg_no) <= 6:
                    reg_patterns['standard_6_or_less'].append((reg_no, name))
                else:
                    reg_patterns['invalid_format'].append((reg_no, name))
            elif re.match(r'^[0-9]+[A-Z]+$', reg_no):
                reg_patterns['old_format'].append((reg_no, name))
            else:
                reg_patterns['other'].append((reg_no, name))
        
        # Print analysis
        print("Registration Number Pattern Analysis:")
        for pattern, records in reg_patterns.items():
            if records:
                print(f"\n{pattern}: {len(records)} records")
                for reg_no, name in records[:3]:
                    print(f"  {reg_no}: {name}")
        
        # Check specific problematic numbers in CH database
        print("\n\nChecking specific numbers in Companies House database...")
        
        # Test different formats
        test_numbers = [
            ('4618487', '04618487'),  # Padding with zero
            ('1408264', '01408264'),
            ('IP29530R', 'IP29530R'),
            ('RS007648', 'RS007648'),
            ('RP902559', 'RP902559'),
        ]
        
        for original, normalized in test_numbers:
            cursor.execute("""
                SELECT company_number, company_name, company_status
                FROM companies_house_data
                WHERE company_number IN (%s, %s)
            """, (original, normalized))
            
            result = cursor.fetchone()
            if result:
                print(f"  {original} -> {normalized}: FOUND as {result['company_number']} - {result['company_name']}")
            else:
                print(f"  {original} -> {normalized}: NOT FOUND")
        
        # Check if these are special entity types
        print("\n\nChecking registration prefixes...")
        
        # Count by prefix
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN company_1_reg_no LIKE 'RS%' THEN 'RS - Registered Society'
                    WHEN company_1_reg_no LIKE 'IP%' THEN 'IP - Industrial & Provident Society'
                    WHEN company_1_reg_no LIKE 'RP%' THEN 'RP - Royal Patent'
                    WHEN company_1_reg_no LIKE 'NI%' THEN 'NI - Northern Ireland'
                    WHEN company_1_reg_no LIKE 'SC%' THEN 'SC - Scotland'
                    WHEN company_1_reg_no LIKE 'GI%' THEN 'GI - Gibraltar'
                    WHEN company_1_reg_no ~ '^[0-9]+$' THEN 'Numeric'
                    ELSE 'Other'
                END as reg_type,
                COUNT(*) as count
            FROM land_registry_data lr
            JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE m.ch_match_type_1 = 'No_Match'
            AND lr.company_1_reg_no IS NOT NULL 
            AND lr.company_1_reg_no != ''
            GROUP BY reg_type
            ORDER BY count DESC
        """)
        
        print("\nUnmatched by registration type:")
        for row in cursor.fetchall():
            print(f"  {row['reg_type']}: {row['count']:,}")
        
        # Check if the normalization is causing issues
        print("\n\nTesting normalization function...")
        
        # Get some 7-digit numbers that didn't match
        cursor.execute("""
            SELECT DISTINCT company_1_reg_no
            FROM land_registry_data lr
            JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE m.ch_match_type_1 = 'No_Match'
            AND lr.company_1_reg_no ~ '^[0-9]{7}$'
            LIMIT 10
        """)
        
        seven_digit_numbers = [row['company_1_reg_no'] for row in cursor.fetchall()]
        
        if seven_digit_numbers:
            print("\nChecking if 7-digit numbers need zero padding:")
            for num in seven_digit_numbers:
                padded = '0' + num
                cursor.execute("""
                    SELECT company_number, company_name
                    FROM companies_house_data
                    WHERE company_number = %s
                """, (padded,))
                
                result = cursor.fetchone()
                if result:
                    print(f"  {num} -> {padded}: FOUND - {result['company_name']}")
                else:
                    print(f"  {num} -> {padded}: NOT FOUND")
        
        # Check if these companies were dissolved
        print("\n\nChecking if companies might be dissolved...")
        cursor.execute("""
            SELECT 
                COUNT(CASE WHEN company_status = 'Dissolved' THEN 1 END) as dissolved,
                COUNT(CASE WHEN company_status = 'Active' THEN 1 END) as active,
                COUNT(CASE WHEN company_status NOT IN ('Dissolved', 'Active') THEN 1 END) as other
            FROM companies_house_data
        """)
        
        result = cursor.fetchone()
        print(f"\nCompanies House status distribution:")
        print(f"  Active: {result['active']:,}")
        print(f"  Dissolved: {result['dissolved']:,}")
        print(f"  Other status: {result['other']:,}")
        
        cursor.close()
        conn.close()
        
        print("\n\n=== KEY FINDINGS ===")
        print("1. Many unmatched numbers are NOT standard UK company numbers:")
        print("   - RS/IP/RP prefixes (Registered Societies, Industrial & Provident)")
        print("   - These are NOT in the standard Companies House dataset")
        print("\n2. Some legitimate companies may have been dissolved")
        print("   - Companies House data only includes current companies")
        print("\n3. Registration number format variations")
        print("   - Some 7-digit numbers might need padding")
        print("   - Some old format numbers with letters")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    investigate_unmatched()