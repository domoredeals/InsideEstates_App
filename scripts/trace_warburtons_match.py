#!/usr/bin/env python3
"""
Trace through the exact matching process for WARBURTONS LIMITED
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
    """Exact normalization from matching script"""
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
    
    if number.isdigit():
        return number.zfill(8)
    
    return number

def trace_match():
    """Trace the matching process"""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("=== TRACING WARBURTONS MATCHING PROCESS ===\n")
        
        # Get a WARBURTONS LR record
        cursor.execute("""
            SELECT 
                lr.id,
                lr.proprietor_1_name,
                lr.company_1_reg_no,
                m.ch_match_type_1,
                m.ch_matched_name_1,
                m.ch_matched_number_1
            FROM land_registry_data lr
            JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE lr.proprietor_1_name = 'WARBURTONS LIMITED'
            AND lr.company_1_reg_no = '00178711'
            LIMIT 1
        """)
        
        lr_record = cursor.fetchone()
        if not lr_record:
            print("No WARBURTONS record found!")
            return
            
        print(f"Land Registry Record:")
        print(f"  ID: {lr_record['id']}")
        print(f"  Name: {lr_record['proprietor_1_name']}")
        print(f"  Reg No: {lr_record['company_1_reg_no']}")
        print(f"  Match Result: {lr_record['ch_match_type_1']}")
        
        # Normalize LR data
        lr_name = lr_record['proprietor_1_name']
        lr_number = lr_record['company_1_reg_no']
        
        clean_name = normalize_company_name(lr_name)
        clean_number = normalize_company_number(lr_number)
        
        print(f"\nNormalized LR data:")
        print(f"  Name: '{clean_name}'")
        print(f"  Number: '{clean_number}'")
        print(f"  Name+Number key: '{clean_name + clean_number}'")
        
        # Check CH side
        cursor.execute("""
            SELECT company_name, company_number, company_status
            FROM companies_house_data
            WHERE company_number = '00178711'
        """)
        
        ch_record = cursor.fetchone()
        if ch_record:
            print(f"\nCompanies House Record:")
            print(f"  Name: {ch_record['company_name']}")
            print(f"  Number: {ch_record['company_number']}")
            print(f"  Status: {ch_record['company_status']}")
            
            ch_clean_name = normalize_company_name(ch_record['company_name'])
            ch_clean_number = normalize_company_number(ch_record['company_number'])
            
            print(f"\nNormalized CH data:")
            print(f"  Name: '{ch_clean_name}'")
            print(f"  Number: '{ch_clean_number}'")
            print(f"  Name+Number key: '{ch_clean_name + ch_clean_number}'")
            
            print(f"\nMatching tests:")
            print(f"  Name+Number match: {clean_name + clean_number == ch_clean_name + ch_clean_number}")
            print(f"  Number only match: {clean_number == ch_clean_number}")
            print(f"  Name only match: {clean_name == ch_clean_name}")
        
        # Check if there might be a data type issue
        print(f"\n\nData type checks:")
        print(f"  LR company_1_reg_no type in DB: {type(lr_record['company_1_reg_no'])}")
        print(f"  LR company_1_reg_no value: '{lr_record['company_1_reg_no']}'")
        print(f"  LR company_1_reg_no repr: {repr(lr_record['company_1_reg_no'])}")
        
        # Check for any weird characters
        if lr_record['company_1_reg_no']:
            print(f"  Character codes: {[ord(c) for c in lr_record['company_1_reg_no']]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    trace_match()