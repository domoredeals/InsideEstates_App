#!/usr/bin/env python3
"""
Quick test to verify the normalization fix improves matching
"""

import psycopg2
import re
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

def normalize_company_name_old(name):
    """Old normalization that kept suffixes"""
    if not name:
        return None
    
    name = str(name).upper().strip()
    
    # Standardize common variations
    name = name.replace(' AND ', ' ')
    name = name.replace('&', ' ')
    
    # Old approach: standardize but KEEP suffixes
    name = re.sub(r'\bLTD\.?(?:\s|$)', 'LIMITED', name)
    name = re.sub(r'\bCO\.?(?:\s|$)', 'COMPANY', name)
    name = re.sub(r'\bPLC\.?(?:\s|$)', 'PUBLIC LIMITED COMPANY', name)
    
    # Remove special characters
    name = ''.join(char for char in name if char.isalnum() or char.isspace())
    name = ' '.join(name.split())
    
    return name

def normalize_company_name_fixed(name):
    """Fixed normalization that REMOVES suffixes"""
    if not name:
        return None
        
    name = str(name).upper().strip()
    
    # Replace common separators
    name = name.replace(' AND ', ' ')
    name = name.replace(' & ', ' ')
    
    # REMOVE company type suffixes (proven to increase matches)
    suffix_pattern = r'\s*(LIMITED LIABILITY PARTNERSHIP|LIMITED|COMPANY|LTD\.|LLP|LTD|PLC|CO\.|CO).*$'
    name = re.sub(suffix_pattern, '', name)
    
    # Remove special characters but keep alphanumeric
    name = ''.join(char for char in name if char.isalnum())
    
    return name

def test_normalization():
    """Test the normalization fix on actual No_Match records"""
    
    conn = psycopg2.connect(**POSTGRESQL_CONFIG)
    cursor = conn.cursor()
    
    # Get a sample of No_Match records
    cursor.execute("""
        SELECT DISTINCT
            lr.proprietor_1_name,
            lr.company_1_reg_no,
            lr.proprietor_2_name,
            lr.company_2_reg_no,
            lr.proprietor_3_name,
            lr.company_3_reg_no,
            lr.proprietor_4_name,
            lr.company_4_reg_no
        FROM land_registry_ch_matches m
        JOIN land_registry_data lr ON m.id = lr.id
        WHERE m.ch_match_type_1 = 'No_Match'
        LIMIT 100
    """)
    
    no_match_records = cursor.fetchall()
    
    # Get all company names and numbers from CH for testing
    print("Loading CH company data for testing...")
    cursor.execute("""
        SELECT company_name, company_number 
        FROM companies_house_data 
        WHERE company_name IS NOT NULL
        LIMIT 500000
    """)
    
    ch_companies = {}
    ch_by_name_old = {}
    ch_by_name_fixed = {}
    
    for name, number in cursor.fetchall():
        if number:
            ch_companies[number] = name
        
        # Create lookup by normalized name (old way)
        norm_old = normalize_company_name_old(name)
        if norm_old:
            ch_by_name_old[norm_old] = number
            
        # Create lookup by normalized name (fixed way)
        norm_fixed = normalize_company_name_fixed(name)
        if norm_fixed:
            ch_by_name_fixed[norm_fixed] = number
    
    print(f"\nLoaded {len(ch_companies)} CH companies for testing")
    print(f"Testing {len(no_match_records)} No_Match records...\n")
    
    # Test each No_Match record
    improvements = 0
    examples = []
    
    for record in no_match_records:
        for i in range(0, 8, 2):  # Check all 4 proprietors
            lr_name = record[i]
            lr_number = record[i+1]
            
            if not lr_name:
                continue
                
            # Try old normalization
            norm_old = normalize_company_name_old(lr_name)
            found_old = norm_old in ch_by_name_old if norm_old else False
            
            # Try fixed normalization  
            norm_fixed = normalize_company_name_fixed(lr_name)
            found_fixed = norm_fixed in ch_by_name_fixed if norm_fixed else False
            
            # Check if fixed normalization found a match that old didn't
            if found_fixed and not found_old:
                improvements += 1
                if len(examples) < 10:
                    ch_number = ch_by_name_fixed[norm_fixed]
                    ch_name = ch_companies.get(ch_number, "Unknown")
                    examples.append({
                        'lr_name': lr_name,
                        'lr_norm_old': norm_old,
                        'lr_norm_fixed': norm_fixed,
                        'ch_name': ch_name,
                        'ch_number': ch_number
                    })
    
    print("=== NORMALIZATION TEST RESULTS ===\n")
    print(f"Total No_Match proprietors tested: {len(no_match_records) * 4}")
    print(f"Improvements with fixed normalization: {improvements}")
    print(f"Improvement rate: {improvements / (len(no_match_records) * 4) * 100:.1f}%\n")
    
    if examples:
        print("=== EXAMPLE IMPROVEMENTS ===\n")
        for ex in examples:
            print(f"LR Name: '{ex['lr_name']}'")
            print(f"Old Norm: '{ex['lr_norm_old']}' → NOT FOUND")
            print(f"Fixed Norm: '{ex['lr_norm_fixed']}' → FOUND")
            print(f"Matched to: '{ex['ch_name']}' ({ex['ch_number']})")
            print("-" * 70)
    
    cursor.close()
    conn.close()

if __name__ == '__main__':
    test_normalization()