#!/usr/bin/env python3
"""
Test specifically for suffix variations (LTD vs LIMITED)
"""

import psycopg2
import re
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

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

def test_suffix_variations():
    """Test specific suffix variation matches"""
    
    conn = psycopg2.connect(**POSTGRESQL_CONFIG)
    cursor = conn.cursor()
    
    # Find LR companies ending with LTD
    print("Finding Land Registry companies ending with 'LTD'...")
    cursor.execute("""
        SELECT DISTINCT proprietor_1_name, company_1_reg_no 
        FROM land_registry_data 
        WHERE proprietor_1_name LIKE '% LTD'
        AND company_1_reg_no IS NOT NULL
        LIMIT 50
    """)
    
    ltd_companies = cursor.fetchall()
    print(f"Found {len(ltd_companies)} companies ending with LTD")
    
    # Check if they exist in CH with LIMITED suffix
    matches_found = 0
    examples = []
    
    for lr_name, lr_number in ltd_companies:
        if not lr_number:
            continue
            
        # Check if the company exists in CH
        cursor.execute("""
            SELECT company_name, company_number 
            FROM companies_house_data 
            WHERE company_number = %s
        """, (lr_number.zfill(8),))
        
        ch_result = cursor.fetchone()
        if ch_result:
            ch_name, ch_number = ch_result
            
            # Check if it's a suffix variation
            if 'LIMITED' in ch_name and lr_name.endswith(' LTD'):
                matches_found += 1
                norm_lr = normalize_company_name_fixed(lr_name)
                norm_ch = normalize_company_name_fixed(ch_name)
                
                if len(examples) < 10:
                    examples.append({
                        'lr_name': lr_name,
                        'ch_name': ch_name,
                        'number': ch_number,
                        'norm_lr': norm_lr,
                        'norm_ch': norm_ch,
                        'match': norm_lr == norm_ch
                    })
    
    print(f"\n=== SUFFIX VARIATION RESULTS ===")
    print(f"Companies with LTD in LR but LIMITED in CH: {matches_found}/{len(ltd_companies)}")
    print(f"Percentage: {matches_found/len(ltd_companies)*100:.1f}%")
    
    print(f"\n=== EXAMPLES ===")
    for ex in examples:
        print(f"LR: '{ex['lr_name']}'")
        print(f"CH: '{ex['ch_name']}' ({ex['number']})")
        print(f"Normalized LR: '{ex['norm_lr']}'")
        print(f"Normalized CH: '{ex['norm_ch']}'")
        print(f"Match with suffix removal: {ex['match']}")
        print("-" * 70)
    
    # Now test the opposite - LIMITED in LR, check if matches LTD in CH  
    print("\n\nFinding Land Registry companies ending with 'LIMITED'...")
    cursor.execute("""
        SELECT DISTINCT proprietor_1_name, company_1_reg_no 
        FROM land_registry_data 
        WHERE proprietor_1_name LIKE '% LIMITED'
        AND company_1_reg_no IS NOT NULL
        LIMIT 50
    """)
    
    limited_companies = cursor.fetchall()
    print(f"Found {len(limited_companies)} companies ending with LIMITED")
    
    # Check matches
    limited_matches = 0
    for lr_name, lr_number in limited_companies:
        if not lr_number:
            continue
            
        cursor.execute("""
            SELECT company_name 
            FROM companies_house_data 
            WHERE company_number = %s
        """, (lr_number.zfill(8),))
        
        ch_result = cursor.fetchone()
        if ch_result:
            limited_matches += 1
    
    print(f"Successfully matched: {limited_matches}/{len(limited_companies)} ({limited_matches/len(limited_companies)*100:.1f}%)")
    
    cursor.close()
    conn.close()

if __name__ == '__main__':
    test_suffix_variations()