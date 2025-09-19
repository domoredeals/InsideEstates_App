#!/usr/bin/env python3
"""
Quick test of the fixed matching logic without loading all CH data
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

def normalize_company_number(number):
    """Normalize company number - pad with zeros to 8 digits"""
    if not number:
        return None
    
    # Remove any non-numeric characters
    number = re.sub(r'[^0-9]', '', str(number))
    
    # Pad with leading zeros to make it 8 digits
    if number and len(number) < 8:
        number = number.zfill(8)
    
    return number

def test_matching_improvement():
    """Test how many No_Match records would be fixed with new normalization"""
    
    conn = psycopg2.connect(**POSTGRESQL_CONFIG)
    cursor = conn.cursor()
    
    print("=== Testing No_Match Records with Fixed Normalization ===\n")
    
    # Get a sample of No_Match records
    cursor.execute("""
        SELECT 
            lr.id,
            lr.proprietor_1_name, lr.company_1_reg_no,
            lr.proprietor_2_name, lr.company_2_reg_no,
            lr.proprietor_3_name, lr.company_3_reg_no,
            lr.proprietor_4_name, lr.company_4_reg_no
        FROM land_registry_ch_matches m
        JOIN land_registry_data lr ON m.id = lr.id
        WHERE m.ch_match_type_1 = 'No_Match'
        LIMIT 1000
    """)
    
    no_match_records = cursor.fetchall()
    print(f"Testing {len(no_match_records)} No_Match records...")
    
    improvements = 0
    examples = []
    
    for record in no_match_records:
        lr_id = record[0]
        improved = False
        
        # Check each proprietor
        for i in range(1, 9, 2):
            prop_name = record[i]
            prop_number = record[i+1]
            
            if not prop_name:
                continue
            
            norm_name = normalize_company_name_fixed(prop_name)
            norm_number = normalize_company_number(prop_number)
            
            # Try to find match by number first (highest confidence)
            if norm_number:
                cursor.execute("""
                    SELECT company_name, company_number 
                    FROM companies_house_data 
                    WHERE company_number = %s
                    LIMIT 1
                """, (norm_number,))
                
                result = cursor.fetchone()
                if result:
                    improved = True
                    if len(examples) < 10:
                        examples.append({
                            'lr_name': prop_name,
                            'lr_number': prop_number,
                            'ch_name': result[0],
                            'ch_number': result[1],
                            'match_type': 'Number' if norm_name != normalize_company_name_fixed(result[0]) else 'Name+Number'
                        })
                    break
            
            # If no number match, try name only
            if not improved and norm_name:
                cursor.execute("""
                    SELECT company_name, company_number 
                    FROM companies_house_data 
                    WHERE company_name ILIKE %s
                    LIMIT 1
                """, (f"%{prop_name}%",))
                
                result = cursor.fetchone()
                if result and normalize_company_name_fixed(result[0]) == norm_name:
                    improved = True
                    if len(examples) < 10:
                        examples.append({
                            'lr_name': prop_name,
                            'lr_number': prop_number,
                            'ch_name': result[0],
                            'ch_number': result[1],
                            'match_type': 'Name'
                        })
                    break
        
        if improved:
            improvements += 1
    
    print(f"\n=== RESULTS ===")
    print(f"Records that would be matched: {improvements}/{len(no_match_records)}")
    print(f"Improvement rate: {improvements/len(no_match_records)*100:.1f}%")
    
    if examples:
        print(f"\n=== EXAMPLE IMPROVEMENTS ===")
        for ex in examples:
            print(f"\nLR: '{ex['lr_name']}' ({ex['lr_number'] or 'No number'})")
            print(f"CH: '{ex['ch_name']}' ({ex['ch_number']})")
            print(f"Match Type: {ex['match_type']}")
    
    # Extrapolate to full dataset
    total_no_match = 3754510  # From earlier query
    estimated_improvements = int(total_no_match * (improvements/len(no_match_records)))
    print(f"\n=== ESTIMATED IMPACT ===")
    print(f"Total No_Match records: {total_no_match:,}")
    print(f"Estimated improvements: {estimated_improvements:,}")
    print(f"New match rate would be: ~{(4489568 + estimated_improvements)/8237535*100:.1f}%")
    
    cursor.close()
    conn.close()

if __name__ == '__main__':
    test_matching_improvement()