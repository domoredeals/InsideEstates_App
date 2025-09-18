#!/usr/bin/env python3
"""
Verify the normalization fix works for ROWANFIELD OAK LTD
"""

import re

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

# Test the specific case
lr_name = "ROWANFIELD OAK LTD"
lr_number = "15483533"
ch_name = "ROWANFIELD OAK LTD"
ch_number = "15483533"

print("=== NORMALIZATION TEST ===")
print(f"LR Name: '{lr_name}' → Normalized: '{normalize_company_name_fixed(lr_name)}'")
print(f"CH Name: '{ch_name}' → Normalized: '{normalize_company_name_fixed(ch_name)}'")
print(f"Name match: {normalize_company_name_fixed(lr_name) == normalize_company_name_fixed(ch_name)}")

print(f"\nLR Number: '{lr_number}' → Normalized: '{normalize_company_number(lr_number)}'")
print(f"CH Number: '{ch_number}' → Normalized: '{normalize_company_number(ch_number)}'")
print(f"Number match: {normalize_company_number(lr_number) == normalize_company_number(ch_number)}")

print("\n=== MATCHING RESULT ===")
if normalize_company_number(lr_number) == normalize_company_number(ch_number):
    if normalize_company_name_fixed(lr_name) == normalize_company_name_fixed(ch_name):
        print("✅ Would match as: Name+Number (Tier 1, confidence 1.0)")
    else:
        print("✅ Would match as: Number (Tier 2, confidence 0.9)")
else:
    if normalize_company_name_fixed(lr_name) == normalize_company_name_fixed(ch_name):
        print("✅ Would match as: Name (Tier 3, confidence 0.7)")
    else:
        print("❌ Would be: No_Match (confidence 0.0)")

# Test some variations
print("\n=== TESTING SUFFIX VARIATIONS ===")
test_cases = [
    ("ABC LIMITED", "ABC LTD"),
    ("XYZ LTD", "XYZ LIMITED"),
    ("COMPANY ABC LTD.", "COMPANY ABC LIMITED"),
    ("TEST & CO LTD", "TEST AND COMPANY LIMITED")
]

for name1, name2 in test_cases:
    norm1 = normalize_company_name_fixed(name1)
    norm2 = normalize_company_name_fixed(name2)
    match = norm1 == norm2
    print(f"'{name1}' vs '{name2}'")
    print(f"  → '{norm1}' vs '{norm2}' = {match}")
    print()