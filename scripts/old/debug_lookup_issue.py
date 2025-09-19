#!/usr/bin/env python3
"""
Debug why S NOTARO LIMITED matches as Number only instead of Name+Number
"""

import re

def normalize_company_name_fixed(name):
    """Fixed normalization that REMOVES suffixes"""
    if not name or name.strip() == '':
        return ""
    
    name = str(name).upper().strip()
    name = name.replace(' AND ', ' ').replace(' & ', ' ')
    
    # Pre-compiled regex for suffixes - REMOVE THEM
    suffix_pattern = r'\s*(LIMITED LIABILITY PARTNERSHIP|LIMITED|COMPANY|LTD\.|LLP|LTD|PLC|CO\.|CO).*$'
    name = re.sub(suffix_pattern, '', name)
    
    # Keep only alphanumeric
    name = ''.join(char for char in name if char.isalnum())
    
    return name

def normalize_company_number_fixed(number):
    """Normalize company number for matching"""
    if not number or str(number).strip() == '':
        return ""
    
    number = str(number).strip().upper()
    number = re.sub(r'[^A-Z0-9]', '', number)
    
    if number.startswith('SC'):
        return number
    
    if number.isdigit():
        return number.zfill(8)
    
    return number

# Test the normalization
lr_name = "S NOTARO LIMITED"
lr_number = "845344"
ch_name = "S. NOTARO LIMITED"
ch_number = "00845344"

print("=== Testing S NOTARO LIMITED Matching Logic ===\n")

# Normalize everything
lr_norm_name = normalize_company_name_fixed(lr_name)
lr_norm_number = normalize_company_number_fixed(lr_number)
ch_norm_name = normalize_company_name_fixed(ch_name)
ch_norm_number = normalize_company_number_fixed(ch_number)

print(f"Land Registry:")
print(f"  Original: '{lr_name}' ({lr_number})")
print(f"  Normalized: '{lr_norm_name}' ({lr_norm_number})")

print(f"\nCompanies House:")
print(f"  Original: '{ch_name}' ({ch_number})")
print(f"  Normalized: '{ch_norm_name}' ({ch_norm_number})")

print(f"\nMatching:")
print(f"  Names match: {lr_norm_name == ch_norm_name}")
print(f"  Numbers match: {lr_norm_number == ch_norm_number}")

# Test what keys would be generated
lr_key = lr_norm_name + lr_norm_number
ch_key = ch_norm_name + ch_norm_number

print(f"\nLookup Keys:")
print(f"  LR would generate key: '{lr_key}'")
print(f"  CH would generate key: '{ch_key}'")
print(f"  Keys match: {lr_key == ch_key}")

print("\n=== How the matching works ===")
print("1. Script builds CH lookup dictionaries:")
print(f"   - Name+Number lookup key: '{ch_key}'")
print(f"   - Number only lookup key: '{ch_norm_number}'")
print(f"   - Name only lookup key: '{ch_norm_name}'")

print("\n2. When matching LR record:")
print(f"   - Tries Name+Number with key: '{lr_key}' → {'FOUND' if lr_key == ch_key else 'NOT FOUND'}")
print(f"   - Tries Number only with key: '{lr_norm_number}' → FOUND (Tier 2)")
print(f"   - Would try Name only with key: '{lr_norm_name}' → FOUND (Tier 3)")

print("\n=== CONCLUSION ===")
if lr_key == ch_key:
    print("✅ The keys match! This should be a Tier 1 (Name+Number) match.")
else:
    print("❌ The keys don't match! That's why it's only matching as Tier 2 (Number).")
    print(f"   Difference: LR key='{lr_key}' vs CH key='{ch_key}'")