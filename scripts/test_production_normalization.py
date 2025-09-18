#!/usr/bin/env python3
"""
Test the production script's fixed normalization functions
"""

import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the production script
try:
    from scripts.script_03_match_lr_to_ch_production import ProductionMatcher
except ImportError:
    # Try alternative import method
    import importlib.util
    spec = importlib.util.spec_from_file_location("production_matcher", 
        os.path.join(os.path.dirname(__file__), "03_match_lr_to_ch_production.py"))
    production_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(production_module)
    ProductionMatcher = production_module.ProductionMatcher

def test_company_name_normalization():
    """Test the fixed company name normalization"""
    print("=== Testing Fixed Company Name Normalization ===\n")
    
    # Create a temporary matcher instance just for testing normalization
    matcher = ProductionMatcher()
    
    test_cases = [
        ('TENSATOR LIMITED', 'Original company name'),
        ('TENSATOR LTD', 'With LTD suffix'),
        ('PATHTOP PROPERTY CO LIMITED', 'With CO suffix'),
        ('HARRY TAYLOR & CO. LIMITED', 'With ampersand and dots'),
        ('Circle Thirty Three Housing Trust Limited', 'Mixed case'),
        ('WARBURTONS LIMITED', 'Test case we know exists'),
        ('ADACTUS HOUSING ASSOCIATION LIMITED', 'Housing association'),
        ('TEST COMPANY PLC', 'With PLC suffix'),
        ('EXAMPLE LLP', 'Limited liability partnership'),
        ('SAMPLE CO.', 'Company with dot'),
    ]
    
    print("Testing company name normalization:")
    print("-" * 80)
    
    for name, description in test_cases:
        normalized = matcher.normalize_company_name_fixed(name)
        print(f"Original:    {name}")
        print(f"Normalized:  {normalized}")
        print(f"Description: {description}")
        print()

def test_company_number_normalization():
    """Test the fixed company number normalization"""
    print("=== Testing Fixed Company Number Normalization ===\n")
    
    # Create a temporary matcher instance
    matcher = ProductionMatcher()
    
    test_cases = [
        ('178711', 'Number without leading zeros'),
        ('00178711', 'Number with leading zeros'), 
        ('12345678', '8-digit number'),
        ('SC123456', 'Scottish company'),
        ('NI123456', 'Northern Ireland company'),
        ('GI123456', 'Gibraltar company'),
        ('  178711  ', 'Number with spaces'),
        ('OC123456', 'Overseas company'),
        ('123', 'Short number (should be padded)'),
        ('', 'Empty string'),
        (None, 'None value'),
    ]
    
    print("Testing company number normalization:")
    print("-" * 60)
    
    for number, description in test_cases:
        try:
            normalized = matcher.normalize_company_number_fixed(number)
            print(f"Original:    {repr(number)}")
            print(f"Normalized:  {repr(normalized)}")
            print(f"Description: {description}")
            print()
        except Exception as e:
            print(f"ERROR with {repr(number)}: {e}")
            print()

def test_specific_cases():
    """Test specific cases that we know should work"""
    print("=== Testing Specific Known Cases ===\n")
    
    matcher = ProductionMatcher()
    
    # Test the WARBURTONS case that we know exists
    warburtons_name = "WARBURTONS LIMITED" 
    warburtons_number = "00178711"
    
    norm_name = matcher.normalize_company_name_fixed(warburtons_name)
    norm_number = matcher.normalize_company_number_fixed(warburtons_number)
    
    print(f"WARBURTONS test case:")
    print(f"  Original name: {warburtons_name}")
    print(f"  Normalized name: {norm_name}")
    print(f"  Original number: {warburtons_number}")
    print(f"  Normalized number: {norm_number}")
    print()
    
    # Test that LTD variations normalize to the same thing
    ltd_variations = [
        "TEST COMPANY LIMITED",
        "TEST COMPANY LTD", 
        "TEST COMPANY LTD.",
    ]
    
    print("Testing LTD variation consistency:")
    normalized_names = []
    for name in ltd_variations:
        normalized = matcher.normalize_company_name_fixed(name)
        normalized_names.append(normalized)
        print(f"  {name} -> {normalized}")
    
    # Check if they're all the same
    if len(set(normalized_names)) == 1:
        print("  ✅ All variations normalize to the same value")
    else:
        print("  ❌ Variations normalize to different values!")
    print()
    
    # Test number padding consistency
    number_variations = [
        "178711",
        "00178711", 
        "000178711"
    ]
    
    print("Testing number padding consistency:")
    normalized_numbers = []
    for number in number_variations:
        normalized = matcher.normalize_company_number_fixed(number)
        normalized_numbers.append(normalized)
        print(f"  {number} -> {normalized}")
    
    if len(set(normalized_numbers)) == 1:
        print("  ✅ All number variations normalize to the same value")
    else:
        print("  ❌ Number variations normalize to different values!")

if __name__ == '__main__':
    print("🧪 TESTING PRODUCTION SCRIPT NORMALIZATION\n")
    
    try:
        test_company_name_normalization()
        test_company_number_normalization() 
        test_specific_cases()
        
        print("✅ All normalization tests completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()