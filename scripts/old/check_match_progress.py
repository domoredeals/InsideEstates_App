#!/usr/bin/env python3
"""
Check matching progress and see if companies with reg numbers are matching better
"""

import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

try:
    conn = psycopg2.connect(**POSTGRESQL_CONFIG)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    print("=== CHECKING MATCH PROGRESS ===\n")
    
    # Overall stats
    cursor.execute("""
        SELECT 
            COUNT(*) as total_processed,
            COUNT(CASE WHEN ch_match_type_1 != 'No_Match' THEN 1 END) as matched,
            COUNT(CASE WHEN ch_match_type_1 = 'No_Match' THEN 1 END) as not_matched
        FROM land_registry_ch_matches
        WHERE ch_match_type_1 IS NOT NULL
    """)
    
    result = cursor.fetchone()
    match_rate = result['matched'] / result['total_processed'] * 100
    print(f"Total processed so far: {result['total_processed']:,}")
    print(f"Matched: {result['matched']:,} ({match_rate:.1f}%)")
    print(f"Not matched: {result['not_matched']:,}")
    
    # Check match rate for those WITH registration numbers
    cursor.execute("""
        SELECT 
            COUNT(*) as total_with_regno,
            COUNT(CASE WHEN m.ch_match_type_1 != 'No_Match' THEN 1 END) as matched_with_regno
        FROM land_registry_data lr
        JOIN land_registry_ch_matches m ON lr.id = m.id
        WHERE lr.company_1_reg_no IS NOT NULL 
        AND lr.company_1_reg_no != ''
        AND m.ch_match_type_1 IS NOT NULL
    """)
    
    with_regno = cursor.fetchone()
    if with_regno['total_with_regno'] > 0:
        regno_match_rate = with_regno['matched_with_regno'] / with_regno['total_with_regno'] * 100
        print(f"\nFor proprietors WITH registration numbers:")
        print(f"Total: {with_regno['total_with_regno']:,}")
        print(f"Match rate: {regno_match_rate:.1f}%")
    
    # Check match rate for those WITHOUT registration numbers
    cursor.execute("""
        SELECT 
            COUNT(*) as total_without_regno,
            COUNT(CASE WHEN m.ch_match_type_1 != 'No_Match' THEN 1 END) as matched_without_regno
        FROM land_registry_data lr
        JOIN land_registry_ch_matches m ON lr.id = m.id
        WHERE (lr.company_1_reg_no IS NULL OR lr.company_1_reg_no = '')
        AND m.ch_match_type_1 IS NOT NULL
    """)
    
    without_regno = cursor.fetchone()
    if without_regno['total_without_regno'] > 0:
        no_regno_match_rate = without_regno['matched_without_regno'] / without_regno['total_without_regno'] * 100
        print(f"\nFor proprietors WITHOUT registration numbers:")
        print(f"Total: {without_regno['total_without_regno']:,}")
        print(f"Match rate: {no_regno_match_rate:.1f}%")
    
    # Check WARBURTONS specifically
    cursor.execute("""
        SELECT 
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
    
    warburtons = cursor.fetchone()
    if warburtons:
        print(f"\nWARBURTONS LIMITED (00178711) status:")
        print(f"Match type: {warburtons['ch_match_type_1']}")
        if warburtons['ch_matched_name_1']:
            print(f"Matched to: {warburtons['ch_matched_name_1']} ({warburtons['ch_matched_number_1']})")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()