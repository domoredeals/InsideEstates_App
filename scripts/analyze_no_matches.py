#\!/usr/bin/env python3
"""
Analyze failed matches by proprietorship category
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
    
    print("=== ANALYZING FAILED MATCHES BY PROPRIETORSHIP CATEGORY ===\n")
    
    # Get overall statistics first
    cursor.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN ch_match_type_1 <> 'No_Match' THEN 1 END) as matched,
            COUNT(CASE WHEN ch_match_type_1 = 'No_Match' THEN 1 END) as not_matched,
            ROUND(COUNT(CASE WHEN ch_match_type_1 <> 'No_Match' THEN 1 END) * 100.0 / COUNT(*), 2) as match_rate
        FROM land_registry_ch_matches
        WHERE ch_match_type_1 IS NOT NULL
    """)
    
    stats = cursor.fetchone()
    print(f"Total records processed: {stats['total_records']:,}")
    print(f"Successfully matched: {stats['matched']:,} ({stats['match_rate']}%)")
    print(f"Failed to match: {stats['not_matched']:,} ({100 - stats['match_rate']:.2f}%)")
    
    # Analyze failed matches by proprietorship category
    print("\n\nFAILED MATCHES BY PROPRIETORSHIP CATEGORY:")
    print("-" * 100)
    
    cursor.execute("""
        SELECT 
            COALESCE(lr.proprietorship_1_category, 'Not Specified') as category,
            COUNT(*) as failed_matches,
            COUNT(CASE WHEN lr.company_1_reg_no IS NOT NULL AND lr.company_1_reg_no <> '' THEN 1 END) as with_reg_no,
            COUNT(CASE WHEN lr.company_1_reg_no IS NULL OR lr.company_1_reg_no = '' THEN 1 END) as without_reg_no,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pct_of_failed
        FROM land_registry_data lr
        JOIN land_registry_ch_matches m ON lr.id = m.id
        WHERE m.ch_match_type_1 = 'No_Match'
        GROUP BY lr.proprietorship_1_category
        ORDER BY COUNT(*) DESC
    """)
    
    print(f"{'Category':<60} | {'Failed':<10} | {'With RegNo':<10} | {'No RegNo':<10} | {'% of Failed':<10}")
    print("-" * 110)
    
    for row in cursor.fetchall():
        print(f"{row['category']:<60} | {row['failed_matches']:>9,} | {row['with_reg_no']:>10,} | {row['without_reg_no']:>9,} | {row['pct_of_failed']:>9.1f}%")
    
    # Get some examples from each major category
    print("\n\nEXAMPLE FAILED MATCHES BY CATEGORY:")
    print("-" * 100)
    
    major_categories = [
        'Limited Company or Public Limited Company',
        'Corporate Body',
        'Local Authority',
        'Housing Association/Society (Company)',
        'Registered Society (Company)'
    ]
    
    for category in major_categories:
        print(f"\n{category}:")
        
        cursor.execute("""
            SELECT 
                lr.proprietor_1_name,
                lr.company_1_reg_no,
                lr.proprietorship_1_category
            FROM land_registry_data lr
            JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE m.ch_match_type_1 = 'No_Match'
            AND lr.proprietorship_1_category = %s
            ORDER BY RANDOM()
            LIMIT 5
        """, (category,))
        
        examples = cursor.fetchall()
        for ex in examples:
            reg_no = ex['company_1_reg_no'] or 'None'
            print(f"  - {ex['proprietor_1_name']} (Reg: {reg_no})")
    
    # Check specific patterns
    print("\n\nSPECIFIC PATTERNS IN FAILED MATCHES:")
    print("-" * 100)
    
    # Government bodies
    cursor.execute("""
        SELECT COUNT(*) as count
        FROM land_registry_data lr
        JOIN land_registry_ch_matches m ON lr.id = m.id
        WHERE m.ch_match_type_1 = 'No_Match'
        AND (
            lr.proprietor_1_name LIKE '%SECRETARY OF STATE%' OR
            lr.proprietor_1_name LIKE '%MINISTER%' OR
            lr.proprietor_1_name LIKE '%COUNCIL%' OR
            lr.proprietor_1_name LIKE '%AUTHORITY%' OR
            lr.proprietor_1_name LIKE '%NHS%' OR
            lr.proprietor_1_name LIKE '%GOVERNMENT%'
        )
    """)
    
    gov_count = cursor.fetchone()['count']
    print(f"Government/Public bodies: {gov_count:,}")
    
    # Housing associations
    cursor.execute("""
        SELECT COUNT(*) as count
        FROM land_registry_data lr
        JOIN land_registry_ch_matches m ON lr.id = m.id
        WHERE m.ch_match_type_1 = 'No_Match'
        AND lr.proprietor_1_name LIKE '%HOUSING%'
    """)
    
    housing_count = cursor.fetchone()['count']
    print(f"Housing-related entities: {housing_count:,}")
    
    # Overseas
    cursor.execute("""
        SELECT COUNT(*) as count
        FROM land_registry_data lr
        JOIN land_registry_ch_matches m ON lr.id = m.id
        WHERE m.ch_match_type_1 = 'No_Match'
        AND (
            lr.proprietor_1_name LIKE '%(JERSEY)%' OR
            lr.proprietor_1_name LIKE '%(GUERNSEY)%' OR
            lr.proprietor_1_name LIKE '%(ISLE OF MAN)%' OR
            lr.proprietor_1_name LIKE '%(BVI)%' OR
            lr.proprietor_1_name LIKE '%(CAYMAN%'
        )
    """)
    
    overseas_count = cursor.fetchone()['count']
    print(f"Overseas entities: {overseas_count:,}")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()