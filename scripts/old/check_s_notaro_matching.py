#!/usr/bin/env python3
"""
Check why S NOTARO LIMITED records weren't matched despite normalization
"""

import psycopg2
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

print("=== Checking S NOTARO LIMITED Matching Status ===\n")

# Check some specific S NOTARO LIMITED records
cursor.execute("""
    SELECT 
        lr.id,
        lr.proprietor_1_name,
        lr.company_1_reg_no,
        lr.title_number,
        m.ch_match_type_1,
        m.ch_matched_name_1,
        m.ch_matched_number_1,
        m.updated_at
    FROM land_registry_data lr
    LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
    WHERE lr.proprietor_1_name IN ('S NOTARO LIMITED', 'S. NOTARO LIMITED')
    AND lr.company_1_reg_no IN ('00845344', '845344')
    ORDER BY m.ch_match_type_1, lr.proprietor_1_name
    LIMIT 10
""")

results = cursor.fetchall()

print(f"Sample of S NOTARO LIMITED records with registration numbers:\n")
for record in results:
    lr_id, name, reg_no, title, match_type, ch_name, ch_num, updated = record
    print(f"ID: {lr_id} | Title: {title}")
    print(f"  LR: '{name}' ({reg_no})")
    print(f"  Match: {match_type or 'NOT IN MATCH TABLE'}")
    if ch_name:
        print(f"  Matched to: {ch_name} ({ch_num})")
    print(f"  Updated: {updated}")
    print()

# Check overall stats for S NOTARO
cursor.execute("""
    SELECT 
        lr.proprietor_1_name,
        lr.company_1_reg_no,
        m.ch_match_type_1,
        COUNT(*) as count
    FROM land_registry_data lr
    LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
    WHERE lr.proprietor_1_name LIKE '%NOTARO LIMITED'
    GROUP BY lr.proprietor_1_name, lr.company_1_reg_no, m.ch_match_type_1
    ORDER BY lr.proprietor_1_name, lr.company_1_reg_no, count DESC
""")

stats = cursor.fetchall()

print("\n=== Summary Statistics for S NOTARO LIMITED ===")
current_name = None
for name, reg_no, match_type, count in stats:
    if name != current_name:
        print(f"\n{name}:")
        current_name = name
    print(f"  Reg No: {reg_no or 'None':<10} | Match: {match_type or 'No Match':<15} | Count: {count}")

# Test what SHOULD happen with proper matching
print("\n\n=== What SHOULD happen with fixed matching ===")
print("LR: 'S NOTARO LIMITED' (00845344) → CH: 'S. NOTARO LIMITED' (00845344)")
print("  - Should match as Tier 1 (Name+Number) after normalization")
print("  - Both names normalize to 'SNOTARO'")
print("  - Numbers match exactly")
print("\nLR: 'S NOTARO LIMITED' (no number) → CH: 'S. NOTARO LIMITED' (00845344)")
print("  - Should match as Tier 3 (Name only) after normalization")
print("  - Both names normalize to 'SNOTARO'")

cursor.close()
conn.close()