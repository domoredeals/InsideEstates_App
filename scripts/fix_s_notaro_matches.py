#!/usr/bin/env python3
"""
Fix S NOTARO LIMITED matches to demonstrate proper tier matching
"""

import psycopg2
import re
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

def normalize_company_number(number):
    """Normalize company number - pad with zeros to 8 digits"""
    if not number:
        return None
    
    # Remove any non-numeric characters
    number = re.sub(r'[^0-9]', '', str(number))
    
    # Pad with leading zeros to make it 8 digits
    if number and len(number) < 8:
        number = number.zfill(8)
    
    return number if number else None

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

print("=== Fixing S NOTARO LIMITED Matches ===\n")

# The company in CH
ch_name = "S. NOTARO LIMITED"
ch_number = "00845344"

# Fix different tiers
fixes = [
    # Tier 1: Name+Number (normalized names match AND numbers match)
    ("S NOTARO LIMITED", "00845344", "Name+Number", 1.0),
    ("S. NOTARO LIMITED", "00845344", "Name+Number", 1.0),
    
    # Tier 2: Number only (numbers match but names have dots)
    ("S NOTARO LIMITED", "845344", "Number", 0.9),
    ("S NOTARO LIMITED", "0845344", "Number", 0.9),
    ("S.NOTARO LIMITED", "845344", "Number", 0.9),
    ("S. NOTARO LIMITED", "845344", "Number", 0.9),
    
    # Tier 3: Name only (no number but names normalize the same)
    ("S NOTARO LIMITED", None, "Name", 0.7),
    ("S. NOTARO LIMITED", None, "Name", 0.7),
]

for lr_name, lr_reg_no, match_type, confidence in fixes:
    print(f"\nFixing: {lr_name} ({lr_reg_no or 'No RegNo'}) → {match_type}")
    
    # Build query based on whether we have a reg number
    if lr_reg_no:
        # For Number and Name+Number matches
        norm_number = normalize_company_number(lr_reg_no)
        
        cursor.execute("""
            SELECT lr.id
            FROM land_registry_data lr
            LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE lr.proprietor_1_name = %s
            AND lr.company_1_reg_no = %s
            AND (m.ch_match_type_1 = 'No_Match' OR m.ch_match_type_1 IS NULL)
        """, (lr_name, lr_reg_no))
    else:
        # For Name only matches
        cursor.execute("""
            SELECT lr.id
            FROM land_registry_data lr
            LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE lr.proprietor_1_name = %s
            AND (lr.company_1_reg_no IS NULL OR lr.company_1_reg_no = '')
            AND (m.ch_match_type_1 = 'No_Match' OR m.ch_match_type_1 IS NULL)
        """, (lr_name,))
    
    record_ids = cursor.fetchall()
    
    if record_ids:
        print(f"  Found {len(record_ids)} records to update")
        
        for (record_id,) in record_ids:
            try:
                cursor.execute("""
                    INSERT INTO land_registry_ch_matches (
                        id,
                        ch_matched_name_1, ch_matched_number_1, 
                        ch_match_type_1, ch_match_confidence_1
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        ch_matched_name_1 = EXCLUDED.ch_matched_name_1,
                        ch_matched_number_1 = EXCLUDED.ch_matched_number_1,
                        ch_match_type_1 = EXCLUDED.ch_match_type_1,
                        ch_match_confidence_1 = EXCLUDED.ch_match_confidence_1,
                        updated_at = CURRENT_TIMESTAMP
                """, (record_id, ch_name, ch_number, match_type, confidence))
                
            except Exception as e:
                print(f"  ❌ Error updating record {record_id}: {e}")
                conn.rollback()
                continue
        
        conn.commit()
        print(f"  ✅ Updated {len(record_ids)} records as {match_type} matches")
    else:
        print(f"  ℹ️  No unmatched records found for this combination")

# Summary
print("\n\n=== Summary of S NOTARO LIMITED Matches ===")
cursor.execute("""
    SELECT 
        ch_match_type_1,
        COUNT(*) as count,
        AVG(ch_match_confidence_1) as avg_confidence
    FROM land_registry_ch_matches m
    JOIN land_registry_data lr ON m.id = lr.id
    WHERE lr.proprietor_1_name LIKE '%NOTARO LIMITED'
    AND ch_matched_name_1 = 'S. NOTARO LIMITED'
    GROUP BY ch_match_type_1
    ORDER BY avg_confidence DESC
""")

results = cursor.fetchall()
total = 0
for match_type, count, confidence in results:
    print(f"{match_type}: {count} records (confidence {confidence:.1f})")
    total += count

print(f"\nTotal S NOTARO LIMITED records now matched: {total}")
print("\nThis demonstrates how the tier matching SHOULD work:")
print("- Exact name+number → Tier 1 (confidence 1.0)")  
print("- Number variations → Tier 2 (confidence 0.9)")
print("- Name only matches → Tier 3 (confidence 0.7)")

cursor.close()
conn.close()