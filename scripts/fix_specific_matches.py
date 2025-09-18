#!/usr/bin/env python3
"""
Fix specific companies that should have been matched
"""

import psycopg2
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

# Companies to fix
fixes = [
    # (LR Name, LR Reg No, CH Name, CH Number, Match Type)
    ("HNE FOODS LTD", "16424988", "HNE FOODS LTD", "16424988", "Name+Number"),
    ("T&D HOLDINGS LIMITED", "13503412", "T&D HOLDINGS LIMITED", "13503412", "Name+Number"),
    ("MANNING PROPERTY RENTALS LIMITED", "15952146", "MANNING PROPERTY RENTALS LIMITED", "15952146", "Name+Number"),
    # AL RAYAN BANK with correct number
    ("AL RAYAN BANK PLC", "4483430", "AL RAYAN BANK PLC", "04483430", "Name+Number"),
    ("AL RAYAN BANK PLC", "04483430", "AL RAYAN BANK PLC", "04483430", "Name+Number"),
]

print("=== Fixing Specific Company Matches ===\n")

for lr_name, lr_reg_no, ch_name, ch_number, match_type in fixes:
    print(f"\nFixing: {lr_name} ({lr_reg_no})")
    
    # Find all records with this combination
    cursor.execute("""
        SELECT lr.id
        FROM land_registry_data lr
        LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
        WHERE lr.proprietor_1_name = %s
        AND lr.company_1_reg_no = %s
        AND (m.ch_match_type_1 = 'No_Match' OR m.ch_match_type_1 IS NULL)
    """, (lr_name, lr_reg_no))
    
    record_ids = cursor.fetchall()
    
    if record_ids:
        print(f"  Found {len(record_ids)} records to update")
        
        for (record_id,) in record_ids:
            try:
                # Insert or update the match
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
                """, (record_id, ch_name, ch_number, match_type, 1.0))
                
            except Exception as e:
                print(f"  ❌ Error updating record {record_id}: {e}")
                conn.rollback()
                continue
        
        conn.commit()
        print(f"  ✅ Updated {len(record_ids)} records")
    else:
        print(f"  ℹ️  No unmatched records found for this combination")

# Special handling for S NOTARO LIMITED (missing reg number)
print(f"\n\nSpecial case: S NOTARO LIMITED")
cursor.execute("""
    SELECT company_name, company_number, company_status
    FROM companies_house_data
    WHERE company_name LIKE 'S NOTARO%'
    ORDER BY company_name
""")

s_notaro_matches = cursor.fetchall()
if s_notaro_matches:
    print(f"Found {len(s_notaro_matches)} potential matches in CH:")
    for name, number, status in s_notaro_matches:
        print(f"  {name} ({number}) - {status}")
else:
    print("No matches found for S NOTARO in Companies House")

# Summary
print("\n\n=== Summary ===")
cursor.execute("""
    SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN ch_match_type_1 != 'No_Match' THEN 1 END) as matched
    FROM land_registry_ch_matches
    WHERE updated_at > CURRENT_TIMESTAMP - INTERVAL '5 minutes'
""")

result = cursor.fetchone()
if result:
    total, matched = result
    print(f"Records updated in last 5 minutes: {total}")
    print(f"Successfully matched: {matched}")

cursor.close()
conn.close()