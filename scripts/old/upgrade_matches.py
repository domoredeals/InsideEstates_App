#!/usr/bin/env python3
"""
Upgrade Number-only matches to Name+Number where both actually match
"""

import psycopg2
import re
import sys
from pathlib import Path
from tqdm import tqdm

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

def normalize_company_name_fixed(name):
    """Fixed normalization that REMOVES suffixes"""
    if not name or name.strip() == '':
        return ""
    
    name = str(name).upper().strip()
    name = name.replace(' AND ', ' ').replace(' & ', ' ')
    
    suffix_pattern = r'\s*(LIMITED LIABILITY PARTNERSHIP|LIMITED|COMPANY|LTD\.|LLP|LTD|PLC|CO\.|CO).*$'
    name = re.sub(suffix_pattern, '', name)
    
    name = ''.join(char for char in name if char.isalnum())
    
    return name

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

print("=== Upgrading Number-only matches to Name+Number where appropriate ===\n")

# First, count how many Number-only matches we have
cursor.execute("""
    SELECT COUNT(*)
    FROM land_registry_ch_matches
    WHERE ch_match_type_1 = 'Number'
""")
total_number_matches = cursor.fetchone()[0]
print(f"Total Number-only matches: {total_number_matches:,}")

# Get a sample to test
print("\nChecking sample of Number matches that could be upgraded...")

cursor.execute("""
    SELECT 
        m.id,
        lr.proprietor_1_name,
        lr.company_1_reg_no,
        m.ch_matched_name_1,
        m.ch_matched_number_1
    FROM land_registry_ch_matches m
    JOIN land_registry_data lr ON m.id = lr.id
    WHERE m.ch_match_type_1 = 'Number'
    AND lr.proprietor_1_name IS NOT NULL
    AND m.ch_matched_name_1 IS NOT NULL
    LIMIT 10000
""")

upgradeable = 0
sample_upgrades = []

for record in cursor.fetchall():
    record_id, lr_name, lr_number, ch_name, ch_number = record
    
    # Normalize both names
    lr_norm = normalize_company_name_fixed(lr_name)
    ch_norm = normalize_company_name_fixed(ch_name)
    
    # If normalized names match, this can be upgraded to Name+Number
    if lr_norm == ch_norm:
        upgradeable += 1
        if len(sample_upgrades) < 10:
            sample_upgrades.append({
                'id': record_id,
                'lr_name': lr_name,
                'ch_name': ch_name,
                'norm': lr_norm
            })

print(f"\nFrom sample of 10,000: {upgradeable} can be upgraded to Name+Number ({upgradeable/100:.1f}%)")

if sample_upgrades:
    print("\nExamples of upgradeable matches:")
    for ex in sample_upgrades[:5]:
        print(f"  LR: '{ex['lr_name']}' → CH: '{ex['ch_name']}' (both normalize to '{ex['norm']}')")

# Now do the actual upgrade
proceed = True  # Auto-proceed
if proceed:
    print("\nUpgrading matches...")
    
    # Use a more efficient approach - do it in batches
    batch_size = 10000
    offset = 0
    total_upgraded = 0
    
    with tqdm(total=total_number_matches, desc="Processing") as pbar:
        while offset < total_number_matches:
            cursor.execute("""
                WITH upgradeable AS (
                    SELECT 
                        m.id,
                        lr.proprietor_1_name,
                        m.ch_matched_name_1
                    FROM land_registry_ch_matches m
                    JOIN land_registry_data lr ON m.id = lr.id
                    WHERE m.ch_match_type_1 = 'Number'
                    AND lr.proprietor_1_name IS NOT NULL
                    AND m.ch_matched_name_1 IS NOT NULL
                    ORDER BY m.id
                    LIMIT %s OFFSET %s
                )
                SELECT * FROM upgradeable
            """, (batch_size, offset))
            
            batch = cursor.fetchall()
            if not batch:
                break
                
            batch_upgrades = 0
            for record_id, lr_name, ch_name in batch:
                if normalize_company_name_fixed(lr_name) == normalize_company_name_fixed(ch_name):
                    cursor.execute("""
                        UPDATE land_registry_ch_matches
                        SET ch_match_type_1 = 'Name+Number',
                            ch_match_confidence_1 = 1.0,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (record_id,))
                    batch_upgrades += 1
            
            conn.commit()
            total_upgraded += batch_upgrades
            offset += batch_size
            pbar.update(len(batch))
            
            if batch_upgrades > 0:
                print(f"\nBatch complete: upgraded {batch_upgrades} matches")
    
    print(f"\n✅ Upgrade complete!")
    print(f"Total matches upgraded from Number to Name+Number: {total_upgraded:,}")
    
    # Show new statistics
    cursor.execute("""
        SELECT 
            ch_match_type_1,
            COUNT(*) as count
        FROM land_registry_ch_matches
        WHERE ch_match_type_1 IN ('Name+Number', 'Number')
        GROUP BY ch_match_type_1
    """)
    
    print("\nUpdated match type counts:")
    for match_type, count in cursor.fetchall():
        print(f"  {match_type}: {count:,}")

cursor.close()
conn.close()