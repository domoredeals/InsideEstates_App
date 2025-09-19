#!/usr/bin/env python3
"""
Incremental Land Registry to Companies House Matcher
====================================================
Only matches Land Registry records that don't already have matches

Key Features:
- Only processes unmatched records (much faster for updates)
- Uses same proven normalization and matching logic as production script
- 4-tier matching: Name+Number (1.0), Number (0.9), Name (0.7), Previous Name (0.5)
- Shows progress and match statistics
"""

import psycopg2
from psycopg2.extras import execute_values
import re
import sys
from pathlib import Path
from tqdm import tqdm
from datetime import datetime

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

def normalize_company_name(name):
    """PROVEN normalization that REMOVES suffixes"""
    if not name or name.strip() == '':
        return ""
    
    name = str(name).upper().strip()
    name = name.replace(' AND ', ' ').replace(' & ', ' ')
    
    # CRITICAL FIX: Remove suffixes AND anything after them
    suffix_pattern = r'\s*(LIMITED LIABILITY PARTNERSHIP|LIMITED|COMPANY|LTD\.|LLP|LTD|PLC|CO\.|CO).*$'
    name = re.sub(suffix_pattern, '', name)
    
    # Keep only alphanumeric
    name = ''.join(char for char in name if char.isalnum())
    
    return name

def normalize_company_number(number):
    """Normalize company registration numbers"""
    if not number or str(number).strip() == '':
        return ''
    
    number = str(number).strip().upper()
    number = re.sub(r'[^A-Z0-9]', '', number)
    
    # Handle Scottish numbers
    if number.startswith('SC'):
        return number
    
    # Pad regular numbers to 8 digits
    if number.isdigit():
        return number.zfill(8)
    
    return number

print("=== INCREMENTAL MATCHING SCRIPT ===")
print("This will only match Land Registry records that don't already have matches")
print(f"Started at: {datetime.now()}\n")

# Connect to database
conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

# Check how many records need matching
print("Step 1: Checking unmatched records...")
cursor.execute("""
    SELECT COUNT(DISTINCT lr.id)
    FROM land_registry_data lr
    WHERE NOT EXISTS (
        SELECT 1 FROM land_registry_ch_matches m WHERE m.id = lr.id
    )
""")
unmatched_count = cursor.fetchone()[0]

if unmatched_count == 0:
    print("✅ All records are already matched! Nothing to do.")
    cursor.close()
    conn.close()
    sys.exit(0)

print(f"Found {unmatched_count:,} unmatched Land Registry records\n")

# Load ALL Companies House data into memory (still needed for matching)
print("Step 2: Loading Companies House data...")
cursor.execute("SELECT COUNT(*) FROM companies_house_data")
ch_count = cursor.fetchone()[0]
print(f"Loading {ch_count:,} companies into memory...")

ch_by_number = {}
ch_by_name = {}
ch_previous_names = {}

cursor.execute("""
    SELECT company_number, company_name, 
           previous_name_1_name, previous_name_2_name, previous_name_3_name, 
           previous_name_4_name, previous_name_5_name
    FROM companies_house_data
""")

with tqdm(total=ch_count, desc="Loading CH data") as pbar:
    while True:
        batch = cursor.fetchmany(10000)
        if not batch:
            break
            
        for row in batch:
            ch_number = row[0]
            ch_name = row[1]
            
            # Store by company number
            if ch_number:
                norm_number = normalize_company_number(ch_number)
                if norm_number:
                    ch_by_number[norm_number] = (ch_name, ch_number)
            
            # Store by normalized name
            norm_name = normalize_company_name(ch_name)
            if norm_name:
                if norm_name not in ch_by_name:
                    ch_by_name[norm_name] = []
                ch_by_name[norm_name].append((ch_name, ch_number))
            
            # Store previous names
            for i in range(2, 7):
                prev_name = row[i]
                if prev_name:
                    norm_prev = normalize_company_name(prev_name)
                    if norm_prev:
                        if norm_prev not in ch_previous_names:
                            ch_previous_names[norm_prev] = []
                        ch_previous_names[norm_prev].append((ch_name, ch_number))
        
        pbar.update(len(batch))

print(f"✅ Loaded {len(ch_by_number):,} companies by number")
print(f"✅ Loaded {len(ch_by_name):,} unique normalized names\n")

# Process only unmatched records in batches
print("Step 3: Matching unmatched proprietors...")
batch_size = 5000
total_matched = 0
total_no_match = 0

# Query to get only unmatched records
query = """
    SELECT lr.id, 
           lr.proprietor_1_name, lr.company_1_reg_no,
           lr.proprietor_2_name, lr.company_2_reg_no,
           lr.proprietor_3_name, lr.company_3_reg_no,
           lr.proprietor_4_name, lr.company_4_reg_no
    FROM land_registry_data lr
    WHERE NOT EXISTS (
        SELECT 1 FROM land_registry_ch_matches m WHERE m.id = lr.id
    )
    ORDER BY lr.id
    LIMIT %s OFFSET %s
"""

insert_query = """
    INSERT INTO land_registry_ch_matches (
        id,
        ch_matched_name_1, ch_matched_number_1, ch_match_type_1, ch_match_confidence_1,
        ch_matched_name_2, ch_matched_number_2, ch_match_type_2, ch_match_confidence_2,
        ch_matched_name_3, ch_matched_number_3, ch_match_type_3, ch_match_confidence_3,
        ch_matched_name_4, ch_matched_number_4, ch_match_type_4, ch_match_confidence_4,
        updated_at
    ) VALUES %s
"""

offset = 0
with tqdm(total=unmatched_count, desc="Matching records") as pbar:
    while offset < unmatched_count:
        # Fetch batch of unmatched records
        cursor.execute(query, (batch_size, offset))
        batch = cursor.fetchall()
        
        if not batch:
            break
        
        batch_values = []
        for row in batch:
            record_id = row[0]
            match_values = [record_id]
            
            # Process each proprietor (1-4)
            for i in range(1, 5):
                prop_name = row[i*2 - 1]
                prop_reg_no = row[i*2]
                
                if not prop_name or prop_name.strip() == '':
                    match_values.extend([None, None, None, None])
                    continue
                
                # Try to match
                matched = False
                
                # 1. Try Name+Number match
                if prop_reg_no:
                    norm_number = normalize_company_number(prop_reg_no)
                    if norm_number and norm_number in ch_by_number:
                        ch_name, ch_number = ch_by_number[norm_number]
                        
                        # Verify name similarity
                        norm_prop_name = normalize_company_name(prop_name)
                        norm_ch_name = normalize_company_name(ch_name)
                        
                        if norm_prop_name == norm_ch_name:
                            # Tier 1: Name+Number match
                            match_values.extend([ch_name, ch_number, 'Name+Number', 1.0])
                            total_matched += 1
                            matched = True
                        else:
                            # Tier 2: Number only match
                            match_values.extend([ch_name, ch_number, 'Number', 0.9])
                            total_matched += 1
                            matched = True
                
                # 2. Try Name only match
                if not matched:
                    norm_name = normalize_company_name(prop_name)
                    
                    if norm_name and norm_name in ch_by_name:
                        # Get the first match
                        ch_name, ch_number = ch_by_name[norm_name][0]
                        match_values.extend([ch_name, ch_number, 'Name', 0.7])
                        total_matched += 1
                        matched = True
                
                # 3. Try Previous name match
                if not matched:
                    norm_name = normalize_company_name(prop_name)
                    
                    if norm_name and norm_name in ch_previous_names:
                        ch_name, ch_number = ch_previous_names[norm_name][0]
                        match_values.extend([ch_name, ch_number, 'Previous_Name', 0.5])
                        total_matched += 1
                        matched = True
                
                # 4. No match
                if not matched:
                    match_values.extend([None, None, 'No_Match', 0.0])
                    total_no_match += 1
            
            # Add timestamp
            match_values.append(datetime.now())
            batch_values.append(match_values)
        
        # Insert batch
        if batch_values:
            execute_values(cursor, insert_query, batch_values)
            conn.commit()
        
        pbar.update(len(batch))
        offset += batch_size

print(f"\n✅ INCREMENTAL MATCHING COMPLETE!")
print(f"New proprietors matched: {total_matched:,}")
print(f"New records with no match: {total_no_match:,}")
if total_matched + total_no_match > 0:
    print(f"Match rate for new records: {(total_matched / (total_matched + total_no_match) * 100):.2f}%")

# Show overall statistics
cursor.execute("""
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN ch_match_type_1 != 'No_Match' OR ch_match_type_2 != 'No_Match' 
                   OR ch_match_type_3 != 'No_Match' OR ch_match_type_4 != 'No_Match' 
              THEN 1 END) as matched_records
    FROM land_registry_ch_matches
""")
total, matched = cursor.fetchone()
print(f"\nOverall database statistics:")
print(f"Total Land Registry records with matches: {total:,}")
print(f"Total with at least one match: {matched:,}")
print(f"Overall match rate: {(matched / total * 100):.2f}%")

cursor.close()
conn.close()

print(f"\nCompleted at: {datetime.now()}")
print("\n🎉 Incremental matching complete!")