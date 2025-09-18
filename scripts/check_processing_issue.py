#!/usr/bin/env python3
"""
Check if there's an issue with the no_match_only processing logic
"""

import psycopg2
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

print("=== Checking No_Match processing logic ===\n")

# First, let's see what the no_match_only query would return
print("1. Testing the no_match_only query logic:")

# This is what the production script uses to find No_Match records
cursor.execute("""
    SELECT COUNT(*)
    FROM land_registry_data lr
    JOIN land_registry_ch_matches m ON lr.id = m.id
    WHERE m.ch_match_type_1 = 'No_Match'
    AND lr.proprietor_1_name = 'ROWANFIELD OAK LTD'
""")

count = cursor.fetchone()[0]
print(f"   ROWANFIELD OAK LTD records with ch_match_type_1 = 'No_Match': {count}")

# Check if it's looking at ALL match type columns
cursor.execute("""
    SELECT COUNT(*)
    FROM land_registry_data lr
    JOIN land_registry_ch_matches m ON lr.id = m.id
    WHERE (
        m.ch_match_type_1 = 'No_Match' OR
        m.ch_match_type_2 = 'No_Match' OR  
        m.ch_match_type_3 = 'No_Match' OR
        m.ch_match_type_4 = 'No_Match'
    )
    AND lr.proprietor_1_name = 'ROWANFIELD OAK LTD'
""")

count_all = cursor.fetchone()[0]
print(f"   ROWANFIELD OAK LTD with ANY No_Match: {count_all}")

# Check the actual match data for one ROWANFIELD record
print("\n2. Checking actual match data for ROWANFIELD OAK LTD:")
cursor.execute("""
    SELECT 
        lr.id,
        lr.proprietor_1_name,
        lr.company_1_reg_no,
        m.ch_match_type_1,
        m.ch_match_type_2,
        m.ch_match_type_3,
        m.ch_match_type_4,
        m.updated_at
    FROM land_registry_data lr
    JOIN land_registry_ch_matches m ON lr.id = m.id
    WHERE lr.proprietor_1_name = 'ROWANFIELD OAK LTD'
    AND lr.company_1_reg_no = '15483533'
    LIMIT 1
""")

result = cursor.fetchone()
if result:
    lr_id, name, reg_no, mt1, mt2, mt3, mt4, updated = result
    print(f"   ID: {lr_id}")
    print(f"   Match Types: 1={mt1}, 2={mt2}, 3={mt3}, 4={mt4}")
    print(f"   Last Updated: {updated}")
    
    # Check if this record would be selected by no_match_only mode
    would_be_selected = (mt1 == 'No_Match' or mt2 == 'No_Match' or 
                        mt3 == 'No_Match' or mt4 == 'No_Match')
    print(f"   Would be selected by no_match_only: {would_be_selected}")

# Now check if the matching logic in the script might have an issue
print("\n3. Potential issue diagnosis:")

# Check if maybe the script is only updating certain columns
cursor.execute("""
    SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN ch_match_type_1 != 'No_Match' THEN 1 END) as type1_matched,
        COUNT(CASE WHEN ch_match_type_2 != 'No_Match' THEN 1 END) as type2_matched,
        COUNT(CASE WHEN ch_match_type_3 != 'No_Match' THEN 1 END) as type3_matched,
        COUNT(CASE WHEN ch_match_type_4 != 'No_Match' THEN 1 END) as type4_matched
    FROM land_registry_ch_matches
    WHERE updated_at > '2025-09-18 11:00:00'  -- After the recent run
""")

result = cursor.fetchone()
if result:
    total, t1, t2, t3, t4 = result
    print(f"   Records updated in recent run: {total}")
    print(f"   Type 1 matches: {t1}")
    print(f"   Type 2 matches: {t2}")
    print(f"   Type 3 matches: {t3}")
    print(f"   Type 4 matches: {t4}")
    
print("\n4. Checking if script might be filtering wrong:")
# The script might be checking if ALL types are No_Match instead of ANY
cursor.execute("""
    SELECT COUNT(*)
    FROM land_registry_ch_matches
    WHERE ch_match_type_1 = 'No_Match'
    AND ch_match_type_2 = 'No_Match'
    AND ch_match_type_3 = 'No_Match'
    AND ch_match_type_4 = 'No_Match'
""")
all_no_match = cursor.fetchone()[0]
print(f"   Records where ALL 4 types are No_Match: {all_no_match:,}")

cursor.execute("""
    SELECT COUNT(*)
    FROM land_registry_ch_matches
    WHERE ch_match_type_1 = 'No_Match'
""")
type1_no_match = cursor.fetchone()[0]
print(f"   Records where type_1 is No_Match: {type1_no_match:,}")

cursor.close()
conn.close()