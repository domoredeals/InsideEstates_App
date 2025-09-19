#!/usr/bin/env python3
"""
Check if ST181927 was updated in the latest run
"""

import psycopg2
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

print("=== Checking ST181927 update status ===\n")

# Get the full details
cursor.execute("""
    SELECT 
        lr.id,
        lr.title_number,
        lr.proprietor_1_name,
        lr.company_1_reg_no,
        m.ch_match_type_1,
        m.ch_matched_name_1,
        m.ch_matched_number_1,
        m.updated_at
    FROM land_registry_data lr
    JOIN land_registry_ch_matches m ON lr.id = m.id
    WHERE lr.title_number = 'ST181927'
""")

result = cursor.fetchone()
if result:
    record_id, title, name, reg_no, match_type, ch_name, ch_number, updated = result
    
    print(f"Title: {title}")
    print(f"Record ID: {record_id}")
    print(f"Proprietor: {name}")
    print(f"Reg No: {reg_no}")
    print(f"Match Status: {match_type}")
    print(f"CH Matched Name: {ch_name}")
    print(f"CH Matched Number: {ch_number}")
    print(f"Last Updated: {updated}")
    
    # Check if it was updated today
    today = datetime.now().date()
    if updated and updated.date() == today:
        print("\n✅ Updated TODAY in the latest run!")
    else:
        print("\n❌ NOT updated in today's run")
        print("This suggests it wasn't processed even though it should have been")
        
# Check how many records were updated today around this ID
print("\n=== Records updated today near this ID ===")
cursor.execute("""
    SELECT 
        COUNT(*) as count,
        MIN(id) as min_id, 
        MAX(id) as max_id
    FROM land_registry_ch_matches
    WHERE updated_at::date = CURRENT_DATE
    AND id BETWEEN 15970000 AND 15973000
""")

count, min_id, max_id = cursor.fetchone()
print(f"Records updated today in range 15970000-15973000: {count}")
if count > 0:
    print(f"ID range updated: {min_id} to {max_id}")

cursor.close()
conn.close()