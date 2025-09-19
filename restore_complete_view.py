#!/usr/bin/env python3
"""
Restore complete v_ownership_history view with all fields
"""

import psycopg2
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))
from config.postgresql_config import POSTGRESQL_CONFIG

print("Restoring complete v_ownership_history view with all fields...")

try:
    conn = psycopg2.connect(**POSTGRESQL_CONFIG)
    cursor = conn.cursor()
    
    # Read the SQL file
    with open('create_complete_ownership_view.sql', 'r') as f:
        sql_content = f.read()
    
    # Execute the SQL
    cursor.execute(sql_content)
    conn.commit()
    
    print("✅ Successfully created complete v_ownership_history view!")
    
    # Verify all expected fields exist
    expected_fields = [
        "date_proprietor_added_yyyy_mm",
        "date_proprietor_added_yyyy",
        "date_proprietor_added_yyyy_q",
        "ch_matched_number",
        "ch_country_of_origin",
        "ch_reg_address_line1",
        "ch_reg_address_line2",
        "ch_reg_address_post_town",
        "ch_reg_address_county",
        "ch_reg_address_country",
        "ch_reg_address_postcode",
        "ch_accounts_next_due_date",
        "ch_accounts_last_made_up_date",
        "ch_accounts_category",
        "match_quality_description",
        "change_description",
        "ownership_status",
        "ownership_duration_months"
    ]
    
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'v_ownership_history'
        AND column_name = ANY(%s)
        ORDER BY column_name
    """, (expected_fields,))
    
    found_fields = [row[0] for row in cursor.fetchall()]
    print(f"\nConfirmed - {len(found_fields)} critical fields are now available:")
    for field in found_fields:
        print(f"  ✓ {field}")
    
    # Check total field count
    cursor.execute("""
        SELECT COUNT(*) 
        FROM information_schema.columns 
        WHERE table_name = 'v_ownership_history'
    """)
    total_fields = cursor.fetchone()[0]
    print(f"\nTotal fields in view: {total_fields}")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)