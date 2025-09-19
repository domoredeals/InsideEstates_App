#!/usr/bin/env python3
"""
Restore date fields to v_ownership_history view
"""

import psycopg2
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))
from config.postgresql_config import POSTGRESQL_CONFIG

print("Restoring date fields to v_ownership_history view...")

try:
    conn = psycopg2.connect(**POSTGRESQL_CONFIG)
    cursor = conn.cursor()
    
    # Read the SQL file
    with open('add_date_fields_to_view.sql', 'r') as f:
        sql_content = f.read()
    
    # Execute the SQL
    cursor.execute(sql_content)
    conn.commit()
    
    print("✅ Successfully restored date fields to v_ownership_history view!")
    
    # Verify the fields exist
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'v_ownership_history'
        AND column_name IN ('date_proprietor_added_yyyy_mm', 
                           'date_proprietor_added_yyyy', 
                           'date_proprietor_added_yyyy_q')
        ORDER BY column_name
    """)
    
    restored_fields = [row[0] for row in cursor.fetchall()]
    if restored_fields:
        print("\nConfirmed - the following date fields are now available:")
        for field in restored_fields:
            print(f"  - {field}")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)