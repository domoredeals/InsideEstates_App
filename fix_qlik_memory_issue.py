#!/usr/bin/env python3
"""
Fix Qlik memory issue by creating optimized views
"""

import psycopg2
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))
from config.postgresql_config import POSTGRESQL_CONFIG

print("Fixing Qlik memory issue...")

try:
    conn = psycopg2.connect(**POSTGRESQL_CONFIG)
    cursor = conn.cursor()
    
    # First check how much data we're dealing with
    cursor.execute("SELECT COUNT(*) FROM land_registry_data")
    total_rows = cursor.fetchone()[0]
    print(f"\nTotal rows in land_registry_data: {total_rows:,}")
    
    # Check how many rows the view would generate
    cursor.execute("""
        SELECT 
            COUNT(CASE WHEN proprietor_1_name IS NOT NULL THEN 1 END) +
            COUNT(CASE WHEN proprietor_2_name IS NOT NULL THEN 1 END) +
            COUNT(CASE WHEN proprietor_3_name IS NOT NULL THEN 1 END) +
            COUNT(CASE WHEN proprietor_4_name IS NOT NULL THEN 1 END) as total_proprietors
        FROM land_registry_data
    """)
    total_proprietors = cursor.fetchone()[0]
    print(f"Total proprietor records that would be in view: {total_proprietors:,}")
    
    if total_proprietors > 10000000:  # If more than 10 million
        print("\n⚠️  WARNING: View would contain over 10 million rows!")
        print("This is causing the Qlik memory error.")
        
    # Create the limited view
    print("\nCreating limited view for Qlik...")
    with open('create_limited_ownership_view.sql', 'r') as f:
        sql_content = f.read()
    
    cursor.execute(sql_content)
    conn.commit()
    
    print("✅ Created v_ownership_history_limited (latest month only)")
    print("✅ Created get_ownership_history() function for filtered queries")
    
    # Check limited view size
    cursor.execute("SELECT COUNT(*) FROM v_ownership_history_limited")
    limited_rows = cursor.fetchone()[0]
    print(f"\nLimited view contains: {limited_rows:,} rows (much more manageable!)")
    
    print("\n📝 QLIK INTEGRATION RECOMMENDATIONS:")
    print("1. Use v_ownership_history_limited for general queries (latest month only)")
    print("2. Always add WHERE clauses to filter data:")
    print("   - WHERE postcode = 'SW1A 1AA'")
    print("   - WHERE ch_matched_number = '12345678'")
    print("   - WHERE title_number = 'ABC123'")
    print("3. For specific queries, use the function:")
    print("   SELECT * FROM get_ownership_history(p_company_number := '12345678')")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)