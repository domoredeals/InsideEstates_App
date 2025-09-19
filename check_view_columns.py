#!/usr/bin/env python3
import psycopg2
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent))
from config.postgresql_config import POSTGRESQL_CONFIG

# Connect to database
conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

try:
    # Get column information for the view
    cursor.execute("""
        SELECT column_name, data_type, ordinal_position
        FROM information_schema.columns
        WHERE table_name = 'v_ownership_history'
        AND table_schema = 'public'
        ORDER BY ordinal_position
    """)
    
    columns = cursor.fetchall()
    
    print("Columns in v_ownership_history view:")
    print(f"{'Position':<10} {'Column Name':<40} {'Data Type':<20}")
    print("-" * 80)
    
    for col in columns:
        print(f"{col[2]:<10} {col[0]:<40} {col[1]:<20}")
    
    print(f"\nTotal columns: {len(columns)}")
    
    # Check if proprietor_sequence exists
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_name = 'v_ownership_history'
        AND column_name = 'proprietor_sequence'
    """)
    
    if cursor.fetchone()[0] == 0:
        print("\n❌ 'proprietor_sequence' column does NOT exist in the view")
    else:
        print("\n✅ 'proprietor_sequence' column exists in the view")

except Exception as e:
    print(f"Error: {e}")
    
cursor.close()
conn.close()