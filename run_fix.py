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
    # Read SQL file
    with open('fix_ch_company_name.sql', 'r') as f:
        sql = f.read()

    # Execute
    cursor.execute(sql)
    conn.commit()

    print("View updated successfully!")
    
    # Show results
    results = cursor.fetchall()
    print("\nResults by match type:")
    print(f"{'Match Type':<20} {'Total':<10} {'With Name':<12} {'Without Name'}")
    print("-" * 60)
    for row in results:
        match_type = row[0] if row[0] else 'NULL'
        print(f"{match_type:<20} {row[1]:<10,} {row[2]:<12,} {row[3]:,}")

except Exception as e:
    print(f"Error: {e}")
    conn.rollback()
    
cursor.close()
conn.close()