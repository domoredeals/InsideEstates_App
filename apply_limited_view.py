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
    print("Applying limited view (last 3 months only)...")
    
    # Read and execute SQL
    with open('create_limited_view.sql', 'r') as f:
        sql = f.read()
    
    cursor.execute(sql)
    conn.commit()
    
    print("✅ Limited ownership view created successfully!")
    
    # Test the view
    cursor.execute("SELECT COUNT(*) FROM v_ownership_history")
    count = cursor.fetchone()[0]
    print(f"\nView now contains {count:,} rows (last 3 months only)")
    
    # Check some sample data
    cursor.execute("""
        SELECT 
            ch_match_type,
            COUNT(*) as count,
            COUNT(CASE WHEN ch_company_name IS NOT NULL THEN 1 END) as with_name
        FROM v_ownership_history
        GROUP BY ch_match_type
        ORDER BY count DESC
        LIMIT 5
    """)
    
    print("\nSample data by match type:")
    print(f"{'Match Type':<20} {'Count':<10} {'With Name'}")
    print("-" * 45)
    for row in cursor.fetchall():
        match_type = row[0] if row[0] else 'NULL'
        print(f"{match_type:<20} {row[1]:<10,} {row[2]:,}")
    
    print("""
✅ Success! The view now defaults to showing only the last 3 months of data.

This prevents memory issues. To query older data, use specific WHERE clauses:
- WHERE title_number = 'ABC123'
- WHERE ch_matched_number = '13051169'
- WHERE file_month >= '2024-01-01'
""")

except Exception as e:
    print(f"❌ Error: {e}")
    conn.rollback()
    
cursor.close()
conn.close()