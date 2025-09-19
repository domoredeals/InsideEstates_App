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
    print("Applying optimized ownership view...")
    
    # Read and execute SQL
    with open('create_optimized_ownership_view.sql', 'r') as f:
        sql = f.read()
    
    cursor.execute(sql)
    conn.commit()
    
    print("✅ Optimized ownership view and function created successfully!")
    
    # Test the function with a specific title
    print("\nTesting function with sample query...")
    cursor.execute("""
        SELECT COUNT(*) 
        FROM get_ownership_history('ABC123', NULL, NULL, 100)
    """)
    count = cursor.fetchone()[0]
    print(f"Sample query returned {count} rows")
    
    # Test with company number
    cursor.execute("""
        SELECT COUNT(*) 
        FROM get_ownership_history(NULL, '13051169', NULL, 1000)
    """)
    count = cursor.fetchone()[0]
    print(f"Query by company number returned {count} rows")
    
    print("""
✅ Success! The view has been optimized.

IMPORTANT: Your ODBC queries must now include WHERE clauses to avoid memory issues:

Examples:
- WHERE title_number = 'ABC123'
- WHERE ch_matched_number = '13051169' 
- WHERE postcode = 'SW1A 1AA'

Or modify your application to use the function directly:
SELECT * FROM get_ownership_history('title', 'company', 'postcode', limit)
""")

except Exception as e:
    print(f"❌ Error: {e}")
    conn.rollback()
    
cursor.close()
conn.close()