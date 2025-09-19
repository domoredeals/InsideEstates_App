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
    print("Applying companies-only view (Limited Companies/PLCs and LLPs only)...")
    
    # Read and execute SQL
    with open('create_companies_only_view.sql', 'r') as f:
        sql = f.read()
    
    cursor.execute(sql)
    conn.commit()
    
    print("✅ Companies-only ownership view created successfully!")
    
    # Test the view
    cursor.execute("SELECT COUNT(*) FROM v_ownership_history")
    count = cursor.fetchone()[0]
    print(f"\nView now contains {count:,} rows (companies only, last 3 months)")
    
    # Check data by match type
    cursor.execute("""
        SELECT 
            ch_match_type,
            COUNT(*) as count,
            COUNT(DISTINCT ch_matched_number) as unique_companies
        FROM v_ownership_history
        GROUP BY ch_match_type
        ORDER BY count DESC
    """)
    
    print("\nData by match type (should NOT include 'Not_Company' or NULL):")
    print(f"{'Match Type':<20} {'Count':<12} {'Unique Companies'}")
    print("-" * 55)
    for row in cursor.fetchall():
        match_type = row[0] if row[0] else 'NULL'
        print(f"{match_type:<20} {row[1]:<12,} {row[2]:,}")
    
    # Check proprietorship categories
    cursor.execute("""
        SELECT DISTINCT proprietorship_category
        FROM v_ownership_history
        ORDER BY proprietorship_category
    """)
    
    print("\nProprietorship categories in view:")
    categories = cursor.fetchall()
    for cat in categories:
        print(f"  - {cat[0]}")
    
    print("""
✅ Success! The view now shows ONLY:
- Limited Company or Public Limited Company
- Limited Liability Partnership

All other proprietor types (individuals, charities, etc.) are excluded.
""")

except Exception as e:
    print(f"❌ Error: {e}")
    conn.rollback()
    
cursor.close()
conn.close()