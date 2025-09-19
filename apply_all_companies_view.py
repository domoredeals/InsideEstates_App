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
    print("Applying ALL companies view (no date restrictions)...")
    
    # Read and execute SQL
    with open('create_all_companies_view.sql', 'r') as f:
        sql = f.read()
    
    cursor.execute(sql)
    conn.commit()
    
    print("✅ All companies ownership view created successfully!")
    
    # Get count but with a limit to avoid loading all data
    cursor.execute("""
        SELECT COUNT(*) FROM (
            SELECT 1 FROM v_ownership_history LIMIT 1000000
        ) t
    """)
    count = cursor.fetchone()[0]
    
    if count >= 1000000:
        print(f"\n⚠️  View contains more than 1,000,000 rows (companies only, all dates)")
    else:
        print(f"\nView contains {count:,} rows (companies only, all dates)")
    
    # Check date range
    cursor.execute("""
        SELECT 
            MIN(file_month) as earliest,
            MAX(file_month) as latest,
            COUNT(DISTINCT file_month) as month_count
        FROM land_registry_data lr
        JOIN land_registry_ch_matches m ON lr.id = m.id
        WHERE 
            (lr.proprietorship_1_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership') AND m.ch_match_type_1 != 'Not_Company') OR
            (lr.proprietorship_2_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership') AND m.ch_match_type_2 != 'Not_Company') OR
            (lr.proprietorship_3_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership') AND m.ch_match_type_3 != 'Not_Company') OR
            (lr.proprietorship_4_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership') AND m.ch_match_type_4 != 'Not_Company')
    """)
    
    result = cursor.fetchone()
    if result:
        print(f"\nData range in view:")
        print(f"  Earliest: {result[0]}")
        print(f"  Latest: {result[1]}")
        print(f"  Months covered: {result[2]}")
    
    print("""
✅ Success! The view now shows ALL historical data for:
- Limited Company or Public Limited Company
- Limited Liability Partnership

⚠️  WARNING: This is a large dataset. Always use WHERE clauses:
- WHERE title_number = 'ABC123'
- WHERE ch_matched_number = '13051169'
- WHERE postcode = 'SW1A 1AA'
- WHERE file_month >= '2024-01-01'

Without filters, queries may cause memory issues!
""")

except Exception as e:
    print(f"❌ Error: {e}")
    conn.rollback()
    
cursor.close()
conn.close()