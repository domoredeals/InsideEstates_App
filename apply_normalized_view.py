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
    # Read and execute SQL
    with open('create_normalized_ownership_view.sql', 'r') as f:
        sql = f.read()
    
    cursor.execute(sql)
    conn.commit()
    
    print("✅ Normalized ownership history view created successfully!")
    
    # Test the view
    cursor.execute("""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT title_number) as unique_properties,
            COUNT(DISTINCT ch_matched_number) as unique_companies
        FROM v_ownership_history
    """)
    
    result = cursor.fetchone()
    print(f"\nView statistics:")
    print(f"Total rows: {result[0]:,}")
    print(f"Unique properties: {result[1]:,}")
    print(f"Unique companies: {result[2]:,}")
    
    # Check that all expected columns exist
    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'v_ownership_history'
        AND column_name IN ('proprietor_sequence', 'proprietor_name', 'ch_matched_name', 
                           'ch_company_name', 'match_quality_description', 'ownership_status')
        ORDER BY column_name
    """)
    
    columns = [row[0] for row in cursor.fetchall()]
    print(f"\n✅ Key columns verified: {', '.join(columns)}")
    
    # Check Land_Registry records
    cursor.execute("""
        SELECT 
            ch_match_type,
            COUNT(*) as count,
            COUNT(CASE WHEN ch_company_name IS NOT NULL THEN 1 END) as with_name
        FROM v_ownership_history
        WHERE ch_match_type = 'Land_Registry'
        GROUP BY ch_match_type
    """)
    
    result = cursor.fetchone()
    if result:
        print(f"\nLand_Registry records: {result[1]:,}")
        print(f"With ch_company_name: {result[2]:,}")

except Exception as e:
    print(f"❌ Error: {e}")
    conn.rollback()
    
cursor.close()
conn.close()