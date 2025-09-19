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
    # Drop existing view
    cursor.execute("DROP VIEW IF EXISTS v_ownership_history CASCADE")
    
    # Create new view with proper ch_company_name handling
    cursor.execute("""
        CREATE VIEW v_ownership_history AS
        SELECT 
            lr.*,
            m.*,
            -- Use COALESCE to populate ch_company_name from matched name when no CH record
            COALESCE(ch1.company_name, m.ch_matched_name_1) as ch_company_name_1,
            COALESCE(ch2.company_name, m.ch_matched_name_2) as ch_company_name_2,
            COALESCE(ch3.company_name, m.ch_matched_name_3) as ch_company_name_3,
            COALESCE(ch4.company_name, m.ch_matched_name_4) as ch_company_name_4
        FROM land_registry_data lr
        LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
        LEFT JOIN companies_house_data ch1 ON m.ch_matched_number_1 = ch1.company_number
        LEFT JOIN companies_house_data ch2 ON m.ch_matched_number_2 = ch2.company_number
        LEFT JOIN companies_house_data ch3 ON m.ch_matched_number_3 = ch3.company_number
        LEFT JOIN companies_house_data ch4 ON m.ch_matched_number_4 = ch4.company_number
    """)
    
    conn.commit()
    print("View recreated successfully!")
    
    # Test the results
    cursor.execute("""
        SELECT 
            ch_match_type_1,
            COUNT(*) as total_records,
            COUNT(CASE WHEN ch_company_name_1 IS NOT NULL THEN 1 END) as with_name,
            COUNT(CASE WHEN ch_company_name_1 IS NULL AND proprietor_1_name IS NOT NULL THEN 1 END) as missing_name
        FROM v_ownership_history
        WHERE proprietor_1_name IS NOT NULL
        GROUP BY ch_match_type_1
        ORDER BY total_records DESC
    """)
    
    results = cursor.fetchall()
    print("\nResults for proprietor 1:")
    print(f"{'Match Type':<20} {'Total':<12} {'With Name':<12} {'Missing Name'}")
    print("-" * 60)
    for row in results:
        match_type = row[0] if row[0] else 'No Match'
        print(f"{match_type:<20} {row[1]:<12,} {row[2]:<12,} {row[3]:,}")
    
    # Specifically check Land_Registry type
    cursor.execute("""
        SELECT COUNT(*) 
        FROM v_ownership_history 
        WHERE ch_match_type_1 = 'Land_Registry' 
        AND ch_company_name_1 IS NULL 
        AND proprietor_1_name IS NOT NULL
    """)
    missing = cursor.fetchone()[0]
    
    print(f"\nLand_Registry records still missing ch_company_name_1: {missing:,}")

except Exception as e:
    print(f"Error: {e}")
    conn.rollback()
    
cursor.close()
conn.close()