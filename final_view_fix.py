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
    
    # Create new view based on the joined tables structure
    cursor.execute("""
        CREATE VIEW v_ownership_history AS
        SELECT 
            lr.id,
            lr.title_number,
            lr.tenure,
            lr.property_address,
            lr.district,
            lr.county,
            lr.region,
            lr.postcode,
            lr.multiple_address_indicator,
            lr.price_paid,
            lr.proprietor_1_name,
            lr.company_1_reg_no,
            lr.proprietorship_1_category,
            lr.country_1_incorporated,
            lr.proprietor_1_address_1,
            lr.proprietor_1_address_2,
            lr.proprietor_1_address_3,
            lr.proprietor_2_name,
            lr.company_2_reg_no,
            lr.proprietorship_2_category,
            lr.country_2_incorporated,
            lr.proprietor_2_address_1,
            lr.proprietor_2_address_2,
            lr.proprietor_2_address_3,
            lr.proprietor_3_name,
            lr.company_3_reg_no,
            lr.proprietorship_3_category,
            lr.country_3_incorporated,
            lr.proprietor_3_address_1,
            lr.proprietor_3_address_2,
            lr.proprietor_3_address_3,
            lr.proprietor_4_name,
            lr.company_4_reg_no,
            lr.proprietorship_4_category,
            lr.country_4_incorporated,
            lr.proprietor_4_address_1,
            lr.proprietor_4_address_2,
            lr.proprietor_4_address_3,
            lr.date_proprietor_added,
            lr.additional_proprietor_indicator,
            lr.dataset_type,
            lr.change_indicator,
            lr.change_date,
            lr.update_type,
            lr.file_month,
            lr.source_filename,
            lr.created_at,
            lr.updated_at,
            m.ch_matched_name_1,
            m.ch_matched_number_1,
            m.ch_match_type_1,
            m.ch_match_confidence_1,
            m.ch_matched_name_2,
            m.ch_matched_number_2,
            m.ch_match_type_2,
            m.ch_match_confidence_2,
            m.ch_matched_name_3,
            m.ch_matched_number_3,
            m.ch_match_type_3,
            m.ch_match_confidence_3,
            m.ch_matched_name_4,
            m.ch_matched_number_4,
            m.ch_match_type_4,
            m.ch_match_confidence_4,
            m.ch_match_date,
            -- CRITICAL FIX: Use COALESCE to ensure ch_company_name is populated
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
    print("✅ View recreated successfully with ch_company_name fix!")
    
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
    print("\n📊 Results for proprietor 1:")
    print(f"{'Match Type':<20} {'Total':<12} {'With Name':<12} {'Missing Name'}")
    print("-" * 60)
    for row in results:
        match_type = row[0] if row[0] else 'NULL'
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
    
    if missing == 0:
        print(f"\n✅ SUCCESS! All Land_Registry records now have ch_company_name_1 populated!")
    else:
        print(f"\n❌ WARNING: {missing:,} Land_Registry records still missing ch_company_name_1")

except Exception as e:
    print(f"❌ Error: {e}")
    conn.rollback()
    
cursor.close()
conn.close()