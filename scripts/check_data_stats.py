#!/usr/bin/env python3
"""
Check data statistics for InsideEstates database
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection parameters
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'insideestates_app'),
    'user': os.getenv('DB_USER', 'insideestates_user'),
    'password': os.getenv('DB_PASSWORD', 'InsideEstates2024!')
}

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    print("=== InsideEstates Data Statistics ===\n")
    
    # Companies House stats
    print("COMPANIES HOUSE DATA:")
    cur.execute("SELECT COUNT(*) as total FROM companies_house_data")
    result = cur.fetchone()
    print(f"Total companies: {result['total']:,}")
    
    cur.execute("""
        SELECT company_status, COUNT(*) as count 
        FROM companies_house_data 
        GROUP BY company_status 
        ORDER BY count DESC 
        LIMIT 5
    """)
    print("\nTop company statuses:")
    for row in cur.fetchall():
        print(f"  {row['company_status']}: {row['count']:,}")
    
    # Land Registry stats
    print("\n\nLAND REGISTRY DATA:")
    cur.execute("SELECT COUNT(DISTINCT title_number) as total FROM land_registry_data")
    result = cur.fetchone()
    print(f"Total properties: {result['total']:,}")
    
    cur.execute("""
        SELECT dataset_type, COUNT(DISTINCT title_number) as count 
        FROM land_registry_data 
        GROUP BY dataset_type
    """)
    print("\nProperties by dataset:")
    for row in cur.fetchall():
        print(f"  {row['dataset_type']}: {row['count']:,}")
    
    # Combined analysis
    print("\n\nCOMBINED ANALYSIS:")
    cur.execute("""
        SELECT COUNT(DISTINCT ch.company_number) as companies_with_property
        FROM companies_house_data ch
        WHERE EXISTS (
            SELECT 1 FROM land_registry_data lr 
            WHERE ch.company_number IN (
                lr.company_1_reg_no, lr.company_2_reg_no, 
                lr.company_3_reg_no, lr.company_4_reg_no
            )
        )
    """)
    result = cur.fetchone()
    print(f"Companies that own property: {result['companies_with_property']:,}")
    
    # Top property owners
    print("\nTop 10 property owners (by property count):")
    cur.execute("""
        WITH company_properties AS (
            SELECT 
                COALESCE(ch.company_name, 'Unknown') as company_name,
                lr.company_1_reg_no as company_number,
                COUNT(DISTINCT lr.title_number) as property_count
            FROM land_registry_data lr
            LEFT JOIN companies_house_data ch ON ch.company_number = lr.company_1_reg_no
            WHERE lr.company_1_reg_no IS NOT NULL AND lr.company_1_reg_no != ''
            GROUP BY ch.company_name, lr.company_1_reg_no
            ORDER BY property_count DESC
            LIMIT 10
        )
        SELECT * FROM company_properties
    """)
    for row in cur.fetchall():
        print(f"  {row['company_name']} ({row['company_number']}): {row['property_count']:,} properties")
    
    cur.close()
    conn.close()

if __name__ == '__main__':
    main()