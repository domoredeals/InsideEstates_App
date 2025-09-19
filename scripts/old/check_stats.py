#!/usr/bin/env python3
"""
Check Land Registry import statistics
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from config.postgresql_config import POSTGRESQL_CONFIG
import tabulate

def check_stats():
    conn = psycopg2.connect(**POSTGRESQL_CONFIG)
    cursor = conn.cursor()
    
    print("=== LAND REGISTRY IMPORT STATISTICS ===\n")
    
    # Overall counts
    print("OVERALL COUNTS:")
    cursor.execute("""
        SELECT 
            (SELECT COUNT(DISTINCT title_number) FROM properties) as unique_properties,
            (SELECT COUNT(*) FROM properties) as total_property_records,
            (SELECT COUNT(*) FROM proprietors) as total_proprietors,
            (SELECT COUNT(*) FROM companies) as unique_companies
    """)
    result = cursor.fetchone()
    print(f"Unique Properties: {result[0]:,}")
    print(f"Total Property Records: {result[1]:,}")
    print(f"Total Proprietors: {result[2]:,}")
    print(f"Unique Companies: {result[3]:,}")
    
    # By dataset type
    print("\n\nBY DATASET TYPE:")
    cursor.execute("""
        SELECT 
            dataset_type,
            COUNT(DISTINCT title_number) as unique_properties,
            COUNT(*) as total_records,
            COUNT(DISTINCT file_month) as months_loaded
        FROM properties
        GROUP BY dataset_type
        ORDER BY dataset_type
    """)
    results = cursor.fetchall()
    headers = ['Dataset', 'Unique Properties', 'Total Records', 'Months Loaded']
    print(tabulate.tabulate(results, headers=headers, tablefmt='grid', floatfmt=","))
    
    # Import summary
    print("\n\nIMPORT SUMMARY:")
    cursor.execute("""
        SELECT 
            dataset_type,
            COUNT(*) as files_imported,
            SUM(rows_processed) as total_rows,
            SUM(rows_inserted) as inserted,
            SUM(rows_updated) as updated,
            SUM(rows_failed) as failed
        FROM import_history
        WHERE status = 'completed'
        GROUP BY dataset_type
        ORDER BY dataset_type
    """)
    results = cursor.fetchall()
    headers = ['Dataset', 'Files', 'Rows Processed', 'Inserted', 'Updated', 'Failed']
    print(tabulate.tabulate(results, headers=headers, tablefmt='grid', floatfmt=","))
    
    # Top companies
    print("\n\nTOP 20 COMPANIES BY PROPERTY COUNT:")
    cursor.execute("""
        SELECT 
            SUBSTRING(company_name, 1, 50) as company,
            company_registration_no as reg_no,
            property_count,
            CASE 
                WHEN total_value_owned IS NOT NULL 
                THEN '£' || TO_CHAR(total_value_owned, 'FM999,999,999,999')
                ELSE 'N/A'
            END as total_value,
            COALESCE(country_incorporated, 'UK') as country
        FROM companies
        WHERE property_count > 0
        ORDER BY property_count DESC
        LIMIT 20
    """)
    results = cursor.fetchall()
    headers = ['Company', 'Reg No', 'Properties', 'Total Value', 'Country']
    print(tabulate.tabulate(results, headers=headers, tablefmt='grid'))
    
    # Properties with prices
    print("\n\nPROPERTIES WITH PRICE DATA:")
    cursor.execute("""
        SELECT 
            dataset_type,
            COUNT(*) FILTER (WHERE price_paid IS NOT NULL) as with_price,
            COUNT(*) as total,
            ROUND(100.0 * COUNT(*) FILTER (WHERE price_paid IS NOT NULL) / COUNT(*), 2) as percentage
        FROM properties
        GROUP BY dataset_type
        ORDER BY dataset_type
    """)
    results = cursor.fetchall()
    headers = ['Dataset', 'With Price', 'Total', 'Percentage']
    print(tabulate.tabulate(results, headers=headers, tablefmt='grid', floatfmt=","))
    
    # Date range
    print("\n\nDATE RANGE:")
    cursor.execute("""
        SELECT 
            dataset_type,
            MIN(file_month) as earliest,
            MAX(file_month) as latest,
            COUNT(DISTINCT file_month) as months
        FROM properties
        GROUP BY dataset_type
        ORDER BY dataset_type
    """)
    results = cursor.fetchall()
    headers = ['Dataset', 'Earliest', 'Latest', 'Total Months']
    print(tabulate.tabulate(results, headers=headers, tablefmt='grid'))
    
    # Sample properties
    print("\n\nSAMPLE PROPERTIES:")
    cursor.execute("""
        SELECT 
            p.title_number,
            p.tenure,
            SUBSTRING(p.property_address, 1, 40) as address,
            p.postcode,
            COUNT(pr.*) as owners
        FROM properties p
        LEFT JOIN proprietors pr ON p.id = pr.property_id
        WHERE p.property_address != ''
        GROUP BY p.id, p.title_number, p.tenure, p.property_address, p.postcode
        LIMIT 10
    """)
    results = cursor.fetchall()
    headers = ['Title Number', 'Tenure', 'Address', 'Postcode', 'Owners']
    print(tabulate.tabulate(results, headers=headers, tablefmt='grid'))
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    # Install tabulate if needed
    try:
        import tabulate
    except ImportError:
        print("Installing tabulate...")
        os.system("pip install tabulate")
    
    check_stats()