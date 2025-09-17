#!/usr/bin/env python3
"""
Verify all CSV files were imported to PostgreSQL
"""
import sys
import os
from pathlib import Path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from config.postgresql_config import POSTGRESQL_CONFIG

def verify_import():
    conn = psycopg2.connect(**POSTGRESQL_CONFIG)
    cursor = conn.cursor()
    
    # Count CSV files
    ccod_path = Path('/home/adc/Projects/InsideEstates_App/DATA/SOURCE/LR/CCOD')
    ocod_path = Path('/home/adc/Projects/InsideEstates_App/DATA/SOURCE/LR/OCOD')
    
    ccod_files = list(ccod_path.glob('*.csv'))
    ocod_files = list(ocod_path.glob('*.csv'))
    
    print("=== CSV FILES vs DATABASE IMPORT ===\n")
    print(f"CCOD CSV files found: {len(ccod_files)}")
    print(f"OCOD CSV files found: {len(ocod_files)}")
    print(f"Total CSV files: {len(ccod_files) + len(ocod_files)}\n")
    
    # Check import history
    cursor.execute("""
        SELECT 
            dataset_type,
            COUNT(*) as files_imported,
            COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
            COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
            COUNT(CASE WHEN status = 'running' THEN 1 END) as running
        FROM import_history
        GROUP BY dataset_type
        ORDER BY dataset_type
    """)
    
    print("DATABASE IMPORT STATUS:")
    print("Dataset | Files Imported | Completed | Failed | Running")
    print("-" * 55)
    
    total_imported = 0
    for row in cursor.fetchall():
        print(f"{row[0]:<7} | {row[1]:>14} | {row[2]:>9} | {row[3]:>6} | {row[4]:>7}")
        total_imported += row[1]
    
    print(f"\nTotal files in import history: {total_imported}")
    
    # Check for missing files
    cursor.execute("SELECT filename FROM import_history")
    imported_files = {row[0] for row in cursor.fetchall()}
    
    all_csv_files = [f.name for f in ccod_files + ocod_files]
    missing_files = [f for f in all_csv_files if f not in imported_files]
    
    if missing_files:
        print(f"\nWARNING: {len(missing_files)} files not in import history:")
        for f in missing_files[:10]:  # Show first 10
            print(f"  - {f}")
        if len(missing_files) > 10:
            print(f"  ... and {len(missing_files) - 10} more")
    else:
        print("\n✓ All CSV files are in import history")
    
    # Check data completeness
    print("\n\nDATA COMPLETENESS CHECK:")
    cursor.execute("""
        SELECT 
            'Total unique properties' as metric,
            COUNT(DISTINCT title_number) as count
        FROM properties
        UNION ALL
        SELECT 
            'Properties with addresses' as metric,
            COUNT(DISTINCT title_number) as count
        FROM properties
        WHERE property_address != ''
        UNION ALL
        SELECT 
            'Properties with postcodes' as metric,
            COUNT(DISTINCT title_number) as count
        FROM properties
        WHERE postcode != ''
        UNION ALL
        SELECT 
            'Properties with price data' as metric,
            COUNT(DISTINCT title_number) as count
        FROM properties
        WHERE price_paid IS NOT NULL
        UNION ALL
        SELECT 
            'Properties with change dates' as metric,
            COUNT(DISTINCT title_number) as count
        FROM properties
        WHERE change_date IS NOT NULL
    """)
    
    for row in cursor.fetchall():
        print(f"{row[0]:<30}: {row[1]:>10,}")
    
    # Check proprietor data
    print("\n\nPROPRIETOR DATA CHECK:")
    cursor.execute("""
        SELECT 
            COUNT(*) as total_proprietors,
            COUNT(DISTINCT property_id) as properties_with_owners,
            COUNT(DISTINCT company_registration_no) as unique_companies
        FROM proprietors
    """)
    
    result = cursor.fetchone()
    print(f"Total proprietor records: {result[0]:,}")
    print(f"Properties with owners: {result[1]:,}")
    print(f"Unique company numbers: {result[2]:,}")
    
    if result[0] == 0:
        print("\n⚠️  WARNING: No proprietor data found!")
        print("This suggests the proprietor data wasn't imported properly.")
        print("You may need to re-run the import to populate proprietor relationships.")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    verify_import()