#!/usr/bin/env python3
"""
Quick check of ownership status distribution
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_db_connection():
    """Get database connection"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        database=os.getenv('DB_NAME', 'insideestates_app'),
        user=os.getenv('DB_USER', 'insideestates_user'),
        password=os.getenv('DB_PASSWORD', 'InsideEstates2024!')
    )

def main():
    """Main function"""
    conn = None
    try:
        # Connect to database
        conn = get_db_connection()
        print("Connected to database")
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # First check if the comprehensive view exists and get a quick count
            print("\nChecking if comprehensive view exists...")
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM pg_catalog.pg_views
                    WHERE schemaname = 'public' 
                    AND viewname = 'v_ownership_history_comprehensive'
                ) as view_exists
            """)
            result = cur.fetchone()
            
            if not result['view_exists']:
                print("View v_ownership_history_comprehensive does not exist!")
                print("\nApplying the comprehensive view...")
                with open('create_ownership_history_view_comprehensive.sql', 'r') as f:
                    cur.execute(f.read())
                conn.commit()
                print("View created successfully!")
            
            # Quick check on the underlying data
            print("\nChecking latest file months in raw data...")
            cur.execute("""
                SELECT 
                    file_month,
                    COUNT(DISTINCT title_number) as property_count
                FROM land_registry_data
                GROUP BY file_month
                ORDER BY file_month DESC
                LIMIT 5
            """)
            results = cur.fetchall()
            
            print("\nLatest file months:")
            for row in results:
                print(f"  {row['file_month']}: {row['property_count']:,} properties")
            
            # Get the absolute latest file month
            cur.execute("SELECT MAX(file_month) as latest FROM land_registry_data")
            latest_month = cur.fetchone()['latest']
            print(f"\nLatest file month: {latest_month}")
            
            # Count properties in the latest month
            cur.execute("""
                SELECT 
                    COUNT(DISTINCT title_number) as total_properties,
                    COUNT(*) as total_records,
                    COUNT(DISTINCT CASE WHEN change_indicator = 'D' THEN title_number END) as deleted_properties,
                    COUNT(DISTINCT CASE WHEN change_indicator IS NULL OR change_indicator != 'D' THEN title_number END) as active_properties
                FROM land_registry_data
                WHERE file_month = %s
            """, (latest_month,))
            
            result = cur.fetchone()
            print(f"\nIn latest month ({latest_month}):")
            print(f"  Total properties: {result['total_properties']:,}")
            print(f"  Total records: {result['total_records']:,}")
            print(f"  Deleted properties: {result['deleted_properties']:,}")
            print(f"  Active properties: {result['active_properties']:,}")
            
            # Test the ownership status logic directly
            print("\nTesting ownership status logic on raw data...")
            cur.execute("""
                SELECT 
                    CASE 
                        WHEN change_indicator = 'D' THEN 'Historical'
                        WHEN file_month = %s AND (change_indicator IS NULL OR change_indicator != 'D') THEN 'Current'
                        ELSE 'Historical'
                    END as ownership_status,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT title_number) as unique_properties
                FROM land_registry_data
                WHERE proprietor_1_name IS NOT NULL AND proprietor_1_name != ''
                GROUP BY 1
            """, (latest_month,))
            
            results = cur.fetchall()
            print("\nOwnership status distribution (proprietor 1 only):")
            for row in results:
                print(f"  {row['ownership_status']}: {row['record_count']:,} records, {row['unique_properties']:,} properties")
            
            # Check if the view is working with a limited query
            print("\nChecking view with LIMIT 1000...")
            cur.execute("""
                SELECT 
                    ownership_status,
                    COUNT(*) as count
                FROM (
                    SELECT ownership_status 
                    FROM v_ownership_history_comprehensive 
                    LIMIT 1000
                ) t
                GROUP BY ownership_status
            """)
            results = cur.fetchall()
            
            print("Sample from view:")
            for row in results:
                print(f"  {row['ownership_status']}: {row['count']} records")
                
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed")

if __name__ == "__main__":
    main()