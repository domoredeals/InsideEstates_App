#!/usr/bin/env python3
"""
Analyze the types of files imported to understand FULL vs COU distribution
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
            # Check update types by month
            print("\nChecking update types by month...")
            cur.execute("""
                SELECT 
                    file_month,
                    update_type,
                    dataset_type,
                    COUNT(DISTINCT title_number) as property_count,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT source_filename) as file_count
                FROM land_registry_data
                GROUP BY file_month, update_type, dataset_type
                ORDER BY file_month DESC
                LIMIT 20
            """)
            results = cur.fetchall()
            
            print("\nFile distribution by month:")
            print(f"{'File Month':<12} {'Type':<8} {'Dataset':<8} {'Properties':>12} {'Records':>12}")
            print("-" * 60)
            for row in results:
                print(f"{str(row['file_month']):<12} {row['update_type'] or 'NULL':<8} {row['dataset_type']:<8} {row['property_count']:>12,} {row['record_count']:>12,}")
            
            # Find the most recent FULL update
            print("\nFinding most recent FULL updates...")
            cur.execute("""
                SELECT 
                    file_month,
                    dataset_type,
                    COUNT(DISTINCT title_number) as property_count,
                    MIN(created_at) as import_date
                FROM land_registry_data
                WHERE update_type = 'FULL'
                GROUP BY file_month, dataset_type
                ORDER BY file_month DESC
                LIMIT 10
            """)
            results = cur.fetchall()
            
            if results:
                print("\nMost recent FULL updates:")
                for row in results:
                    print(f"  {row['file_month']} - {row['dataset_type']}: {row['property_count']:,} properties")
                
                # Use the most recent FULL update for Current status
                latest_full_month = results[0]['file_month']
                print(f"\nLatest FULL update month: {latest_full_month}")
                
                # Count Current ownership based on FULL update
                cur.execute("""
                    SELECT 
                        COUNT(DISTINCT title_number) as current_properties,
                        COUNT(*) as current_records
                    FROM land_registry_data
                    WHERE file_month = %s
                    AND (change_indicator IS NULL OR change_indicator != 'D')
                """, (latest_full_month,))
                
                result = cur.fetchone()
                print(f"\nProperties in latest FULL update ({latest_full_month}):")
                print(f"  Current properties: {result['current_properties']:,}")
                print(f"  Current records: {result['current_records']:,}")
            
            # Check source filenames to understand the pattern
            print("\n\nChecking source filenames...")
            cur.execute("""
                SELECT DISTINCT 
                    source_filename,
                    file_month,
                    update_type,
                    dataset_type
                FROM land_registry_data
                ORDER BY file_month DESC, source_filename
                LIMIT 20
            """)
            results = cur.fetchall()
            
            print("\nRecent source files:")
            for row in results:
                print(f"  {row['source_filename']} ({row['file_month']}, {row['update_type']})")
                
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed")

if __name__ == "__main__":
    main()