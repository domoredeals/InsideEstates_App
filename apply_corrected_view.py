#!/usr/bin/env python3
"""
Apply the corrected ownership view and verify results
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import os
from datetime import datetime
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

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
        logging.info("Connected to database")
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Apply the corrected view
            logging.info("Applying corrected ownership view...")
            with open('create_ownership_history_view_corrected.sql', 'r') as f:
                cur.execute(f.read())
            conn.commit()
            logging.info("View created successfully!")
            
            # Test with a limited query first
            logging.info("\nTesting view with limited query...")
            cur.execute("""
                SELECT 
                    ownership_status,
                    COUNT(*) as count
                FROM (
                    SELECT ownership_status 
                    FROM v_ownership_history_comprehensive 
                    LIMIT 10000
                ) t
                GROUP BY ownership_status
            """)
            results = cur.fetchall()
            
            print("\nSample from view (first 10,000 records):")
            for row in results:
                print(f"  {row['ownership_status']}: {row['count']:,} records")
            
            # Count unique properties with current ownership (this might be slow)
            logging.info("\nCounting properties with current ownership...")
            cur.execute("""
                SELECT COUNT(DISTINCT title_number) as count
                FROM v_ownership_history_comprehensive
                WHERE ownership_status = 'Current'
                LIMIT 1
            """)
            result = cur.fetchone()
            
            print(f"\nProperties with current ownership: {result['count']:,}")
            
            # Check the materialized view
            logging.info("\nChecking materialized view...")
            cur.execute("""
                SELECT 
                    COUNT(*) as total_current_records,
                    COUNT(DISTINCT title_number) as unique_properties
                FROM mv_current_ownership
            """)
            result = cur.fetchone()
            
            print(f"\nMaterialized view stats:")
            print(f"  Total current records: {result['total_current_records']:,}")
            print(f"  Unique properties: {result['unique_properties']:,}")
            
            # Sample some current ownership records
            cur.execute("""
                SELECT 
                    title_number,
                    proprietor_name,
                    property_address,
                    file_month,
                    ownership_status
                FROM mv_current_ownership
                LIMIT 5
            """)
            results = cur.fetchall()
            
            print("\nSample current ownership records:")
            for row in results:
                print(f"  {row['title_number']}: {row['proprietor_name']} - {row['property_address'][:50]}...")
                
    except Exception as e:
        logging.error(f"Error: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logging.info("\nDatabase connection closed")

if __name__ == "__main__":
    main()