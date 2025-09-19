#!/usr/bin/env python3
"""
Apply the comprehensive ownership views and verify the data distribution
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import os
from datetime import datetime
from dotenv import load_dotenv
import logging
from tabulate import tabulate

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'verify_ownership_views_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
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

def apply_views(conn):
    """Apply the comprehensive ownership views"""
    logging.info("Applying comprehensive ownership views...")
    
    try:
        with conn.cursor() as cur:
            # Apply the main comprehensive view
            logging.info("Creating comprehensive ownership view...")
            with open('create_ownership_history_view_comprehensive.sql', 'r') as f:
                cur.execute(f.read())
            
            # Apply the suite of specialized views
            logging.info("Creating specialized views suite...")
            with open('create_ownership_views_suite.sql', 'r') as f:
                cur.execute(f.read())
            
            conn.commit()
            logging.info("Views created successfully!")
            
    except Exception as e:
        logging.error(f"Error creating views: {e}")
        conn.rollback()
        raise

def verify_ownership_status_distribution(conn):
    """Verify the distribution of ownership_status values"""
    logging.info("\nVerifying ownership status distribution...")
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # 1. Overall ownership status distribution
        cur.execute("""
            SELECT 
                ownership_status,
                COUNT(*) as record_count,
                COUNT(DISTINCT title_number) as unique_properties,
                ROUND(COUNT(*)::numeric / SUM(COUNT(*)) OVER() * 100, 2) as percentage
            FROM v_ownership_history
            GROUP BY ownership_status
            ORDER BY record_count DESC
        """)
        results = cur.fetchall()
        
        print("\n=== Ownership Status Distribution ===")
        print(tabulate(results, headers='keys', tablefmt='grid'))
        
        # 2. Check total properties with current ownership
        cur.execute("""
            SELECT 
                COUNT(DISTINCT title_number) as properties_with_current_ownership,
                (SELECT COUNT(DISTINCT title_number) FROM land_registry_data) as total_properties,
                ROUND(COUNT(DISTINCT title_number)::numeric / 
                      (SELECT COUNT(DISTINCT title_number) FROM land_registry_data) * 100, 2) as percentage
            FROM v_ownership_history
            WHERE ownership_status = 'Current'
        """)
        result = cur.fetchone()
        
        print(f"\n=== Properties with Current Ownership ===")
        print(f"Current: {result['properties_with_current_ownership']:,}")
        print(f"Total: {result['total_properties']:,}")
        print(f"Percentage: {result['percentage']}%")
        
        # 3. Distribution by dataset type
        cur.execute("""
            SELECT 
                dataset_type,
                ownership_status,
                COUNT(DISTINCT title_number) as property_count
            FROM v_ownership_history
            GROUP BY dataset_type, ownership_status
            ORDER BY dataset_type, ownership_status
        """)
        results = cur.fetchall()
        
        print("\n=== Distribution by Dataset Type ===")
        print(tabulate(results, headers='keys', tablefmt='grid'))
        
        # 4. Latest file months in the data
        cur.execute("""
            SELECT 
                file_month,
                COUNT(DISTINCT title_number) as property_count,
                COUNT(*) as record_count
            FROM land_registry_data
            WHERE file_month IN (
                SELECT DISTINCT file_month 
                FROM land_registry_data 
                ORDER BY file_month DESC 
                LIMIT 5
            )
            GROUP BY file_month
            ORDER BY file_month DESC
        """)
        results = cur.fetchall()
        
        print("\n=== Latest File Months in Data ===")
        print(tabulate(results, headers='keys', tablefmt='grid'))
        
        # 5. Sample of current ownership records
        cur.execute("""
            SELECT 
                title_number,
                proprietor_name,
                file_month,
                ownership_status,
                change_indicator
            FROM v_current_ownership
            LIMIT 10
        """)
        results = cur.fetchall()
        
        print("\n=== Sample Current Ownership Records ===")
        print(tabulate(results, headers='keys', tablefmt='grid'))
        
        # 6. Companies with most properties
        cur.execute("""
            SELECT 
                ch_company_name as company_name,
                company_number,
                current_properties,
                historical_properties,
                total_properties_ever_owned
            FROM v_company_property_portfolio
            WHERE current_properties > 10
            ORDER BY current_properties DESC
            LIMIT 20
        """)
        results = cur.fetchall()
        
        print("\n=== Top Property-Owning Companies ===")
        print(tabulate(results, headers='keys', tablefmt='grid'))

def main():
    """Main function"""
    conn = None
    try:
        # Connect to database
        conn = get_db_connection()
        logging.info("Connected to database")
        
        # Apply the views
        apply_views(conn)
        
        # Verify the data
        verify_ownership_status_distribution(conn)
        
        logging.info("\nVerification complete!")
        
    except Exception as e:
        logging.error(f"Error: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed")

if __name__ == "__main__":
    main()