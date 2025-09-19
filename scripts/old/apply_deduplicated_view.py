#!/usr/bin/env python3
"""
Apply the deduplicated ownership history view.
"""

import os
import sys
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import logging

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def apply_view():
    """Apply the deduplicated ownership history view to the database."""
    
    # Database connection parameters
    conn_params = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'insideestates_app'),
        'user': os.getenv('DB_USER', 'insideestates_user'),
        'password': os.getenv('DB_PASSWORD', 'InsideEstates2024!')
    }
    
    logging.info("Connecting to database...")
    
    try:
        # Connect to database
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        cur = conn.cursor()
        
        # Read the SQL file
        sql_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 
                                'create_ownership_history_view_deduplicated.sql')
        
        logging.info(f"Reading SQL file: {sql_file}")
        
        with open(sql_file, 'r') as f:
            sql_content = f.read()
        
        # Execute the SQL
        logging.info("Applying deduplicated view...")
        cur.execute(sql_content)
        
        # Test the view
        logging.info("Testing the view...")
        
        # Count total records
        cur.execute("SELECT COUNT(*) FROM v_ownership_history")
        total_count = cur.fetchone()[0]
        logging.info(f"Total records in view: {total_count:,}")
        
        # Check for any duplicates
        cur.execute("""
            SELECT 
                title_number,
                proprietor_name,
                COUNT(*) as record_count
            FROM v_ownership_history
            GROUP BY title_number, proprietor_name
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC
            LIMIT 10
        """)
        
        duplicates = cur.fetchall()
        
        if duplicates:
            logging.warning(f"Still found {len(duplicates)} duplicate title/owner combinations:")
            for dup in duplicates:
                logging.warning(f"  Title: {dup[0]}, Owner: {dup[1]}, Count: {dup[2]}")
        else:
            logging.info("SUCCESS: No duplicate title/owner combinations found!")
        
        # Check the specific case we were investigating
        logging.info("\nChecking specific case (NGL896367, YASHAR HOLDINGS LIMITED):")
        cur.execute("""
            SELECT 
                title_number,
                proprietor_name,
                proprietor_sequence,
                file_month,
                ownership_status
            FROM v_ownership_history
            WHERE title_number = 'NGL896367' 
            AND proprietor_name = 'YASHAR HOLDINGS LIMITED'
            ORDER BY file_month DESC
        """)
        
        results = cur.fetchall()
        logging.info(f"Found {len(results)} record(s) (should be 1):")
        for row in results:
            logging.info(f"  Sequence: {row[2]}, Month: {row[3]}, Status: {row[4]}")
        
        # Show statistics
        logging.info("\nView Statistics:")
        
        cur.execute("""
            SELECT 
                COUNT(DISTINCT title_number) as unique_titles,
                COUNT(DISTINCT proprietor_name) as unique_owners,
                COUNT(*) as total_records,
                COUNT(CASE WHEN ownership_status = 'Current' THEN 1 END) as current_records,
                COUNT(CASE WHEN ownership_status = 'Historical' THEN 1 END) as historical_records
            FROM v_ownership_history
        """)
        
        stats = cur.fetchone()
        logging.info(f"Unique titles: {stats[0]:,}")
        logging.info(f"Unique owners: {stats[1]:,}")
        logging.info(f"Total records: {stats[2]:,}")
        logging.info(f"Current records: {stats[3]:,}")
        logging.info(f"Historical records: {stats[4]:,}")
        
        logging.info("\nView updated successfully!")
        
        # Close connection
        cur.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"Error applying view: {e}")
        raise

if __name__ == "__main__":
    apply_view()