#!/usr/bin/env python3
"""
Apply the updated ownership history view that shows only the latest record per title/owner.
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
    """Apply the updated ownership history view to the database."""
    
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
                                'create_ownership_history_view_latest_only.sql')
        
        logging.info(f"Reading SQL file: {sql_file}")
        
        with open(sql_file, 'r') as f:
            sql_content = f.read()
        
        # Execute the SQL
        logging.info("Applying updated view...")
        cur.execute(sql_content)
        
        # Test the view
        logging.info("Testing the view...")
        
        # Count total records
        cur.execute("SELECT COUNT(*) FROM v_ownership_history")
        total_count = cur.fetchone()[0]
        logging.info(f"Total records in view: {total_count:,}")
        
        # Check a sample
        cur.execute("""
            SELECT title_number, proprietor_name, file_month, ownership_status
            FROM v_ownership_history
            WHERE title_number IN (
                SELECT title_number 
                FROM v_ownership_history 
                GROUP BY title_number 
                HAVING COUNT(*) > 1 
                LIMIT 1
            )
            ORDER BY title_number, proprietor_name, file_month DESC
        """)
        
        sample_results = cur.fetchall()
        if sample_results:
            logging.info("\nSample of results (should show only latest record per title/owner):")
            for row in sample_results[:5]:
                logging.info(f"  Title: {row[0]}, Owner: {row[1]}, Month: {row[2]}, Status: {row[3]}")
        
        logging.info("\nView updated successfully!")
        
        # Close connection
        cur.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"Error applying view: {e}")
        raise

if __name__ == "__main__":
    apply_view()