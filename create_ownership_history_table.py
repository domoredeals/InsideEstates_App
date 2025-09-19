#!/usr/bin/env python3
"""
Create the ownership_history table
"""

import psycopg2
import os
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def main():
    """Create the ownership_history table"""
    conn = None
    try:
        # Connect to database
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432'),
            database=os.getenv('DB_NAME', 'insideestates_app'),
            user=os.getenv('DB_USER', 'insideestates_user'),
            password=os.getenv('DB_PASSWORD', 'InsideEstates2024!')
        )
        logger.info("Connected to database")
        
        # Read and execute the SQL file
        with open('create_ownership_history_table.sql', 'r') as f:
            sql = f.read()
        
        with conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()
        
        logger.info("✅ Successfully created ownership_history table and indexes")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    main()