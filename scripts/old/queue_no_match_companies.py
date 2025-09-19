#!/usr/bin/env python3
"""
Queue No_Match Limited Companies and LLPs for scraping.
This script adds companies to ch_scrape_queue for the existing scraper to process.
"""

import psycopg2
import os
from dotenv import load_dotenv
import logging
import argparse

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('queue_no_match_companies.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def connect_to_db():
    """Connect to the PostgreSQL database."""
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

def get_no_match_companies(conn, limit=None):
    """Get No_Match Limited Companies and LLPs only."""
    cursor = conn.cursor()
    
    try:
        logger.info("Querying No_Match Limited Companies and LLPs...")
        
        query = """
            WITH no_match_companies AS (
                -- Proprietor 1
                SELECT DISTINCT lr.proprietor_1_name as company_name
                FROM land_registry_data lr
                JOIN land_registry_ch_matches m ON lr.id = m.id
                WHERE m.ch_match_type_1 = 'No_Match'
                AND lr.proprietor_1_name IS NOT NULL
                AND lr.proprietorship_1_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership')
                
                UNION
                
                -- Proprietor 2
                SELECT DISTINCT lr.proprietor_2_name as company_name
                FROM land_registry_data lr
                JOIN land_registry_ch_matches m ON lr.id = m.id
                WHERE m.ch_match_type_2 = 'No_Match'
                AND lr.proprietor_2_name IS NOT NULL
                AND lr.proprietorship_2_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership')
                
                UNION
                
                -- Proprietor 3
                SELECT DISTINCT lr.proprietor_3_name as company_name
                FROM land_registry_data lr
                JOIN land_registry_ch_matches m ON lr.id = m.id
                WHERE m.ch_match_type_3 = 'No_Match'
                AND lr.proprietor_3_name IS NOT NULL
                AND lr.proprietorship_3_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership')
                
                UNION
                
                -- Proprietor 4
                SELECT DISTINCT lr.proprietor_4_name as company_name
                FROM land_registry_data lr
                JOIN land_registry_ch_matches m ON lr.id = m.id
                WHERE m.ch_match_type_4 = 'No_Match'
                AND lr.proprietor_4_name IS NOT NULL
                AND lr.proprietorship_4_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership')
            )
            SELECT company_name
            FROM no_match_companies
            ORDER BY company_name
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        cursor.execute(query)
        return [row[0] for row in cursor.fetchall()]
        
    finally:
        cursor.close()

def add_to_scrape_queue(conn, companies, batch_size=1000):
    """Add companies to ch_scrape_queue for scraping."""
    cursor = conn.cursor()
    added_count = 0
    skipped_count = 0
    
    try:
        # First check what's already in the queue
        cursor.execute("SELECT search_name FROM ch_scrape_queue")
        existing = set(row[0].upper().strip() for row in cursor.fetchall())
        logger.info(f"Found {len(existing)} companies already in scrape queue")
        
        # Filter out existing
        new_companies = []
        for company in companies:
            if company.upper().strip() not in existing:
                new_companies.append(company)
            else:
                skipped_count += 1
        
        logger.info(f"Skipping {skipped_count} companies already in queue")
        logger.info(f"Adding {len(new_companies)} new companies to queue")
        
        # Add in batches
        for i in range(0, len(new_companies), batch_size):
            batch = new_companies[i:i + batch_size]
            
            # Prepare batch insert
            values = [(company,) for company in batch]
            
            # Batch insert with ON CONFLICT DO NOTHING
            cursor.executemany("""
                INSERT INTO ch_scrape_queue (search_name, search_status)
                VALUES (%s, 'pending')
                ON CONFLICT (search_name) DO NOTHING
            """, values)
            
            added_count += cursor.rowcount
            conn.commit()
            
            if (i + batch_size) % 10000 == 0:
                logger.info(f"Progress: {i + batch_size}/{len(new_companies)}")
        
        logger.info(f"Successfully added {added_count} companies to scrape queue")
        
    except Exception as e:
        logger.error(f"Error adding to scrape queue: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

def main():
    """Main function to queue companies for scraping."""
    parser = argparse.ArgumentParser(
        description='Queue No_Match Limited Companies and LLPs for scraping'
    )
    parser.add_argument(
        '--limit', 
        type=int, 
        help='Limit number of companies to queue (for testing)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be queued without actually adding to database'
    )
    args = parser.parse_args()
    
    conn = None
    
    try:
        conn = connect_to_db()
        logger.info("Connected to database successfully")
        
        # Get No_Match companies
        companies = get_no_match_companies(conn, limit=args.limit)
        logger.info(f"Found {len(companies)} No_Match Limited Companies and LLPs")
        
        if args.dry_run:
            logger.info("DRY RUN - Would add the following companies:")
            for i, company in enumerate(companies[:10]):
                print(f"  {i+1}. {company}")
            if len(companies) > 10:
                print(f"  ... and {len(companies) - 10} more")
        else:
            # Add to scrape queue
            add_to_scrape_queue(conn, companies)
        
        # Show queue statistics
        cursor = conn.cursor()
        cursor.execute("""
            SELECT search_status, COUNT(*) 
            FROM ch_scrape_queue 
            GROUP BY search_status 
            ORDER BY search_status
        """)
        
        print("\nCurrent scrape queue status:")
        total = 0
        for status, count in cursor.fetchall():
            print(f"  {status}: {count:,}")
            total += count
        print(f"  Total: {total:,}")
        
        cursor.close()
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    main()