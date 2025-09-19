#!/usr/bin/env python3
"""
Export all unique No_Match companies from land_registry_ch_matches
to prepare them for scraping from Companies House.
"""

import psycopg2
import os
import csv
from datetime import datetime
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
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
    """Get all unique companies marked as No_Match."""
    cursor = conn.cursor()
    
    # Define categories that are actual companies (should be scraped)
    company_categories = (
        'Limited Company or Public Limited Company',
        'Limited Liability Partnership',
        'Unlimited Company',
        'Industrial and Provident Society (Company)',
        'Registered Society (Company)',
        'Community Benefit Society (Company)',
        'Housing Association/Society (Company)',
        'Housing Association Registered Society (Company)',
        'Co-operative Society (Company)',
        'Housing Association Community Benefit Society (Company)',
        'Housing Association Co-operative Society (Company)'
    )
    
    try:
        logger.info("Querying No_Match companies (excluding Local Authorities, Corporate Bodies, etc.)...")
        
        # Query to get all unique No_Match companies across all proprietor positions
        query = """
            WITH no_match_companies AS (
                -- Proprietor 1
                SELECT DISTINCT 
                    lr.proprietor_1_name as company_name,
                    lr.company_1_reg_no as reg_no,
                    lr.dataset_type,
                    lr.proprietorship_1_category as category,
                    COUNT(*) as property_count
                FROM land_registry_data lr
                JOIN land_registry_ch_matches m ON lr.id = m.id
                WHERE m.ch_match_type_1 = 'No_Match'
                AND lr.proprietor_1_name IS NOT NULL
                AND lr.proprietorship_1_category IN %s
                GROUP BY lr.proprietor_1_name, lr.company_1_reg_no, lr.dataset_type, lr.proprietorship_1_category
                
                UNION
                
                -- Proprietor 2
                SELECT DISTINCT 
                    lr.proprietor_2_name as company_name,
                    lr.company_2_reg_no as reg_no,
                    lr.dataset_type,
                    lr.proprietorship_2_category as category,
                    COUNT(*) as property_count
                FROM land_registry_data lr
                JOIN land_registry_ch_matches m ON lr.id = m.id
                WHERE m.ch_match_type_2 = 'No_Match'
                AND lr.proprietor_2_name IS NOT NULL
                AND lr.proprietorship_2_category IN %s
                GROUP BY lr.proprietor_2_name, lr.company_2_reg_no, lr.dataset_type, lr.proprietorship_2_category
                
                UNION
                
                -- Proprietor 3
                SELECT DISTINCT 
                    lr.proprietor_3_name as company_name,
                    lr.company_3_reg_no as reg_no,
                    lr.dataset_type,
                    lr.proprietorship_3_category as category,
                    COUNT(*) as property_count
                FROM land_registry_data lr
                JOIN land_registry_ch_matches m ON lr.id = m.id
                WHERE m.ch_match_type_3 = 'No_Match'
                AND lr.proprietor_3_name IS NOT NULL
                AND lr.proprietorship_3_category IN %s
                GROUP BY lr.proprietor_3_name, lr.company_3_reg_no, lr.dataset_type, lr.proprietorship_3_category
                
                UNION
                
                -- Proprietor 4
                SELECT DISTINCT 
                    lr.proprietor_4_name as company_name,
                    lr.company_4_reg_no as reg_no,
                    lr.dataset_type,
                    lr.proprietorship_4_category as category,
                    COUNT(*) as property_count
                FROM land_registry_data lr
                JOIN land_registry_ch_matches m ON lr.id = m.id
                WHERE m.ch_match_type_4 = 'No_Match'
                AND lr.proprietor_4_name IS NOT NULL
                AND lr.proprietorship_4_category IN %s
                GROUP BY lr.proprietor_4_name, lr.company_4_reg_no, lr.dataset_type, lr.proprietorship_4_category
            )
            SELECT 
                company_name,
                reg_no,
                dataset_type,
                MAX(category) as category,
                SUM(property_count) as total_properties
            FROM no_match_companies
            GROUP BY company_name, reg_no, dataset_type
            ORDER BY total_properties DESC, company_name
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        cursor.execute(query, (company_categories, company_categories, company_categories, company_categories))
        
        return cursor.fetchall()
        
    finally:
        cursor.close()

def check_already_scraped(conn, companies):
    """Check which companies are already in scrape queue or scraped."""
    cursor = conn.cursor()
    already_processed = set()
    
    try:
        # Get all companies already in scrape queue
        cursor.execute("SELECT search_name FROM ch_scrape_queue")
        for row in cursor.fetchall():
            already_processed.add(row[0].upper().strip())
        
        # Filter out already processed companies
        new_companies = []
        skipped_count = 0
        
        for company_name, reg_no, dataset_type, category, property_count in companies:
            if company_name.upper().strip() not in already_processed:
                new_companies.append((company_name, reg_no, dataset_type, category, property_count))
            else:
                skipped_count += 1
        
        logger.info(f"Skipped {skipped_count} companies already in scrape queue")
        return new_companies
        
    finally:
        cursor.close()

def export_to_csv(companies, filename):
    """Export companies to CSV file."""
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['company_name', 'registration_number', 'dataset_type', 'category', 'property_count'])
        
        for company_name, reg_no, dataset_type, category, property_count in companies:
            writer.writerow([company_name, reg_no or '', dataset_type, category, property_count])
    
    logger.info(f"Exported {len(companies)} companies to {filename}")

def add_to_scrape_queue(conn, companies, batch_size=1000):
    """Add companies to ch_scrape_queue for scraping."""
    cursor = conn.cursor()
    added_count = 0
    
    try:
        for i in range(0, len(companies), batch_size):
            batch = companies[i:i + batch_size]
            
            # Prepare batch insert
            values = []
            for company_name, reg_no, dataset_type, category, property_count in batch:
                values.append((company_name,))
            
            # Batch insert with ON CONFLICT DO NOTHING
            cursor.executemany("""
                INSERT INTO ch_scrape_queue (search_name)
                VALUES (%s)
                ON CONFLICT (search_name) DO NOTHING
            """, values)
            
            added_count += cursor.rowcount
            conn.commit()
            
            logger.info(f"Added batch {i//batch_size + 1}: {cursor.rowcount} companies")
        
        logger.info(f"Total companies added to scrape queue: {added_count}")
        
    except Exception as e:
        logger.error(f"Error adding to scrape queue: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

def main():
    """Main function to export No_Match companies."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Export No_Match companies for scraping')
    parser.add_argument('--add-to-queue', action='store_true', 
                        help='Automatically add companies to scrape queue')
    parser.add_argument('--limit', type=int, 
                        help='Limit number of companies to export')
    args = parser.parse_args()
    
    conn = None
    
    try:
        # Connect to database
        conn = connect_to_db()
        logger.info("Connected to database successfully.")
        
        # Get all No_Match companies
        companies = get_no_match_companies(conn, limit=args.limit)
        logger.info(f"Found {len(companies)} unique No_Match companies (actual companies only, excluding Local Authorities/Corporate Bodies)")
        
        # Check which ones are already scraped/queued
        new_companies = check_already_scraped(conn, companies)
        logger.info(f"{len(new_companies)} companies need to be scraped")
        
        if new_companies:
            # Export to CSV
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            csv_filename = f'no_match_companies_{timestamp}.csv'
            export_to_csv(new_companies, csv_filename)
            
            # Optionally add to scrape queue
            if args.add_to_queue:
                add_to_scrape_queue(conn, new_companies)
                logger.info("Companies added to scrape queue successfully")
            else:
                logger.info("Companies exported to CSV only")
                logger.info("Use --add-to-queue flag to automatically add to scrape queue")
        else:
            logger.info("No new companies to scrape")
        
        # Show statistics
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                search_status, 
                COUNT(*) as count
            FROM ch_scrape_queue
            GROUP BY search_status
            ORDER BY search_status
        """)
        
        print("\nScrape queue status:")
        for status, count in cursor.fetchall():
            print(f"  {status}: {count:,}")
        cursor.close()
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    main()