#!/usr/bin/env python3
"""
Direct import of scraped Companies House data into companies_house_data table
No intermediate tables, no unnecessary complexity
"""

import os
import sys
import csv
import psycopg2
from psycopg2.extras import execute_values
import logging
from datetime import datetime
import argparse
from tqdm import tqdm

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_date(date_str):
    """Parse date string to date object"""
    if not date_str or date_str.strip() == '':
        return None
        
    # Try different date formats
    formats = [
        '%d %B %Y',  # e.g., "31 March 2025"
        '%d %b %Y',  # e.g., "31 Mar 2025"
        '%Y-%m-%d',  # ISO format
        '%d/%m/%Y',  # UK format
        '%d-%m-%Y'   # Alternative UK format
    ]
    
    date_str = date_str.strip()
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    
    return None

def import_csv(csv_file, batch_size=1000):
    """Import scraped data directly into companies_house_data"""
    conn = psycopg2.connect(**POSTGRESQL_CONFIG)
    cursor = conn.cursor()
    
    stats = {
        'total': 0,
        'updated': 0,
        'inserted': 0,
        'skipped': 0
    }
    
    try:
        # Read CSV file
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
        logger.info(f"Processing {len(rows)} companies from {csv_file}")
        
        # Process in batches
        for i in tqdm(range(0, len(rows), batch_size), desc="Importing"):
            batch = rows[i:i + batch_size]
            values = []
            seen_numbers = set()
            
            for row in batch:
                stats['total'] += 1
                
                # Skip if not found
                if row.get('Status') != 'FOUND':
                    stats['skipped'] += 1
                    continue
                
                # Skip duplicates within batch
                company_number = row['Company Number']
                if company_number in seen_numbers:
                    continue
                seen_numbers.add(company_number)
                
                # Prepare data
                values.append((
                    company_number,
                    row['Found Name'],
                    row.get('Company Status', ''),
                    row.get('Company Type', ''),
                    parse_date(row.get('Incorporated On', '')),
                    parse_date(row.get('Accounts Next Due', '')),
                    parse_date(row.get('Confirmation Statement Next Due', ''))
                ))
            
            if values:
                # Insert or update
                query = """
                    INSERT INTO companies_house_data (
                        company_number,
                        company_name,
                        company_status,
                        company_category,
                        incorporation_date,
                        accounts_next_due_date,
                        conf_stmt_next_due_date
                    ) VALUES %s
                    ON CONFLICT (company_number) DO UPDATE SET
                        company_name = EXCLUDED.company_name,
                        company_status = COALESCE(NULLIF(EXCLUDED.company_status, ''), companies_house_data.company_status),
                        company_category = COALESCE(NULLIF(EXCLUDED.company_category, ''), companies_house_data.company_category),
                        incorporation_date = COALESCE(EXCLUDED.incorporation_date, companies_house_data.incorporation_date),
                        accounts_next_due_date = COALESCE(EXCLUDED.accounts_next_due_date, companies_house_data.accounts_next_due_date),
                        conf_stmt_next_due_date = COALESCE(EXCLUDED.conf_stmt_next_due_date, companies_house_data.conf_stmt_next_due_date)
                """
                
                execute_values(cursor, query, values)
                conn.commit()
                stats['updated'] += len(values)
        
        # Print summary
        logger.info(f"\nIMPORT COMPLETE:")
        logger.info(f"Total rows: {stats['total']}")
        logger.info(f"Updated/Inserted: {stats['updated']}")
        logger.info(f"Skipped (not found): {stats['skipped']}")
        
        # Update scraped addresses
        logger.info("Updating scraped addresses...")
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('Status') == 'FOUND' and row.get('Registered Office Address'):
                    cursor.execute("""
                        UPDATE companies_house_data 
                        SET reg_address_scraped = %s
                        WHERE company_number = %s
                    """, (row['Registered Office Address'], row['Company Number']))
        conn.commit()
        logger.info("Scraped addresses updated")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def main():
    parser = argparse.ArgumentParser(description='Import scraped CH data directly')
    parser.add_argument('csv_file', help='Path to the scraped CSV file')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size (default: 1000)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.csv_file):
        logger.error(f"File not found: {args.csv_file}")
        sys.exit(1)
    
    import_csv(args.csv_file, args.batch_size)

if __name__ == '__main__':
    main()