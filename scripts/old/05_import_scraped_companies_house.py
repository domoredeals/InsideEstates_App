#!/usr/bin/env python3
"""
Import Scraped Companies House Overview Data
Part 5 of the InsideEstates data pipeline

This script imports scraped Companies House Overview page data into the existing
companies_house_data table, updating fields that may be missing or outdated
in the basic data file.
"""

import os
import sys
import csv
import psycopg2
from psycopg2.extras import execute_values
import logging
from datetime import datetime
from pathlib import Path
import argparse
from tqdm import tqdm

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

# Configure logging
log_filename = f'ch_scraped_import_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ScrapedDataImporter:
    def __init__(self, batch_size=1000):
        self.batch_size = batch_size
        self.conn = None
        self.cursor = None
        self.stats = {
            'total_records': 0,
            'found_companies': 0,
            'not_found_companies': 0,
            'updated': 0,
            'new_companies': 0,
            'errors': 0,
            'skipped': 0
        }
        
    def connect(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(**POSTGRESQL_CONFIG)
            self.cursor = self.conn.cursor()
            logger.info("Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
            
    def disconnect(self):
        """Disconnect from database"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    
    def parse_date(self, date_str):
        """Parse date string to date object"""
        if not date_str or date_str.strip() == '':
            return None
            
        # Try different date formats
        formats = [
            '%d %B %Y',  # e.g., "31 December 2024"
            '%d %b %Y',  # e.g., "31 Dec 2024"
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
        
        logger.warning(f"Could not parse date: {date_str}")
        return None
    
    def process_sic_codes(self, sic_codes_str):
        """Extract up to 4 SIC codes from the scraped string"""
        if not sic_codes_str:
            return [None, None, None, None]
            
        # Split by pipe separator and extract just the code numbers
        sic_list = []
        for sic in sic_codes_str.split('|'):
            sic = sic.strip()
            # Extract just the numeric code (usually first 5 characters)
            if sic and len(sic) >= 5:
                code = sic[:5].strip()
                if code.isdigit():
                    sic_list.append(code)
        
        # Pad with None to ensure we have 4 values
        while len(sic_list) < 4:
            sic_list.append(None)
            
        return sic_list[:4]  # Return only first 4
    
    def update_company_data(self, scraped_data):
        """Update or insert company data from scraped results"""
        batch_data = []
        
        for row in scraped_data:
            # Skip if company was not found
            if row['Status'] != 'FOUND':
                if row['Status'] == 'NOT_FOUND':
                    self.stats['not_found_companies'] += 1
                else:
                    self.stats['errors'] += 1
                continue
                
            self.stats['found_companies'] += 1
            
            # Parse dates
            incorporated_date = self.parse_date(row.get('Incorporated On', ''))
            accounts_due = self.parse_date(row.get('Accounts Next Due', ''))
            conf_stmt_due = self.parse_date(row.get('Confirmation Statement Next Due', ''))
            
            # Process SIC codes
            sic_codes = self.process_sic_codes(row.get('SIC Codes', ''))
            
            # Map company type to company category
            company_category = row.get('Company Type', '')
            
            # Prepare data for update
            update_data = {
                'company_number': row['Company Number'],
                'company_name': row['Found Name'],
                'company_status': row.get('Company Status', ''),
                'company_category': company_category,
                'incorporation_date': incorporated_date,
                'accounts_next_due_date': accounts_due,
                'conf_stmt_next_due_date': conf_stmt_due,
                'sic_code_1': sic_codes[0],
                'sic_code_2': sic_codes[1],
                'sic_code_3': sic_codes[2],
                'sic_code_4': sic_codes[3],
                'reg_address_full': row.get('Registered Office Address', ''),
                'updated_at': datetime.now()
            }
            
            batch_data.append(update_data)
            
            # Process batch when it reaches the batch size
            if len(batch_data) >= self.batch_size:
                self._execute_batch_update(batch_data)
                batch_data = []
        
        # Process remaining data
        if batch_data:
            self._execute_batch_update(batch_data)
    
    def _execute_batch_update(self, batch_data):
        """Execute batch update using INSERT ... ON CONFLICT"""
        try:
            # Build the INSERT ... ON CONFLICT query
            insert_query = """
                INSERT INTO companies_house_data (
                    company_number, company_name, company_status, 
                    company_category, incorporation_date,
                    accounts_next_due_date, conf_stmt_next_due_date,
                    sic_code_1, sic_code_2, sic_code_3, sic_code_4,
                    data_source, scraped_data, last_scraped_at,
                    updated_at
                ) VALUES %s
                ON CONFLICT (company_number) DO UPDATE SET
                    company_name = EXCLUDED.company_name,
                    company_status = COALESCE(NULLIF(EXCLUDED.company_status, ''), companies_house_data.company_status),
                    company_category = COALESCE(NULLIF(EXCLUDED.company_category, ''), companies_house_data.company_category),
                    incorporation_date = COALESCE(EXCLUDED.incorporation_date, companies_house_data.incorporation_date),
                    accounts_next_due_date = COALESCE(EXCLUDED.accounts_next_due_date, companies_house_data.accounts_next_due_date),
                    conf_stmt_next_due_date = COALESCE(EXCLUDED.conf_stmt_next_due_date, companies_house_data.conf_stmt_next_due_date),
                    sic_code_1 = COALESCE(EXCLUDED.sic_code_1, companies_house_data.sic_code_1),
                    sic_code_2 = COALESCE(EXCLUDED.sic_code_2, companies_house_data.sic_code_2),
                    sic_code_3 = COALESCE(EXCLUDED.sic_code_3, companies_house_data.sic_code_3),
                    sic_code_4 = COALESCE(EXCLUDED.sic_code_4, companies_house_data.sic_code_4),
                    data_source = CASE 
                        WHEN companies_house_data.scraped_data = TRUE 
                        THEN 'basic_file+scrape' 
                        ELSE 'scrape' 
                    END,
                    scraped_data = TRUE,
                    last_scraped_at = EXCLUDED.last_scraped_at,
                    updated_at = EXCLUDED.updated_at
                WHERE companies_house_data.company_number = EXCLUDED.company_number
            """
            
            # Prepare values for execute_values
            values = []
            current_time = datetime.now()
            for data in batch_data:
                values.append((
                    data['company_number'],
                    data['company_name'],
                    data['company_status'],
                    data['company_category'],
                    data['incorporation_date'],
                    data['accounts_next_due_date'],
                    data['conf_stmt_next_due_date'],
                    data['sic_code_1'],
                    data['sic_code_2'],
                    data['sic_code_3'],
                    data['sic_code_4'],
                    'scrape',  # data_source
                    True,      # scraped_data
                    current_time,  # last_scraped_at
                    data['updated_at']
                ))
            
            execute_values(self.cursor, insert_query, values)
            self.conn.commit()
            
            # Update stats
            self.stats['updated'] += len(batch_data)
            
        except Exception as e:
            logger.error(f"Error in batch update: {e}")
            self.conn.rollback()
            self.stats['errors'] += len(batch_data)
    
    def import_scraped_file(self, csv_file):
        """Import a scraped Companies House CSV file"""
        logger.info(f"Processing file: {csv_file}")
        
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                # Count total rows for progress bar
                f.seek(0)
                total_rows = sum(1 for line in f) - 1  # Subtract header
                f.seek(0)
                next(f)  # Skip header
                
                # Process in batches
                batch = []
                with tqdm(total=total_rows, desc="Processing scraped data") as pbar:
                    for row in reader:
                        self.stats['total_records'] += 1
                        batch.append(row)
                        
                        if len(batch) >= self.batch_size:
                            self.update_company_data(batch)
                            batch = []
                            pbar.update(self.batch_size)
                    
                    # Process remaining batch
                    if batch:
                        self.update_company_data(batch)
                        pbar.update(len(batch))
                        
        except Exception as e:
            logger.error(f"Error processing file {csv_file}: {e}")
            raise
    
    def add_source_tracking_columns(self):
        """Add columns to track data source and scraping status"""
        try:
            # Add data source column
            self.cursor.execute("""
                ALTER TABLE companies_house_data 
                ADD COLUMN IF NOT EXISTS data_source VARCHAR(50) DEFAULT 'basic_file'
            """)
            
            # Add scraped flag column
            self.cursor.execute("""
                ALTER TABLE companies_house_data 
                ADD COLUMN IF NOT EXISTS scraped_data BOOLEAN DEFAULT FALSE
            """)
            
            # Add last scraped timestamp
            self.cursor.execute("""
                ALTER TABLE companies_house_data 
                ADD COLUMN IF NOT EXISTS last_scraped_at TIMESTAMP
            """)
            
            # Add scraped address column
            self.cursor.execute("""
                ALTER TABLE companies_house_data 
                ADD COLUMN IF NOT EXISTS reg_address_scraped TEXT
            """)
            
            self.conn.commit()
            logger.info("Added/verified source tracking columns")
        except Exception as e:
            logger.warning(f"Could not add source tracking columns: {e}")
            self.conn.rollback()
    
    def store_scraped_addresses(self, scraped_data):
        """Store the scraped addresses in a separate column"""
        batch_updates = []
        
        for row in scraped_data:
            if row['Status'] == 'FOUND' and row.get('Registered Office Address'):
                batch_updates.append((
                    row['Registered Office Address'],
                    row['Company Number']
                ))
        
        if batch_updates:
            try:
                self.cursor.executemany("""
                    UPDATE companies_house_data 
                    SET reg_address_scraped = %s 
                    WHERE company_number = %s
                """, batch_updates)
                self.conn.commit()
                logger.info(f"Updated {len(batch_updates)} scraped addresses")
            except Exception as e:
                logger.error(f"Error updating scraped addresses: {e}")
                self.conn.rollback()
    
    def print_summary(self):
        """Print import summary"""
        logger.info("\n" + "="*60)
        logger.info("IMPORT SUMMARY")
        logger.info("="*60)
        logger.info(f"Total records processed: {self.stats['total_records']:,}")
        logger.info(f"Found companies: {self.stats['found_companies']:,}")
        logger.info(f"Not found companies: {self.stats['not_found_companies']:,}")
        logger.info(f"Updated in database: {self.stats['updated']:,}")
        logger.info(f"Errors: {self.stats['errors']:,}")
        logger.info("="*60)


def main():
    parser = argparse.ArgumentParser(
        description='Import scraped Companies House Overview data into PostgreSQL'
    )
    parser.add_argument(
        'csv_file',
        help='Path to the scraped Companies House CSV file'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Number of records to process in each batch (default: 1000)'
    )
    parser.add_argument(
        '--add-address-column',
        action='store_true',
        help='Add a column for storing scraped addresses'
    )
    
    args = parser.parse_args()
    
    # Verify file exists
    if not os.path.exists(args.csv_file):
        logger.error(f"File not found: {args.csv_file}")
        sys.exit(1)
    
    # Create importer and run
    importer = ScrapedDataImporter(batch_size=args.batch_size)
    
    try:
        importer.connect()
        
        # Always add source tracking columns
        importer.add_source_tracking_columns()
        
        # Import the file
        importer.import_scraped_file(args.csv_file)
        
        # Print summary
        importer.print_summary()
        
    except Exception as e:
        logger.error(f"Import failed: {e}")
        sys.exit(1)
    finally:
        importer.disconnect()
        logger.info(f"Log file: {log_filename}")


if __name__ == '__main__':
    main()