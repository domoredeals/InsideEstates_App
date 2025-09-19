#!/usr/bin/env python3
"""
Import Scraped Companies House Overview Results
Enhanced version for inserting into scraping tables and updating main Companies House data

This script:
1. Imports scraped overview data into ch_scrape_overview table
2. Updates the ch_scrape_queue table with results
3. Updates the companies_house_data table with enhanced information
4. Provides matching analysis against Land Registry data
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
log_filename = f'scraped_overview_import_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ScrapedOverviewImporter:
    def __init__(self, batch_size=1000):
        self.batch_size = batch_size
        self.conn = None
        self.cursor = None
        self.stats = {
            'total_records': 0,
            'found_companies': 0,
            'not_found_companies': 0,
            'error_companies': 0,
            'queue_updates': 0,
            'overview_inserts': 0,
            'ch_data_updates': 0,
            'new_matches_found': 0,
            'errors': 0
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
    
    def parse_previous_names(self, prev_names_str):
        """Parse previous names string into array"""
        if not prev_names_str or prev_names_str.strip() == '':
            return []
        
        # Split by common separators and clean up
        names = []
        for name in prev_names_str.split('|'):
            name = name.strip()
            if name and name not in names:
                names.append(name)
        
        return names
    
    def parse_sic_codes(self, sic_str):
        """Parse SIC codes string into array"""
        if not sic_str or sic_str.strip() == '':
            return []
        
        codes = []
        for code in sic_str.split('|'):
            code = code.strip()
            if code:
                codes.append(code)
        
        return codes
    
    def process_scraped_data(self, csv_file):
        """Process the scraped data file and import into database"""
        logger.info(f"Processing scraped file: {csv_file}")
        
        # First, read and count the data
        scraped_records = []
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                scraped_records.append(row)
                self.stats['total_records'] += 1
        
        logger.info(f"Found {len(scraped_records)} records to process")
        
        # Process in batches
        with tqdm(total=len(scraped_records), desc="Processing scraped records") as pbar:
            for i in range(0, len(scraped_records), self.batch_size):
                batch = scraped_records[i:i + self.batch_size]
                self._process_batch(batch)
                pbar.update(len(batch))
    
    def _process_batch(self, batch):
        """Process a batch of scraped records"""
        queue_updates = []
        overview_inserts = []
        ch_data_updates = []
        
        for row in batch:
            search_name = row['Search Name']
            found_name = row['Found Name']
            company_number = row['Company Number']
            status = row['Status']
            error_msg = row.get('Error', '')
            timestamp = row.get('Timestamp', '')
            
            # Update statistics
            if status == 'FOUND':
                self.stats['found_companies'] += 1
            elif status == 'NOT_FOUND':
                self.stats['not_found_companies'] += 1
            else:
                self.stats['error_companies'] += 1
            
            # Prepare queue update
            queue_update = (
                found_name if status == 'FOUND' else None,
                company_number if status == 'FOUND' else None,
                row.get('Company URL', '') if status == 'FOUND' else None,
                'found' if status == 'FOUND' else 'not_found' if status == 'NOT_FOUND' else 'error',
                timestamp,
                error_msg,
                search_name
            )
            queue_updates.append(queue_update)
            
            # If company was found, prepare overview insert and CH data update
            if status == 'FOUND' and company_number:
                # Parse dates
                incorporation_date = self.parse_date(row.get('Incorporated On', ''))
                accounts_due = self.parse_date(row.get('Accounts Next Due', ''))
                conf_stmt_due = self.parse_date(row.get('Confirmation Statement Next Due', ''))
                
                # Parse arrays
                previous_names = self.parse_previous_names(row.get('Previous Names', ''))
                sic_codes = self.parse_sic_codes(row.get('SIC Codes', ''))
                
                # Prepare overview insert
                overview_insert = (
                    company_number,
                    row.get('Company URL', ''),
                    found_name,
                    row.get('Company Status', ''),
                    incorporation_date,
                    row.get('Company Type', ''),
                    row.get('Registered Office Address', ''),
                    sic_codes,
                    previous_names,
                    accounts_due,
                    conf_stmt_due,
                    'parsed',
                    datetime.now(),
                    datetime.now()
                )
                overview_inserts.append(overview_insert)
                
                # Prepare CH data update (extract first 4 SIC codes)
                sic_1 = sic_codes[0] if len(sic_codes) > 0 else None
                sic_2 = sic_codes[1] if len(sic_codes) > 1 else None
                sic_3 = sic_codes[2] if len(sic_codes) > 2 else None
                sic_4 = sic_codes[3] if len(sic_codes) > 3 else None
                
                ch_data_update = (
                    company_number,
                    found_name,
                    row.get('Company Status', ''),
                    row.get('Company Type', ''),
                    incorporation_date,
                    accounts_due,
                    conf_stmt_due,
                    sic_1, sic_2, sic_3, sic_4,
                    row.get('Registered Office Address', ''),
                    'scraped_overview',
                    True,
                    datetime.now(),
                    datetime.now()
                )
                ch_data_updates.append(ch_data_update)
        
        # Execute batch updates
        self._execute_queue_updates(queue_updates)
        self._execute_overview_inserts(overview_inserts)
        self._execute_ch_data_updates(ch_data_updates)
    
    def _execute_queue_updates(self, updates):
        """Update the ch_scrape_queue table"""
        if not updates:
            return
        
        try:
            query = """
                UPDATE ch_scrape_queue 
                SET found_name = %s,
                    company_number = %s,
                    company_url = %s,
                    search_status = %s,
                    search_timestamp = %s,
                    search_error = %s
                WHERE search_name = %s
            """
            self.cursor.executemany(query, updates)
            self.conn.commit()
            self.stats['queue_updates'] += len(updates)
            
        except Exception as e:
            logger.error(f"Error updating queue: {e}")
            self.conn.rollback()
            self.stats['errors'] += len(updates)
    
    def _execute_overview_inserts(self, inserts):
        """Insert into ch_scrape_overview table"""
        if not inserts:
            return
        
        # Remove duplicates by company_number to avoid conflict
        unique_inserts = {}
        for insert in inserts:
            company_number = insert[0]
            unique_inserts[company_number] = insert
        
        unique_list = list(unique_inserts.values())
        
        try:
            query = """
                INSERT INTO ch_scrape_overview (
                    company_number, company_url, company_name, company_status,
                    incorporation_date, company_type, registered_office_address,
                    sic_codes, previous_names, accounts_next_due, 
                    confirmation_statement_next_due, scrape_status,
                    parse_timestamp, scrape_timestamp
                ) VALUES %s
                ON CONFLICT (company_number) DO UPDATE SET
                    company_name = EXCLUDED.company_name,
                    company_status = EXCLUDED.company_status,
                    incorporation_date = EXCLUDED.incorporation_date,
                    company_type = EXCLUDED.company_type,
                    registered_office_address = EXCLUDED.registered_office_address,
                    sic_codes = EXCLUDED.sic_codes,
                    previous_names = EXCLUDED.previous_names,
                    accounts_next_due = EXCLUDED.accounts_next_due,
                    confirmation_statement_next_due = EXCLUDED.confirmation_statement_next_due,
                    parse_timestamp = EXCLUDED.parse_timestamp
            """
            execute_values(self.cursor, query, unique_list)
            self.conn.commit()
            self.stats['overview_inserts'] += len(unique_list)
            
        except Exception as e:
            logger.error(f"Error inserting overview data: {e}")
            self.conn.rollback()
            self.stats['errors'] += len(unique_list)
    
    def _execute_ch_data_updates(self, updates):
        """Update/Insert into companies_house_data table"""
        if not updates:
            return
        
        try:
            query = """
                INSERT INTO companies_house_data (
                    company_number, company_name, company_status, company_category,
                    incorporation_date, accounts_next_due_date, conf_stmt_next_due_date,
                    sic_code_1, sic_code_2, sic_code_3, sic_code_4,
                    reg_address_full, data_source, scraped_data, 
                    last_scraped_at, updated_at
                ) VALUES %s
                ON CONFLICT (company_number) DO UPDATE SET
                    company_name = COALESCE(NULLIF(EXCLUDED.company_name, ''), companies_house_data.company_name),
                    company_status = COALESCE(NULLIF(EXCLUDED.company_status, ''), companies_house_data.company_status),
                    company_category = COALESCE(NULLIF(EXCLUDED.company_category, ''), companies_house_data.company_category),
                    incorporation_date = COALESCE(EXCLUDED.incorporation_date, companies_house_data.incorporation_date),
                    accounts_next_due_date = COALESCE(EXCLUDED.accounts_next_due_date, companies_house_data.accounts_next_due_date),
                    conf_stmt_next_due_date = COALESCE(EXCLUDED.conf_stmt_next_due_date, companies_house_data.conf_stmt_next_due_date),
                    sic_code_1 = COALESCE(EXCLUDED.sic_code_1, companies_house_data.sic_code_1),
                    sic_code_2 = COALESCE(EXCLUDED.sic_code_2, companies_house_data.sic_code_2),
                    sic_code_3 = COALESCE(EXCLUDED.sic_code_3, companies_house_data.sic_code_3),
                    sic_code_4 = COALESCE(EXCLUDED.sic_code_4, companies_house_data.sic_code_4),
                    reg_address_full = COALESCE(NULLIF(EXCLUDED.reg_address_full, ''), companies_house_data.reg_address_full),
                    data_source = CASE 
                        WHEN companies_house_data.data_source = 'basic_file' THEN 'basic_file+overview_scrape'
                        WHEN companies_house_data.data_source LIKE '%scrape%' THEN companies_house_data.data_source
                        ELSE EXCLUDED.data_source
                    END,
                    scraped_data = TRUE,
                    last_scraped_at = EXCLUDED.last_scraped_at,
                    updated_at = EXCLUDED.updated_at
            """
            execute_values(self.cursor, query, updates)
            self.conn.commit()
            self.stats['ch_data_updates'] += len(updates)
            
        except Exception as e:
            logger.error(f"Error updating CH data: {e}")
            self.conn.rollback()
            self.stats['errors'] += len(updates)
    
    def analyze_new_matches(self):
        """Analyze potential new matches with Land Registry data"""
        logger.info("Analyzing potential new matches...")
        
        try:
            # Check for matches using previous names
            self.cursor.execute("""
                WITH scraped_companies AS (
                    SELECT DISTINCT 
                        q.search_name,
                        o.company_number,
                        o.company_name,
                        o.company_status,
                        o.previous_names
                    FROM ch_scrape_queue q
                    JOIN ch_scrape_overview o ON q.company_number = o.company_number
                    WHERE q.search_status = 'found'
                ),
                potential_matches AS (
                    SELECT DISTINCT
                        lr.title_number,
                        lr.proprietor_1_name as lr_company_name,
                        sc.company_number,
                        sc.company_name as ch_company_name,
                        sc.company_status,
                        CASE 
                            WHEN lr.proprietor_1_name = sc.company_name THEN 'exact_match'
                            WHEN lr.proprietor_1_name = ANY(sc.previous_names) THEN 'previous_name_match'
                            ELSE 'search_name_match'
                        END as match_type
                    FROM land_registry_data lr
                    JOIN scraped_companies sc ON (
                        lr.proprietor_1_name = sc.search_name OR
                        lr.proprietor_1_name = sc.company_name OR
                        lr.proprietor_1_name = ANY(sc.previous_names)
                    )
                    WHERE lr.company_1_reg_no IS NULL  -- Currently unmatched
                )
                SELECT 
                    match_type,
                    company_status,
                    COUNT(*) as match_count,
                    COUNT(DISTINCT company_number) as unique_companies,
                    COUNT(DISTINCT title_number) as unique_properties
                FROM potential_matches
                GROUP BY match_type, company_status
                ORDER BY match_type, company_status
            """)
            
            results = self.cursor.fetchall()
            
            logger.info("\nPOTENTIAL NEW MATCHES ANALYSIS:")
            logger.info("="*60)
            total_matches = 0
            for match_type, status, match_count, companies, properties in results:
                logger.info(f"{match_type} ({status}): {match_count:,} matches, {companies:,} companies, {properties:,} properties")
                total_matches += match_count
            
            logger.info(f"\nTotal potential new matches: {total_matches:,}")
            self.stats['new_matches_found'] = total_matches
            
        except Exception as e:
            logger.error(f"Error analyzing matches: {e}")
    
    def create_update_lr_matching_script(self):
        """Create a script to update Land Registry matching based on scraped data"""
        script_content = """
-- Update Land Registry matching with scraped Companies House data
-- Generated automatically by import_scraped_overview_results.py

-- Update exact name matches
UPDATE land_registry_data lr
SET 
    company_1_reg_no = o.company_number,
    company_1_status = o.company_status,
    company_1_matched_name = o.company_name,
    updated_at = NOW()
FROM ch_scrape_queue q
JOIN ch_scrape_overview o ON q.company_number = o.company_number
WHERE q.search_status = 'found'
  AND lr.proprietor_1_name = q.search_name
  AND lr.company_1_reg_no IS NULL  -- Only update unmatched records
  AND o.company_number IS NOT NULL;

-- Update previous name matches
UPDATE land_registry_data lr
SET 
    company_1_reg_no = o.company_number,
    company_1_status = o.company_status,
    company_1_matched_name = o.company_name,
    company_1_match_type = 'previous_name',
    updated_at = NOW()
FROM ch_scrape_overview o
WHERE lr.proprietor_1_name = ANY(o.previous_names)
  AND lr.company_1_reg_no IS NULL  -- Only update unmatched records
  AND o.company_number IS NOT NULL;

-- Generate summary of updates
SELECT 
    'Updates Applied' as status,
    COUNT(*) as total_updates,
    COUNT(DISTINCT company_1_reg_no) as unique_companies
FROM land_registry_data 
WHERE company_1_matched_name IS NOT NULL 
  AND updated_at > NOW() - INTERVAL '1 hour';
"""
        
        script_file = f'update_lr_matching_scraped_{datetime.now().strftime("%Y%m%d_%H%M%S")}.sql'
        with open(script_file, 'w') as f:
            f.write(script_content)
        
        logger.info(f"Created LR matching update script: {script_file}")
        return script_file
    
    def print_summary(self):
        """Print import summary"""
        logger.info("\n" + "="*70)
        logger.info("SCRAPED OVERVIEW IMPORT SUMMARY")
        logger.info("="*70)
        logger.info(f"Total records processed: {self.stats['total_records']:,}")
        logger.info(f"Found companies: {self.stats['found_companies']:,}")
        logger.info(f"Not found companies: {self.stats['not_found_companies']:,}")
        logger.info(f"Error companies: {self.stats['error_companies']:,}")
        logger.info(f"Queue updates: {self.stats['queue_updates']:,}")
        logger.info(f"Overview inserts: {self.stats['overview_inserts']:,}")
        logger.info(f"CH data updates: {self.stats['ch_data_updates']:,}")
        logger.info(f"Potential new matches: {self.stats['new_matches_found']:,}")
        logger.info(f"Errors: {self.stats['errors']:,}")
        logger.info("="*70)


def main():
    parser = argparse.ArgumentParser(
        description='Import scraped Companies House Overview results'
    )
    parser.add_argument(
        'csv_file',
        help='Path to the scraped overview results CSV file'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Number of records to process in each batch (default: 1000)'
    )
    parser.add_argument(
        '--analyze-matches',
        action='store_true',
        help='Analyze potential new Land Registry matches'
    )
    parser.add_argument(
        '--create-update-script',
        action='store_true',
        help='Create SQL script to update Land Registry matching'
    )
    
    args = parser.parse_args()
    
    # Verify file exists
    if not os.path.exists(args.csv_file):
        logger.error(f"File not found: {args.csv_file}")
        sys.exit(1)
    
    # Create importer and run
    importer = ScrapedOverviewImporter(batch_size=args.batch_size)
    
    try:
        importer.connect()
        
        # Process the scraped data
        importer.process_scraped_data(args.csv_file)
        
        # Analyze matches if requested
        if args.analyze_matches:
            importer.analyze_new_matches()
        
        # Create update script if requested
        if args.create_update_script:
            script_file = importer.create_update_lr_matching_script()
            logger.info(f"Run this script to update LR matching: psql -f {script_file}")
        
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