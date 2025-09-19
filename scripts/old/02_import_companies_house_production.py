#!/usr/bin/env python3
"""
Production Companies House Import Script
Part 2 of 3 in the InsideEstates data pipeline

This script imports Companies House Basic Company Data into PostgreSQL.
Handles the large CSV files efficiently with batch processing.
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
import gc

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

# Configure logging
log_filename = f'ch_import_production_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CompaniesHouseImporter:
    def __init__(self, batch_size=10000):
        self.batch_size = batch_size
        self.conn = None
        self.cursor = None
        self.stats = {
            'total_records': 0,
            'inserted': 0,
            'updated': 0,
            'errors': 0,
            'files_processed': 0
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
            
    def ensure_table_exists(self):
        """Ensure the companies_house_data table exists"""
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS companies_house_data (
                company_name TEXT,
                company_number VARCHAR(20) PRIMARY KEY,
                reg_address_care_of TEXT,
                reg_address_po_box TEXT,
                reg_address_line1 TEXT,
                reg_address_line2 TEXT,
                reg_address_post_town TEXT,
                reg_address_county TEXT,
                reg_address_country TEXT,
                reg_address_postcode VARCHAR(20),
                company_category VARCHAR(100),
                company_status VARCHAR(100),
                country_of_origin VARCHAR(100),
                dissolution_date DATE,
                incorporation_date DATE,
                accounts_accounting_ref_day TEXT,
                accounts_accounting_ref_month TEXT,
                accounts_next_due_date DATE,
                accounts_last_made_up_date DATE,
                accounts_category VARCHAR(100),
                returns_next_due_date DATE,
                returns_last_made_up_date DATE,
                mortgages_num_charges INTEGER,
                mortgages_num_outstanding INTEGER,
                mortgages_num_part_satisfied INTEGER,
                mortgages_num_satisfied INTEGER,
                sic_code_1 VARCHAR(10),
                sic_code_2 VARCHAR(10),
                sic_code_3 VARCHAR(10),
                sic_code_4 VARCHAR(10),
                limited_partnerships_general_partners TEXT,
                limited_partnerships_limited_partners TEXT,
                uri TEXT,
                previous_name_1_name TEXT,
                previous_name_1_date DATE,
                previous_name_2_name TEXT,
                previous_name_2_date DATE,
                previous_name_3_name TEXT,
                previous_name_3_date DATE,
                previous_name_4_name TEXT,
                previous_name_4_date DATE,
                previous_name_5_name TEXT,
                previous_name_5_date DATE,
                previous_name_6_name TEXT,
                previous_name_6_date DATE,
                previous_name_7_name TEXT,
                previous_name_7_date DATE,
                previous_name_8_name TEXT,
                previous_name_8_date DATE,
                previous_name_9_name TEXT,
                previous_name_9_date DATE,
                previous_name_10_name TEXT,
                previous_name_10_date DATE,
                conf_stmt_next_due_date DATE,
                conf_stmt_last_made_up_date DATE,
                data_source VARCHAR(50) DEFAULT 'basic_file',
                scraped_data BOOLEAN DEFAULT FALSE,
                last_scraped_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Create indexes for performance
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_ch_company_name ON companies_house_data(company_name);",
            "CREATE INDEX IF NOT EXISTS idx_ch_postcode ON companies_house_data(reg_address_postcode);",
            "CREATE INDEX IF NOT EXISTS idx_ch_status ON companies_house_data(company_status);",
            "CREATE INDEX IF NOT EXISTS idx_ch_incorporation_date ON companies_house_data(incorporation_date);",
            "CREATE INDEX IF NOT EXISTS idx_ch_sic_codes ON companies_house_data(sic_code_1, sic_code_2, sic_code_3, sic_code_4);",
            # Index for active companies
            "CREATE INDEX IF NOT EXISTS idx_ch_active_companies ON companies_house_data(company_number) WHERE company_status = 'Active';",
            # Indexes for previous names
            "CREATE INDEX IF NOT EXISTS idx_ch_previous_name_1 ON companies_house_data(previous_name_1_name) WHERE previous_name_1_name IS NOT NULL;",
            "CREATE INDEX IF NOT EXISTS idx_ch_previous_name_2 ON companies_house_data(previous_name_2_name) WHERE previous_name_2_name IS NOT NULL;"
        ]
        
        for idx_sql in indexes:
            self.cursor.execute(idx_sql)
            
        self.conn.commit()
        logger.info("Ensured companies_house_data table exists with all indexes")
        
    def parse_date(self, date_str):
        """Parse date from various formats"""
        if not date_str or date_str.strip() == '' or date_str == 'None':
            return None
        try:
            # Try different date formats
            for fmt in ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y']:
                try:
                    return datetime.strptime(date_str.strip(), fmt).date()
                except:
                    continue
            return None
        except:
            return None
            
    def clean_value(self, value):
        """Clean and standardize values"""
        if value is None or value.strip() == '' or value.strip().upper() == 'NONE':
            return None
        # Remove extra whitespace
        value = ' '.join(value.strip().split())
        return value if value else None
        
    def import_file(self, filepath):
        """Import a Companies House data file"""
        filename = os.path.basename(filepath)
        logger.info(f"Starting import of {filename}")
        
        # Check if we need to clear existing data
        self.cursor.execute("SELECT COUNT(*) FROM companies_house_data")
        existing_count = self.cursor.fetchone()[0]
        
        if existing_count > 0:
            logger.warning(f"Found {existing_count:,} existing records")
            response = input("Do you want to TRUNCATE the table and reimport? (yes/no): ")
            if response.lower() == 'yes':
                logger.info("Truncating companies_house_data table...")
                self.cursor.execute("TRUNCATE TABLE companies_house_data")
                self.conn.commit()
            else:
                logger.info("Proceeding with update mode (INSERT ... ON CONFLICT UPDATE)")
        
        # Count lines for progress bar
        logger.info("Counting lines in file...")
        total_lines = sum(1 for _ in open(filepath, 'r', encoding='utf-8'))
        logger.info(f"Total lines to process: {total_lines:,}")
        
        batch_data = []
        
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            with tqdm(total=total_lines-1, desc=f"Importing {filename}") as pbar:
                for row in reader:
                    # Parse the row
                    record = {
                        'company_name': self.clean_value(row.get('CompanyName')),
                        'company_number': self.clean_value(row.get('CompanyNumber')),
                        'reg_address_care_of': self.clean_value(row.get('RegAddress.CareOf')),
                        'reg_address_po_box': self.clean_value(row.get('RegAddress.POBox')),
                        'reg_address_line1': self.clean_value(row.get('RegAddress.AddressLine1')),
                        'reg_address_line2': self.clean_value(row.get('RegAddress.AddressLine2')),
                        'reg_address_post_town': self.clean_value(row.get('RegAddress.PostTown')),
                        'reg_address_county': self.clean_value(row.get('RegAddress.County')),
                        'reg_address_country': self.clean_value(row.get('RegAddress.Country')),
                        'reg_address_postcode': self.clean_value(row.get('RegAddress.PostCode')),
                        'company_category': self.clean_value(row.get('CompanyCategory')),
                        'company_status': self.clean_value(row.get('CompanyStatus')),
                        'country_of_origin': self.clean_value(row.get('CountryOfOrigin')),
                        'dissolution_date': self.parse_date(row.get('DissolutionDate')),
                        'incorporation_date': self.parse_date(row.get('IncorporationDate')),
                        'accounts_accounting_ref_day': self.clean_value(row.get('Accounts.AccountRefDay')),
                        'accounts_accounting_ref_month': self.clean_value(row.get('Accounts.AccountRefMonth')),
                        'accounts_next_due_date': self.parse_date(row.get('Accounts.NextDueDate')),
                        'accounts_last_made_up_date': self.parse_date(row.get('Accounts.LastMadeUpDate')),
                        'accounts_category': self.clean_value(row.get('Accounts.AccountCategory')),
                        'returns_next_due_date': self.parse_date(row.get('Returns.NextDueDate')),
                        'returns_last_made_up_date': self.parse_date(row.get('Returns.LastMadeUpDate')),
                        'mortgages_num_charges': int(row['Mortgages.NumCharges']) if row.get('Mortgages.NumCharges') and row['Mortgages.NumCharges'].strip() and row['Mortgages.NumCharges'].strip() != 'None' else None,
                        'mortgages_num_outstanding': int(row['Mortgages.NumOutstanding']) if row.get('Mortgages.NumOutstanding') and row['Mortgages.NumOutstanding'].strip() and row['Mortgages.NumOutstanding'].strip() != 'None' else None,
                        'mortgages_num_part_satisfied': int(row['Mortgages.NumPartSatisfied']) if row.get('Mortgages.NumPartSatisfied') and row['Mortgages.NumPartSatisfied'].strip() and row['Mortgages.NumPartSatisfied'].strip() != 'None' else None,
                        'mortgages_num_satisfied': int(row['Mortgages.NumSatisfied']) if row.get('Mortgages.NumSatisfied') and row['Mortgages.NumSatisfied'].strip() and row['Mortgages.NumSatisfied'].strip() != 'None' else None,
                        'sic_code_1': self.clean_value(row.get('SICCode.SicText_1')),
                        'sic_code_2': self.clean_value(row.get('SICCode.SicText_2')),
                        'sic_code_3': self.clean_value(row.get('SICCode.SicText_3')),
                        'sic_code_4': self.clean_value(row.get('SICCode.SicText_4')),
                        'limited_partnerships_general_partners': self.clean_value(row.get('LimitedPartnerships.NumGenPartners')),
                        'limited_partnerships_limited_partners': self.clean_value(row.get('LimitedPartnerships.NumLimPartners')),
                        'uri': self.clean_value(row.get('URI')),
                        'conf_stmt_next_due_date': self.parse_date(row.get('ConfStmtNextDueDate')),
                        'conf_stmt_last_made_up_date': self.parse_date(row.get('ConfStmtLastMadeUpDate'))
                    }
                    
                    # Add previous names
                    for i in range(1, 11):
                        record[f'previous_name_{i}_name'] = self.clean_value(row.get(f'PreviousName_{i}.CompanyName'))
                        record[f'previous_name_{i}_date'] = self.parse_date(row.get(f'PreviousName_{i}.CONDATE'))
                    
                    # Skip if no company number
                    if not record['company_number']:
                        continue
                        
                    batch_data.append(record)
                    
                    # Process batch
                    if len(batch_data) >= self.batch_size:
                        self.insert_batch(batch_data)
                        batch_data = []
                        
                    pbar.update(1)
                    self.stats['total_records'] += 1
                    
                    # Periodic garbage collection
                    if self.stats['total_records'] % 100000 == 0:
                        gc.collect()
                        
                # Insert remaining records
                if batch_data:
                    self.insert_batch(batch_data)
                    
        self.stats['files_processed'] += 1
        logger.info(f"Completed importing {filename}")
        
    def insert_batch(self, batch_data):
        """Insert a batch of records using ON CONFLICT UPDATE"""
        if not batch_data:
            return
            
        try:
            columns = list(batch_data[0].keys())
            
            # Build INSERT ... ON CONFLICT UPDATE query
            insert_query = f"""
                INSERT INTO companies_house_data ({', '.join(columns)})
                VALUES %s
                ON CONFLICT (company_number) DO UPDATE SET
                    {', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'company_number'])},
                    updated_at = CURRENT_TIMESTAMP
            """
            
            # Prepare values
            values = []
            for record in batch_data:
                values.append(tuple(record[col] for col in columns))
                
            execute_values(self.cursor, insert_query, values)
            self.conn.commit()
            
            self.stats['inserted'] += len(batch_data)
            
        except Exception as e:
            logger.error(f"Error inserting batch: {e}")
            self.conn.rollback()
            self.stats['errors'] += len(batch_data)
            
    def run_import(self, ch_file=None, ch_dir=None):
        """Main import process"""
        start_time = datetime.now()
        
        try:
            self.connect()
            self.ensure_table_exists()
            
            # Find CH file(s) to import
            files_to_import = []
            
            if ch_file and os.path.exists(ch_file):
                files_to_import.append(ch_file)
            elif ch_dir and os.path.exists(ch_dir):
                # Find the most recent BasicCompanyData file
                ch_files = sorted(Path(ch_dir).glob('BasicCompanyDataAsOneFile*.csv'))
                if ch_files:
                    files_to_import.append(str(ch_files[-1]))  # Use the most recent
                    
            if not files_to_import:
                logger.error("No Companies House files found to import")
                return
                
            # Import each file
            for filepath in files_to_import:
                self.import_file(filepath)
                
            # Run VACUUM ANALYZE
            logger.info("Running VACUUM ANALYZE on companies_house_data...")
            self.cursor.execute("VACUUM ANALYZE companies_house_data")
            self.conn.commit()
            
            # Final statistics
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info("\n" + "="*50)
            logger.info("IMPORT COMPLETE")
            logger.info("="*50)
            logger.info(f"Files processed: {self.stats['files_processed']}")
            logger.info(f"Total records: {self.stats['total_records']:,}")
            logger.info(f"Inserted/Updated: {self.stats['inserted']:,}")
            logger.info(f"Errors: {self.stats['errors']:,}")
            logger.info(f"Time taken: {elapsed/60:.1f} minutes")
            logger.info(f"Log file: {log_filename}")
            
        except Exception as e:
            logger.error(f"Fatal error during import: {e}")
            raise
        finally:
            self.disconnect()

def main():
    parser = argparse.ArgumentParser(description='Import Companies House Basic Company Data')
    parser.add_argument('--file', type=str,
                       help='Specific CH file to import')
    parser.add_argument('--ch-dir', type=str,
                       default='DATA/SOURCE/CH',
                       help='Directory containing CH files (will use most recent)')
    parser.add_argument('--batch-size', type=int, default=10000,
                       help='Batch size for inserts (default: 10000)')
    
    args = parser.parse_args()
    
    importer = CompaniesHouseImporter(batch_size=args.batch_size)
    importer.run_import(ch_file=args.file, ch_dir=args.ch_dir)

if __name__ == '__main__':
    main()