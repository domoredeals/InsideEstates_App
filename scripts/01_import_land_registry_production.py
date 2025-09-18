#!/usr/bin/env python3
"""
Production Land Registry Import Script
Part 1 of 3 in the InsideEstates data pipeline

This script imports Land Registry CCOD/OCOD data files into PostgreSQL.
Handles both FULL updates and Change Only Updates (COU).
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
import re

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

# Configure logging
log_filename = f'lr_import_production_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class LandRegistryImporter:
    def __init__(self, batch_size=5000):
        self.batch_size = batch_size
        self.conn = None
        self.cursor = None
        self.imported_files = set()
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
        """Ensure the land_registry_data table exists with all columns"""
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS land_registry_data (
                id BIGSERIAL PRIMARY KEY,
                title_number VARCHAR(50),
                tenure VARCHAR(50),
                property_address TEXT,
                district TEXT,
                county TEXT,
                region TEXT,
                postcode VARCHAR(20),
                multiple_address_indicator CHAR(1),
                additional_proprietor_indicator CHAR(1),
                price_paid BIGINT,
                
                -- Proprietor 1
                proprietor_1_name TEXT,
                company_1_reg_no VARCHAR(50),
                proprietorship_1_category VARCHAR(100),
                country_1_incorporated TEXT,
                proprietor_1_address_1 TEXT,
                proprietor_1_address_2 TEXT,
                proprietor_1_address_3 TEXT,
                
                -- Proprietor 2
                proprietor_2_name TEXT,
                company_2_reg_no VARCHAR(50),
                proprietorship_2_category VARCHAR(100),
                country_2_incorporated TEXT,
                proprietor_2_address_1 TEXT,
                proprietor_2_address_2 TEXT,
                proprietor_2_address_3 TEXT,
                
                -- Proprietor 3
                proprietor_3_name TEXT,
                company_3_reg_no VARCHAR(50),
                proprietorship_3_category VARCHAR(100),
                country_3_incorporated TEXT,
                proprietor_3_address_1 TEXT,
                proprietor_3_address_2 TEXT,
                proprietor_3_address_3 TEXT,
                
                -- Proprietor 4
                proprietor_4_name TEXT,
                company_4_reg_no VARCHAR(50),
                proprietorship_4_category VARCHAR(100),
                country_4_incorporated TEXT,
                proprietor_4_address_1 TEXT,
                proprietor_4_address_2 TEXT,
                proprietor_4_address_3 TEXT,
                
                date_proprietor_added DATE,
                change_indicator CHAR(1),
                change_date DATE,
                
                -- Metadata
                dataset_type VARCHAR(10),
                file_month DATE,
                update_type VARCHAR(10),
                source_filename TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                -- Unique constraint
                CONSTRAINT uk_title_file UNIQUE (title_number, file_month)
            );
        """)
        
        # Create indexes for performance
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_lr_title_number ON land_registry_data(title_number);",
            "CREATE INDEX IF NOT EXISTS idx_lr_file_month ON land_registry_data(file_month);",
            "CREATE INDEX IF NOT EXISTS idx_lr_postcode ON land_registry_data(postcode);",
            "CREATE INDEX IF NOT EXISTS idx_lr_dataset_type ON land_registry_data(dataset_type);",
            "CREATE INDEX IF NOT EXISTS idx_lr_company_1_reg ON land_registry_data(company_1_reg_no) WHERE company_1_reg_no IS NOT NULL;",
            "CREATE INDEX IF NOT EXISTS idx_lr_company_2_reg ON land_registry_data(company_2_reg_no) WHERE company_2_reg_no IS NOT NULL;",
            "CREATE INDEX IF NOT EXISTS idx_lr_company_3_reg ON land_registry_data(company_3_reg_no) WHERE company_3_reg_no IS NOT NULL;",
            "CREATE INDEX IF NOT EXISTS idx_lr_company_4_reg ON land_registry_data(company_4_reg_no) WHERE company_4_reg_no IS NOT NULL;"
        ]
        
        for idx_sql in indexes:
            self.cursor.execute(idx_sql)
            
        self.conn.commit()
        logger.info("Ensured land_registry_data table exists with all indexes")
        
    def get_imported_files(self):
        """Get list of already imported files"""
        self.cursor.execute("""
            SELECT DISTINCT source_filename 
            FROM land_registry_data 
            WHERE source_filename IS NOT NULL
        """)
        self.imported_files = {row[0] for row in self.cursor.fetchall()}
        logger.info(f"Found {len(self.imported_files)} already imported files")
        
    def extract_file_info(self, filename):
        """Extract dataset type, date, and update type from filename"""
        # Parse filename like CCOD_FULL_2024_10.csv or OCOD_COU_2024_10.csv
        base_name = os.path.basename(filename)
        match = re.match(r'(CCOD|OCOD)_(FULL|COU)_(\d{4})_(\d{2})', base_name)
        
        if match:
            dataset_type = match.group(1)
            update_type = match.group(2)
            year = int(match.group(3))
            month = int(match.group(4))
            file_month = datetime(year, month, 1).date()
            return dataset_type, update_type, file_month
        else:
            logger.warning(f"Could not parse filename: {filename}")
            return None, None, None
            
    def parse_date(self, date_str):
        """Parse date in DD-MM-YYYY format"""
        if not date_str or date_str.strip() == '':
            return None
        try:
            # Handle different date formats
            if '-' in date_str:
                parts = date_str.strip().split('-')
                if len(parts) == 3:
                    if len(parts[0]) == 4:  # YYYY-MM-DD
                        return datetime.strptime(date_str.strip(), '%Y-%m-%d').date()
                    else:  # DD-MM-YYYY
                        return datetime.strptime(date_str.strip(), '%d-%m-%Y').date()
            return None
        except:
            return None
            
    def clean_value(self, value):
        """Clean and standardize values"""
        if value is None or value.strip() == '':
            return None
        # Remove extra whitespace
        value = ' '.join(value.strip().split())
        return value if value else None
        
    def import_file(self, filepath):
        """Import a single Land Registry file"""
        filename = os.path.basename(filepath)
        
        # Skip if already imported
        if filename in self.imported_files:
            logger.info(f"Skipping already imported file: {filename}")
            return
            
        # Extract metadata from filename
        dataset_type, update_type, file_month = self.extract_file_info(filename)
        if not dataset_type:
            logger.error(f"Skipping file with unparseable name: {filename}")
            return
            
        logger.info(f"Importing {filename} - {dataset_type} {update_type} {file_month}")
        
        # Count lines for progress bar
        total_lines = sum(1 for _ in open(filepath, 'r', encoding='utf-8'))
        
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            batch_data = []
            pbar = tqdm(total=total_lines-1, desc=f"Processing {filename}")
            
            for row in reader:
                # Handle COU deletions
                if update_type == 'COU' and row.get('Change Indicator') == 'D':
                    # For deletions, we might want to mark records as deleted
                    # For now, we'll skip them
                    pbar.update(1)
                    continue
                    
                # Parse the row
                record = {
                    'title_number': self.clean_value(row.get('Title Number')),
                    'tenure': self.clean_value(row.get('Tenure')),
                    'property_address': self.clean_value(row.get('Property Address')),
                    'district': self.clean_value(row.get('District')),
                    'county': self.clean_value(row.get('County')),
                    'region': self.clean_value(row.get('Region')),
                    'postcode': self.clean_value(row.get('Postcode')),
                    'multiple_address_indicator': row.get('Multiple Address Indicator'),
                    'additional_proprietor_indicator': row.get('Additional Proprietor Indicator'),
                    'price_paid': int(row['Price Paid']) if row.get('Price Paid') and row['Price Paid'].strip() else None,
                    
                    # Proprietors
                    'proprietor_1_name': self.clean_value(row.get('Proprietor Name (1)')),
                    'company_1_reg_no': self.clean_value(row.get('Company Registration No. (1)')),
                    'proprietorship_1_category': self.clean_value(row.get('Proprietorship Category (1)')),
                    'country_1_incorporated': self.clean_value(row.get('Country Incorporated (1)')),
                    'proprietor_1_address_1': self.clean_value(row.get('Proprietor (1) Address (1)')),
                    'proprietor_1_address_2': self.clean_value(row.get('Proprietor (1) Address (2)')),
                    'proprietor_1_address_3': self.clean_value(row.get('Proprietor (1) Address (3)')),
                    
                    'proprietor_2_name': self.clean_value(row.get('Proprietor Name (2)')),
                    'company_2_reg_no': self.clean_value(row.get('Company Registration No. (2)')),
                    'proprietorship_2_category': self.clean_value(row.get('Proprietorship Category (2)')),
                    'country_2_incorporated': self.clean_value(row.get('Country Incorporated (2)')),
                    'proprietor_2_address_1': self.clean_value(row.get('Proprietor (2) Address (1)')),
                    'proprietor_2_address_2': self.clean_value(row.get('Proprietor (2) Address (2)')),
                    'proprietor_2_address_3': self.clean_value(row.get('Proprietor (2) Address (3)')),
                    
                    'proprietor_3_name': self.clean_value(row.get('Proprietor Name (3)')),
                    'company_3_reg_no': self.clean_value(row.get('Company Registration No. (3)')),
                    'proprietorship_3_category': self.clean_value(row.get('Proprietorship Category (3)')),
                    'country_3_incorporated': self.clean_value(row.get('Country Incorporated (3)')),
                    'proprietor_3_address_1': self.clean_value(row.get('Proprietor (3) Address (1)')),
                    'proprietor_3_address_2': self.clean_value(row.get('Proprietor (3) Address (2)')),
                    'proprietor_3_address_3': self.clean_value(row.get('Proprietor (3) Address (3)')),
                    
                    'proprietor_4_name': self.clean_value(row.get('Proprietor Name (4)')),
                    'company_4_reg_no': self.clean_value(row.get('Company Registration No. (4)')),
                    'proprietorship_4_category': self.clean_value(row.get('Proprietorship Category (4)')),
                    'country_4_incorporated': self.clean_value(row.get('Country Incorporated (4)')),
                    'proprietor_4_address_1': self.clean_value(row.get('Proprietor (4) Address (1)')),
                    'proprietor_4_address_2': self.clean_value(row.get('Proprietor (4) Address (2)')),
                    'proprietor_4_address_3': self.clean_value(row.get('Proprietor (4) Address (3)')),
                    
                    'date_proprietor_added': self.parse_date(row.get('Date Proprietor Added')),
                    'change_indicator': row.get('Change Indicator'),
                    'change_date': self.parse_date(row.get('Change Date')),
                    
                    # Metadata
                    'dataset_type': dataset_type,
                    'file_month': file_month,
                    'update_type': update_type,
                    'source_filename': filename
                }
                
                batch_data.append(record)
                
                # Process batch
                if len(batch_data) >= self.batch_size:
                    self.insert_batch(batch_data)
                    batch_data = []
                    
                pbar.update(1)
                self.stats['total_records'] += 1
                
            # Insert remaining records
            if batch_data:
                self.insert_batch(batch_data)
                
            pbar.close()
            
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
                INSERT INTO land_registry_data ({', '.join(columns)})
                VALUES %s
                ON CONFLICT (title_number, file_month) DO UPDATE SET
                    {', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col not in ['title_number', 'file_month']])},
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
            
    def import_directory(self, directory_path, dataset_filter=None):
        """Import all CSV files from a directory"""
        csv_files = sorted([f for f in Path(directory_path).glob('*.csv') if f.is_file()])
        
        # Filter by dataset type if specified
        if dataset_filter:
            csv_files = [f for f in csv_files if dataset_filter in f.name]
            
        logger.info(f"Found {len(csv_files)} CSV files to process")
        
        for csv_file in csv_files:
            self.import_file(str(csv_file))
            
    def run_import(self, ccod_dir=None, ocod_dir=None):
        """Main import process"""
        start_time = datetime.now()
        
        try:
            self.connect()
            self.ensure_table_exists()
            self.get_imported_files()
            
            # Import CCOD files
            if ccod_dir and os.path.exists(ccod_dir):
                logger.info(f"Importing CCOD files from {ccod_dir}")
                self.import_directory(ccod_dir, 'CCOD')
                
            # Import OCOD files
            if ocod_dir and os.path.exists(ocod_dir):
                logger.info(f"Importing OCOD files from {ocod_dir}")
                self.import_directory(ocod_dir, 'OCOD')
                
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
    parser = argparse.ArgumentParser(description='Import Land Registry CCOD/OCOD data')
    parser.add_argument('--ccod-dir', type=str, 
                       default='DATA/SOURCE/LR/CCOD',
                       help='Directory containing CCOD CSV files')
    parser.add_argument('--ocod-dir', type=str,
                       default='DATA/SOURCE/LR/OCOD',
                       help='Directory containing OCOD CSV files')
    parser.add_argument('--batch-size', type=int, default=5000,
                       help='Batch size for inserts (default: 5000)')
    
    args = parser.parse_args()
    
    importer = LandRegistryImporter(batch_size=args.batch_size)
    importer.run_import(ccod_dir=args.ccod_dir, ocod_dir=args.ocod_dir)

if __name__ == '__main__':
    main()