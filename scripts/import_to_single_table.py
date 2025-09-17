#!/usr/bin/env python3
"""
Import Land Registry CCOD and OCOD data into a single PostgreSQL table
All property and proprietor data combined in one table
"""
import os
import sys
import csv
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import logging
from pathlib import Path
from tqdm import tqdm
import re

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('land_registry_single_table_import.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SingleTableImporter:
    def __init__(self, batch_size=5000):
        self.batch_size = batch_size
        self.conn = None
        self.cursor = None
        self.batch = []
        
    def connect(self):
        """Connect to PostgreSQL with optimized settings"""
        try:
            self.conn = psycopg2.connect(**POSTGRESQL_CONFIG)
            self.cursor = self.conn.cursor()
            
            # Apply bulk loading optimizations
            logger.info("Applying bulk load optimizations...")
            optimizations = [
                "SET synchronous_commit = OFF",
                "SET work_mem = '1GB'",
                "SET maintenance_work_mem = '8GB'",
            ]
            
            for opt in optimizations:
                self.cursor.execute(opt)
                
            self.conn.commit()
            logger.info("Connected to PostgreSQL")
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
            
    def disconnect(self):
        """Disconnect and cleanup"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            
    def extract_file_info(self, filename):
        """Extract dataset type, update type, year and month from filename"""
        # Pattern: CCOD_FULL_2025_08.csv or OCOD_COU_2025_08.csv
        match = re.search(r'(CCOD|OCOD)_(FULL|COU)_(\d{4})_(\d{2})\.csv', filename)
        if match:
            dataset_type, update_type, year, month = match.groups()
            return dataset_type, update_type, f"{year}-{month}-01"
        return None, None, None
        
    def clean_string(self, value):
        """Clean string values - remove NUL characters and handle None"""
        if value is None:
            return ''
        if isinstance(value, str):
            # Remove NUL (0x00) characters that PostgreSQL doesn't allow
            return value.replace('\x00', '').strip()
        return str(value).strip()
    
    def process_row(self, row, dataset_type, update_type, file_month, source_filename):
        """Process a single CSV row into a flat record"""
        record = {
            # Property fields
            'title_number': self.clean_string(row.get('Title Number', '')),
            'tenure': self.clean_string(row.get('Tenure', '')),
            'property_address': self.clean_string(row.get('Property Address', '')),
            'district': self.clean_string(row.get('District', '')),
            'county': self.clean_string(row.get('County', '')),
            'region': self.clean_string(row.get('Region', '')),
            'postcode': self.clean_string(row.get('Postcode', '')),
            'multiple_address_indicator': self.clean_string(row.get('Multiple Address Indicator', '')),
            'additional_proprietor_indicator': self.clean_string(row.get('Additional Proprietor Indicator', '')),
            'price_paid': self.clean_string(row.get('Price Paid', '')) or None,
            'date_proprietor_added': self.clean_string(row.get('Date Proprietor Added', '')) or None,
            'change_indicator': self.clean_string(row.get('Change Indicator', '')),
            'change_date': self.clean_string(row.get('Change Date', '')) or None,
            'dataset_type': dataset_type,
            'update_type': update_type,
            'file_month': file_month,
            'source_filename': source_filename,
            
            # Proprietor 1 fields
            'proprietor_1_name': self.clean_string(row.get('Proprietor Name (1)', '')),
            'company_1_reg_no': self.clean_string(row.get('Company Registration No. (1)', '')),
            'proprietorship_1_category': self.clean_string(row.get('Proprietorship Category (1)', '')),
            'country_1_incorporated': self.clean_string(row.get('Country Incorporated (1)', '')) if dataset_type == 'OCOD' else '',
            'proprietor_1_address_1': self.clean_string(row.get('Proprietor (1) Address (1)', '')),
            'proprietor_1_address_2': self.clean_string(row.get('Proprietor (1) Address (2)', '')),
            'proprietor_1_address_3': self.clean_string(row.get('Proprietor (1) Address (3)', '')),
            
            # Proprietor 2 fields
            'proprietor_2_name': self.clean_string(row.get('Proprietor Name (2)', '')),
            'company_2_reg_no': self.clean_string(row.get('Company Registration No. (2)', '')),
            'proprietorship_2_category': self.clean_string(row.get('Proprietorship Category (2)', '')),
            'country_2_incorporated': self.clean_string(row.get('Country Incorporated (2)', '')) if dataset_type == 'OCOD' else '',
            'proprietor_2_address_1': self.clean_string(row.get('Proprietor (2) Address (1)', '')),
            'proprietor_2_address_2': self.clean_string(row.get('Proprietor (2) Address (2)', '')),
            'proprietor_2_address_3': self.clean_string(row.get('Proprietor (2) Address (3)', '')),
            
            # Proprietor 3 fields
            'proprietor_3_name': self.clean_string(row.get('Proprietor Name (3)', '')),
            'company_3_reg_no': self.clean_string(row.get('Company Registration No. (3)', '')),
            'proprietorship_3_category': self.clean_string(row.get('Proprietorship Category (3)', '')),
            'country_3_incorporated': self.clean_string(row.get('Country Incorporated (3)', '')) if dataset_type == 'OCOD' else '',
            'proprietor_3_address_1': self.clean_string(row.get('Proprietor (3) Address (1)', '')),
            'proprietor_3_address_2': self.clean_string(row.get('Proprietor (3) Address (2)', '')),
            'proprietor_3_address_3': self.clean_string(row.get('Proprietor (3) Address (3)', '')),
            
            # Proprietor 4 fields
            'proprietor_4_name': self.clean_string(row.get('Proprietor Name (4)', '')),
            'company_4_reg_no': self.clean_string(row.get('Company Registration No. (4)', '')),
            'proprietorship_4_category': self.clean_string(row.get('Proprietorship Category (4)', '')),
            'country_4_incorporated': self.clean_string(row.get('Country Incorporated (4)', '')) if dataset_type == 'OCOD' else '',
            'proprietor_4_address_1': self.clean_string(row.get('Proprietor (4) Address (1)', '')),
            'proprietor_4_address_2': self.clean_string(row.get('Proprietor (4) Address (2)', '')),
            'proprietor_4_address_3': self.clean_string(row.get('Proprietor (4) Address (3)', ''))
        }
        
        # Clean price_paid
        if record['price_paid']:
            try:
                record['price_paid'] = float(record['price_paid'])
            except ValueError:
                record['price_paid'] = None
                
        # Parse dates
        for date_field in ['date_proprietor_added', 'change_date']:
            if record[date_field]:
                try:
                    record[date_field] = datetime.strptime(
                        record[date_field], '%d-%m-%Y'
                    ).date()
                except:
                    record[date_field] = None
        
        # For FULL files, set change_indicator to 'A' and change_date to date_proprietor_added
        if update_type == 'FULL':
            record['change_indicator'] = 'A'
            if record['date_proprietor_added'] and not record['change_date']:
                record['change_date'] = record['date_proprietor_added']
                    
        return record
        
    def flush_batch(self):
        """Flush current batch to database"""
        if not self.batch:
            return 0
            
        try:
            # Deduplicate within batch by title_number + file_month
            # Keep the last occurrence of each duplicate
            seen = {}
            for i, record in enumerate(self.batch):
                key = (record['title_number'], record['file_month'])
                seen[key] = i
            
            # Get unique records (last occurrence of each)
            unique_records = [self.batch[i] for i in sorted(seen.values())]
            
            # Build values list with all columns
            values = []
            for record in unique_records:
                values.append((
                    # Property fields
                    record['title_number'], record['tenure'], record['property_address'],
                    record['district'], record['county'], record['region'], record['postcode'],
                    record['multiple_address_indicator'], record['additional_proprietor_indicator'],
                    record['price_paid'], record['date_proprietor_added'], 
                    record['change_indicator'], record['change_date'],
                    record['dataset_type'], record['update_type'], record['file_month'],
                    record['source_filename'],
                    
                    # Proprietor 1
                    record['proprietor_1_name'], record['company_1_reg_no'], 
                    record['proprietorship_1_category'], record['country_1_incorporated'],
                    record['proprietor_1_address_1'], record['proprietor_1_address_2'], 
                    record['proprietor_1_address_3'],
                    
                    # Proprietor 2
                    record['proprietor_2_name'], record['company_2_reg_no'], 
                    record['proprietorship_2_category'], record['country_2_incorporated'],
                    record['proprietor_2_address_1'], record['proprietor_2_address_2'], 
                    record['proprietor_2_address_3'],
                    
                    # Proprietor 3
                    record['proprietor_3_name'], record['company_3_reg_no'], 
                    record['proprietorship_3_category'], record['country_3_incorporated'],
                    record['proprietor_3_address_1'], record['proprietor_3_address_2'], 
                    record['proprietor_3_address_3'],
                    
                    # Proprietor 4
                    record['proprietor_4_name'], record['company_4_reg_no'], 
                    record['proprietorship_4_category'], record['country_4_incorporated'],
                    record['proprietor_4_address_1'], record['proprietor_4_address_2'], 
                    record['proprietor_4_address_3']
                ))
            
            # Use execute_values for batch insert
            execute_values(
                self.cursor,
                """
                INSERT INTO land_registry_data (
                    title_number, tenure, property_address, district, county, region, postcode,
                    multiple_address_indicator, additional_proprietor_indicator,
                    price_paid, date_proprietor_added, change_indicator, change_date,
                    dataset_type, update_type, file_month, source_filename,
                    
                    proprietor_1_name, company_1_reg_no, proprietorship_1_category, country_1_incorporated,
                    proprietor_1_address_1, proprietor_1_address_2, proprietor_1_address_3,
                    
                    proprietor_2_name, company_2_reg_no, proprietorship_2_category, country_2_incorporated,
                    proprietor_2_address_1, proprietor_2_address_2, proprietor_2_address_3,
                    
                    proprietor_3_name, company_3_reg_no, proprietorship_3_category, country_3_incorporated,
                    proprietor_3_address_1, proprietor_3_address_2, proprietor_3_address_3,
                    
                    proprietor_4_name, company_4_reg_no, proprietorship_4_category, country_4_incorporated,
                    proprietor_4_address_1, proprietor_4_address_2, proprietor_4_address_3
                ) VALUES %s
                ON CONFLICT (title_number, file_month) DO UPDATE SET
                    tenure = EXCLUDED.tenure,
                    property_address = EXCLUDED.property_address,
                    district = EXCLUDED.district,
                    county = EXCLUDED.county,
                    region = EXCLUDED.region,
                    postcode = EXCLUDED.postcode,
                    multiple_address_indicator = EXCLUDED.multiple_address_indicator,
                    additional_proprietor_indicator = EXCLUDED.additional_proprietor_indicator,
                    price_paid = COALESCE(EXCLUDED.price_paid, land_registry_data.price_paid),
                    date_proprietor_added = EXCLUDED.date_proprietor_added,
                    change_indicator = EXCLUDED.change_indicator,
                    change_date = EXCLUDED.change_date,
                    update_type = EXCLUDED.update_type,
                    
                    proprietor_1_name = EXCLUDED.proprietor_1_name,
                    company_1_reg_no = EXCLUDED.company_1_reg_no,
                    proprietorship_1_category = EXCLUDED.proprietorship_1_category,
                    country_1_incorporated = EXCLUDED.country_1_incorporated,
                    proprietor_1_address_1 = EXCLUDED.proprietor_1_address_1,
                    proprietor_1_address_2 = EXCLUDED.proprietor_1_address_2,
                    proprietor_1_address_3 = EXCLUDED.proprietor_1_address_3,
                    
                    proprietor_2_name = EXCLUDED.proprietor_2_name,
                    company_2_reg_no = EXCLUDED.company_2_reg_no,
                    proprietorship_2_category = EXCLUDED.proprietorship_2_category,
                    country_2_incorporated = EXCLUDED.country_2_incorporated,
                    proprietor_2_address_1 = EXCLUDED.proprietor_2_address_1,
                    proprietor_2_address_2 = EXCLUDED.proprietor_2_address_2,
                    proprietor_2_address_3 = EXCLUDED.proprietor_2_address_3,
                    
                    proprietor_3_name = EXCLUDED.proprietor_3_name,
                    company_3_reg_no = EXCLUDED.company_3_reg_no,
                    proprietorship_3_category = EXCLUDED.proprietorship_3_category,
                    country_3_incorporated = EXCLUDED.country_3_incorporated,
                    proprietor_3_address_1 = EXCLUDED.proprietor_3_address_1,
                    proprietor_3_address_2 = EXCLUDED.proprietor_3_address_2,
                    proprietor_3_address_3 = EXCLUDED.proprietor_3_address_3,
                    
                    proprietor_4_name = EXCLUDED.proprietor_4_name,
                    company_4_reg_no = EXCLUDED.company_4_reg_no,
                    proprietorship_4_category = EXCLUDED.proprietorship_4_category,
                    country_4_incorporated = EXCLUDED.country_4_incorporated,
                    proprietor_4_address_1 = EXCLUDED.proprietor_4_address_1,
                    proprietor_4_address_2 = EXCLUDED.proprietor_4_address_2,
                    proprietor_4_address_3 = EXCLUDED.proprietor_4_address_3,
                    
                    updated_at = CURRENT_TIMESTAMP
                """,
                values,
                template="(" + ",".join(["%s"] * 45) + ")"
            )
            
            self.conn.commit()
            count = len(unique_records)
            self.batch = []
            return count
            
        except Exception as e:
            logger.error(f"Error flushing batch: {e}")
            self.conn.rollback()
            raise
            
    def import_file(self, filepath):
        """Import a single CSV file"""
        filename = os.path.basename(filepath)
        dataset_type, update_type, file_month = self.extract_file_info(filename)
        
        if not dataset_type:
            logger.error(f"Could not extract info from filename: {filename}")
            return
            
        logger.info(f"Importing {filename} ({dataset_type} {update_type})...")
        
        rows_processed = 0
        rows_failed = 0
        
        try:
            # Count total rows for progress bar
            with open(filepath, 'r', encoding='utf-8-sig') as f:
                total_rows = sum(1 for line in f) - 1  # Subtract header
                
            with open(filepath, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                
                with tqdm(total=total_rows, desc=filename) as pbar:
                    for row in reader:
                        try:
                            # Skip empty rows
                            if not any(row.values()):
                                continue
                                
                            record = self.process_row(row, dataset_type, update_type, file_month, filename)
                            
                            # Skip footer/summary rows
                            if record['title_number'] == 'Row Count:':
                                continue
                                
                            if record['title_number']:
                                self.batch.append(record)
                                    
                                # Flush when batch is full
                                if len(self.batch) >= self.batch_size:
                                    self.flush_batch()
                                    
                            rows_processed += 1
                            
                        except Exception as e:
                            logger.error(f"Error processing row {rows_processed + 1}: {e}")
                            logger.debug(f"Row data: {row}")
                            rows_failed += 1
                            
                        pbar.update(1)
                        
                # Flush remaining
                if self.batch:
                    self.flush_batch()
                    
            logger.info(f"Completed {filename}: {rows_processed} processed, {rows_failed} failed")
            
        except Exception as e:
            logger.error(f"Failed to import {filename}: {e}")
            raise
            
    def import_directory(self, directory_path):
        """Import all CSV files from a directory"""
        csv_files = sorted(Path(directory_path).glob('*.csv'))
        logger.info(f"Found {len(csv_files)} CSV files in {directory_path}")
        
        for csv_file in csv_files:
            try:
                self.import_file(str(csv_file))
            except Exception as e:
                logger.error(f"Failed to import {csv_file}: {e}")
                continue
                
def main():
    """Main import function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Import Land Registry data to single PostgreSQL table')
    parser.add_argument('--ccod-dir', help='Path to CCOD CSV directory')
    parser.add_argument('--ocod-dir', help='Path to OCOD CSV directory')
    parser.add_argument('--batch-size', type=int, default=5000, help='Batch size for inserts')
    parser.add_argument('--create-table', action='store_true', help='Create the single table first')
    
    args = parser.parse_args()
    
    # Default paths if not provided
    if not args.ccod_dir and not args.ocod_dir:
        base_path = '/home/adc/Projects/InsideEstates_App/DATA/SOURCE/LR'
        args.ccod_dir = os.path.join(base_path, 'CCOD')
        args.ocod_dir = os.path.join(base_path, 'OCOD')
    
    importer = SingleTableImporter(batch_size=args.batch_size)
    
    try:
        importer.connect()
        
        # Create table if requested
        if args.create_table:
            logger.info("Creating single table schema...")
            with open('/home/adc/Projects/InsideEstates_App/scripts/create_single_table.sql', 'r') as f:
                importer.cursor.execute(f.read())
                importer.conn.commit()
            logger.info("Table created successfully")
        
        # Import CCOD files
        if args.ccod_dir and os.path.exists(args.ccod_dir):
            logger.info(f"Starting CCOD import from {args.ccod_dir}")
            importer.import_directory(args.ccod_dir)
            
        # Import OCOD files
        if args.ocod_dir and os.path.exists(args.ocod_dir):
            logger.info(f"Starting OCOD import from {args.ocod_dir}")
            importer.import_directory(args.ocod_dir)
            
        logger.info("Import completed successfully!")
        
    except Exception as e:
        logger.error(f"Import failed: {e}")
        raise
    finally:
        importer.disconnect()
        
if __name__ == "__main__":
    main()