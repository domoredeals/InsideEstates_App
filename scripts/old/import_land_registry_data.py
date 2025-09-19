#!/usr/bin/env python3
"""
Import Land Registry CCOD and OCOD data into PostgreSQL
Optimized for large-scale data processing with progress tracking
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
        logging.FileHandler('land_registry_import.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class LandRegistryImporter:
    def __init__(self, batch_size=5000):
        self.batch_size = batch_size
        self.conn = None
        self.cursor = None
        self.properties_batch = []
        self.proprietors_batch = []
        
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
            
    def extract_file_month(self, filename):
        """Extract year and month from filename"""
        # Pattern: CCOD_COU_2025_08.csv or OCOD_COU_2025_08.csv
        match = re.search(r'(\d{4})_(\d{2})\.csv', filename)
        if match:
            year, month = match.groups()
            return f"{year}-{month}-01"
        return None
        
    def check_already_imported(self, filename):
        """Check if file has already been imported"""
        self.cursor.execute(
            "SELECT id FROM import_history WHERE filename = %s AND status = 'completed'",
            (filename,)
        )
        return self.cursor.fetchone() is not None
        
    def start_import_tracking(self, filename, dataset_type, file_month):
        """Start tracking import progress"""
        self.cursor.execute("""
            INSERT INTO import_history (filename, dataset_type, file_month, import_started, status)
            VALUES (%s, %s, %s, %s, 'running')
            ON CONFLICT (filename) 
            DO UPDATE SET 
                import_started = EXCLUDED.import_started,
                status = 'running'
            RETURNING id
        """, (filename, dataset_type, file_month, datetime.now()))
        
        import_id = self.cursor.fetchone()[0]
        self.conn.commit()
        return import_id
        
    def complete_import_tracking(self, import_id, rows_processed, rows_inserted, rows_updated, rows_failed):
        """Complete import tracking"""
        self.cursor.execute("""
            UPDATE import_history 
            SET import_completed = %s,
                rows_processed = %s,
                rows_inserted = %s,
                rows_updated = %s,
                rows_failed = %s,
                status = 'completed'
            WHERE id = %s
        """, (datetime.now(), rows_processed, rows_inserted, rows_updated, rows_failed, import_id))
        self.conn.commit()
        
    def fail_import_tracking(self, import_id, error_message):
        """Mark import as failed"""
        self.cursor.execute("""
            UPDATE import_history 
            SET import_completed = %s,
                status = 'failed',
                error_message = %s
            WHERE id = %s
        """, (datetime.now(), str(error_message)[:1000], import_id))
        self.conn.commit()
        
    def clean_string(self, value):
        """Clean string values - remove NUL characters and handle None"""
        if value is None:
            return ''
        if isinstance(value, str):
            # Remove NUL (0x00) characters that PostgreSQL doesn't allow
            return value.replace('\x00', '').strip()
        return str(value).strip()
    
    def process_row(self, row, dataset_type, file_month):
        """Process a single CSV row"""
        # Extract property data with cleaned values
        property_data = {
            'title_number': self.clean_string(row.get('Title Number', '')),
            'tenure': self.clean_string(row.get('Tenure', '')),
            'property_address': self.clean_string(row.get('Property Address', '')),
            'district': self.clean_string(row.get('District', '')),
            'county': self.clean_string(row.get('County', '')),
            'region': self.clean_string(row.get('Region', '')),
            'postcode': self.clean_string(row.get('Postcode', '')),
            'multiple_address_indicator': self.clean_string(row.get('Multiple Address Indicator', '')),
            'price_paid': self.clean_string(row.get('Price Paid', '')) or None,
            'date_added': self.clean_string(row.get('Date Proprietor Added', '')) or None,
            'change_indicator': self.clean_string(row.get('Change Indicator', '')),
            'change_date': self.clean_string(row.get('Change Date', '')) or None,
            'dataset_type': dataset_type,
            'file_month': file_month
        }
        
        # Clean price_paid
        if property_data['price_paid']:
            try:
                property_data['price_paid'] = float(property_data['price_paid'])
            except ValueError:
                property_data['price_paid'] = None
                
        # Parse dates
        for date_field in ['date_added', 'change_date']:
            if property_data[date_field]:
                try:
                    property_data[date_field] = datetime.strptime(
                        property_data[date_field], '%d-%m-%Y'
                    ).date()
                except:
                    property_data[date_field] = None
                    
        # Extract proprietors (up to 4)
        proprietors = []
        for i in range(1, 5):
            prop_name = self.clean_string(row.get(f'Proprietor Name ({i})', ''))
            if prop_name:
                proprietor = {
                    'proprietor_number': i,
                    'proprietor_name': prop_name,
                    'company_registration_no': self.clean_string(row.get(f'Company Registration No. ({i})', '')) or None,
                    'proprietorship_category': self.clean_string(row.get(f'Proprietorship Category ({i})', '')) or None,
                    'country_incorporated': self.clean_string(row.get(f'Country Incorporated ({i})', '')) or None if dataset_type == 'OCOD' else None,
                    'address_1': self.clean_string(row.get(f'Proprietor ({i}) Address (1)', '')) or None,
                    'address_2': self.clean_string(row.get(f'Proprietor ({i}) Address (2)', '')) or None,
                    'address_3': self.clean_string(row.get(f'Proprietor ({i}) Address (3)', '')) or None,
                    'date_proprietor_added': property_data['date_added']
                }
                proprietors.append(proprietor)
                
        return property_data, proprietors
        
    def flush_batch(self):
        """Flush current batch to database"""
        rows_inserted = 0
        rows_updated = 0
        
        if not self.properties_batch:
            return rows_inserted, rows_updated
            
        try:
            # Deduplicate properties within batch by title_number (keep last occurrence)
            seen_titles = {}
            for i, prop in enumerate(self.properties_batch):
                seen_titles[prop['title_number']] = i
            
            unique_properties = [self.properties_batch[i] for i in sorted(seen_titles.values())]
            
            # Insert properties
            property_values = [
                (p['title_number'], p['tenure'], p['property_address'], 
                 p['district'], p['county'], p['region'], p['postcode'],
                 p['multiple_address_indicator'], p['price_paid'], 
                 p['date_added'], p['change_indicator'], p['change_date'],
                 p['dataset_type'], p['file_month'])
                for p in unique_properties
            ]
            
            # Use ON CONFLICT to handle updates
            execute_values(
                self.cursor,
                """
                INSERT INTO properties (
                    title_number, tenure, property_address, district, county, 
                    region, postcode, multiple_address_indicator, price_paid,
                    date_added, change_indicator, change_date, dataset_type, file_month
                ) VALUES %s
                ON CONFLICT (title_number) DO UPDATE SET
                    tenure = EXCLUDED.tenure,
                    property_address = EXCLUDED.property_address,
                    district = EXCLUDED.district,
                    county = EXCLUDED.county,
                    region = EXCLUDED.region,
                    postcode = EXCLUDED.postcode,
                    price_paid = COALESCE(EXCLUDED.price_paid, properties.price_paid),
                    change_indicator = EXCLUDED.change_indicator,
                    change_date = EXCLUDED.change_date,
                    file_month = EXCLUDED.file_month,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id, (xmax = 0) as inserted
                """,
                property_values,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                fetch=True
            )
            
            results = self.cursor.fetchall()
            property_ids = {p['title_number']: r[0] for p, r in zip(unique_properties, results)}
            rows_inserted = sum(1 for r in results if r[1])
            rows_updated = len(results) - rows_inserted
            
            # Insert proprietors (also handle deduplication)
            if self.proprietors_batch:
                # Only keep proprietors for unique properties
                unique_title_numbers = set(p['title_number'] for p in unique_properties)
                unique_proprietors = [(tn, props) for tn, props in self.proprietors_batch 
                                    if tn in unique_title_numbers]
                
                proprietor_values = []
                for title_number, props in unique_proprietors:
                    if title_number in property_ids:
                        property_id = property_ids[title_number]
                        for prop in props:
                            proprietor_values.append((
                                property_id, prop['proprietor_number'],
                                prop['proprietor_name'], prop['company_registration_no'],
                                prop['proprietorship_category'], prop['country_incorporated'],
                                prop['address_1'], prop['address_2'], prop['address_3'],
                                prop['date_proprietor_added']
                            ))
                            
                if proprietor_values:
                    execute_values(
                        self.cursor,
                        """
                        INSERT INTO proprietors (
                            property_id, proprietor_number, proprietor_name,
                            company_registration_no, proprietorship_category,
                            country_incorporated, address_1, address_2, address_3,
                            date_proprietor_added
                        ) VALUES %s
                        ON CONFLICT (property_id, proprietor_number) DO UPDATE SET
                            proprietor_name = EXCLUDED.proprietor_name,
                            company_registration_no = EXCLUDED.company_registration_no,
                            proprietorship_category = EXCLUDED.proprietorship_category,
                            country_incorporated = EXCLUDED.country_incorporated,
                            address_1 = EXCLUDED.address_1,
                            address_2 = EXCLUDED.address_2,
                            address_3 = EXCLUDED.address_3,
                            date_proprietor_added = EXCLUDED.date_proprietor_added
                        """,
                        proprietor_values,
                        template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    )
                    
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Error flushing batch: {e}")
            self.conn.rollback()
            raise
        finally:
            # Clear batches
            self.properties_batch = []
            self.proprietors_batch = []
            
        return rows_inserted, rows_updated
        
    def import_file(self, filepath, dataset_type):
        """Import a single CSV file"""
        filename = os.path.basename(filepath)
        file_month = self.extract_file_month(filename)
        
        if not file_month:
            logger.error(f"Could not extract date from filename: {filename}")
            return
            
        # Check if already imported
        if self.check_already_imported(filename):
            logger.info(f"File {filename} already imported, skipping...")
            return
            
        logger.info(f"Importing {filename} ({dataset_type})...")
        import_id = self.start_import_tracking(filename, dataset_type, file_month)
        
        rows_processed = 0
        rows_failed = 0
        total_inserted = 0
        total_updated = 0
        
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
                                
                            property_data, proprietors = self.process_row(row, dataset_type, file_month)
                            
                            if property_data['title_number']:
                                self.properties_batch.append(property_data)
                                if proprietors:
                                    self.proprietors_batch.append((property_data['title_number'], proprietors))
                                    
                                # Flush when batch is full
                                if len(self.properties_batch) >= self.batch_size:
                                    inserted, updated = self.flush_batch()
                                    total_inserted += inserted
                                    total_updated += updated
                                    
                            rows_processed += 1
                            
                        except Exception as e:
                            logger.error(f"Error processing row {rows_processed + 1}: {e}")
                            logger.debug(f"Row data: {row}")
                            rows_failed += 1
                            
                        pbar.update(1)
                        
                # Flush remaining
                if self.properties_batch:
                    inserted, updated = self.flush_batch()
                    total_inserted += inserted
                    total_updated += updated
                    
            # Update company statistics
            logger.info("Updating company statistics...")
            self.cursor.execute("SELECT update_company_stats()")
            self.conn.commit()
            
            self.complete_import_tracking(import_id, rows_processed, total_inserted, total_updated, rows_failed)
            logger.info(f"Completed {filename}: {rows_processed} processed, {total_inserted} inserted, {total_updated} updated, {rows_failed} failed")
            
        except Exception as e:
            logger.error(f"Failed to import {filename}: {e}")
            self.fail_import_tracking(import_id, e)
            raise
            
    def import_directory(self, directory_path, dataset_type):
        """Import all CSV files from a directory"""
        csv_files = sorted(Path(directory_path).glob('*.csv'))
        logger.info(f"Found {len(csv_files)} CSV files in {directory_path}")
        
        for csv_file in csv_files:
            try:
                self.import_file(str(csv_file), dataset_type)
            except Exception as e:
                logger.error(f"Failed to import {csv_file}: {e}")
                continue
                
def main():
    """Main import function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Import Land Registry data to PostgreSQL')
    parser.add_argument('--ccod-dir', help='Path to CCOD CSV directory')
    parser.add_argument('--ocod-dir', help='Path to OCOD CSV directory')
    parser.add_argument('--batch-size', type=int, default=5000, help='Batch size for inserts')
    parser.add_argument('--create-schema', action='store_true', help='Create database schema first')
    
    args = parser.parse_args()
    
    # Default paths if not provided
    if not args.ccod_dir and not args.ocod_dir:
        base_path = '/home/adc/Projects/InsideEstates_App/DATA/SOURCE/LR'
        args.ccod_dir = os.path.join(base_path, 'CCOD')
        args.ocod_dir = os.path.join(base_path, 'OCOD')
    
    importer = LandRegistryImporter(batch_size=args.batch_size)
    
    try:
        importer.connect()
        
        # Create schema if requested
        if args.create_schema:
            logger.info("Creating database schema...")
            with open('/home/adc/Projects/InsideEstates_App/scripts/create_lr_schema.sql', 'r') as f:
                importer.cursor.execute(f.read())
                importer.conn.commit()
            logger.info("Schema created successfully")
        
        # Import CCOD files
        if args.ccod_dir and os.path.exists(args.ccod_dir):
            logger.info(f"Starting CCOD import from {args.ccod_dir}")
            importer.import_directory(args.ccod_dir, 'CCOD')
            
        # Import OCOD files
        if args.ocod_dir and os.path.exists(args.ocod_dir):
            logger.info(f"Starting OCOD import from {args.ocod_dir}")
            importer.import_directory(args.ocod_dir, 'OCOD')
            
        logger.info("Import completed successfully!")
        
    except Exception as e:
        logger.error(f"Import failed: {e}")
        raise
    finally:
        importer.disconnect()
        
if __name__ == "__main__":
    main()