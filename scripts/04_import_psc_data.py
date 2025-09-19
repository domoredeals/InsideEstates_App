#!/usr/bin/env python3
"""
Production PSC (Persons with Significant Control) Import Script
Part 4 of the InsideEstates data pipeline

This script imports Companies House PSC data into PostgreSQL.
Handles JSON Lines format with various PSC types (individual, corporate-entity, etc.)
"""

import os
import sys
import json
import psycopg2
from psycopg2.extras import execute_values, Json
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
log_filename = f'psc_import_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PSCImporter:
    def __init__(self, batch_size=5000):
        self.batch_size = batch_size
        self.conn = None
        self.cursor = None
        self.stats = {
            'total_records': 0,
            'individual_psc': 0,
            'corporate_psc': 0,
            'legal_person_psc': 0,
            'other_psc': 0,
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
            
    def create_table(self):
        """Create the psc_data table if requested"""
        logger.info("Creating psc_data table...")
        
        # Drop existing table if requested
        self.cursor.execute("DROP TABLE IF EXISTS psc_data CASCADE")
        
        # Create the table
        self.cursor.execute("""
            CREATE TABLE psc_data (
                id BIGSERIAL PRIMARY KEY,
                company_number VARCHAR(20) NOT NULL,
                psc_type VARCHAR(100) NOT NULL,
                name TEXT,
                name_elements JSONB,
                date_of_birth JSONB,
                country_of_residence TEXT,
                nationality TEXT,
                address JSONB,
                identification JSONB,
                natures_of_control TEXT[],
                notified_on DATE,
                ceased_on DATE,
                etag TEXT,
                links JSONB,
                source_filename TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                -- Unique constraint on company_number + etag to handle updates
                CONSTRAINT uk_company_psc UNIQUE (company_number, etag)
            );
        """)
        
        # Create indexes for performance
        indexes = [
            "CREATE INDEX idx_psc_company_number ON psc_data(company_number);",
            "CREATE INDEX idx_psc_type ON psc_data(psc_type);",
            "CREATE INDEX idx_psc_name ON psc_data(name);",
            "CREATE INDEX idx_psc_ceased ON psc_data(ceased_on) WHERE ceased_on IS NULL;",
            "CREATE INDEX idx_psc_notified ON psc_data(notified_on);",
            "CREATE INDEX idx_psc_natures ON psc_data USING GIN (natures_of_control);"
        ]
        
        for idx_sql in indexes:
            self.cursor.execute(idx_sql)
            
        self.conn.commit()
        logger.info("Created psc_data table with indexes")
        
    def ensure_table_exists(self):
        """Ensure the psc_data table exists"""
        self.cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'psc_data'
            );
        """)
        
        if not self.cursor.fetchone()[0]:
            logger.error("Table psc_data does not exist. Run with --create-table flag first.")
            raise Exception("Table psc_data does not exist")
            
    def parse_date(self, date_str):
        """Parse date from various formats"""
        if not date_str:
            return None
        try:
            # Handle timezone-aware dates (remove Z suffix)
            if date_str.endswith('Z'):
                date_str = date_str[:-1]
            
            # Try different date formats
            for fmt in ['%Y-%m-%d', '%Y-%m-%dT%H:%M:%S']:
                try:
                    return datetime.strptime(date_str, fmt).date()
                except:
                    continue
                    
            logger.warning(f"Could not parse date: {date_str}")
            return None
        except Exception as e:
            logger.warning(f"Date parsing error: {e} - Date: {date_str}")
            return None
            
    def process_psc_record(self, line, filename):
        """Process a single PSC record from JSON"""
        try:
            # Parse JSON line
            record = json.loads(line.strip())
            
            # Extract company number and data
            company_number = record.get('company_number')
            data = record.get('data', {})
            
            if not company_number or not data:
                return None
                
            # Determine PSC type
            psc_type = data.get('kind', 'unknown')
            
            # Count by type
            if 'individual' in psc_type:
                self.stats['individual_psc'] += 1
            elif 'corporate-entity' in psc_type:
                self.stats['corporate_psc'] += 1
            elif 'legal-person' in psc_type:
                self.stats['legal_person_psc'] += 1
            else:
                self.stats['other_psc'] += 1
            
            # Parse the record based on type
            parsed_record = {
                'company_number': company_number,
                'psc_type': psc_type,
                'name': data.get('name'),
                'name_elements': Json(data.get('name_elements')) if data.get('name_elements') else None,
                'date_of_birth': Json(data.get('date_of_birth')) if data.get('date_of_birth') else None,
                'country_of_residence': data.get('country_of_residence'),
                'nationality': data.get('nationality'),
                'address': Json(data.get('address')) if data.get('address') else None,
                'identification': Json(data.get('identification')) if data.get('identification') else None,
                'natures_of_control': data.get('natures_of_control', []),
                'notified_on': self.parse_date(data.get('notified_on')),
                'ceased_on': self.parse_date(data.get('ceased_on')),
                'etag': data.get('etag'),
                'links': Json(data.get('links')) if data.get('links') else None,
                'source_filename': filename
            }
            
            return parsed_record
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e} - Line: {line[:100]}...")
            return None
        except Exception as e:
            logger.error(f"Error processing record: {e} - Line: {line[:100]}...")
            return None
            
    def insert_batch(self, batch_data):
        """Insert a batch of records using ON CONFLICT UPDATE"""
        if not batch_data:
            return
            
        try:
            columns = list(batch_data[0].keys())
            
            # Build INSERT ... ON CONFLICT UPDATE query
            insert_query = f"""
                INSERT INTO psc_data ({', '.join(columns)})
                VALUES %s
                ON CONFLICT (company_number, etag) DO UPDATE SET
                    {', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col not in ['company_number', 'etag']])},
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
            # Log first record for debugging
            if batch_data:
                logger.error(f"First record in failed batch: {batch_data[0]}")
            
    def import_file(self, filepath):
        """Import a PSC data file"""
        filename = os.path.basename(filepath)
        logger.info(f"Starting import of {filename}")
        
        # Count lines for progress bar
        logger.info("Counting lines in file...")
        total_lines = sum(1 for _ in open(filepath, 'r', encoding='utf-8'))
        logger.info(f"Total lines to process: {total_lines:,}")
        
        batch_data = []
        
        with open(filepath, 'r', encoding='utf-8') as f:
            with tqdm(total=total_lines, desc=f"Importing {filename}") as pbar:
                for line in f:
                    if not line.strip():
                        pbar.update(1)
                        continue
                        
                    # Process the record
                    record = self.process_psc_record(line, filename)
                    
                    if record:
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
        
    def run_import(self, psc_file=None, psc_dir=None, create_table=False):
        """Main import process"""
        start_time = datetime.now()
        
        try:
            self.connect()
            
            if create_table:
                self.create_table()
            else:
                self.ensure_table_exists()
            
            # Find PSC file(s) to import
            files_to_import = []
            
            if psc_file and os.path.exists(psc_file):
                files_to_import.append(psc_file)
            elif psc_dir and os.path.exists(psc_dir):
                # Find all PSC snapshot files
                psc_files = sorted(Path(psc_dir).glob('persons-with-significant-control-snapshot-*.txt'))
                if psc_files:
                    files_to_import.extend([str(f) for f in psc_files])
                    
            if not files_to_import:
                logger.error("No PSC files found to import")
                return
                
            # Import each file
            for filepath in files_to_import:
                self.import_file(filepath)
                
            # Run VACUUM ANALYZE (needs autocommit mode)
            logger.info("Running VACUUM ANALYZE on psc_data...")
            self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            self.cursor.execute("VACUUM ANALYZE psc_data")
            self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
            
            # Final statistics
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info("\n" + "="*50)
            logger.info("IMPORT COMPLETE")
            logger.info("="*50)
            logger.info(f"Files processed: {self.stats['files_processed']}")
            logger.info(f"Total records: {self.stats['total_records']:,}")
            logger.info(f"  - Individual PSCs: {self.stats['individual_psc']:,}")
            logger.info(f"  - Corporate PSCs: {self.stats['corporate_psc']:,}")
            logger.info(f"  - Legal Person PSCs: {self.stats['legal_person_psc']:,}")
            logger.info(f"  - Other PSCs: {self.stats['other_psc']:,}")
            logger.info(f"Inserted/Updated: {self.stats['inserted']:,}")
            logger.info(f"Errors: {self.stats['errors']:,}")
            logger.info(f"Time taken: {elapsed/60:.1f} minutes")
            logger.info(f"Log file: {log_filename}")
            
            # Show sample queries
            logger.info("\n" + "="*50)
            logger.info("SAMPLE QUERIES")
            logger.info("="*50)
            logger.info("-- Find PSCs for a company:")
            logger.info("SELECT * FROM psc_data WHERE company_number = '00445790' AND ceased_on IS NULL;")
            logger.info("\n-- Companies with the most PSCs:")
            logger.info("""SELECT company_number, COUNT(*) as psc_count 
FROM psc_data 
WHERE ceased_on IS NULL 
GROUP BY company_number 
ORDER BY psc_count DESC 
LIMIT 10;""")
            logger.info("\n-- Join with Companies House data:")
            logger.info("""SELECT 
    ch.company_name,
    ch.company_status,
    p.name as psc_name,
    p.natures_of_control
FROM psc_data p
JOIN companies_house_data ch ON p.company_number = ch.company_number
WHERE ch.company_status = 'Active' 
AND p.ceased_on IS NULL
LIMIT 10;""")
            
        except Exception as e:
            logger.error(f"Fatal error during import: {e}")
            raise
        finally:
            self.disconnect()

def main():
    parser = argparse.ArgumentParser(description='Import Companies House PSC Data')
    parser.add_argument('--file', type=str,
                       help='Specific PSC file to import')
    parser.add_argument('--psc-dir', type=str,
                       default='/home/adc/Projects/InsideEstates_App/DATA/SOURCE/CH/PSC',
                       help='Directory containing PSC files')
    parser.add_argument('--batch-size', type=int, default=5000,
                       help='Batch size for inserts (default: 5000)')
    parser.add_argument('--create-table', action='store_true',
                       help='Create the psc_data table (drops existing table)')
    
    args = parser.parse_args()
    
    importer = PSCImporter(batch_size=args.batch_size)
    importer.run_import(psc_file=args.file, psc_dir=args.psc_dir, create_table=args.create_table)

if __name__ == '__main__':
    main()