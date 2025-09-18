#!/usr/bin/env python3
"""
Match Land Registry proprietors to Companies House data using 4-tier matching logic
Updates the land_registry_data table with matched Companies House information
"""

import os
import sys
import re
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
import logging
from datetime import datetime
from tqdm import tqdm
import argparse
import gc
from pathlib import Path

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('lr_ch_matching.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class LandRegistryCompaniesHouseMatcher:
    def __init__(self, batch_size=10000):
        self.batch_size = batch_size
        self.conn = None
        self.cursor = None
        
        # Memory structures for 4-tier matching
        self.ch_lookup_name_number = {}
        self.ch_lookup_number = {}
        self.ch_lookup_name = {}
        self.ch_lookup_previous_name = {}
        
        # Statistics
        self.stats = {
            'total_processed': 0,
            'name_number_matches': 0,
            'number_matches': 0,
            'name_matches': 0,
            'previous_name_matches': 0,
            'no_matches': 0
        }
        
    def connect(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(**POSTGRESQL_CONFIG)
            self.cursor = self.conn.cursor()
            logger.info("Connected to PostgreSQL database")
            
            # Check if CH match columns exist
            self.cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'land_registry_data' 
                AND column_name = 'ch_matched_number_1'
            """)
            
            if not self.cursor.fetchone():
                logger.error("CH match columns not found. Please run add_ch_match_columns.sql first.")
                raise Exception("Missing CH match columns in land_registry_data table")
                
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
            
    def disconnect(self):
        """Disconnect from database"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            
    def normalize_company_name(self, name):
        """Normalize company name for matching"""
        if not name or name.strip() == '':
            return ""
        
        name = str(name).upper().strip()
        
        # Replace common variations
        name = name.replace(' AND ', ' ')
        name = name.replace(' & ', ' ')
        name = name.replace('.', '')
        name = name.replace(',', '')
        
        # Remove common suffixes
        suffix_pattern = r'\s*(LIMITED LIABILITY PARTNERSHIP|LIMITED|COMPANY|LTD\.|LLP|LTD|PLC|CO\.|CO|LP|L\.P\.)$'
        name = re.sub(suffix_pattern, '', name)
        
        # Remove extra spaces and keep only alphanumeric
        name = ' '.join(name.split())
        name = ''.join(char for char in name if char.isalnum())
        
        return name
    
    def normalize_company_number(self, number):
        """Normalize company registration number for matching"""
        if not number or number.strip() == '':
            return ""
        
        number = str(number).strip().upper()
        
        # Remove any non-alphanumeric characters
        number = re.sub(r'[^A-Z0-9]', '', number)
        
        # Handle Scottish numbers (start with SC)
        if number.startswith('SC'):
            return number
        
        # Handle Northern Ireland numbers (start with NI)
        if number.startswith('NI'):
            return number
            
        # Handle Gibraltar numbers (start with GI)
        if number.startswith('GI'):
            return number
        
        # If it's all digits, pad with zeros to 8 digits
        if number.isdigit():
            return number.zfill(8)
        
        return number
    
    def load_companies_house_to_memory(self):
        """Load Companies House data into memory for fast matching"""
        logger.info("Loading Companies House data into memory...")
        start_time = datetime.now()
        
        # Count total companies
        self.cursor.execute("SELECT COUNT(*) FROM companies_house_data")
        total_companies = self.cursor.fetchone()[0]
        logger.info(f"Total companies to load: {total_companies:,}")
        
        # Load in batches
        processed = 0
        chunk_size = 50000
        
        with tqdm(total=total_companies, desc="Loading CH data") as pbar:
            offset = 0
            
            while True:
                self.cursor.execute("""
                    SELECT 
                        company_number,
                        company_name,
                        company_status,
                        company_category
                    FROM companies_house_data
                    LIMIT %s OFFSET %s
                """, (chunk_size, offset))
                
                rows = self.cursor.fetchall()
                if not rows:
                    break
                
                for company_number, company_name, company_status, company_category in rows:
                    if not company_name:
                        continue
                    
                    clean_name = self.normalize_company_name(company_name)
                    clean_number = self.normalize_company_number(company_number)
                    
                    company_data = {
                        'company_name': company_name,
                        'company_number': company_number,
                        'company_status': company_status or '',
                        'company_category': company_category or '',
                        'match_type': 'current'
                    }
                    
                    # Build lookup dictionaries
                    if clean_name and clean_number:
                        key = clean_name + clean_number
                        if key not in self.ch_lookup_name_number:
                            self.ch_lookup_name_number[key] = company_data
                    
                    if clean_number and clean_number not in self.ch_lookup_number:
                        self.ch_lookup_number[clean_number] = company_data
                        
                    if clean_name and clean_name not in self.ch_lookup_name:
                        self.ch_lookup_name[clean_name] = company_data
                
                processed += len(rows)
                offset += chunk_size
                pbar.update(len(rows))
                
                if processed % 100000 == 0:
                    gc.collect()
        
        logger.info(f"Loaded {processed:,} companies")
        
        # Load previous names
        self.load_previous_names_to_memory()
        
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"CH memory loading complete in {elapsed:.1f}s")
        logger.info(f"  Name+Number lookup: {len(self.ch_lookup_name_number):,}")
        logger.info(f"  Number lookup: {len(self.ch_lookup_number):,}")
        logger.info(f"  Current name lookup: {len(self.ch_lookup_name):,}")
        logger.info(f"  Previous name lookup: {len(self.ch_lookup_previous_name):,}")
    
    def load_previous_names_to_memory(self):
        """Load previous company names into memory"""
        logger.info("Loading previous company names...")
        
        # Extract previous names from companies_house_data table
        previous_names_count = 0
        
        for i in range(1, 11):  # Check all 10 previous name columns
            self.cursor.execute(f"""
                SELECT 
                    company_number,
                    previous_name_{i}_name,
                    company_name,
                    company_status,
                    company_category
                FROM companies_house_data
                WHERE previous_name_{i}_name IS NOT NULL 
                AND previous_name_{i}_name != ''
            """)
            
            for row in self.cursor.fetchall():
                company_number, previous_name, current_name, status, category = row
                clean_previous_name = self.normalize_company_name(previous_name)
                clean_number = self.normalize_company_number(company_number)
                
                if clean_previous_name and clean_previous_name not in self.ch_lookup_name:
                    company_data = {
                        'company_name': current_name,  # Use current name in match result
                        'company_number': company_number,
                        'company_status': status or '',
                        'company_category': category or '',
                        'match_type': 'previous',
                        'matched_previous_name': previous_name
                    }
                    self.ch_lookup_previous_name[clean_previous_name] = company_data
                    previous_names_count += 1
        
        logger.info(f"Loaded {previous_names_count:,} unique previous names")
    
    def match_to_companies_house(self, lr_owner_name, lr_company_number):
        """4-tier matching logic to find best Companies House match"""
        clean_name = self.normalize_company_name(lr_owner_name)
        clean_number = self.normalize_company_number(lr_company_number)
        
        # Tier 1: Name + Number (highest confidence - 1.0)
        if clean_name and clean_number:
            key = clean_name + clean_number
            if key in self.ch_lookup_name_number:
                return self.ch_lookup_name_number[key], 'Name+Number', 1.0
        
        # Tier 2: Number only (high confidence - 0.9)
        if clean_number and clean_number in self.ch_lookup_number:
            return self.ch_lookup_number[clean_number], 'Number', 0.9
        
        # Tier 3: Current Name only (medium confidence - 0.7)
        if clean_name and clean_name in self.ch_lookup_name:
            return self.ch_lookup_name[clean_name], 'Name', 0.7
        
        # Tier 4: Previous Name only (low confidence - 0.5)
        if clean_name and clean_name in self.ch_lookup_previous_name:
            return self.ch_lookup_previous_name[clean_name], 'Previous_Name', 0.5
        
        # Tier 5: No match (0.0)
        return None, 'No_Match', 0.0
    
    def process_batch(self, records):
        """Process a batch of Land Registry records and update with CH matches"""
        update_data = []
        
        for record in records:
            record_id = record['id']
            updates = {'id': record_id, 'ch_match_date': datetime.now()}
            
            # Process each proprietor
            for i in range(1, 5):
                proprietor_name = record.get(f'proprietor_{i}_name', '')
                company_reg_no = record.get(f'company_{i}_reg_no', '')
                
                if proprietor_name:
                    ch_match, match_type, confidence = self.match_to_companies_house(
                        proprietor_name, company_reg_no
                    )
                    
                    if ch_match:
                        updates[f'ch_matched_name_{i}'] = ch_match['company_name']
                        updates[f'ch_matched_number_{i}'] = ch_match['company_number']
                        updates[f'ch_match_type_{i}'] = match_type
                        updates[f'ch_match_confidence_{i}'] = confidence
                        
                        # Update statistics
                        if match_type == 'Name+Number':
                            self.stats['name_number_matches'] += 1
                        elif match_type == 'Number':
                            self.stats['number_matches'] += 1
                        elif match_type == 'Name':
                            self.stats['name_matches'] += 1
                        elif match_type == 'Previous_Name':
                            self.stats['previous_name_matches'] += 1
                    else:
                        updates[f'ch_matched_name_{i}'] = None
                        updates[f'ch_matched_number_{i}'] = None
                        updates[f'ch_match_type_{i}'] = 'No_Match'
                        updates[f'ch_match_confidence_{i}'] = 0.0
                        self.stats['no_matches'] += 1
                else:
                    # No proprietor in this slot
                    updates[f'ch_matched_name_{i}'] = None
                    updates[f'ch_matched_number_{i}'] = None
                    updates[f'ch_match_type_{i}'] = None
                    updates[f'ch_match_confidence_{i}'] = None
            
            update_data.append(updates)
        
        # Perform batch update
        if update_data:
            self.update_batch_in_database(update_data)
    
    def update_batch_in_database(self, update_data):
        """Update the database with matched CH data"""
        try:
            # Build UPDATE statement with all fields
            update_query = """
                UPDATE land_registry_data SET
                    ch_matched_name_1 = data.ch_matched_name_1,
                    ch_matched_number_1 = data.ch_matched_number_1,
                    ch_match_type_1 = data.ch_match_type_1,
                    ch_match_confidence_1 = data.ch_match_confidence_1::numeric,
                    
                    ch_matched_name_2 = data.ch_matched_name_2,
                    ch_matched_number_2 = data.ch_matched_number_2,
                    ch_match_type_2 = data.ch_match_type_2,
                    ch_match_confidence_2 = data.ch_match_confidence_2::numeric,
                    
                    ch_matched_name_3 = data.ch_matched_name_3,
                    ch_matched_number_3 = data.ch_matched_number_3,
                    ch_match_type_3 = data.ch_match_type_3,
                    ch_match_confidence_3 = data.ch_match_confidence_3::numeric,
                    
                    ch_matched_name_4 = data.ch_matched_name_4,
                    ch_matched_number_4 = data.ch_matched_number_4,
                    ch_match_type_4 = data.ch_match_type_4,
                    ch_match_confidence_4 = data.ch_match_confidence_4::numeric,
                    
                    ch_match_date = data.ch_match_date,
                    updated_at = CURRENT_TIMESTAMP
                FROM (VALUES %s) AS data(
                    id,
                    ch_matched_name_1, ch_matched_number_1, ch_match_type_1, ch_match_confidence_1,
                    ch_matched_name_2, ch_matched_number_2, ch_match_type_2, ch_match_confidence_2,
                    ch_matched_name_3, ch_matched_number_3, ch_match_type_3, ch_match_confidence_3,
                    ch_matched_name_4, ch_matched_number_4, ch_match_type_4, ch_match_confidence_4,
                    ch_match_date
                )
                WHERE land_registry_data.id = data.id::bigint
            """
            
            # Prepare values for execute_values
            values = []
            for record in update_data:
                values.append((
                    record['id'],
                    record.get('ch_matched_name_1'),
                    record.get('ch_matched_number_1'),
                    record.get('ch_match_type_1'),
                    record.get('ch_match_confidence_1'),
                    record.get('ch_matched_name_2'),
                    record.get('ch_matched_number_2'),
                    record.get('ch_match_type_2'),
                    record.get('ch_match_confidence_2'),
                    record.get('ch_matched_name_3'),
                    record.get('ch_matched_number_3'),
                    record.get('ch_match_type_3'),
                    record.get('ch_match_confidence_3'),
                    record.get('ch_matched_name_4'),
                    record.get('ch_matched_number_4'),
                    record.get('ch_match_type_4'),
                    record.get('ch_match_confidence_4'),
                    record['ch_match_date']
                ))
            
            execute_values(self.cursor, update_query, values)
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Error updating batch: {e}")
            self.conn.rollback()
            raise
    
    def process_all_records(self, test_limit=None):
        """Process all Land Registry records"""
        logger.info("Starting to process Land Registry records...")
        
        # Get total count
        where_clause = ""
        if test_limit:
            where_clause = f"LIMIT {test_limit}"
            
        self.cursor.execute(f"SELECT COUNT(*) FROM land_registry_data {where_clause}")
        total_records = self.cursor.fetchone()[0]
        logger.info(f"Total records to process: {total_records:,}")
        
        # Process in batches
        offset = 0
        with tqdm(total=total_records, desc="Matching records") as pbar:
            while offset < total_records:
                # Use a dictionary cursor for easier field access
                dict_cursor = self.conn.cursor(cursor_factory=RealDictCursor)
                
                dict_cursor.execute(f"""
                    SELECT 
                        id,
                        proprietor_1_name, company_1_reg_no,
                        proprietor_2_name, company_2_reg_no,
                        proprietor_3_name, company_3_reg_no,
                        proprietor_4_name, company_4_reg_no
                    FROM land_registry_data
                    ORDER BY id
                    LIMIT %s OFFSET %s
                """, (self.batch_size, offset))
                
                records = dict_cursor.fetchall()
                dict_cursor.close()
                
                if not records:
                    break
                
                self.process_batch(records)
                
                self.stats['total_processed'] += len(records)
                offset += self.batch_size
                pbar.update(len(records))
                
                # Periodic garbage collection
                if offset % 50000 == 0:
                    gc.collect()
    
    def print_statistics(self):
        """Print matching statistics"""
        logger.info("\n" + "="*50)
        logger.info("MATCHING STATISTICS")
        logger.info("="*50)
        logger.info(f"Total records processed: {self.stats['total_processed']:,}")
        logger.info(f"Name+Number matches (Tier 1): {self.stats['name_number_matches']:,}")
        logger.info(f"Number only matches (Tier 2): {self.stats['number_matches']:,}")
        logger.info(f"Name only matches (Tier 3): {self.stats['name_matches']:,}")
        logger.info(f"Previous name matches (Tier 4): {self.stats['previous_name_matches']:,}")
        logger.info(f"No matches (Tier 5): {self.stats['no_matches']:,}")
        
        total_matches = (self.stats['name_number_matches'] + self.stats['number_matches'] + 
                        self.stats['name_matches'] + self.stats['previous_name_matches'])
        total_attempts = total_matches + self.stats['no_matches']
        
        if total_attempts > 0:
            match_rate = (total_matches / total_attempts) * 100
            logger.info(f"\nOverall match rate: {match_rate:.2f}%")
    
    def run(self, test_limit=None):
        """Main execution method"""
        try:
            # Connect to database
            self.connect()
            
            # Load Companies House data
            self.load_companies_house_to_memory()
            
            # Process all records
            self.process_all_records(test_limit)
            
            # Print statistics
            self.print_statistics()
            
            # Run VACUUM ANALYZE
            logger.info("Running VACUUM ANALYZE on land_registry_data...")
            self.cursor.execute("VACUUM ANALYZE land_registry_data")
            self.conn.commit()
            
            logger.info("Matching complete!")
            
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            raise
        finally:
            self.disconnect()

def main():
    parser = argparse.ArgumentParser(description='Match Land Registry data to Companies House data')
    parser.add_argument('--batch-size', type=int, default=10000, 
                       help='Batch size for processing (default: 10000)')
    parser.add_argument('--test', type=int, 
                       help='Test mode - process only N records')
    
    args = parser.parse_args()
    
    # Check if CH match columns exist
    logger.info("Checking database setup...")
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'land_registry_data' 
            AND column_name = 'ch_matched_number_1'
        """)
        
        if not cursor.fetchone():
            cursor.close()
            conn.close()
            logger.error("CH match columns not found. Running add_ch_match_columns.sql...")
            
            # Try to run the SQL script
            sql_path = Path(__file__).parent / 'add_ch_match_columns.sql'
            if sql_path.exists():
                conn = psycopg2.connect(**POSTGRESQL_CONFIG)
                cursor = conn.cursor()
                with open(sql_path, 'r') as f:
                    cursor.execute(f.read())
                conn.commit()
                logger.info("CH match columns added successfully")
            else:
                logger.error(f"SQL script not found at {sql_path}")
                logger.error("Please run add_ch_match_columns.sql manually first")
                return
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Database setup check failed: {e}")
        return
    
    # Run the matcher
    matcher = LandRegistryCompaniesHouseMatcher(batch_size=args.batch_size)
    
    start_time = datetime.now()
    matcher.run(test_limit=args.test)
    elapsed = (datetime.now() - start_time).total_seconds()
    
    logger.info(f"\nTotal execution time: {elapsed/60:.1f} minutes")

if __name__ == '__main__':
    main()