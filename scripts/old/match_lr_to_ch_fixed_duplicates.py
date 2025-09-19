#!/usr/bin/env python3
"""
FIXED VERSION: Match Land Registry to Companies House with proper duplicate handling
This version handles multiple companies with the same normalized name
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
from collections import defaultdict

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('lr_ch_matching_fixed_duplicates.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class LandRegistryCompaniesHouseMatcherFixed:
    def __init__(self, batch_size=10000):
        self.batch_size = batch_size
        self.conn = None
        self.cursor = None
        
        # FIXED: Use lists to handle multiple companies with same normalized values
        self.ch_lookup_name_number = defaultdict(list)  # Can have multiple companies
        self.ch_lookup_number = {}  # Numbers are unique, keep as dict
        self.ch_lookup_name = defaultdict(list)  # Can have multiple companies
        self.ch_lookup_previous_name = defaultdict(list)  # Can have multiple companies
        
        # Statistics
        self.stats = {
            'total_processed': 0,
            'name_number_matches': 0,
            'number_matches': 0,
            'name_matches': 0,
            'previous_name_matches': 0,
            'no_matches': 0,
            'total_proprietors': 0,
            'matched_proprietors': 0,
            'duplicate_name_issues': 0
        }
        
    def connect(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(**POSTGRESQL_CONFIG)
            self.cursor = self.conn.cursor()
            logger.info("Connected to PostgreSQL database")
            
            # Check if match table exists
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'land_registry_ch_matches'
                )
            """)
            
            if not self.cursor.fetchone()[0]:
                logger.error("Match table not found. Please run create_match_table.sql first.")
                raise Exception("Missing land_registry_ch_matches table")
                
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
        
        # Remove common suffixes - THIS IS CORRECT for matching variations
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
        
        # Handle overseas prefixes
        if number.startswith(('FC', 'OE', 'OC', 'BR', 'NF', 'SF')):
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
        duplicate_names = 0
        
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
                    
                    # FIXED: Build lookup dictionaries allowing duplicates
                    if clean_name and clean_number:
                        key = clean_name + clean_number
                        self.ch_lookup_name_number[key].append(company_data)
                    
                    # Numbers are unique, so keep as single entry
                    if clean_number and clean_number not in self.ch_lookup_number:
                        self.ch_lookup_number[clean_number] = company_data
                        
                    # FIXED: Names can have duplicates
                    if clean_name:
                        self.ch_lookup_name[clean_name].append(company_data)
                        if len(self.ch_lookup_name[clean_name]) > 1:
                            duplicate_names += 1
                
                processed += len(rows)
                offset += chunk_size
                pbar.update(len(rows))
                
                if processed % 100000 == 0:
                    gc.collect()
        
        logger.info(f"Loaded {processed:,} companies")
        logger.info(f"Found {duplicate_names:,} normalized names with multiple companies")
        
        # Load previous names
        self.load_previous_names_to_memory()
        
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"CH memory loading complete in {elapsed:.1f}s")
        logger.info(f"  Name+Number lookup: {len(self.ch_lookup_name_number):,} unique keys")
        logger.info(f"  Number lookup: {len(self.ch_lookup_number):,}")
        logger.info(f"  Current name lookup: {len(self.ch_lookup_name):,} unique names")
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
                
                if clean_previous_name:
                    company_data = {
                        'company_name': current_name,  # Use current name in match result
                        'company_number': company_number,
                        'company_status': status or '',
                        'company_category': category or '',
                        'match_type': 'previous',
                        'matched_previous_name': previous_name
                    }
                    self.ch_lookup_previous_name[clean_previous_name].append(company_data)
                    previous_names_count += 1
        
        logger.info(f"Loaded {previous_names_count:,} previous name entries")
    
    def match_to_companies_house(self, lr_owner_name, lr_company_number):
        """4-tier matching logic to find best Companies House match"""
        clean_name = self.normalize_company_name(lr_owner_name)
        clean_number = self.normalize_company_number(lr_company_number)
        
        # Tier 1: Name + Number (highest confidence - 1.0)
        if clean_name and clean_number:
            key = clean_name + clean_number
            if key in self.ch_lookup_name_number:
                matches = self.ch_lookup_name_number[key]
                # Should only be one match for name+number combo
                return matches[0], 'Name+Number', 1.0
        
        # Tier 2: Number only (high confidence - 0.9)
        if clean_number and clean_number in self.ch_lookup_number:
            return self.ch_lookup_number[clean_number], 'Number', 0.9
        
        # Tier 3: Current Name only (medium confidence - 0.7)
        if clean_name and clean_name in self.ch_lookup_name:
            matches = self.ch_lookup_name[clean_name]
            if len(matches) == 1:
                # Only one company with this name - safe to match
                return matches[0], 'Name', 0.7
            elif len(matches) > 1:
                # Multiple companies with same normalized name
                # Without a registration number, we can't disambiguate
                self.stats['duplicate_name_issues'] += 1
                # For now, return no match when ambiguous
                # Could potentially return the first active one, but safer not to
                return None, 'No_Match', 0.0
        
        # Tier 4: Previous Name only (low confidence - 0.5)
        if clean_name and clean_name in self.ch_lookup_previous_name:
            matches = self.ch_lookup_previous_name[clean_name]
            if len(matches) == 1:
                return matches[0], 'Previous_Name', 0.5
            else:
                # Multiple companies had this previous name
                self.stats['duplicate_name_issues'] += 1
                return None, 'No_Match', 0.0
        
        # Tier 5: No match (0.0)
        return None, 'No_Match', 0.0
    
    def process_batch(self, records):
        """Process a batch of Land Registry records and prepare match data"""
        match_data = []
        
        for record in records:
            record_id = record['id']
            match_record = {'id': record_id}
            has_any_match = False
            
            # Process each proprietor
            for i in range(1, 5):
                proprietor_name = record.get(f'proprietor_{i}_name', '')
                company_reg_no = record.get(f'company_{i}_reg_no', '')
                
                if proprietor_name:
                    self.stats['total_proprietors'] += 1
                    ch_match, match_type, confidence = self.match_to_companies_house(
                        proprietor_name, company_reg_no
                    )
                    
                    if ch_match:
                        match_record[f'ch_matched_name_{i}'] = ch_match['company_name']
                        match_record[f'ch_matched_number_{i}'] = ch_match['company_number']
                        match_record[f'ch_match_type_{i}'] = match_type
                        match_record[f'ch_match_confidence_{i}'] = confidence
                        has_any_match = True
                        
                        # Update statistics
                        if match_type == 'Name+Number':
                            self.stats['name_number_matches'] += 1
                            self.stats['matched_proprietors'] += 1
                        elif match_type == 'Number':
                            self.stats['number_matches'] += 1
                            self.stats['matched_proprietors'] += 1
                        elif match_type == 'Name':
                            self.stats['name_matches'] += 1
                            self.stats['matched_proprietors'] += 1
                        elif match_type == 'Previous_Name':
                            self.stats['previous_name_matches'] += 1
                            self.stats['matched_proprietors'] += 1
                    else:
                        match_record[f'ch_matched_name_{i}'] = None
                        match_record[f'ch_matched_number_{i}'] = None
                        match_record[f'ch_match_type_{i}'] = 'No_Match'
                        match_record[f'ch_match_confidence_{i}'] = 0.0
                        self.stats['no_matches'] += 1
                else:
                    # No proprietor in this slot
                    match_record[f'ch_matched_name_{i}'] = None
                    match_record[f'ch_matched_number_{i}'] = None
                    match_record[f'ch_match_type_{i}'] = None
                    match_record[f'ch_match_confidence_{i}'] = None
            
            # Only add records that have at least one match or have proprietors
            if has_any_match or any(record.get(f'proprietor_{i}_name') for i in range(1, 5)):
                match_data.append(match_record)
        
        # Insert batch into match table
        if match_data:
            self.insert_batch_to_match_table(match_data)
    
    def insert_batch_to_match_table(self, match_data):
        """Insert batch of matches into the separate match table"""
        try:
            # Prepare insert query with ON CONFLICT UPDATE
            insert_query = """
                INSERT INTO land_registry_ch_matches (
                    id,
                    ch_matched_name_1, ch_matched_number_1, ch_match_type_1, ch_match_confidence_1,
                    ch_matched_name_2, ch_matched_number_2, ch_match_type_2, ch_match_confidence_2,
                    ch_matched_name_3, ch_matched_number_3, ch_match_type_3, ch_match_confidence_3,
                    ch_matched_name_4, ch_matched_number_4, ch_match_type_4, ch_match_confidence_4
                ) VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    ch_matched_name_1 = EXCLUDED.ch_matched_name_1,
                    ch_matched_number_1 = EXCLUDED.ch_matched_number_1,
                    ch_match_type_1 = EXCLUDED.ch_match_type_1,
                    ch_match_confidence_1 = EXCLUDED.ch_match_confidence_1,
                    ch_matched_name_2 = EXCLUDED.ch_matched_name_2,
                    ch_matched_number_2 = EXCLUDED.ch_matched_number_2,
                    ch_match_type_2 = EXCLUDED.ch_match_type_2,
                    ch_match_confidence_2 = EXCLUDED.ch_match_confidence_2,
                    ch_matched_name_3 = EXCLUDED.ch_matched_name_3,
                    ch_matched_number_3 = EXCLUDED.ch_matched_number_3,
                    ch_match_type_3 = EXCLUDED.ch_match_type_3,
                    ch_match_confidence_3 = EXCLUDED.ch_match_confidence_3,
                    ch_matched_name_4 = EXCLUDED.ch_matched_name_4,
                    ch_matched_number_4 = EXCLUDED.ch_matched_number_4,
                    ch_match_type_4 = EXCLUDED.ch_match_type_4,
                    ch_match_confidence_4 = EXCLUDED.ch_match_confidence_4,
                    updated_at = CURRENT_TIMESTAMP
            """
            
            # Prepare values for execute_values
            values = []
            for record in match_data:
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
                    record.get('ch_match_confidence_4')
                ))
            
            execute_values(self.cursor, insert_query, values)
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Error inserting batch: {e}")
            self.conn.rollback()
            raise
    
    def process_all_records(self, test_limit=None):
        """Process all Land Registry records"""
        logger.info("Starting to process Land Registry records...")
        
        # Get total count
        if test_limit:
            self.cursor.execute(f"SELECT COUNT(*) FROM land_registry_data LIMIT {test_limit}")
            total_records = min(self.cursor.fetchone()[0], test_limit)
        else:
            self.cursor.execute("SELECT COUNT(*) FROM land_registry_data")
            total_records = self.cursor.fetchone()[0]
            
        logger.info(f"Total records to process: {total_records:,}")
        
        # Process in batches
        processed = 0
        with tqdm(total=total_records, desc="Matching records") as pbar:
            while processed < total_records:
                # Use a dictionary cursor for easier field access
                dict_cursor = self.conn.cursor(cursor_factory=RealDictCursor)
                
                query = """
                    SELECT 
                        id,
                        proprietor_1_name, company_1_reg_no,
                        proprietor_2_name, company_2_reg_no,
                        proprietor_3_name, company_3_reg_no,
                        proprietor_4_name, company_4_reg_no
                    FROM land_registry_data
                    ORDER BY id
                    LIMIT %s OFFSET %s
                """
                
                dict_cursor.execute(query, (self.batch_size, processed))
                records = dict_cursor.fetchall()
                dict_cursor.close()
                
                if not records:
                    break
                
                self.process_batch(records)
                
                batch_size = len(records)
                self.stats['total_processed'] += batch_size
                processed += batch_size
                pbar.update(batch_size)
                
                # Show progress every 100k records
                if processed % 100000 == 0:
                    match_rate = (self.stats['matched_proprietors'] / self.stats['total_proprietors'] * 100) if self.stats['total_proprietors'] > 0 else 0
                    logger.info(f"Progress: {processed:,}/{total_records:,} records. Match rate: {match_rate:.1f}%")
                    logger.info(f"  Duplicate name issues so far: {self.stats['duplicate_name_issues']:,}")
                
                # Periodic garbage collection
                if processed % 50000 == 0:
                    gc.collect()
                    
                # Break if we've reached the test limit
                if test_limit and processed >= test_limit:
                    break
    
    def print_statistics(self):
        """Print matching statistics"""
        logger.info("\n" + "="*50)
        logger.info("MATCHING STATISTICS (WITH DUPLICATE FIX)")
        logger.info("="*50)
        logger.info(f"Total records processed: {self.stats['total_processed']:,}")
        logger.info(f"Total proprietors examined: {self.stats['total_proprietors']:,}")
        logger.info(f"Total proprietors matched: {self.stats['matched_proprietors']:,}")
        logger.info(f"\nMatch breakdown:")
        logger.info(f"  Name+Number matches (Tier 1): {self.stats['name_number_matches']:,}")
        logger.info(f"  Number only matches (Tier 2): {self.stats['number_matches']:,}")
        logger.info(f"  Name only matches (Tier 3): {self.stats['name_matches']:,}")
        logger.info(f"  Previous name matches (Tier 4): {self.stats['previous_name_matches']:,}")
        logger.info(f"  No matches (Tier 5): {self.stats['no_matches']:,}")
        logger.info(f"\nDuplicate name issues (ambiguous matches): {self.stats['duplicate_name_issues']:,}")
        
        if self.stats['total_proprietors'] > 0:
            match_rate = (self.stats['matched_proprietors'] / self.stats['total_proprietors']) * 100
            logger.info(f"\nOverall match rate: {match_rate:.2f}%")
            
        logger.info("\n🔧 This version handles duplicate company names properly!")
        logger.info("Companies with registration numbers will now match correctly.")
        logger.info("Name-only matches are skipped when multiple companies share the same name.")
    
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
            
            # Run VACUUM ANALYZE on match table (requires separate connection)
            logger.info("Running VACUUM ANALYZE on land_registry_ch_matches...")
            try:
                # Close current transaction
                self.conn.commit()
                # Set autocommit for VACUUM
                self.conn.autocommit = True
                self.cursor.execute("VACUUM ANALYZE land_registry_ch_matches")
                self.conn.autocommit = False
            except Exception as e:
                logger.warning(f"Could not run VACUUM ANALYZE: {e}")
            
            logger.info("\n✅ Matching complete with FIXED duplicate handling!")
            logger.info("\nYou can now query the matched data using:")
            logger.info("  SELECT * FROM v_land_registry_with_ch")
            logger.info("  WHERE ch_matched_number_1 = '12345678'")
            
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            raise
        finally:
            self.disconnect()

def main():
    parser = argparse.ArgumentParser(description='Match Land Registry data to Companies House data with FIXED duplicate handling')
    parser.add_argument('--batch-size', type=int, default=10000, 
                       help='Batch size for processing (default: 10000)')
    parser.add_argument('--test', type=int, 
                       help='Test mode - process only N records')
    
    args = parser.parse_args()
    
    # Check if match table exists
    logger.info("Checking database setup...")
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'land_registry_ch_matches'
            )
        """)
        
        if not cursor.fetchone()[0]:
            cursor.close()
            conn.close()
            logger.error("Match table not found.")
            logger.error("Please run: python scripts/run_sql_script.py scripts/create_match_table.sql")
            return
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Database setup check failed: {e}")
        return
    
    # Run the matcher
    matcher = LandRegistryCompaniesHouseMatcherFixed(batch_size=args.batch_size)
    
    logger.info("\n🔧 FIXED VERSION - HANDLES DUPLICATE COMPANY NAMES")
    logger.info("This version correctly matches companies with registration numbers")
    logger.info("even when multiple companies share the same normalized name.\n")
    
    start_time = datetime.now()
    matcher.run(test_limit=args.test)
    elapsed = (datetime.now() - start_time).total_seconds()
    
    logger.info(f"\nTotal execution time: {elapsed/60:.1f} minutes")

if __name__ == '__main__':
    main()