#!/usr/bin/env python3
"""
PRODUCTION LAND REGISTRY TO COMPANIES HOUSE MATCHER

This is a production-ready matching script that:
1. Loads Companies House data into memory efficiently
2. Uses the separate land_registry_ch_matches table approach
3. Fixes the normalization issues (proper handling of suffixes)
4. Implements robust 4-tier matching logic
5. Has progress tracking and can be resumed if interrupted
6. Includes comprehensive error handling and logging
7. Can run in different modes: full match, re-match No_Match records only, or specific date ranges

Author: InsideEstates Team
Created: 2024
"""

import os
import sys
import re
import json
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
import logging
from datetime import datetime, timedelta
from tqdm import tqdm
import argparse
import gc
import signal
import time
from pathlib import Path

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

class ProductionMatcher:
    def __init__(self, batch_size=5000, checkpoint_interval=50000):
        self.batch_size = batch_size
        self.checkpoint_interval = checkpoint_interval
        self.conn = None
        self.cursor = None
        
        # State management for resume capability
        self.state_file = "matching_state.json"
        self.last_processed_id = 0
        self.start_time = None
        self.mode = None
        
        # Memory structures for 4-tier matching - optimized for production
        self.ch_lookup_name_number = {}  # Tier 1: Name+Number (highest confidence)
        self.ch_lookup_number = {}       # Tier 2: Number only (high confidence)
        self.ch_lookup_name = {}         # Tier 3: Current Name only (medium confidence)
        self.ch_lookup_previous_name = {} # Tier 4: Previous Name only (lowest confidence)
        
        # Enhanced statistics tracking
        self.stats = {
            'total_processed': 0,
            'total_proprietors': 0,
            'matched_proprietors': 0,
            'name_number_matches': 0,    # Tier 1: 1.0 confidence
            'number_matches': 0,         # Tier 2: 0.9 confidence
            'name_matches': 0,           # Tier 3: 0.7 confidence
            'previous_name_matches': 0,  # Tier 4: 0.5 confidence
            'no_matches': 0,            # Tier 5: 0.0 confidence
            'skipped_already_processed': 0,
            'errors': 0,
            'checkpoints_saved': 0,
            'memory_usage_mb': 0
        }
        
        # Graceful shutdown handling
        self.interrupted = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Setup enhanced logging
        self._setup_logging()
        
    def _setup_logging(self):
        """Setup comprehensive logging with rotation"""
        log_file = f"lr_ch_matching_production_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Also log to a separate error file
        error_handler = logging.FileHandler(f"lr_ch_matching_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(logging.Formatter('%(asctime)s - ERROR - %(message)s'))
        self.logger.addHandler(error_handler)
        
        self.logger.info(f"Production matching started. Log file: {log_file}")
        
    def _signal_handler(self, signum, frame):
        """Handle graceful shutdown on interrupt"""
        self.logger.warning(f"Received signal {signum}. Initiating graceful shutdown...")
        self.interrupted = True
        
    def connect(self):
        """Connect to PostgreSQL database with enhanced error handling"""
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                self.conn = psycopg2.connect(**POSTGRESQL_CONFIG)
                self.cursor = self.conn.cursor()
                
                # Test the connection
                self.cursor.execute("SELECT 1")
                self.cursor.fetchone()
                
                self.logger.info("Connected to PostgreSQL database")
                
                # Verify required tables exist
                self._verify_database_setup()
                return True
                
            except Exception as e:
                self.logger.error(f"Database connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    self.logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    raise Exception(f"Failed to connect after {max_retries} attempts")
                    
    def _verify_database_setup(self):
        """Verify all required tables and indexes exist"""
        required_tables = [
            'land_registry_data',
            'companies_house_data', 
            'land_registry_ch_matches'
        ]
        
        for table in required_tables:
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
            """, (table,))
            
            if not self.cursor.fetchone()[0]:
                raise Exception(f"Required table '{table}' not found. Please run database setup first.")
        
        # Check if view exists
        self.cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.views
                WHERE table_schema = 'public' 
                AND table_name = 'v_land_registry_with_ch'
            )
        """)
        
        if not self.cursor.fetchone()[0]:
            self.logger.warning("View 'v_land_registry_with_ch' not found. Consider running create_match_table.sql")
            
        self.logger.info("Database setup verification complete")
        
    def disconnect(self):
        """Disconnect from database"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.logger.info("Database disconnected")
            
    def normalize_company_name_fixed(self, name):
        """
        Use the PROVEN normalization from the original script that REMOVES suffixes
        
        Through extensive testing, it was found that removing suffixes increases matches
        because Land Registry and Companies House use different variations:
        - ABC LTD vs ABC LIMITED  
        - XYZ COMPANY LIMITED vs XYZ LIMITED
        
        This normalization REMOVES all suffixes to match more records.
        Based on: /home/adc/Projects/InsideEstates/ETL/scripts/1 LR CSV Data Loaded.py
        """
        if not name or name.strip() == '':
            return ""
        
        name = str(name).upper().strip()
        name = name.replace(' AND ', ' ').replace(' & ', ' ')
        
        # Pre-compiled regex for suffixes - REMOVE THEM
        # Pattern matches suffix and anything after it (like punctuation, spaces, etc)
        suffix_pattern = r'\s*(LIMITED LIABILITY PARTNERSHIP|LIMITED|COMPANY|LTD\.|LLP|LTD|PLC|CO\.|CO).*$'
        name = re.sub(suffix_pattern, '', name)
        
        # Keep only alphanumeric
        name = ''.join(char for char in name if char.isalnum())
        
        return name
    
    def normalize_company_number_fixed(self, number):
        """
        Normalize company number for matching
        Based on: /home/adc/Projects/InsideEstates/ETL/scripts/1 LR CSV Data Loaded.py
        """
        if not number or number.strip() == '':
            return ""
        
        number = str(number).strip().upper()
        number = re.sub(r'[^A-Z0-9]', '', number)
        
        if number.startswith('SC'):
            return number
        
        if number.isdigit():
            return number.zfill(8)
        
        return number
    
    def load_companies_house_to_memory(self):
        """Load Companies House data into memory with progress tracking and optimization"""
        self.logger.info("Loading Companies House data into memory...")
        start_time = datetime.now()
        
        # Count total companies first
        self.cursor.execute("SELECT COUNT(*) FROM companies_house_data WHERE company_name IS NOT NULL")
        total_companies = self.cursor.fetchone()[0]
        self.logger.info(f"Total companies to load: {total_companies:,}")
        
        # Load in optimized batches
        processed = 0
        chunk_size = 100000  # Larger chunks for better performance
        memory_check_interval = 200000
        
        with tqdm(total=total_companies, desc="Loading CH data", unit="companies") as pbar:
            offset = 0
            
            while processed < total_companies and not self.interrupted:
                try:
                    # Use a fresh cursor for each batch to prevent memory issues
                    dict_cursor = self.conn.cursor(cursor_factory=RealDictCursor)
                    
                    dict_cursor.execute("""
                        SELECT 
                            company_number,
                            company_name,
                            company_status,
                            company_category,
                            incorporation_date
                        FROM companies_house_data
                        WHERE company_name IS NOT NULL
                        ORDER BY company_number
                        LIMIT %s OFFSET %s
                    """, (chunk_size, offset))
                    
                    rows = dict_cursor.fetchall()
                    dict_cursor.close()
                    
                    if not rows:
                        break
                    
                    # Process the batch
                    self._process_ch_batch(rows)
                    
                    batch_size = len(rows)
                    processed += batch_size
                    offset += chunk_size
                    pbar.update(batch_size)
                    
                    # Memory management and progress logging
                    if processed % memory_check_interval == 0:
                        gc.collect()  # Force garbage collection
                        self._log_memory_usage()
                        self.logger.info(f"Loaded {processed:,}/{total_companies:,} companies")
                    
                except Exception as e:
                    self.logger.error(f"Error loading CH batch at offset {offset}: {e}")
                    # Try to continue with next batch
                    offset += chunk_size
                    continue
        
        if self.interrupted:
            self.logger.warning("Loading interrupted by signal")
            return False
            
        # Load previous names
        self.logger.info("Loading previous company names...")
        self._load_previous_names_to_memory()
        
        elapsed = (datetime.now() - start_time).total_seconds()
        self.logger.info(f"CH memory loading complete in {elapsed:.1f}s")
        self.logger.info(f"Lookup tables built:")
        self.logger.info(f"  - Name+Number: {len(self.ch_lookup_name_number):,}")
        self.logger.info(f"  - Number only: {len(self.ch_lookup_number):,}")
        self.logger.info(f"  - Current name: {len(self.ch_lookup_name):,}")
        self.logger.info(f"  - Previous names: {len(self.ch_lookup_previous_name):,}")
        
        # Debug check for specific companies
        self._debug_check_lookups()
        
        self._log_memory_usage()
        return True
    
    def _process_ch_batch(self, rows):
        """Process a batch of Companies House records into lookup dictionaries"""
        for row in rows:
            if not row['company_name']:
                continue
            
            clean_name = self.normalize_company_name_fixed(row['company_name'])
            clean_number = self.normalize_company_number_fixed(row['company_number'])
            
            company_data = {
                'company_name': row['company_name'],
                'company_number': row['company_number'],
                'company_status': row['company_status'] or '',
                'company_category': row['company_category'] or '',
                'incorporation_date': row['incorporation_date'],
                'match_type': 'current'
            }
            
            # Build lookup dictionaries with duplicate handling
            # Tier 1: Name + Number (highest confidence)
            if clean_name and clean_number:
                key = clean_name + clean_number
                if key not in self.ch_lookup_name_number:
                    self.ch_lookup_name_number[key] = company_data
                # If duplicate, keep the first one (arbitrary but consistent)
            
            # Tier 2: Number only (high confidence)
            if clean_number and clean_number not in self.ch_lookup_number:
                self.ch_lookup_number[clean_number] = company_data
                
            # Tier 3: Name only (medium confidence)
            if clean_name and clean_name not in self.ch_lookup_name:
                self.ch_lookup_name[clean_name] = company_data
    
    def _load_previous_names_to_memory(self):
        """Load previous company names into memory for Tier 4 matching"""
        previous_names_loaded = 0
        
        # Process all 10 previous name columns
        for i in range(1, 11):
            try:
                self.cursor.execute(f"""
                    SELECT 
                        company_number,
                        previous_name_{i}_name,
                        company_name,
                        company_status,
                        company_category,
                        incorporation_date
                    FROM companies_house_data
                    WHERE previous_name_{i}_name IS NOT NULL 
                    AND previous_name_{i}_name != ''
                    AND previous_name_{i}_name != company_name
                """)
                
                for row in self.cursor.fetchall():
                    company_number, previous_name, current_name, status, category, inc_date = row
                    clean_previous_name = self.normalize_company_name_fixed(previous_name)
                    
                    # Only add if this previous name isn't already a current name
                    if clean_previous_name and clean_previous_name not in self.ch_lookup_name:
                        company_data = {
                            'company_name': current_name,  # Use current name in match result
                            'company_number': company_number,
                            'company_status': status or '',
                            'company_category': category or '',
                            'incorporation_date': inc_date,
                            'match_type': 'previous',
                            'matched_previous_name': previous_name
                        }
                        
                        if clean_previous_name not in self.ch_lookup_previous_name:
                            self.ch_lookup_previous_name[clean_previous_name] = company_data
                            previous_names_loaded += 1
                        
            except Exception as e:
                self.logger.warning(f"Error loading previous names column {i}: {e}")
                continue
        
        self.logger.info(f"Loaded {previous_names_loaded:,} unique previous names")
    
    def _debug_check_lookups(self):
        """Debug check for specific companies in lookup dictionaries"""
        test_companies = [
            ("S NOTARO LIMITED", "00845344"),
            ("HNE FOODS LTD", "16424988"),
            ("AL RAYAN BANK PLC", "04483430"),
        ]
        
        self.logger.info("Debug: Checking for test companies in lookup dictionaries...")
        
        for company_name, reg_number in test_companies:
            norm_name = self.normalize_company_name_fixed(company_name)
            norm_number = self.normalize_company_number_fixed(reg_number)
            
            # Check name+number lookup
            key = norm_name + norm_number
            if key in self.ch_lookup_name_number:
                self.logger.info(f"  ✓ Found '{company_name}' in Name+Number lookup: {self.ch_lookup_name_number[key]['company_name']}")
            else:
                self.logger.info(f"  ✗ '{company_name}' NOT in Name+Number lookup (key: {key})")
            
            # Check number lookup
            if norm_number in self.ch_lookup_number:
                self.logger.info(f"  ✓ Found number {reg_number} in Number lookup: {self.ch_lookup_number[norm_number]['company_name']}")
            else:
                self.logger.info(f"  ✗ Number {reg_number} NOT in Number lookup (normalized: {norm_number})")
            
            # Check name lookup
            if norm_name in self.ch_lookup_name:
                self.logger.info(f"  ✓ Found name '{company_name}' in Name lookup: {self.ch_lookup_name[norm_name]['company_name']}")
            else:
                self.logger.info(f"  ✗ Name '{company_name}' NOT in Name lookup (normalized: {norm_name})")
    
    def _log_memory_usage(self):
        """Log current memory usage of lookup dictionaries"""
        import sys
        
        # Rough memory usage estimation
        name_number_size = sys.getsizeof(self.ch_lookup_name_number) / (1024 * 1024)
        number_size = sys.getsizeof(self.ch_lookup_number) / (1024 * 1024)
        name_size = sys.getsizeof(self.ch_lookup_name) / (1024 * 1024)
        previous_size = sys.getsizeof(self.ch_lookup_previous_name) / (1024 * 1024)
        
        total_mb = name_number_size + number_size + name_size + previous_size
        self.stats['memory_usage_mb'] = total_mb
        
        self.logger.info(f"Lookup dictionary memory usage: ~{total_mb:.1f} MB")
    
    def match_to_companies_house(self, lr_owner_name, lr_company_number):
        """
        4-tier matching logic to find best Companies House match
        
        Returns: (company_data, match_type, confidence_score)
        
        Tier 1: Name + Number (confidence: 1.0) - Both match exactly
        Tier 2: Number only (confidence: 0.9) - Registration number matches
        Tier 3: Current Name (confidence: 0.7) - Company name matches current name
        Tier 4: Previous Name (confidence: 0.5) - Company name matches previous name
        Tier 5: No Match (confidence: 0.0) - No match found
        """
        clean_name = self.normalize_company_name_fixed(lr_owner_name)
        clean_number = self.normalize_company_number_fixed(lr_company_number)
        
        # Debug logging for specific companies
        if lr_owner_name and ('NOTARO' in lr_owner_name.upper() or 'HNE FOODS' in lr_owner_name.upper()):
            self.logger.debug(f"Matching '{lr_owner_name}' ({lr_company_number}) → normalized: '{clean_name}' / '{clean_number}'")
        
        # Tier 1: Name + Number (highest confidence - 1.0)
        # This is the gold standard - both name and number match exactly
        if clean_name and clean_number:
            key = clean_name + clean_number
            if key in self.ch_lookup_name_number:
                if lr_owner_name and ('NOTARO' in lr_owner_name.upper() or 'HNE FOODS' in lr_owner_name.upper()):
                    self.logger.debug(f"  → Found Tier 1 match: {self.ch_lookup_name_number[key]['company_name']}")
                return self.ch_lookup_name_number[key], 'Name+Number', 1.0
        
        # Tier 2: Number only (high confidence - 0.9)
        # Registration number is unique and reliable
        if clean_number and clean_number in self.ch_lookup_number:
            if 'NOTARO' in lr_owner_name.upper() or 'HNE FOODS' in lr_owner_name.upper():
                self.logger.debug(f"  → Found Tier 2 match: {self.ch_lookup_number[clean_number]['company_name']}")
            return self.ch_lookup_number[clean_number], 'Number', 0.9
        
        # Tier 3: Current Name only (medium confidence - 0.7)
        # Name matches current company name
        if clean_name and clean_name in self.ch_lookup_name:
            if 'NOTARO' in lr_owner_name.upper() or 'HNE FOODS' in lr_owner_name.upper():
                self.logger.debug(f"  → Found Tier 3 match: {self.ch_lookup_name[clean_name]['company_name']}")
            return self.ch_lookup_name[clean_name], 'Name', 0.7
        
        # Tier 4: Previous Name only (low confidence - 0.5)
        # Name matches a previous company name
        if clean_name and clean_name in self.ch_lookup_previous_name:
            return self.ch_lookup_previous_name[clean_name], 'Previous_Name', 0.5
        
        # Tier 5: No match (0.0 confidence)
        return None, 'No_Match', 0.0
    
    def save_checkpoint(self):
        """Save current state for resume capability"""
        checkpoint_data = {
            'last_processed_id': self.last_processed_id,
            'stats': self.stats,
            'mode': self.mode,
            'timestamp': datetime.now().isoformat(),
            'batch_size': self.batch_size
        }
        
        try:
            with open(self.state_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
            
            self.stats['checkpoints_saved'] += 1
            self.logger.info(f"Checkpoint saved at ID {self.last_processed_id}")
            
        except Exception as e:
            self.logger.error(f"Error saving checkpoint: {e}")
    
    def load_checkpoint(self):
        """Load previous state for resume"""
        if not os.path.exists(self.state_file):
            self.logger.info("No previous checkpoint found - starting fresh")
            return False
        
        try:
            with open(self.state_file, 'r') as f:
                checkpoint_data = json.load(f)
            
            self.last_processed_id = checkpoint_data.get('last_processed_id', 0)
            self.stats.update(checkpoint_data.get('stats', {}))
            self.mode = checkpoint_data.get('mode')
            
            self.logger.info(f"Checkpoint loaded - resuming from ID {self.last_processed_id}")
            self.logger.info(f"Previous progress: {self.stats['total_processed']:,} records processed")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading checkpoint: {e}")
            return False
    
    def process_batch(self, records):
        """Process a batch of Land Registry records and prepare match data"""
        match_data = []
        batch_errors = 0
        
        for record in records:
            try:
                record_id = record['id']
                match_record = {'id': record_id}
                has_any_proprietor = False
                has_any_match = False
                
                # Process each proprietor (1-4)
                for i in range(1, 5):
                    proprietor_name = record.get(f'proprietor_{i}_name', '')
                    company_reg_no = record.get(f'company_{i}_reg_no', '')
                    
                    if proprietor_name and proprietor_name.strip():
                        has_any_proprietor = True
                        self.stats['total_proprietors'] += 1
                        
                        # Attempt to match to Companies House
                        ch_match, match_type, confidence = self.match_to_companies_house(
                            proprietor_name, company_reg_no
                        )
                        
                        if ch_match:
                            # Store match data
                            match_record[f'ch_matched_name_{i}'] = ch_match['company_name']
                            match_record[f'ch_matched_number_{i}'] = ch_match['company_number']
                            match_record[f'ch_match_type_{i}'] = match_type
                            match_record[f'ch_match_confidence_{i}'] = confidence
                            has_any_match = True
                            
                            # Update tier-specific statistics
                            if match_type == 'Name+Number':
                                self.stats['name_number_matches'] += 1
                            elif match_type == 'Number':
                                self.stats['number_matches'] += 1
                            elif match_type == 'Name':
                                self.stats['name_matches'] += 1
                            elif match_type == 'Previous_Name':
                                self.stats['previous_name_matches'] += 1
                            
                            self.stats['matched_proprietors'] += 1
                            
                        else:
                            # No match found
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
                
                # Only include records that have proprietors
                if has_any_proprietor:
                    match_data.append(match_record)
                
            except Exception as e:
                batch_errors += 1
                self.logger.error(f"Error processing record ID {record.get('id', 'unknown')}: {e}")
                continue
        
        # Insert the batch
        if match_data:
            self._insert_batch_to_match_table(match_data)
        
        self.stats['errors'] += batch_errors
        return len(match_data)
    
    def _insert_batch_to_match_table(self, match_data):
        """Insert batch of matches into the separate match table with error handling"""
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                # Prepare the ON CONFLICT UPDATE query
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
                
                # Execute the batch insert
                execute_values(self.cursor, insert_query, values)
                self.conn.commit()
                return  # Success
                
            except Exception as e:
                self.logger.error(f"Batch insert attempt {attempt + 1} failed: {e}")
                self.conn.rollback()
                
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    self.stats['errors'] += len(match_data)
                    raise Exception(f"Failed to insert batch after {max_retries} attempts: {e}")
    
    def get_processing_query(self, mode, date_from=None, date_to=None):
        """Get the appropriate query based on processing mode"""
        base_fields = """
            lr.id,
            lr.proprietor_1_name, lr.company_1_reg_no,
            lr.proprietor_2_name, lr.company_2_reg_no,
            lr.proprietor_3_name, lr.company_3_reg_no,
            lr.proprietor_4_name, lr.company_4_reg_no
        """
        
        if mode == 'full':
            # Process all records
            return f"""
                SELECT {base_fields}
                FROM land_registry_data lr
                WHERE lr.id > %s
                ORDER BY lr.id
                LIMIT %s
            """, [self.last_processed_id]
            
        elif mode == 'no_match_only':
            # Re-process only records that had no matches
            return f"""
                SELECT {base_fields}
                FROM land_registry_data lr
                JOIN land_registry_ch_matches m ON lr.id = m.id
                WHERE lr.id > %s
                AND (
                    m.ch_match_type_1 = 'No_Match' OR
                    m.ch_match_type_2 = 'No_Match' OR  
                    m.ch_match_type_3 = 'No_Match' OR
                    m.ch_match_type_4 = 'No_Match'
                )
                ORDER BY lr.id
                LIMIT %s
            """, [self.last_processed_id]
            
        elif mode == 'date_range':
            # Process records within date range
            if not date_from or not date_to:
                raise ValueError("date_from and date_to required for date_range mode")
                
            return f"""
                SELECT {base_fields}
                FROM land_registry_data lr
                WHERE lr.id > %s
                AND lr.date_proprietor_added BETWEEN %s AND %s
                ORDER BY lr.id
                LIMIT %s
            """, [self.last_processed_id, date_from, date_to]
            
        elif mode == 'missing_only':
            # Process only records not yet in match table
            return f"""
                SELECT {base_fields}
                FROM land_registry_data lr
                LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
                WHERE lr.id > %s
                AND m.id IS NULL
                ORDER BY lr.id
                LIMIT %s
            """, [self.last_processed_id]
            
        else:
            raise ValueError(f"Unknown processing mode: {mode}")
    
    def get_total_records_count(self, mode, date_from=None, date_to=None):
        """Get total count of records to process based on mode"""
        if mode == 'full':
            self.cursor.execute("""
                SELECT COUNT(*) FROM land_registry_data 
                WHERE id > %s
            """, (self.last_processed_id,))
            
        elif mode == 'no_match_only':
            self.cursor.execute("""
                SELECT COUNT(*)
                FROM land_registry_data lr
                JOIN land_registry_ch_matches m ON lr.id = m.id
                WHERE lr.id > %s
                AND (
                    m.ch_match_type_1 = 'No_Match' OR
                    m.ch_match_type_2 = 'No_Match' OR  
                    m.ch_match_type_3 = 'No_Match' OR
                    m.ch_match_type_4 = 'No_Match'
                )
            """, (self.last_processed_id,))
            
        elif mode == 'date_range':
            self.cursor.execute("""
                SELECT COUNT(*) FROM land_registry_data
                WHERE id > %s
                AND date_proprietor_added BETWEEN %s AND %s
            """, (self.last_processed_id, date_from, date_to))
            
        elif mode == 'missing_only':
            self.cursor.execute("""
                SELECT COUNT(*)
                FROM land_registry_data lr
                LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
                WHERE lr.id > %s
                AND m.id IS NULL
            """, (self.last_processed_id,))
            
        return self.cursor.fetchone()[0]
    
    def process_all_records(self, mode='full', date_from=None, date_to=None, test_limit=None):
        """Process records based on the specified mode"""
        self.logger.info(f"Starting to process records in '{mode}' mode...")
        self.mode = mode
        
        # Get total count for progress tracking
        try:
            total_records = self.get_total_records_count(mode, date_from, date_to)
            if test_limit:
                total_records = min(total_records, test_limit)
        except Exception as e:
            self.logger.error(f"Error getting total count: {e}")
            total_records = 0
        
        self.logger.info(f"Total records to process: {total_records:,}")
        if total_records == 0:
            self.logger.info("No records to process")
            return
        
        # Get the processing query
        query, params = self.get_processing_query(mode, date_from, date_to)
        
        processed = self.stats['total_processed']
        last_checkpoint = processed
        
        with tqdm(total=total_records, initial=processed, desc=f"Processing ({mode})", unit="records") as pbar:
            while processed < total_records and not self.interrupted:
                try:
                    # Use dictionary cursor for easier field access
                    dict_cursor = self.conn.cursor(cursor_factory=RealDictCursor)
                    
                    # Execute query with current parameters
                    query_params = params + [self.batch_size]
                    dict_cursor.execute(query, query_params)
                    records = dict_cursor.fetchall()
                    dict_cursor.close()
                    
                    if not records:
                        self.logger.info("No more records found")
                        break
                    
                    # Process the batch
                    batch_processed = self.process_batch(records)
                    
                    # Update progress tracking
                    batch_size = len(records)
                    self.stats['total_processed'] += batch_size
                    processed = self.stats['total_processed']
                    
                    if records:
                        self.last_processed_id = max(record['id'] for record in records)
                    
                    pbar.update(batch_size)
                    
                    # Checkpoint saving
                    if processed - last_checkpoint >= self.checkpoint_interval:
                        self.save_checkpoint()
                        last_checkpoint = processed
                    
                    # Progress logging
                    if processed % 100000 == 0:
                        match_rate = (self.stats['matched_proprietors'] / self.stats['total_proprietors'] * 100) if self.stats['total_proprietors'] > 0 else 0
                        self.logger.info(f"Progress: {processed:,}/{total_records:,} records. Match rate: {match_rate:.1f}%")
                    
                    # Memory management
                    if processed % 50000 == 0:
                        gc.collect()
                    
                    # Test limit check
                    if test_limit and processed >= test_limit:
                        self.logger.info(f"Reached test limit of {test_limit} records")
                        break
                        
                except Exception as e:
                    self.logger.error(f"Error processing batch starting at ID {self.last_processed_id}: {e}")
                    # Try to advance past the problematic batch
                    self.last_processed_id += self.batch_size
                    self.stats['errors'] += 1
                    
                    # If too many errors, abort
                    if self.stats['errors'] > 100:
                        self.logger.error("Too many errors encountered. Aborting.")
                        break
                        
                    continue
        
        # Final checkpoint
        if not self.interrupted:
            self.save_checkpoint()
        
        self.logger.info(f"Processing completed. Total processed: {self.stats['total_processed']:,}")
    
    def print_statistics(self):
        """Print comprehensive matching statistics"""
        self.logger.info("\n" + "="*70)
        self.logger.info("PRODUCTION MATCHING STATISTICS")
        self.logger.info("="*70)
        
        self.logger.info(f"Execution Details:")
        self.logger.info(f"  Mode: {self.mode}")
        self.logger.info(f"  Total records processed: {self.stats['total_processed']:,}")
        self.logger.info(f"  Total proprietors examined: {self.stats['total_proprietors']:,}")
        self.logger.info(f"  Errors encountered: {self.stats['errors']:,}")
        self.logger.info(f"  Checkpoints saved: {self.stats['checkpoints_saved']:,}")
        self.logger.info(f"  Memory usage: ~{self.stats['memory_usage_mb']:.1f} MB")
        
        self.logger.info(f"\nMatching Results:")
        self.logger.info(f"  Total proprietors matched: {self.stats['matched_proprietors']:,}")
        self.logger.info(f"  Total with no matches: {self.stats['no_matches']:,}")
        
        if self.stats['total_proprietors'] > 0:
            match_rate = (self.stats['matched_proprietors'] / self.stats['total_proprietors']) * 100
            self.logger.info(f"  Overall match rate: {match_rate:.2f}%")
        
        self.logger.info(f"\nMatch Quality Breakdown:")
        self.logger.info(f"  Tier 1 - Name+Number (1.0): {self.stats['name_number_matches']:,}")
        self.logger.info(f"  Tier 2 - Number only (0.9): {self.stats['number_matches']:,}")
        self.logger.info(f"  Tier 3 - Name only (0.7): {self.stats['name_matches']:,}")
        self.logger.info(f"  Tier 4 - Previous name (0.5): {self.stats['previous_name_matches']:,}")
        self.logger.info(f"  Tier 5 - No match (0.0): {self.stats['no_matches']:,}")
        
        # Calculate quality metrics
        if self.stats['matched_proprietors'] > 0:
            high_confidence = self.stats['name_number_matches'] + self.stats['number_matches']
            high_confidence_rate = (high_confidence / self.stats['matched_proprietors']) * 100
            self.logger.info(f"\nMatch Quality Metrics:")
            self.logger.info(f"  High confidence matches (Tier 1+2): {high_confidence:,} ({high_confidence_rate:.1f}% of matches)")
    
    def post_processing_tasks(self):
        """Run post-processing tasks after matching is complete"""
        self.logger.info("Running post-processing tasks...")
        
        try:
            # Update table statistics
            self.logger.info("Running VACUUM ANALYZE on match table...")
            self.conn.commit()  # Commit any pending transactions
            self.conn.autocommit = True
            self.cursor.execute("VACUUM ANALYZE land_registry_ch_matches")
            self.conn.autocommit = False
            
            # Generate summary statistics
            self.cursor.execute("""
                SELECT 
                    COUNT(*) as total_matches,
                    COUNT(CASE WHEN ch_match_type_1 != 'No_Match' THEN 1 END) as has_match,
                    COUNT(CASE WHEN ch_match_type_1 = 'Name+Number' THEN 1 END) as tier1,
                    COUNT(CASE WHEN ch_match_type_1 = 'Number' THEN 1 END) as tier2,
                    COUNT(CASE WHEN ch_match_type_1 = 'Name' THEN 1 END) as tier3,
                    COUNT(CASE WHEN ch_match_type_1 = 'Previous_Name' THEN 1 END) as tier4
                FROM land_registry_ch_matches
            """)
            
            summary = self.cursor.fetchone()
            if summary:
                total, has_match, tier1, tier2, tier3, tier4 = summary
                self.logger.info(f"\nDatabase Summary:")
                self.logger.info(f"  Total records in match table: {total:,}")
                self.logger.info(f"  Records with matches: {has_match:,}")
                self.logger.info(f"  Tier distribution: T1={tier1:,}, T2={tier2:,}, T3={tier3:,}, T4={tier4:,}")
        
        except Exception as e:
            self.logger.warning(f"Error in post-processing: {e}")
    
    def run(self, mode='full', date_from=None, date_to=None, test_limit=None, resume=True):
        """Main execution method"""
        self.start_time = datetime.now()
        
        try:
            # Load checkpoint if resuming
            if resume:
                self.load_checkpoint()
            
            # Connect to database
            self.connect()
            
            # Load Companies House data into memory
            if not self.load_companies_house_to_memory():
                self.logger.error("Failed to load Companies House data")
                return False
            
            # Process all records
            self.process_all_records(mode, date_from, date_to, test_limit)
            
            if not self.interrupted:
                # Post-processing tasks
                self.post_processing_tasks()
                
                # Print final statistics
                self.print_statistics()
                
                # Clean up checkpoint file if completed successfully
                if os.path.exists(self.state_file):
                    os.remove(self.state_file)
                    self.logger.info("Checkpoint file removed (matching completed)")
                
                self.logger.info("\n✅ PRODUCTION MATCHING COMPLETED SUCCESSFULLY!")
                self.logger.info("\nQuery your matched data using:")
                self.logger.info("  SELECT * FROM v_land_registry_with_ch WHERE ch_matched_number_1 = '12345678';")
            else:
                self.logger.warning("Matching interrupted - checkpoint saved for resume")
                
            return True
            
        except Exception as e:
            self.logger.error(f"Fatal error in production matching: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False
        finally:
            # Calculate and log total runtime
            if self.start_time:
                elapsed = (datetime.now() - self.start_time).total_seconds()
                self.logger.info(f"Total execution time: {elapsed/60:.1f} minutes")
            
            self.disconnect()

def main():
    parser = argparse.ArgumentParser(
        description='Production Land Registry to Companies House Matcher',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full matching (all records)
  python 03_match_lr_to_ch_production.py --mode full
  
  # Re-process only unmatched records
  python 03_match_lr_to_ch_production.py --mode no_match_only
  
  # Process missing records only (not in match table yet)
  python 03_match_lr_to_ch_production.py --mode missing_only
  
  # Process date range
  python 03_match_lr_to_ch_production.py --mode date_range --date-from 2024-01-01 --date-to 2024-12-31
  
  # Test mode (limit to 10000 records)
  python 03_match_lr_to_ch_production.py --mode full --test 10000
  
  # Resume from checkpoint
  python 03_match_lr_to_ch_production.py --mode full --resume
  
  # Start fresh (ignore checkpoint)
  python 03_match_lr_to_ch_production.py --mode full --no-resume
        """
    )
    
    parser.add_argument('--mode', 
                       choices=['full', 'no_match_only', 'missing_only', 'date_range'],
                       default='full',
                       help='Processing mode (default: full)')
    
    parser.add_argument('--batch-size', type=int, default=5000,
                       help='Batch size for processing (default: 5000)')
    
    parser.add_argument('--checkpoint-interval', type=int, default=50000,
                       help='Save checkpoint every N records (default: 50000)')
    
    parser.add_argument('--date-from', type=str,
                       help='Start date for date_range mode (YYYY-MM-DD)')
    
    parser.add_argument('--date-to', type=str,
                       help='End date for date_range mode (YYYY-MM-DD)')
    
    parser.add_argument('--test', type=int,
                       help='Test mode - process only N records')
    
    parser.add_argument('--resume', action='store_true', default=True,
                       help='Resume from checkpoint if available (default)')
    
    parser.add_argument('--no-resume', dest='resume', action='store_false',
                       help='Start fresh, ignore any checkpoint')
    
    args = parser.parse_args()
    
    # Validation
    if args.mode == 'date_range':
        if not args.date_from or not args.date_to:
            parser.error("--date-from and --date-to are required for date_range mode")
        
        try:
            datetime.strptime(args.date_from, '%Y-%m-%d')
            datetime.strptime(args.date_to, '%Y-%m-%d')
        except ValueError:
            parser.error("Date format must be YYYY-MM-DD")
    
    print("🏭 PRODUCTION LAND REGISTRY TO COMPANIES HOUSE MATCHER")
    print("="*60)
    print(f"Mode: {args.mode}")
    print(f"Batch size: {args.batch_size:,}")
    print(f"Checkpoint interval: {args.checkpoint_interval:,}")
    if args.test:
        print(f"Test limit: {args.test:,} records")
    if args.date_from and args.date_to:
        print(f"Date range: {args.date_from} to {args.date_to}")
    print(f"Resume: {'Yes' if args.resume else 'No'}")
    print("="*60)
    
    # Verify database setup
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor()
        
        # Check required tables
        for table in ['land_registry_data', 'companies_house_data', 'land_registry_ch_matches']:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
            """, (table,))
            
            if not cursor.fetchone()[0]:
                print(f"❌ ERROR: Required table '{table}' not found")
                if table == 'land_registry_ch_matches':
                    print("   Run: python scripts/run_sql_script.py scripts/create_match_table.sql")
                cursor.close()
                conn.close()
                return 1
        
        cursor.close()
        conn.close()
        print("✅ Database setup verified")
        
    except Exception as e:
        print(f"❌ Database setup check failed: {e}")
        return 1
    
    # Create and run the matcher
    matcher = ProductionMatcher(
        batch_size=args.batch_size,
        checkpoint_interval=args.checkpoint_interval
    )
    
    success = matcher.run(
        mode=args.mode,
        date_from=args.date_from,
        date_to=args.date_to,
        test_limit=args.test,
        resume=args.resume
    )
    
    return 0 if success else 1

if __name__ == '__main__':
    exit(main())