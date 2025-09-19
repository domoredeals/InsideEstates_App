#!/usr/bin/env python3
"""
PSC Data Normalization Script
Part 5 of the InsideEstates data pipeline

This script normalizes the PSC data by extracting JSON fields into proper columns
for better performance and functionality.
"""

import os
import sys
import psycopg2
from psycopg2.extras import execute_values, Json
import logging
from datetime import datetime
import argparse
from tqdm import tqdm

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

# Configure logging
log_filename = f'psc_normalize_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PSCNormalizer:
    def __init__(self, batch_size=5000):
        self.batch_size = batch_size
        self.conn = None
        self.cursor = None
        self.stats = {
            'total_records': 0,
            'migrated': 0,
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
            
    def create_normalized_table(self):
        """Create the normalized PSC table"""
        logger.info("Creating psc_data_normalized table...")
        
        # Drop existing table if requested
        self.cursor.execute("DROP TABLE IF EXISTS psc_data_normalized CASCADE")
        
        # Create the normalized table
        self.cursor.execute("""
            CREATE TABLE psc_data_normalized (
                id BIGSERIAL PRIMARY KEY,
                company_number VARCHAR(20) NOT NULL,
                psc_type VARCHAR(100) NOT NULL,
                name TEXT,
                
                -- Name elements (normalized from JSON)
                name_title VARCHAR(100),
                name_forename VARCHAR(100),
                name_middle_name VARCHAR(100),
                name_surname VARCHAR(200),
                
                -- Date of birth (normalized from JSON)
                birth_year INTEGER,
                birth_month INTEGER,
                
                -- Address fields (normalized from JSON)
                address_care_of TEXT,
                address_po_box VARCHAR(100),
                address_premises TEXT,
                address_line_1 TEXT,
                address_line_2 TEXT,
                address_locality TEXT,
                address_region TEXT,
                address_country TEXT,
                address_postal_code VARCHAR(50),
                
                -- Other individual fields
                country_of_residence TEXT,
                nationality TEXT,
                
                -- Identification fields for corporate entities (normalized from JSON)
                identification_legal_form TEXT,
                identification_legal_authority TEXT,
                identification_place_registered TEXT,
                identification_country_registered TEXT,
                identification_registration_number VARCHAR(200),
                
                -- Control and dates
                natures_of_control TEXT[],
                notified_on DATE,
                ceased_on DATE,
                
                -- Metadata
                etag TEXT,
                links JSONB,  -- Keep this as JSON since it's just URLs
                source_filename TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                -- Constraints
                CONSTRAINT uk_company_psc_normalized UNIQUE (company_number, etag)
            );
        """)
        
        self.conn.commit()
        logger.info("Created psc_data_normalized table")
        
    def create_indexes(self):
        """Create comprehensive indexes for performance"""
        logger.info("Creating indexes...")
        
        indexes = [
            # Primary lookups
            "CREATE INDEX idx_psc_norm_company_number ON psc_data_normalized(company_number);",
            "CREATE INDEX idx_psc_norm_type ON psc_data_normalized(psc_type);",
            "CREATE INDEX idx_psc_norm_name ON psc_data_normalized(name);",
            "CREATE INDEX idx_psc_norm_surname ON psc_data_normalized(name_surname);",
            
            # Address searches
            "CREATE INDEX idx_psc_norm_postal_code ON psc_data_normalized(address_postal_code);",
            "CREATE INDEX idx_psc_norm_locality ON psc_data_normalized(address_locality);",
            "CREATE INDEX idx_psc_norm_region ON psc_data_normalized(address_region);",
            
            # Date filters
            "CREATE INDEX idx_psc_norm_birth_year ON psc_data_normalized(birth_year);",
            "CREATE INDEX idx_psc_norm_notified_on ON psc_data_normalized(notified_on);",
            "CREATE INDEX idx_psc_norm_ceased_on ON psc_data_normalized(ceased_on) WHERE ceased_on IS NULL;",
            
            # Corporate entity searches
            "CREATE INDEX idx_psc_norm_corp_reg_no ON psc_data_normalized(identification_registration_number) WHERE identification_registration_number IS NOT NULL;",
            
            # Natures of control
            "CREATE INDEX idx_psc_norm_natures ON psc_data_normalized USING GIN (natures_of_control);",
            
            # Composite indexes for common queries
            "CREATE INDEX idx_psc_norm_active_by_postcode ON psc_data_normalized(address_postal_code) WHERE ceased_on IS NULL;",
            "CREATE INDEX idx_psc_norm_company_active ON psc_data_normalized(company_number) WHERE ceased_on IS NULL;"
        ]
        
        for idx_sql in indexes:
            logger.info(f"Creating index: {idx_sql[:50]}...")
            self.cursor.execute(idx_sql)
            
        self.conn.commit()
        logger.info("Created all indexes")
        
    def migrate_data(self):
        """Migrate data from original table to normalized table"""
        logger.info("Starting data migration...")
        
        # Get total count
        self.cursor.execute("SELECT COUNT(*) FROM psc_data")
        total_records = self.cursor.fetchone()[0]
        logger.info(f"Total records to migrate: {total_records:,}")
        
        # Process in batches
        offset = 0
        with tqdm(total=total_records, desc="Migrating PSC data") as pbar:
            while offset < total_records:
                # Fetch batch
                self.cursor.execute("""
                    SELECT 
                        id, company_number, psc_type, name,
                        name_elements, date_of_birth, address,
                        country_of_residence, nationality,
                        identification, natures_of_control,
                        notified_on, ceased_on, etag, links,
                        source_filename
                    FROM psc_data
                    ORDER BY id
                    LIMIT %s OFFSET %s
                """, (self.batch_size, offset))
                
                batch = self.cursor.fetchall()
                if not batch:
                    break
                
                # Transform and insert batch
                transformed_batch = []
                for row in batch:
                    try:
                        transformed = self.transform_record(row)
                        transformed_batch.append(transformed)
                        self.stats['migrated'] += 1
                    except Exception as e:
                        logger.error(f"Error transforming record {row[0]}: {e}")
                        self.stats['errors'] += 1
                
                # Insert transformed batch
                if transformed_batch:
                    self.insert_normalized_batch(transformed_batch)
                
                offset += self.batch_size
                pbar.update(len(batch))
                self.stats['total_records'] += len(batch)
        
        logger.info("Data migration complete")
        
    def transform_record(self, row):
        """Transform a single record from original to normalized format"""
        (id_val, company_number, psc_type, name, name_elements, date_of_birth,
         address, country_of_residence, nationality, identification,
         natures_of_control, notified_on, ceased_on, etag, links,
         source_filename) = row
        
        # Extract name elements
        name_title = name_elements.get('title') if name_elements else None
        name_forename = name_elements.get('forename') if name_elements else None
        name_middle_name = name_elements.get('middle_name') if name_elements else None
        name_surname = name_elements.get('surname') if name_elements else None
        
        # Extract date of birth
        birth_year = date_of_birth.get('year') if date_of_birth else None
        birth_month = date_of_birth.get('month') if date_of_birth else None
        
        # Extract address fields
        if address:
            address_care_of = address.get('care_of')
            address_po_box = address.get('po_box')
            address_premises = address.get('premises')
            address_line_1 = address.get('address_line_1')
            address_line_2 = address.get('address_line_2')
            address_locality = address.get('locality')
            address_region = address.get('region')
            address_country = address.get('country')
            address_postal_code = address.get('postal_code')
        else:
            address_care_of = address_po_box = address_premises = None
            address_line_1 = address_line_2 = address_locality = None
            address_region = address_country = address_postal_code = None
        
        # Extract identification fields
        if identification:
            id_legal_form = identification.get('legal_form')
            id_legal_authority = identification.get('legal_authority')
            id_place_registered = identification.get('place_registered')
            id_country_registered = identification.get('country_registered')
            id_registration_number = identification.get('registration_number')
        else:
            id_legal_form = id_legal_authority = id_place_registered = None
            id_country_registered = id_registration_number = None
        
        return {
            'company_number': company_number,
            'psc_type': psc_type,
            'name': name,
            'name_title': name_title,
            'name_forename': name_forename,
            'name_middle_name': name_middle_name,
            'name_surname': name_surname,
            'birth_year': birth_year,
            'birth_month': birth_month,
            'address_care_of': address_care_of,
            'address_po_box': address_po_box,
            'address_premises': address_premises,
            'address_line_1': address_line_1,
            'address_line_2': address_line_2,
            'address_locality': address_locality,
            'address_region': address_region,
            'address_country': address_country,
            'address_postal_code': address_postal_code,
            'country_of_residence': country_of_residence,
            'nationality': nationality,
            'identification_legal_form': id_legal_form,
            'identification_legal_authority': id_legal_authority,
            'identification_place_registered': id_place_registered,
            'identification_country_registered': id_country_registered,
            'identification_registration_number': id_registration_number,
            'natures_of_control': natures_of_control,
            'notified_on': notified_on,
            'ceased_on': ceased_on,
            'etag': etag,
            'links': Json(links) if links else None,
            'source_filename': source_filename
        }
        
    def insert_normalized_batch(self, batch_data):
        """Insert a batch of normalized records"""
        if not batch_data:
            return
            
        try:
            columns = list(batch_data[0].keys())
            
            insert_query = f"""
                INSERT INTO psc_data_normalized ({', '.join(columns)})
                VALUES %s
            """
            
            values = []
            for record in batch_data:
                values.append(tuple(record[col] for col in columns))
                
            execute_values(self.cursor, insert_query, values)
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Error inserting batch: {e}")
            self.conn.rollback()
            
    def verify_migration(self):
        """Verify the migration was successful"""
        logger.info("Verifying migration...")
        
        # Compare counts
        self.cursor.execute("SELECT COUNT(*) FROM psc_data")
        original_count = self.cursor.fetchone()[0]
        
        self.cursor.execute("SELECT COUNT(*) FROM psc_data_normalized")
        normalized_count = self.cursor.fetchone()[0]
        
        logger.info(f"Original table records: {original_count:,}")
        logger.info(f"Normalized table records: {normalized_count:,}")
        logger.info(f"Migration success rate: {normalized_count/original_count*100:.2f}%")
        
        # Sample queries to show improvement
        logger.info("\n" + "="*60)
        logger.info("SAMPLE QUERIES - PERFORMANCE COMPARISON")
        logger.info("="*60)
        
        # Query 1: Search by postal code
        logger.info("\nQuery 1: Search by postal code 'SW1A 1AA'")
        
        # Original query
        start = datetime.now()
        self.cursor.execute("""
            SELECT COUNT(*) 
            FROM psc_data 
            WHERE address->>'postal_code' = 'SW1A 1AA'
        """)
        original_time = (datetime.now() - start).total_seconds()
        
        # Normalized query
        start = datetime.now()
        self.cursor.execute("""
            SELECT COUNT(*) 
            FROM psc_data_normalized 
            WHERE address_postal_code = 'SW1A 1AA'
        """)
        normalized_time = (datetime.now() - start).total_seconds()
        
        logger.info(f"Original table (JSON): {original_time:.3f} seconds")
        logger.info(f"Normalized table: {normalized_time:.3f} seconds")
        logger.info(f"Speed improvement: {original_time/normalized_time:.1f}x faster")
        
    def run_normalization(self):
        """Main normalization process"""
        start_time = datetime.now()
        
        try:
            self.connect()
            self.create_normalized_table()
            self.migrate_data()
            self.create_indexes()
            self.verify_migration()
            
            # Final statistics
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info("\n" + "="*60)
            logger.info("NORMALIZATION COMPLETE")
            logger.info("="*60)
            logger.info(f"Total records processed: {self.stats['total_records']:,}")
            logger.info(f"Successfully migrated: {self.stats['migrated']:,}")
            logger.info(f"Errors: {self.stats['errors']:,}")
            logger.info(f"Time taken: {elapsed/60:.1f} minutes")
            logger.info(f"Log file: {log_filename}")
            
            # Show example queries
            logger.info("\n" + "="*60)
            logger.info("EXAMPLE QUERIES FOR NORMALIZED TABLE")
            logger.info("="*60)
            logger.info("""
-- Find PSCs by postal code
SELECT company_number, name, address_line_1, address_postal_code 
FROM psc_data_normalized 
WHERE address_postal_code = 'SW1A 1AA' 
AND ceased_on IS NULL;

-- Find PSCs born in a specific year
SELECT name_forename, name_surname, birth_year, birth_month 
FROM psc_data_normalized 
WHERE birth_year = 1970 
LIMIT 10;

-- Find corporate PSCs by registration number
SELECT company_number, name, identification_registration_number 
FROM psc_data_normalized 
WHERE identification_registration_number = '12345678';

-- Geographic analysis
SELECT address_region, COUNT(*) as psc_count 
FROM psc_data_normalized 
WHERE ceased_on IS NULL 
GROUP BY address_region 
ORDER BY psc_count DESC;
            """)
            
        except Exception as e:
            logger.error(f"Fatal error during normalization: {e}")
            raise
        finally:
            self.disconnect()

def main():
    parser = argparse.ArgumentParser(description='Normalize PSC Data')
    parser.add_argument('--batch-size', type=int, default=5000,
                       help='Batch size for migration (default: 5000)')
    
    args = parser.parse_args()
    
    normalizer = PSCNormalizer(batch_size=args.batch_size)
    normalizer.run_normalization()

if __name__ == '__main__':
    main()