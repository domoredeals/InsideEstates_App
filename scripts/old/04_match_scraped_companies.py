#!/usr/bin/env python3
"""
Match companies from ch_scrape_overview with land_registry_data.
This script checks scraped companies that were previously "No Match" 
and updates the matching table with the new information.
"""

import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv
import logging
import argparse
from decimal import Decimal

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('match_scraped_companies.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def connect_to_db():
    """Connect to the PostgreSQL database."""
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

def add_scraped_data_column(conn):
    """Add scraped_data column to land_registry_ch_matches if it doesn't exist."""
    cursor = conn.cursor()
    try:
        # Check if column exists
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'land_registry_ch_matches' 
            AND column_name = 'scraped_data'
        """)
        
        if not cursor.fetchone():
            logger.info("Adding scraped_data column to land_registry_ch_matches...")
            cursor.execute("""
                ALTER TABLE land_registry_ch_matches 
                ADD COLUMN scraped_data CHAR(1) DEFAULT 'N' CHECK (scraped_data IN ('Y', 'N'))
            """)
            
            # Add index for performance
            cursor.execute("""
                CREATE INDEX idx_ch_matches_scraped_data 
                ON land_registry_ch_matches(scraped_data) 
                WHERE scraped_data = 'Y'
            """)
            
            conn.commit()
            logger.info("scraped_data column added successfully.")
        else:
            logger.info("scraped_data column already exists.")
            
    except Exception as e:
        logger.error(f"Error adding scraped_data column: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

def get_scraped_companies(conn):
    """Get all parsed companies from ch_scrape_overview."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT company_number, company_name, company_status, previous_names
            FROM ch_scrape_overview
            WHERE scrape_status = 'parsed'
        """)
        return cursor.fetchall()
    finally:
        cursor.close()

def find_matches_for_scraped_company(conn, company_number, company_name, previous_names=None):
    """Find land registry records that match the scraped company."""
    cursor = conn.cursor()
    matches = []
    
    try:
        # Build list of all names to check (current + previous)
        names_to_check = [company_name]
        if previous_names:
            names_to_check.extend(previous_names)
        
        # Also handle variations of the company number (e.g., SC002116 vs 002116 vs 2116)
        number_variations = [company_number]
        if company_number.startswith('SC') and len(company_number) > 2:
            # Add variations without SC prefix
            base_number = company_number[2:]
            number_variations.append(base_number)
            # Also add without leading zeros
            number_variations.append(base_number.lstrip('0'))
        elif company_number.startswith('0'):
            # Add variation without leading zeros
            number_variations.append(company_number.lstrip('0'))
        
        # Find matches by company number (exact match)
        cursor.execute("""
            SELECT DISTINCT lr.id, lr.proprietor_1_name, lr.company_1_reg_no, 1 as prop_num
            FROM land_registry_data lr
            LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE lr.company_1_reg_no = ANY(%s)
            AND (m.ch_match_type_1 IS NULL OR m.ch_match_type_1 = 'No Match' OR m.ch_match_type_1 = 'No_Match')
            
            UNION
            
            SELECT DISTINCT lr.id, lr.proprietor_2_name, lr.company_2_reg_no, 2 as prop_num
            FROM land_registry_data lr
            LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE lr.company_2_reg_no = ANY(%s)
            AND (m.ch_match_type_2 IS NULL OR m.ch_match_type_2 = 'No Match' OR m.ch_match_type_2 = 'No_Match')
            
            UNION
            
            SELECT DISTINCT lr.id, lr.proprietor_3_name, lr.company_3_reg_no, 3 as prop_num
            FROM land_registry_data lr
            LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE lr.company_3_reg_no = ANY(%s)
            AND (m.ch_match_type_3 IS NULL OR m.ch_match_type_3 = 'No Match' OR m.ch_match_type_3 = 'No_Match')
            
            UNION
            
            SELECT DISTINCT lr.id, lr.proprietor_4_name, lr.company_4_reg_no, 4 as prop_num
            FROM land_registry_data lr
            LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE lr.company_4_reg_no = ANY(%s)
            AND (m.ch_match_type_4 IS NULL OR m.ch_match_type_4 = 'No Match' OR m.ch_match_type_4 = 'No_Match')
        """, (number_variations, number_variations, number_variations, number_variations))
        
        matches.extend([(row[0], row[1], row[2], row[3], 'Number', Decimal('1.00')) for row in cursor.fetchall()])
        
        # Find matches by company name (including previous names)
        if not matches:
            # Normalize all names for matching
            normalized_names = [name.upper().strip() for name in names_to_check]
            
            cursor.execute("""
                SELECT DISTINCT lr.id, lr.proprietor_1_name, lr.company_1_reg_no, 1 as prop_num
                FROM land_registry_data lr
                LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
                WHERE UPPER(TRIM(lr.proprietor_1_name)) = ANY(%s)
                AND (m.ch_match_type_1 IS NULL OR m.ch_match_type_1 = 'No Match' OR m.ch_match_type_1 = 'No_Match')
                
                UNION
                
                SELECT DISTINCT lr.id, lr.proprietor_2_name, lr.company_2_reg_no, 2 as prop_num
                FROM land_registry_data lr
                LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
                WHERE UPPER(TRIM(lr.proprietor_2_name)) = ANY(%s)
                AND (m.ch_match_type_2 IS NULL OR m.ch_match_type_2 = 'No Match' OR m.ch_match_type_2 = 'No_Match')
                
                UNION
                
                SELECT DISTINCT lr.id, lr.proprietor_3_name, lr.company_3_reg_no, 3 as prop_num
                FROM land_registry_data lr
                LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
                WHERE UPPER(TRIM(lr.proprietor_3_name)) = ANY(%s)
                AND (m.ch_match_type_3 IS NULL OR m.ch_match_type_3 = 'No Match' OR m.ch_match_type_3 = 'No_Match')
                
                UNION
                
                SELECT DISTINCT lr.id, lr.proprietor_4_name, lr.company_4_reg_no, 4 as prop_num
                FROM land_registry_data lr
                LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
                WHERE UPPER(TRIM(lr.proprietor_4_name)) = ANY(%s)
                AND (m.ch_match_type_4 IS NULL OR m.ch_match_type_4 = 'No Match' OR m.ch_match_type_4 = 'No_Match')
            """, (normalized_names, normalized_names, normalized_names, normalized_names))
            
            for row in cursor.fetchall():
                # Determine match type based on whether it's current or previous name
                proprietor_name_upper = row[1].upper().strip()
                if proprietor_name_upper == company_name.upper().strip():
                    match_type = 'Name'
                else:
                    match_type = 'PrevName'
                matches.append((row[0], row[1], row[2], row[3], match_type, Decimal('0.90')))
        
        return matches
        
    finally:
        cursor.close()

def update_matches(conn, matches, company_number, company_name):
    """Update land_registry_ch_matches with scraped company data."""
    cursor = conn.cursor()
    updated_count = 0
    
    try:
        for lr_id, prop_name, prop_reg_no, prop_num, match_type, confidence in matches:
            # Check if record exists in matches table
            cursor.execute("SELECT id FROM land_registry_ch_matches WHERE id = %s", (lr_id,))
            exists = cursor.fetchone()
            
            if exists:
                # Update existing record
                update_query = f"""
                    UPDATE land_registry_ch_matches
                    SET ch_matched_name_{prop_num} = %s,
                        ch_matched_number_{prop_num} = %s,
                        ch_match_type_{prop_num} = %s,
                        ch_match_confidence_{prop_num} = %s,
                        scraped_data = 'Y',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """
                cursor.execute(update_query, (company_name, company_number, match_type, confidence, lr_id))
            else:
                # Insert new record
                insert_query = f"""
                    INSERT INTO land_registry_ch_matches 
                    (id, ch_matched_name_{prop_num}, ch_matched_number_{prop_num}, 
                     ch_match_type_{prop_num}, ch_match_confidence_{prop_num}, scraped_data)
                    VALUES (%s, %s, %s, %s, %s, 'Y')
                """
                cursor.execute(insert_query, (lr_id, company_name, company_number, match_type, confidence))
            
            updated_count += 1
            
            if updated_count % 100 == 0:
                logger.info(f"Updated {updated_count} matches...")
        
        return updated_count
        
    except Exception as e:
        logger.error(f"Error updating matches: {e}")
        raise
    finally:
        cursor.close()

def main():
    """Main function to process scraped companies and update matches."""
    parser = argparse.ArgumentParser(description='Match scraped companies with land registry data')
    parser.add_argument('--dry-run', action='store_true', help='Run without making database changes')
    args = parser.parse_args()
    
    conn = None
    try:
        conn = connect_to_db()
        logger.info("Connected to database successfully.")
        
        if not args.dry_run:
            # Add scraped_data column if needed
            add_scraped_data_column(conn)
        
        # Get scraped companies
        scraped_companies = get_scraped_companies(conn)
        logger.info(f"Found {len(scraped_companies)} scraped companies to process.")
        
        total_matches = 0
        
        for company_number, company_name, company_status, previous_names in scraped_companies:
            logger.info(f"\nProcessing: {company_name} ({company_number}) - Status: {company_status}")
            if previous_names:
                logger.info(f"Previous names: {previous_names}")
            
            # Find matches
            matches = find_matches_for_scraped_company(conn, company_number, company_name, previous_names)
            
            if matches:
                logger.info(f"Found {len(matches)} potential matches")
                
                if not args.dry_run:
                    # Update matches
                    updated = update_matches(conn, matches, company_number, company_name)
                    total_matches += updated
                    conn.commit()
                    logger.info(f"Updated {updated} matches in database")
                else:
                    total_matches += len(matches)
                    logger.info(f"Would update {len(matches)} matches (dry run)")
            else:
                logger.info("No matches found")
        
        logger.info(f"\nProcessing complete. Total matches updated: {total_matches}")
        
        # Show summary statistics
        if not args.dry_run:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) 
                FROM land_registry_ch_matches 
                WHERE scraped_data = 'Y'
            """)
            scraped_matches = cursor.fetchone()[0]
            cursor.close()
            
            logger.info(f"Total matches from scraped data: {scraped_matches}")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    main()