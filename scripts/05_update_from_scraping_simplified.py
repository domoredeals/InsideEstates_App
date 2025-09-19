#!/usr/bin/env python3
"""
Update Companies House Data from Scraped Results
===============================================
Updates the companies_house_data table with enriched data from scraping,
then improves NO_MATCH records in land_registry_ch_matches

Key Features:
- Adds scraped data columns to companies_house_data if needed
- Updates existing CH records with full overview data
- Inserts new companies found during scraping
- Attempts to match NO_MATCH records using the enriched data
- Provides detailed statistics and progress tracking

Created: 2025-09-19
"""

import psycopg2
from psycopg2.extras import execute_values
import csv
import sys
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
import logging
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('05_update_from_scraping_simplified.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

def normalize_company_name(name):
    """PROVEN normalization that REMOVES suffixes - must match 03_match script"""
    if not name or name.strip() == '':
        return ""
    
    name = str(name).upper().strip()
    name = name.replace(' AND ', ' ').replace(' & ', ' ')
    
    # CRITICAL FIX: Remove suffixes AND anything after them
    suffix_pattern = r'\s*(LIMITED LIABILITY PARTNERSHIP|LIMITED|COMPANY|LTD\.|LLP|LTD|PLC|CO\.|CO).*$'
    name = re.sub(suffix_pattern, '', name)
    
    # Keep only alphanumeric
    name = ''.join(char for char in name if char.isalnum())
    
    return name

def normalize_company_number(number):
    """Normalize company registration numbers"""
    if not number or str(number).strip() == '':
        return ''
    
    number = str(number).strip().upper()
    number = re.sub(r'[^A-Z0-9]', '', number)
    
    # Handle Scottish numbers
    if number.startswith('SC'):
        return number
    
    # Pad regular numbers to 8 digits
    if number.isdigit():
        return number.zfill(8)
    
    return number

def parse_date(date_str):
    """Parse various date formats from scraped data"""
    if not date_str or date_str.strip() == '':
        return None
    
    date_str = date_str.strip()
    
    # Try common formats
    formats = [
        '%d %B %Y',      # 1 January 2020
        '%d %b %Y',      # 1 Jan 2020
        '%d/%m/%Y',      # 01/01/2020
        '%Y-%m-%d',      # 2020-01-01
        '%d-%m-%Y',      # 01-01-2020
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except:
            continue
    
    logger.warning(f"Could not parse date: {date_str}")
    return None

def ensure_scraped_columns(cursor):
    """Add scraped data columns to companies_house_data if they don't exist"""
    print("Checking/adding scraped data columns...")
    
    columns_to_add = [
        ("scraped_data", "BOOLEAN DEFAULT FALSE"),
        ("scraped_company_type", "VARCHAR(255)"),
        ("scraped_incorporation_date", "DATE"),
        ("scraped_company_status", "VARCHAR(100)"),
        ("scraped_registered_address", "TEXT"),
        ("scraped_sic_codes", "TEXT"),
        ("scraped_previous_names", "TEXT"),
        ("scraped_accounts_due", "VARCHAR(100)"),
        ("scraped_confirmation_due", "VARCHAR(100)"),
        ("last_scraped_date", "TIMESTAMP"),
        ("scraped_search_name", "VARCHAR(500)")
    ]
    
    for column_name, column_def in columns_to_add:
        try:
            cursor.execute(f"""
                ALTER TABLE companies_house_data 
                ADD COLUMN IF NOT EXISTS {column_name} {column_def}
            """)
            logger.info(f"Added/verified column: {column_name}")
        except Exception as e:
            logger.warning(f"Column {column_name} might already exist: {e}")
    
    print("✅ Scraped data columns ready\n")

def update_companies_house_data(cursor, scraped_data):
    """Update or insert Companies House data with scraped information"""
    company_number = normalize_company_number(scraped_data.get('company_number', ''))
    if not company_number:
        return False
    
    try:
        # Check if company exists
        cursor.execute("""
            SELECT company_number FROM companies_house_data 
            WHERE company_number = %s
        """, (company_number,))
        
        exists = cursor.fetchone() is not None
        
        if exists:
            # Update existing record
            cursor.execute("""
                UPDATE companies_house_data
                SET 
                    scraped_data = TRUE,
                    scraped_company_type = %s,
                    scraped_incorporation_date = %s,
                    scraped_company_status = %s,
                    scraped_registered_address = %s,
                    scraped_sic_codes = %s,
                    scraped_previous_names = %s,
                    scraped_accounts_due = %s,
                    scraped_confirmation_due = %s,
                    last_scraped_date = CURRENT_TIMESTAMP,
                    scraped_search_name = %s
                WHERE company_number = %s
            """, (
                scraped_data.get('company_type', ''),
                parse_date(scraped_data.get('incorporated_on', '')),
                scraped_data.get('company_status', ''),
                scraped_data.get('registered_office_address', ''),
                scraped_data.get('sic_codes', ''),
                scraped_data.get('previous_names', ''),
                scraped_data.get('accounts_next_due', ''),
                scraped_data.get('confirmation_statement_next_due', ''),
                scraped_data.get('search_name', ''),
                company_number
            ))
            return 'updated'
        else:
            # Insert new record
            cursor.execute("""
                INSERT INTO companies_house_data (
                    company_name,
                    company_number,
                    company_status,
                    incorporation_date,
                    scraped_data,
                    scraped_company_type,
                    scraped_incorporation_date,
                    scraped_company_status,
                    scraped_registered_address,
                    scraped_sic_codes,
                    scraped_previous_names,
                    scraped_accounts_due,
                    scraped_confirmation_due,
                    last_scraped_date,
                    scraped_search_name
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                scraped_data.get('found_name', ''),
                company_number,
                scraped_data.get('company_status', ''),
                parse_date(scraped_data.get('incorporated_on', '')),
                True,
                scraped_data.get('company_type', ''),
                parse_date(scraped_data.get('incorporated_on', '')),
                scraped_data.get('company_status', ''),
                scraped_data.get('registered_office_address', ''),
                scraped_data.get('sic_codes', ''),
                scraped_data.get('previous_names', ''),
                scraped_data.get('accounts_next_due', ''),
                scraped_data.get('confirmation_statement_next_due', ''),
                datetime.now(),
                scraped_data.get('search_name', '')
            ))
            return 'inserted'
            
    except Exception as e:
        logger.error(f"Error updating/inserting company {company_number}: {e}")
        return False

def attempt_no_match_recovery(cursor):
    """Try to match NO_MATCH records using the enriched scraped data"""
    print("\nAttempting to match NO_MATCH records with scraped data...")
    
    # First check if the table exists and has the expected columns
    cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'land_registry_ch_matches'
        LIMIT 5
    """)
    columns = [row[0] for row in cursor.fetchall()]
    
    if not columns:
        print("land_registry_ch_matches table not found - skipping NO_MATCH recovery")
        return 0
    
    print(f"Found land_registry_ch_matches with columns: {columns[:5]}...")
    
    # For now, just return 0 - we'll implement the actual matching later
    print("NO_MATCH recovery not implemented yet - would need to understand table structure")
    return 0

def main():
    """Main function to process scraped data"""
    print("=== Update Companies House Data from Scraped Results ===")
    print(f"Started at: {datetime.now()}\n")
    
    # Get CSV file path
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    else:
        # Default to the merged file if it exists
        default_path = "/home/adc/Projects/InsideEstates_App/DATA/SOURCE/CH/companies_house_overview_results_merged_20250919_221948.csv"
        if Path(default_path).exists():
            csv_file = default_path
            print(f"Using merged results file: {csv_file}")
        else:
            csv_file = input("Enter path to scraped data CSV file: ").strip()
    
    if not Path(csv_file).exists():
        # Try Windows Desktop path
        csv_file = f"/mnt/c/Users/adcus/OneDrive/Desktop/{Path(csv_file).name}"
        if not Path(csv_file).exists():
            print(f"ERROR: File not found!")
            sys.exit(1)
    
    # Connect to database
    conn = psycopg2.connect(**POSTGRESQL_CONFIG)
    cursor = conn.cursor()
    
    # Ensure scraped columns exist
    ensure_scraped_columns(cursor)
    conn.commit()
    
    # Read CSV and process
    print(f"Reading {csv_file}...")
    
    total_rows = 0
    found_count = 0
    not_found_count = 0
    error_count = 0
    companies_updated = 0
    companies_inserted = 0
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        total_rows = len(rows)
    
    print(f"Found {total_rows:,} rows in CSV file\n")
    
    with tqdm(total=total_rows, desc="Processing scraped data") as pbar:
        for row in rows:
            status = row.get('Status', row.get('status', '')).upper()
            
            if status == 'FOUND':
                found_count += 1
                
                # Prepare scraped data
                scraped_data = {
                    'search_name': row.get('Search Name', ''),
                    'found_name': row.get('Found Name', ''),
                    'company_number': row.get('Company Number', ''),
                    'company_type': row.get('Company Type', ''),
                    'incorporated_on': row.get('Incorporated On', ''),
                    'company_status': row.get('Company Status', ''),
                    'registered_office_address': row.get('Registered Office Address', ''),
                    'sic_codes': row.get('SIC Codes', ''),
                    'previous_names': row.get('Previous Names', ''),
                    'accounts_next_due': row.get('Accounts Next Due', ''),
                    'confirmation_statement_next_due': row.get('Confirmation Statement Next Due', '')
                }
                
                # Update companies_house_data table
                result = update_companies_house_data(cursor, scraped_data)
                if result == 'updated':
                    companies_updated += 1
                elif result == 'inserted':
                    companies_inserted += 1
                    
            elif status == 'NOT_FOUND':
                not_found_count += 1
            else:
                error_count += 1
            
            pbar.update(1)
            
            # Commit every 1000 rows
            if pbar.n % 1000 == 0:
                conn.commit()
    
    # Final commit
    conn.commit()
    
    # Attempt to match NO_MATCH records
    matched_no_match = attempt_no_match_recovery(cursor)
    
    # Print statistics
    print(f"\n{'='*60}")
    print("PROCESSING COMPLETE!")
    print(f"{'='*60}")
    print(f"Total rows processed: {total_rows:,}")
    print(f"Found companies: {found_count:,}")
    print(f"Not found: {not_found_count:,}")
    print(f"Errors: {error_count:,}")
    print(f"\nDatabase Updates:")
    print(f"Companies updated: {companies_updated:,}")
    print(f"New companies added: {companies_inserted:,}")
    print(f"NO_MATCH records potentially fixed: {matched_no_match:,}")
    
    # Show sample of updated companies
    print("\nSample of scraped companies:")
    cursor.execute("""
        SELECT company_number, company_name, scraped_company_status, scraped_search_name
        FROM companies_house_data
        WHERE scraped_data = TRUE
        ORDER BY last_scraped_date DESC
        LIMIT 5
    """)
    
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]} ({row[2]}) - searched as: {row[3]}")
    
    cursor.close()
    conn.close()
    
    print(f"\nCompleted at: {datetime.now()}")
    print("\nNext step: Run the matching script again to use this enriched data!")

if __name__ == '__main__':
    main()