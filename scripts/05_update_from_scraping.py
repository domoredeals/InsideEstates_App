#!/usr/bin/env python3
"""
Update Database from Scraped Companies House Data
=================================================
Processes CSV output from the Windows CH scraping script and updates:
1. The companies table with full CH data
2. The land_registry_ch_matches table to improve match quality

Key Features:
- Reads scraped data CSV with full CH overview information
- Updates companies table with real CH data
- Changes match_type from 'Land_Registry' to 'Scraped' with 0.8 confidence
- Handles various CSV formats from the scraper
- Provides detailed progress tracking and statistics

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
        logging.FileHandler('05_update_from_scraping.log'),
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

def parse_sic_codes(sic_str):
    """Parse SIC codes from scraped data"""
    if not sic_str or sic_str.strip() == '':
        return []
    
    # Clean up the string
    sic_str = sic_str.strip()
    
    # Extract just the numeric codes
    codes = []
    # Match patterns like "12345 - Description" or just "12345"
    for match in re.findall(r'\b(\d{5})\b', sic_str):
        codes.append(match)
    
    return codes[:4]  # Max 4 SIC codes

def update_companies_table(cursor, scraped_data):
    """Update the companies table with scraped data"""
    try:
        # Parse SIC codes
        sic_codes = parse_sic_codes(scraped_data.get('sic_codes', ''))
        sic_code_1 = sic_codes[0] if len(sic_codes) > 0 else None
        sic_code_2 = sic_codes[1] if len(sic_codes) > 1 else None
        sic_code_3 = sic_codes[2] if len(sic_codes) > 2 else None
        sic_code_4 = sic_codes[3] if len(sic_codes) > 3 else None
        
        # Parse dates
        incorporation_date = parse_date(scraped_data.get('incorporated_on', ''))
        
        # Determine if company is dissolved
        company_status = scraped_data.get('company_status', '').lower()
        is_dissolved = 'dissolved' in company_status or 'removed' in company_status
        
        # Update companies table
        cursor.execute("""
            UPDATE companies
            SET 
                company_name = %(company_name)s,
                ch_matched = TRUE,
                scraped_data = TRUE,
                company_status = %(company_status)s,
                company_category = %(company_type)s,
                registered_address_line1 = %(registered_office_address)s,
                incorporation_date = %(incorporation_date)s,
                dissolution_date = CASE WHEN %(is_dissolved)s THEN CURRENT_DATE ELSE NULL END,
                sic_code_1 = %(sic_code_1)s,
                sic_code_2 = %(sic_code_2)s,
                sic_code_3 = %(sic_code_3)s,
                sic_code_4 = %(sic_code_4)s,
                last_scrape_check = CURRENT_DATE,
                updated_at = CURRENT_TIMESTAMP
            WHERE company_number = %(company_number)s
        """, {
            'company_number': scraped_data['company_number'],
            'company_name': scraped_data['found_name'],
            'company_status': scraped_data.get('company_status', ''),
            'company_type': scraped_data.get('company_type', ''),
            'registered_office_address': scraped_data.get('registered_office_address', ''),
            'incorporation_date': incorporation_date,
            'is_dissolved': is_dissolved,
            'sic_code_1': sic_code_1,
            'sic_code_2': sic_code_2,
            'sic_code_3': sic_code_3,
            'sic_code_4': sic_code_4
        })
        
        return cursor.rowcount > 0
        
    except Exception as e:
        logger.error(f"Error updating company {scraped_data.get('company_number', 'UNKNOWN')}: {e}")
        return False

def update_match_table(cursor, company_number, found_name):
    """Update land_registry_ch_matches to reflect scraped data"""
    try:
        # Update all matches for this company number from 'Land_Registry' to 'Scraped'
        cursor.execute("""
            UPDATE land_registry_ch_matches
            SET 
                ch_matched_name_1 = CASE 
                    WHEN ch_matched_number_1 = %s AND ch_match_type_1 = 'Land_Registry' 
                    THEN %s 
                    ELSE ch_matched_name_1 
                END,
                ch_match_type_1 = CASE 
                    WHEN ch_matched_number_1 = %s AND ch_match_type_1 = 'Land_Registry' 
                    THEN 'Scraped' 
                    ELSE ch_match_type_1 
                END,
                ch_match_confidence_1 = CASE 
                    WHEN ch_matched_number_1 = %s AND ch_match_type_1 = 'Land_Registry' 
                    THEN 0.8 
                    ELSE ch_match_confidence_1 
                END,
                
                ch_matched_name_2 = CASE 
                    WHEN ch_matched_number_2 = %s AND ch_match_type_2 = 'Land_Registry' 
                    THEN %s 
                    ELSE ch_matched_name_2 
                END,
                ch_match_type_2 = CASE 
                    WHEN ch_matched_number_2 = %s AND ch_match_type_2 = 'Land_Registry' 
                    THEN 'Scraped' 
                    ELSE ch_match_type_2 
                END,
                ch_match_confidence_2 = CASE 
                    WHEN ch_matched_number_2 = %s AND ch_match_type_2 = 'Land_Registry' 
                    THEN 0.8 
                    ELSE ch_match_confidence_2 
                END,
                
                ch_matched_name_3 = CASE 
                    WHEN ch_matched_number_3 = %s AND ch_match_type_3 = 'Land_Registry' 
                    THEN %s 
                    ELSE ch_matched_name_3 
                END,
                ch_match_type_3 = CASE 
                    WHEN ch_matched_number_3 = %s AND ch_match_type_3 = 'Land_Registry' 
                    THEN 'Scraped' 
                    ELSE ch_match_type_3 
                END,
                ch_match_confidence_3 = CASE 
                    WHEN ch_matched_number_3 = %s AND ch_match_type_3 = 'Land_Registry' 
                    THEN 0.8 
                    ELSE ch_match_confidence_3 
                END,
                
                ch_matched_name_4 = CASE 
                    WHEN ch_matched_number_4 = %s AND ch_match_type_4 = 'Land_Registry' 
                    THEN %s 
                    ELSE ch_matched_name_4 
                END,
                ch_match_type_4 = CASE 
                    WHEN ch_matched_number_4 = %s AND ch_match_type_4 = 'Land_Registry' 
                    THEN 'Scraped' 
                    ELSE ch_match_type_4 
                END,
                ch_match_confidence_4 = CASE 
                    WHEN ch_matched_number_4 = %s AND ch_match_type_4 = 'Land_Registry' 
                    THEN 0.8 
                    ELSE ch_match_confidence_4 
                END,
                
                updated_at = CURRENT_TIMESTAMP
            WHERE 
                (ch_matched_number_1 = %s AND ch_match_type_1 = 'Land_Registry') OR
                (ch_matched_number_2 = %s AND ch_match_type_2 = 'Land_Registry') OR
                (ch_matched_number_3 = %s AND ch_match_type_3 = 'Land_Registry') OR
                (ch_matched_number_4 = %s AND ch_match_type_4 = 'Land_Registry')
        """, (company_number, found_name) * 4 + (company_number,) * 4)
        
        return cursor.rowcount
        
    except Exception as e:
        logger.error(f"Error updating matches for {company_number}: {e}")
        return 0

def main():
    """Main function to process scraped data"""
    print("=== Update Database from Scraped Companies House Data ===")
    print(f"Started at: {datetime.now()}\n")
    
    # Get CSV file path
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    else:
        csv_file = input("Enter path to scraped data CSV file: ").strip()
    
    if not Path(csv_file).exists():
        print(f"ERROR: File {csv_file} not found!")
        sys.exit(1)
    
    # Connect to database
    conn = psycopg2.connect(**POSTGRESQL_CONFIG)
    cursor = conn.cursor()
    
    # Read CSV and process
    print(f"Reading {csv_file}...")
    
    total_rows = 0
    found_count = 0
    not_found_count = 0
    error_count = 0
    companies_updated = 0
    matches_updated = 0
    
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
                
                # Extract company number and name
                company_number = row.get('Company Number', row.get('company_number', ''))
                found_name = row.get('Found Name', row.get('found_name', ''))
                
                if company_number:
                    # Prepare scraped data
                    scraped_data = {
                        'company_number': company_number,
                        'found_name': found_name,
                        'company_type': row.get('Company Type', row.get('company_type', '')),
                        'incorporated_on': row.get('Incorporated On', row.get('incorporated_on', '')),
                        'company_status': row.get('Company Status', row.get('company_status', '')),
                        'registered_office_address': row.get('Registered Office Address', row.get('registered_office_address', '')),
                        'sic_codes': row.get('SIC Codes', row.get('sic_codes', '')),
                        'previous_names': row.get('Previous Names', row.get('previous_names', ''))
                    }
                    
                    # Update companies table
                    if update_companies_table(cursor, scraped_data):
                        companies_updated += 1
                    
                    # Update match table
                    matches_count = update_match_table(cursor, company_number, found_name)
                    if matches_count > 0:
                        matches_updated += matches_count
                    
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
    
    # Print statistics
    print(f"\n{'='*60}")
    print("PROCESSING COMPLETE!")
    print(f"{'='*60}")
    print(f"Total rows processed: {total_rows:,}")
    print(f"Found companies: {found_count:,}")
    print(f"Not found: {not_found_count:,}")
    print(f"Errors: {error_count:,}")
    print(f"\nDatabase Updates:")
    print(f"Companies table updated: {companies_updated:,}")
    print(f"Match records updated: {matches_updated:,}")
    
    # Show sample of updated companies
    print("\nSample of updated companies:")
    cursor.execute("""
        SELECT company_number, company_name, company_status, scraped_data
        FROM companies
        WHERE scraped_data = TRUE
        ORDER BY updated_at DESC
        LIMIT 5
    """)
    
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]} ({row[2]})")
    
    cursor.close()
    conn.close()
    
    print(f"\nCompleted at: {datetime.now()}")

if __name__ == '__main__':
    main()