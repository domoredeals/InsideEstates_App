#!/usr/bin/env python3
"""
Import Companies House Basic Company Data into PostgreSQL
"""

import os
import sys
import csv
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import logging
from tqdm import tqdm
import argparse
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('companies_house_import.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database connection parameters from environment
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'insideestates_app'),
    'user': os.getenv('DB_USER', 'insideestates_user'),
    'password': os.getenv('DB_PASSWORD', 'InsideEstates2024!')
}

def create_companies_house_table(conn):
    """Create the companies_house_data table"""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS companies_house_data (
                -- Primary identifiers
                company_number VARCHAR(20) PRIMARY KEY,
                company_name TEXT,
                
                -- Registered address fields
                reg_address_care_of TEXT,
                reg_address_po_box TEXT,
                reg_address_line1 TEXT,
                reg_address_line2 TEXT,
                reg_address_post_town TEXT,
                reg_address_county TEXT,
                reg_address_country TEXT,
                reg_address_postcode VARCHAR(20),
                
                -- Company details
                company_category VARCHAR(100),
                company_status VARCHAR(100),
                country_of_origin VARCHAR(100),
                dissolution_date DATE,
                incorporation_date DATE,
                
                -- Accounts information
                accounts_ref_day INTEGER,
                accounts_ref_month INTEGER,
                accounts_next_due_date DATE,
                accounts_last_made_up_date DATE,
                accounts_category VARCHAR(100),
                
                -- Returns information
                returns_next_due_date DATE,
                returns_last_made_up_date DATE,
                
                -- Mortgages information
                mortgages_num_charges INTEGER,
                mortgages_num_outstanding INTEGER,
                mortgages_num_part_satisfied INTEGER,
                mortgages_num_satisfied INTEGER,
                
                -- SIC codes
                sic_code_1 TEXT,
                sic_code_2 TEXT,
                sic_code_3 TEXT,
                sic_code_4 TEXT,
                
                -- Limited partnerships
                limited_partnerships_num_gen_partners INTEGER,
                limited_partnerships_num_lim_partners INTEGER,
                
                -- URI
                uri TEXT,
                
                -- Previous names (up to 10)
                previous_name_1_date DATE,
                previous_name_1_name TEXT,
                previous_name_2_date DATE,
                previous_name_2_name TEXT,
                previous_name_3_date DATE,
                previous_name_3_name TEXT,
                previous_name_4_date DATE,
                previous_name_4_name TEXT,
                previous_name_5_date DATE,
                previous_name_5_name TEXT,
                previous_name_6_date DATE,
                previous_name_6_name TEXT,
                previous_name_7_date DATE,
                previous_name_7_name TEXT,
                previous_name_8_date DATE,
                previous_name_8_name TEXT,
                previous_name_9_date DATE,
                previous_name_9_name TEXT,
                previous_name_10_date DATE,
                previous_name_10_name TEXT,
                
                -- Confirmation statement
                conf_stmt_next_due_date DATE,
                conf_stmt_last_made_up_date DATE,
                
                -- Metadata
                import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                file_date DATE
            );
        """)
        
        # Create indexes for common queries
        logger.info("Creating indexes...")
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_ch_company_name ON companies_house_data(company_name);",
            "CREATE INDEX IF NOT EXISTS idx_ch_postcode ON companies_house_data(reg_address_postcode);",
            "CREATE INDEX IF NOT EXISTS idx_ch_status ON companies_house_data(company_status);",
            "CREATE INDEX IF NOT EXISTS idx_ch_incorporation_date ON companies_house_data(incorporation_date);",
            "CREATE INDEX IF NOT EXISTS idx_ch_sic_codes ON companies_house_data(sic_code_1, sic_code_2, sic_code_3, sic_code_4);",
        ]
        
        for index in indexes:
            cur.execute(index)
        
        conn.commit()
        logger.info("Table and indexes created successfully")

def parse_date(date_str):
    """Parse date string in DD/MM/YYYY format"""
    if not date_str or date_str.strip() == '':
        return None
    try:
        return datetime.strptime(date_str.strip(), '%d/%m/%Y').date()
    except ValueError:
        return None

def parse_int(value):
    """Parse integer value"""
    if not value or value.strip() == '':
        return None
    try:
        return int(value.strip())
    except ValueError:
        return None

def clean_dict_keys(d):
    """Clean dictionary keys by stripping whitespace"""
    return {k.strip(): v for k, v in d.items()}

def process_row(row):
    """Process a single CSV row into database format"""
    # Clean keys first
    row = clean_dict_keys(row)
    
    return (
        row.get('CompanyNumber', '').strip(),
        row.get('CompanyName', '').strip(),
        row.get('RegAddress.CareOf', '').strip(),
        row.get('RegAddress.POBox', '').strip(),
        row.get('RegAddress.AddressLine1', '').strip(),
        row.get('RegAddress.AddressLine2', '').strip(),
        row.get('RegAddress.PostTown', '').strip(),
        row.get('RegAddress.County', '').strip(),
        row.get('RegAddress.Country', '').strip(),
        row.get('RegAddress.PostCode', '').strip(),
        row.get('CompanyCategory', '').strip(),
        row.get('CompanyStatus', '').strip(),
        row.get('CountryOfOrigin', '').strip(),
        parse_date(row.get('DissolutionDate', '')),
        parse_date(row.get('IncorporationDate', '')),
        parse_int(row.get('Accounts.AccountRefDay', '')),
        parse_int(row.get('Accounts.AccountRefMonth', '')),
        parse_date(row.get('Accounts.NextDueDate', '')),
        parse_date(row.get('Accounts.LastMadeUpDate', '')),
        row.get('Accounts.AccountCategory', '').strip(),
        parse_date(row.get('Returns.NextDueDate', '')),
        parse_date(row.get('Returns.LastMadeUpDate', '')),
        parse_int(row.get('Mortgages.NumMortCharges', '')),
        parse_int(row.get('Mortgages.NumMortOutstanding', '')),
        parse_int(row.get('Mortgages.NumMortPartSatisfied', '')),
        parse_int(row.get('Mortgages.NumMortSatisfied', '')),
        row.get('SICCode.SicText_1', '').strip(),
        row.get('SICCode.SicText_2', '').strip(),
        row.get('SICCode.SicText_3', '').strip(),
        row.get('SICCode.SicText_4', '').strip(),
        parse_int(row.get('LimitedPartnerships.NumGenPartners', '')),
        parse_int(row.get('LimitedPartnerships.NumLimPartners', '')),
        row.get('URI', '').strip(),
        parse_date(row.get('PreviousName_1.CONDATE', '')),
        row.get('PreviousName_1.CompanyName', '').strip(),
        parse_date(row.get('PreviousName_2.CONDATE', '')),
        row.get('PreviousName_2.CompanyName', '').strip(),
        parse_date(row.get('PreviousName_3.CONDATE', '')),
        row.get('PreviousName_3.CompanyName', '').strip(),
        parse_date(row.get('PreviousName_4.CONDATE', '')),
        row.get('PreviousName_4.CompanyName', '').strip(),
        parse_date(row.get('PreviousName_5.CONDATE', '')),
        row.get('PreviousName_5.CompanyName', '').strip(),
        parse_date(row.get('PreviousName_6.CONDATE', '')),
        row.get('PreviousName_6.CompanyName', '').strip(),
        parse_date(row.get('PreviousName_7.CONDATE', '')),
        row.get('PreviousName_7.CompanyName', '').strip(),
        parse_date(row.get('PreviousName_8.CONDATE', '')),
        row.get('PreviousName_8.CompanyName', '').strip(),
        parse_date(row.get('PreviousName_9.CONDATE', '')),
        row.get('PreviousName_9.CompanyName', '').strip(),
        parse_date(row.get('PreviousName_10.CONDATE', '')),
        row.get('PreviousName_10.CompanyName', '').strip(),
        parse_date(row.get('ConfStmtNextDueDate', '')),
        parse_date(row.get('ConfStmtLastMadeUpDate', ''))
    )

def import_companies_house_data(filepath, conn, batch_size=5000, file_date=None):
    """Import Companies House data from CSV file"""
    filename = os.path.basename(filepath)
    logger.info(f"Starting import of {filename}")
    
    # Extract file date from filename if not provided
    if not file_date:
        try:
            # Extract date from filename format: BasicCompanyDataAsOneFile-YYYY-MM-DD.csv
            date_part = filename.split('-', 1)[1].replace('.csv', '')
            file_date = datetime.strptime(date_part, '%Y-%m-%d').date()
        except:
            file_date = datetime.now().date()
    
    insert_query = """
        INSERT INTO companies_house_data (
            company_number, company_name, 
            reg_address_care_of, reg_address_po_box, reg_address_line1, reg_address_line2,
            reg_address_post_town, reg_address_county, reg_address_country, reg_address_postcode,
            company_category, company_status, country_of_origin, dissolution_date, incorporation_date,
            accounts_ref_day, accounts_ref_month, accounts_next_due_date, accounts_last_made_up_date, accounts_category,
            returns_next_due_date, returns_last_made_up_date,
            mortgages_num_charges, mortgages_num_outstanding, mortgages_num_part_satisfied, mortgages_num_satisfied,
            sic_code_1, sic_code_2, sic_code_3, sic_code_4,
            limited_partnerships_num_gen_partners, limited_partnerships_num_lim_partners,
            uri,
            previous_name_1_date, previous_name_1_name,
            previous_name_2_date, previous_name_2_name,
            previous_name_3_date, previous_name_3_name,
            previous_name_4_date, previous_name_4_name,
            previous_name_5_date, previous_name_5_name,
            previous_name_6_date, previous_name_6_name,
            previous_name_7_date, previous_name_7_name,
            previous_name_8_date, previous_name_8_name,
            previous_name_9_date, previous_name_9_name,
            previous_name_10_date, previous_name_10_name,
            conf_stmt_next_due_date, conf_stmt_last_made_up_date,
            file_date
        ) VALUES %s
        ON CONFLICT (company_number) DO UPDATE SET
            company_name = EXCLUDED.company_name,
            reg_address_care_of = EXCLUDED.reg_address_care_of,
            reg_address_po_box = EXCLUDED.reg_address_po_box,
            reg_address_line1 = EXCLUDED.reg_address_line1,
            reg_address_line2 = EXCLUDED.reg_address_line2,
            reg_address_post_town = EXCLUDED.reg_address_post_town,
            reg_address_county = EXCLUDED.reg_address_county,
            reg_address_country = EXCLUDED.reg_address_country,
            reg_address_postcode = EXCLUDED.reg_address_postcode,
            company_category = EXCLUDED.company_category,
            company_status = EXCLUDED.company_status,
            country_of_origin = EXCLUDED.country_of_origin,
            dissolution_date = EXCLUDED.dissolution_date,
            incorporation_date = EXCLUDED.incorporation_date,
            accounts_ref_day = EXCLUDED.accounts_ref_day,
            accounts_ref_month = EXCLUDED.accounts_ref_month,
            accounts_next_due_date = EXCLUDED.accounts_next_due_date,
            accounts_last_made_up_date = EXCLUDED.accounts_last_made_up_date,
            accounts_category = EXCLUDED.accounts_category,
            returns_next_due_date = EXCLUDED.returns_next_due_date,
            returns_last_made_up_date = EXCLUDED.returns_last_made_up_date,
            mortgages_num_charges = EXCLUDED.mortgages_num_charges,
            mortgages_num_outstanding = EXCLUDED.mortgages_num_outstanding,
            mortgages_num_part_satisfied = EXCLUDED.mortgages_num_part_satisfied,
            mortgages_num_satisfied = EXCLUDED.mortgages_num_satisfied,
            sic_code_1 = EXCLUDED.sic_code_1,
            sic_code_2 = EXCLUDED.sic_code_2,
            sic_code_3 = EXCLUDED.sic_code_3,
            sic_code_4 = EXCLUDED.sic_code_4,
            limited_partnerships_num_gen_partners = EXCLUDED.limited_partnerships_num_gen_partners,
            limited_partnerships_num_lim_partners = EXCLUDED.limited_partnerships_num_lim_partners,
            uri = EXCLUDED.uri,
            previous_name_1_date = EXCLUDED.previous_name_1_date,
            previous_name_1_name = EXCLUDED.previous_name_1_name,
            previous_name_2_date = EXCLUDED.previous_name_2_date,
            previous_name_2_name = EXCLUDED.previous_name_2_name,
            previous_name_3_date = EXCLUDED.previous_name_3_date,
            previous_name_3_name = EXCLUDED.previous_name_3_name,
            previous_name_4_date = EXCLUDED.previous_name_4_date,
            previous_name_4_name = EXCLUDED.previous_name_4_name,
            previous_name_5_date = EXCLUDED.previous_name_5_date,
            previous_name_5_name = EXCLUDED.previous_name_5_name,
            previous_name_6_date = EXCLUDED.previous_name_6_date,
            previous_name_6_name = EXCLUDED.previous_name_6_name,
            previous_name_7_date = EXCLUDED.previous_name_7_date,
            previous_name_7_name = EXCLUDED.previous_name_7_name,
            previous_name_8_date = EXCLUDED.previous_name_8_date,
            previous_name_8_name = EXCLUDED.previous_name_8_name,
            previous_name_9_date = EXCLUDED.previous_name_9_date,
            previous_name_9_name = EXCLUDED.previous_name_9_name,
            previous_name_10_date = EXCLUDED.previous_name_10_date,
            previous_name_10_name = EXCLUDED.previous_name_10_name,
            conf_stmt_next_due_date = EXCLUDED.conf_stmt_next_due_date,
            conf_stmt_last_made_up_date = EXCLUDED.conf_stmt_last_made_up_date,
            file_date = EXCLUDED.file_date,
            import_date = CURRENT_TIMESTAMP
    """
    
    # Count total rows
    total_rows = sum(1 for line in open(filepath, 'r', encoding='utf-8')) - 1
    logger.info(f"Total rows to import: {total_rows:,}")
    
    batch_data = []
    rows_imported = 0
    errors = 0
    
    with open(filepath, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        
        with tqdm(total=total_rows, desc="Importing companies") as pbar:
            for row in reader:
                try:
                    processed_row = process_row(row)
                    # Add file_date to the row
                    processed_row = processed_row + (file_date,)
                    batch_data.append(processed_row)
                    
                    if len(batch_data) >= batch_size:
                        with conn.cursor() as cur:
                            execute_values(
                                cur,
                                insert_query,
                                batch_data,
                                template=None,
                                page_size=batch_size
                            )
                        conn.commit()
                        rows_imported += len(batch_data)
                        batch_data = []
                    
                    pbar.update(1)
                    
                except Exception as e:
                    errors += 1
                    if errors <= 10:
                        # Clean keys first to get company number
                        cleaned_row = clean_dict_keys(row)
                        company_num = cleaned_row.get('CompanyNumber', 'Unknown')
                        logger.error(f"Error processing row {company_num}: {e}")
                    elif errors == 11:
                        logger.error("Suppressing further error messages...")
            
            # Import any remaining records
            if batch_data:
                with conn.cursor() as cur:
                    execute_values(
                        cur,
                        insert_query,
                        batch_data,
                        template=None,
                        page_size=len(batch_data)
                    )
                conn.commit()
                rows_imported += len(batch_data)
    
    logger.info(f"Import completed. Rows imported: {rows_imported:,}, Errors: {errors:,}")
    
    # Analyze table for query optimization
    with conn.cursor() as cur:
        cur.execute("ANALYZE companies_house_data;")
    conn.commit()

def main():
    parser = argparse.ArgumentParser(description='Import Companies House data to PostgreSQL')
    parser.add_argument('--create-table', action='store_true', help='Create the table and exit')
    parser.add_argument('--file', type=str, help='Path to Companies House CSV file')
    parser.add_argument('--batch-size', type=int, default=5000, help='Batch size for imports (default: 5000)')
    
    args = parser.parse_args()
    
    # Connect to database
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Connected to database successfully")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        sys.exit(1)
    
    try:
        if args.create_table:
            create_companies_house_table(conn)
            logger.info("Table created successfully")
            return
        
        if args.file:
            if not os.path.exists(args.file):
                logger.error(f"File not found: {args.file}")
                sys.exit(1)
            
            import_companies_house_data(args.file, conn, batch_size=args.batch_size)
        else:
            # Default: look for Companies House files in the standard directory
            ch_dir = Path('/home/adc/Projects/InsideEstates_App/DATA/SOURCE/CH')
            files = list(ch_dir.glob('BasicCompanyDataAsOneFile-*.csv'))
            
            if not files:
                logger.error("No Companies House files found in DATA/SOURCE/CH/")
                sys.exit(1)
            
            for file in sorted(files):
                logger.info(f"Found file: {file.name}")
                import_companies_house_data(str(file), conn, batch_size=args.batch_size)
    
    finally:
        conn.close()
        logger.info("Database connection closed")

if __name__ == '__main__':
    main()