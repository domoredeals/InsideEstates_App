#!/usr/bin/env python3
"""
Add scraped companies from ch_scrape_overview to companies_house_data table.
This ensures all companies (including dissolved ones) are in the main table.
"""

import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv
import logging
import argparse

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('add_scraped_companies.log'),
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

def get_scraped_companies_to_add(conn):
    """Get scraped companies that aren't in companies_house_data."""
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT 
                so.company_number,
                so.company_name,
                so.company_status,
                so.company_type,
                so.incorporation_date,
                so.registered_office_address,
                so.sic_codes,
                so.previous_names,
                so.accounts_next_due,
                so.confirmation_statement_next_due
            FROM ch_scrape_overview so
            WHERE so.scrape_status = 'parsed'
            AND NOT EXISTS (
                SELECT 1 
                FROM companies_house_data ch 
                WHERE ch.company_number = so.company_number
            )
            ORDER BY so.company_number
        """)
        
        return cursor.fetchall()
        
    finally:
        cursor.close()

def format_date_for_ch(date_value):
    """Format date for companies_house_data table (DD/MM/YYYY)."""
    if date_value is None:
        return None
    if isinstance(date_value, str):
        return date_value
    return date_value.strftime('%d/%m/%Y')

def format_sic_codes(sic_codes_array):
    """Format SIC codes array for companies_house_data."""
    if not sic_codes_array:
        return [None, None, None, None]
    
    # Ensure we have exactly 4 SIC codes (pad with None if needed)
    codes = list(sic_codes_array[:4])  # Take first 4 if more
    while len(codes) < 4:
        codes.append(None)
    
    return codes

def format_previous_names(previous_names_array):
    """Format previous names for companies_house_data (up to 10)."""
    if not previous_names_array:
        return [None] * 10
    
    # Take up to 10 previous names
    names = list(previous_names_array[:10])
    while len(names) < 10:
        names.append(None)
    
    # Return as individual values with dates (we don't have dates from scraping)
    result = []
    for name in names:
        result.extend([name, None])  # name, date (no date available)
    
    return result[:20]  # Ensure exactly 20 values (10 names + 10 dates)

def add_companies_to_ch_data(conn, companies, batch_size=100):
    """Add scraped companies to companies_house_data table."""
    cursor = conn.cursor()
    added_count = 0
    
    try:
        for i in range(0, len(companies), batch_size):
            batch = companies[i:i + batch_size]
            
            values = []
            for company in batch:
                # Unpack company data
                (company_number, company_name, company_status, company_type,
                 incorporation_date, registered_office_address, sic_codes,
                 previous_names, accounts_next_due, confirmation_statement_next_due) = company
                
                # Format dates
                incorporation_date_str = format_date_for_ch(incorporation_date)
                accounts_next_due_str = format_date_for_ch(accounts_next_due)
                confirmation_statement_next_due_str = format_date_for_ch(confirmation_statement_next_due)
                
                # Format SIC codes
                sic_1, sic_2, sic_3, sic_4 = format_sic_codes(sic_codes)
                
                # Format previous names (simplified - we don't have all the date fields)
                prev_names = format_previous_names(previous_names)
                
                # Parse address if possible (simplified)
                address_parts = registered_office_address.split(', ') if registered_office_address else []
                
                # Try to extract postcode (last part if it looks like a postcode)
                postcode = None
                if address_parts and len(address_parts[-1]) <= 10 and ' ' in address_parts[-1]:
                    postcode = address_parts[-1]
                    address_parts = address_parts[:-1]
                
                # Build address lines
                address_line_1 = address_parts[0] if len(address_parts) > 0 else registered_office_address
                address_line_2 = address_parts[1] if len(address_parts) > 1 else None
                
                # Prepare values tuple
                value_tuple = (
                    company_name,
                    company_number,
                    None,  # care_of
                    None,  # po_box
                    address_line_1,  # line1
                    address_line_2,  # line2
                    None,  # post_town
                    None,  # county
                    None,  # country
                    postcode,  # postcode
                    company_type,
                    company_status,
                    None,  # country_of_origin
                    None,  # dissolution_date
                    incorporation_date_str,
                    None,  # accounts_account_ref_day
                    None,  # accounts_account_ref_month
                    None,  # accounts_next_due_date
                    None,  # accounts_last_made_up_date
                    'NO',  # accounts_account_category (default)
                    None,  # returns_next_due_date
                    None,  # returns_last_made_up_date
                    None,  # mortgages_num_mort_charges
                    None,  # mortgages_num_mort_outstanding
                    None,  # mortgages_num_mort_part_satisfied
                    None,  # mortgages_num_mort_satisfied
                    sic_1,
                    sic_2,
                    sic_3,
                    sic_4,
                    None,  # limited_partnerships_num_gen_partners
                    None,  # limited_partnerships_num_lim_partners
                    None,  # uri
                    confirmation_statement_next_due_str,  # conf_stmt_next_made_up_date
                    None   # conf_stmt_last_made_up_date
                )
                
                # Add previous names (20 values)
                value_tuple = value_tuple + tuple(prev_names)
                
                values.append(value_tuple)
            
            # Batch insert
            if values:
                # Use executemany for batch insert
                insert_query = """
                    INSERT INTO companies_house_data (
                        company_name, company_number, 
                        reg_address_care_of, reg_address_po_box,
                        reg_address_line1, reg_address_line2,
                        reg_address_post_town, reg_address_county,
                        reg_address_country, reg_address_postcode, 
                        company_category,
                        company_status, country_of_origin, dissolution_date,
                        incorporation_date, accounts_account_ref_day,
                        accounts_account_ref_month, accounts_next_due_date,
                        accounts_last_made_up_date, accounts_account_category,
                        returns_next_due_date, returns_last_made_up_date,
                        mortgages_num_mort_charges, mortgages_num_mort_outstanding,
                        mortgages_num_mort_part_satisfied, mortgages_num_mort_satisfied,
                        sic_code_sic_text_1, sic_code_sic_text_2, sic_code_sic_text_3,
                        sic_code_sic_text_4, limited_partnerships_num_gen_partners,
                        limited_partnerships_num_lim_partners, uri,
                        conf_stmt_next_made_up_date, conf_stmt_last_made_up_date,
                        previous_name_1_condate, previous_name_1_company_name,
                        previous_name_2_condate, previous_name_2_company_name,
                        previous_name_3_condate, previous_name_3_company_name,
                        previous_name_4_condate, previous_name_4_company_name,
                        previous_name_5_condate, previous_name_5_company_name,
                        previous_name_6_condate, previous_name_6_company_name,
                        previous_name_7_condate, previous_name_7_company_name,
                        previous_name_8_condate, previous_name_8_company_name,
                        previous_name_9_condate, previous_name_9_company_name,
                        previous_name_10_condate, previous_name_10_company_name
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (company_number) DO NOTHING
                """
                
                cursor.executemany(insert_query, values)
                added_count += cursor.rowcount
                
                if (i + batch_size) % 1000 == 0:
                    conn.commit()
                    logger.info(f"Progress: {i + batch_size}/{len(companies)} companies processed")
        
        conn.commit()
        return added_count
        
    except Exception as e:
        logger.error(f"Error adding companies: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

def main():
    """Main function to add scraped companies to companies_house_data."""
    parser = argparse.ArgumentParser(description='Add scraped companies to companies_house_data')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be added without making changes')
    args = parser.parse_args()
    
    conn = None
    
    try:
        conn = connect_to_db()
        logger.info("Connected to database successfully.")
        
        # Get companies to add
        companies = get_scraped_companies_to_add(conn)
        logger.info(f"Found {len(companies)} scraped companies not in companies_house_data")
        
        if args.dry_run:
            logger.info("DRY RUN - Would add the following companies:")
            for i, company in enumerate(companies[:10]):
                logger.info(f"  {company[0]}: {company[1]} - Status: {company[2]}")
            if len(companies) > 10:
                logger.info(f"  ... and {len(companies) - 10} more")
        else:
            if companies:
                # Add companies
                added = add_companies_to_ch_data(conn, companies)
                logger.info(f"Successfully added {added} companies to companies_house_data")
                
                # Show summary
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT company_status, COUNT(*) 
                    FROM companies_house_data
                    WHERE company_number IN (
                        SELECT company_number FROM ch_scrape_overview WHERE scrape_status = 'parsed'
                    )
                    GROUP BY company_status
                    ORDER BY COUNT(*) DESC
                """)
                
                print("\nScraped companies by status in companies_house_data:")
                for status, count in cursor.fetchall():
                    print(f"  {status}: {count:,}")
                cursor.close()
            else:
                logger.info("No new companies to add")
        
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