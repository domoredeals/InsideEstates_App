#!/usr/bin/env python3
"""
Add scraped companies from ch_scrape_overview to companies_house_data table.
Simplified version that only adds essential fields.
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
                so.registered_office_address
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
    """Format date for companies_house_data table."""
    if date_value is None:
        return None
    # Return the date object directly - PostgreSQL will handle it
    return date_value

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
                 incorporation_date, registered_office_address) = company
                
                # Format date
                incorporation_date_str = format_date_for_ch(incorporation_date)
                
                # Parse address to extract postcode
                postcode = None
                address_line1 = registered_office_address
                
                if registered_office_address:
                    # Try to extract postcode (last part if it looks like a postcode)
                    parts = registered_office_address.split(', ')
                    if parts and len(parts[-1]) <= 10 and ' ' in parts[-1]:
                        # Check if last part looks like a postcode
                        last_part = parts[-1].strip()
                        if any(c.isdigit() for c in last_part):
                            postcode = last_part
                            address_line1 = ', '.join(parts[:-1])
                
                # Prepare values tuple - only essential fields
                value_tuple = (
                    company_number,
                    company_name,
                    address_line1,
                    postcode,
                    company_type or 'Limited Company',  # default if not specified
                    company_status,
                    incorporation_date_str
                )
                
                values.append(value_tuple)
            
            # Batch insert with only essential fields
            if values:
                insert_query = """
                    INSERT INTO companies_house_data (
                        company_number,
                        company_name,
                        reg_address_line1,
                        reg_address_postcode,
                        company_category,
                        company_status,
                        incorporation_date
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
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
                
                # Check specific examples
                cursor.execute("""
                    SELECT company_number, company_name, company_status
                    FROM companies_house_data
                    WHERE company_number IN ('01521211', '01834869', '02026504')
                """)
                
                print("\nVerification - these companies are now in companies_house_data:")
                for row in cursor.fetchall():
                    print(f"  {row[0]}: {row[1]} - Status: {row[2]}")
                
                # Summary by status
                cursor.execute("""
                    SELECT company_status, COUNT(*) 
                    FROM companies_house_data
                    WHERE company_number IN (
                        SELECT company_number FROM ch_scrape_overview WHERE scrape_status = 'parsed'
                    )
                    GROUP BY company_status
                    ORDER BY COUNT(*) DESC
                """)
                
                print("\nScraped companies by status:")
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