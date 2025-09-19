#!/usr/bin/env python3
"""
Scrape detailed charge information from Companies House
"""

import os
import sys
import time
import requests
import psycopg2
import bz2
from datetime import datetime
from lxml import html
from dotenv import load_dotenv
import random
import logging
from urllib.parse import urljoin

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

def scrape_charge_details(company_number, limit=None):
    """Scrape detailed information for each charge"""
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get all charges that need detail scraping
        query = """
            SELECT charge_id, charge_link
            FROM ch_scrape_charges
            WHERE company_number = %s
            AND charge_id NOT LIKE 'page_%%'
            AND charge_link IS NULL
            ORDER BY created_date DESC
        """
        if limit:
            query += f" LIMIT {limit}"
        
        cur.execute(query, (company_number,))
        charges = cur.fetchall()
        
        if not charges:
            # Try to get charges that already have links from the overview pages
            cur.execute("""
                SELECT charge_id
                FROM ch_scrape_charges
                WHERE company_number = %s
                AND charge_id NOT LIKE 'page_%%'
                ORDER BY created_date DESC
            """, (company_number,))
            
            charges = [(c[0], None) for c in cur.fetchall()]
            
            if not charges:
                logging.info(f"No charges found for {company_number}")
                return
        
        logging.info(f"Scraping details for {len(charges)} charges of company {company_number}")
        
        for i, (charge_id, charge_link) in enumerate(charges):
            try:
                # Construct the charge detail URL
                if not charge_link:
                    # For charges with proper IDs
                    if len(charge_id) > 20:  # URL-style ID
                        charge_url = f"https://find-and-update.company-information.service.gov.uk/company/{company_number}/charges/{charge_id}"
                    else:
                        # Skip charge codes that don't have URLs
                        logging.warning(f"Skipping charge {charge_id} - no URL available")
                        continue
                else:
                    charge_url = urljoin("https://find-and-update.company-information.service.gov.uk", charge_link)
                
                logging.info(f"Scraping charge {i+1}/{len(charges)}: {charge_url}")
                
                # Random delay to be respectful
                time.sleep(random.uniform(1, 3))
                
                response = session.get(charge_url, timeout=15)
                response.raise_for_status()
                
                # Store the raw HTML
                compressed_html = bz2.compress(response.content)
                
                # Update the charge record with the raw detail HTML
                cur.execute("""
                    UPDATE ch_scrape_charges
                    SET charge_link = %s,
                        raw_html = %s,
                        scrape_status = 'detail_scraped',
                        scrape_timestamp = NOW()
                    WHERE company_number = %s
                    AND charge_id = %s
                """, (charge_url, compressed_html, company_number, charge_id))
                
                conn.commit()
                
                if (i + 1) % 5 == 0:
                    logging.info(f"Progress: {i + 1}/{len(charges)} charge details scraped")
                
            except Exception as e:
                logging.error(f"Error scraping charge {charge_id}: {e}")
                continue
        
        logging.info(f"Completed scraping charge details for {company_number}")
        
    except Exception as e:
        logging.error(f"Error in scrape_charge_details: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape Companies House charge details')
    parser.add_argument('company_number', help='Company number to scrape charges for')
    parser.add_argument('--limit', type=int, help='Limit number of charges to scrape')
    
    args = parser.parse_args()
    
    scrape_charge_details(args.company_number, args.limit)

if __name__ == '__main__':
    main()