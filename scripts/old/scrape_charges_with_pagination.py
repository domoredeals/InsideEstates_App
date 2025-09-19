#!/usr/bin/env python3
"""
Scrape Companies House charges with pagination support
"""

import os
import sys
import time
import requests
import psycopg2
import bz2
from datetime import datetime
from urllib.parse import urljoin
from lxml import html
from dotenv import load_dotenv
import random
import logging

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

def scrape_company_charges(company_number, company_url):
    """Scrape all charges for a company, handling pagination"""
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # First, delete existing charges for this company to avoid duplicates
        cur.execute("""
            DELETE FROM ch_scrape_charges 
            WHERE company_number = %s
        """, (company_number,))
        
        page = 1
        total_charges = 0
        
        while True:
            # Construct URL with page parameter
            if page == 1:
                url = f"{company_url}/charges"
            else:
                url = f"{company_url}/charges?page={page}"
            
            logging.info(f"Scraping charges page {page} for {company_number}: {url}")
            
            # Random delay to be respectful
            time.sleep(random.uniform(1, 3))
            
            response = session.get(url, timeout=15)
            response.raise_for_status()
            
            # Store the full page HTML
            compressed_html = bz2.compress(response.content)
            
            cur.execute("""
                INSERT INTO ch_scrape_charges (company_number, charge_id, raw_html, scrape_status, scrape_timestamp)
                VALUES (%s, %s, %s, 'scraped', NOW())
            """, (company_number, f"page_{page}", compressed_html))
            
            # Parse to check for more pages
            tree = html.fromstring(response.text)
            
            # Count charges on this page
            charge_elements = tree.xpath("//li[contains(@id, 'charge-')]")
            page_charges = len(charge_elements)
            total_charges += page_charges
            
            logging.info(f"Found {page_charges} charges on page {page} (total so far: {total_charges})")
            
            # Check if there's a next page - look for Next link
            next_page_links = tree.xpath("//a[contains(text(), 'Next') and contains(@href, 'page=')]")
            
            if not next_page_links or page_charges == 0:
                # No more pages
                break
            
            page += 1
        
        conn.commit()
        logging.info(f"Successfully scraped {total_charges} charges across {page} pages for {company_number}")
        
        return total_charges, page
        
    except Exception as e:
        logging.error(f"Error scraping charges for {company_number}: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape Companies House charges with pagination')
    parser.add_argument('company_number', help='Company number to scrape charges for')
    parser.add_argument('--url', help='Company URL (will be looked up if not provided)')
    
    args = parser.parse_args()
    
    if not args.url:
        # Look up the URL from the database
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT company_url 
            FROM ch_scrape_overview 
            WHERE company_number = %s
        """, (args.company_number,))
        result = cur.fetchone()
        cur.close()
        conn.close()
        
        if not result:
            print(f"Company {args.company_number} not found in database")
            return
        
        company_url = result[0]
    else:
        company_url = args.url
    
    total_charges, pages = scrape_company_charges(args.company_number, company_url)
    print(f"\nScraped {total_charges} charges across {pages} pages for company {args.company_number}")

if __name__ == '__main__':
    main()