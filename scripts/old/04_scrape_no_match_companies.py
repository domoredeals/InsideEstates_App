#!/usr/bin/env python3
"""
Script 04: Scrape all No_Match companies from Companies House website.
This single script handles the entire scraping process.
"""

import psycopg2
import os
import time
import requests
import bz2
from datetime import datetime
from dotenv import load_dotenv
import logging
from lxml import html
import random

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('04_scrape_no_match.log'),
        logging.StreamHandler()
    ]
)

def get_connection():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

def process_batch(companies_batch, session):
    """Process a batch of companies."""
    conn = get_connection()
    cur = conn.cursor()
    
    processed = 0
    for queue_id, company_name in companies_batch:
        try:
            # Search Companies House
            search_url = f"https://find-and-update.company-information.service.gov.uk/search/companies?q={requests.utils.quote(company_name)}"
            resp = session.get(search_url, timeout=15)
            
            if resp.status_code == 200:
                tree = html.fromstring(resp.text)
                # Get first result
                links = tree.xpath("//a[contains(@href, '/company/')]/@href")
                
                if links:
                    company_url = "https://find-and-update.company-information.service.gov.uk" + links[0]
                    company_number = links[0].split('/')[-1]
                    
                    # Update queue
                    cur.execute("""
                        UPDATE ch_scrape_queue 
                        SET company_number = %s, company_url = %s, search_status = 'found', 
                            search_timestamp = NOW()
                        WHERE id = %s
                    """, (company_number, company_url, queue_id))
                    
                    # Scrape company page
                    time.sleep(random.uniform(0.5, 1.5))
                    company_resp = session.get(company_url, timeout=15)
                    
                    if company_resp.status_code == 200:
                        # Store in ch_scrape_overview
                        compressed_html = bz2.compress(company_resp.content)
                        
                        # Parse key fields
                        company_tree = html.fromstring(company_resp.text)
                        
                        # Extract data
                        status = company_tree.xpath("//dd[@id='company-status']/text()")
                        status = status[0].strip() if status else None
                        
                        inc_date = company_tree.xpath("//dd[@id='company-incorporation-date']/text()")
                        if inc_date:
                            try:
                                inc_date = datetime.strptime(inc_date[0].strip(), '%d %B %Y').date()
                            except:
                                inc_date = None
                        else:
                            inc_date = None
                        
                        company_type = company_tree.xpath("//dd[@id='company-type']/text()")
                        company_type = company_type[0].strip() if company_type else None
                        
                        address_parts = company_tree.xpath("//dd[@id='reg-address']//text()")
                        address = ', '.join(p.strip() for p in address_parts if p.strip())
                        
                        # Get previous names
                        prev_names = company_tree.xpath("//div[@id='previousNameList']//li/text()")
                        prev_names = [n.strip() for n in prev_names if n.strip()]
                        
                        cur.execute("""
                            INSERT INTO ch_scrape_overview (
                                company_number, company_url, raw_html, company_name,
                                company_status, incorporation_date, company_type,
                                registered_office_address, previous_names,
                                scrape_status, scrape_timestamp, parse_timestamp
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'parsed', NOW(), NOW())
                            ON CONFLICT (company_number) DO UPDATE SET
                                raw_html = EXCLUDED.raw_html,
                                company_name = EXCLUDED.company_name,
                                company_status = EXCLUDED.company_status,
                                incorporation_date = EXCLUDED.incorporation_date,
                                company_type = EXCLUDED.company_type,
                                registered_office_address = EXCLUDED.registered_office_address,
                                previous_names = EXCLUDED.previous_names,
                                scrape_status = 'parsed',
                                scrape_timestamp = NOW(),
                                parse_timestamp = NOW()
                        """, (company_number, company_url, compressed_html, company_name,
                              status, inc_date, company_type, address, prev_names))
                        
                        processed += 1
                else:
                    cur.execute("""
                        UPDATE ch_scrape_queue 
                        SET search_status = 'not_found', search_timestamp = NOW()
                        WHERE id = %s
                    """, (queue_id,))
            
            conn.commit()
            
            # Be respectful - shorter delay within batch
            time.sleep(random.uniform(0.2, 0.5))
                
        except Exception as e:
            logging.error(f"Error processing {company_name}: {e}")
            cur.execute("""
                UPDATE ch_scrape_queue 
                SET search_status = 'error', search_error = %s, search_timestamp = NOW()
                WHERE id = %s
            """, (str(e), queue_id))
            conn.commit()
    
    cur.close()
    conn.close()
    return processed

def main():
    """Main function - scrape all No_Match companies in batches of 1000."""
    conn = get_connection()
    cur = conn.cursor()
    
    # Tables already exist, skip creation
    
    # Get total count
    cur.execute("""
        SELECT COUNT(*) 
        FROM ch_scrape_queue 
        WHERE search_status = 'pending'
    """)
    total_pending = cur.fetchone()[0]
    logging.info(f"Total pending companies to scrape: {total_pending}")
    
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0'})
    
    batch_size = 1000
    total_processed = 0
    
    # Process in batches of 1000
    while True:
        # Get next batch
        cur.execute("""
            SELECT id, search_name
            FROM ch_scrape_queue
            WHERE search_status = 'pending'
            ORDER BY id
            LIMIT %s
        """, (batch_size,))
        
        batch = cur.fetchall()
        if not batch:
            break
        
        logging.info(f"Processing batch of {len(batch)} companies...")
        processed = process_batch(batch, session)
        total_processed += processed
        
        logging.info(f"Batch complete. Processed {processed} companies. Total: {total_processed}/{total_pending}")
        
        # Longer delay between batches to be respectful
        time.sleep(random.uniform(5, 10))
    
    cur.close()
    conn.close()
    logging.info(f"Scraping complete! Total processed: {total_processed}")

if __name__ == "__main__":
    main()