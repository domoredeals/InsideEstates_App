#!/usr/bin/env python3
"""
Scrape Companies House website for company information.
Adapted from the original script to use PostgreSQL and be more respectful to the website.
"""

import os
import sys
import time
import requests
import psycopg2
import bz2
import pickle
from datetime import datetime
from urllib.parse import quote_plus, urljoin
from lxml import html
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import random
import threading
from queue import Queue
import logging

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ch_scraper.log'),
        logging.StreamHandler()
    ]
)

class CompaniesHouseScraper:
    def __init__(self, batch_size=10, delay_min=1, delay_max=3):
        """
        Initialize the scraper
        
        Args:
            batch_size: Number of parallel requests (be respectful!)
            delay_min: Minimum delay between requests in seconds
            delay_max: Maximum delay between requests in seconds
        """
        self.batch_size = batch_size
        self.delay_min = delay_min
        self.delay_max = delay_max
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.lock = threading.Lock()
        self.results_queue = Queue()
        
    def get_db_connection(self):
        """Create database connection"""
        return psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
    
    def search_companies(self, limit=None):
        """
        Search for companies on Companies House website
        
        Args:
            limit: Limit number of companies to search (for testing)
        """
        conn = self.get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            # Get companies to search
            query = """
                SELECT id, search_name
                FROM ch_scrape_queue
                WHERE search_status = 'pending'
                ORDER BY id
            """
            if limit:
                query += f" LIMIT {limit}"
            
            cur.execute(query)
            companies = cur.fetchall()
            
            if not companies:
                logging.info("No pending companies to search")
                return
            
            logging.info(f"Starting search for {len(companies)} companies")
            
            # Process in batches
            batch = []
            for i, company in enumerate(companies):
                batch.append(company)
                
                if len(batch) == self.batch_size or i == len(companies) - 1:
                    self._process_search_batch(batch)
                    batch = []
                    
                    # Progress update
                    if (i + 1) % 10 == 0:
                        logging.info(f"Progress: {i + 1}/{len(companies)} companies searched")
            
            conn.commit()
            logging.info("Company search completed")
            
        except Exception as e:
            logging.error(f"Error in search_companies: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()
    
    def _process_search_batch(self, batch):
        """Process a batch of company searches"""
        threads = []
        
        for company in batch:
            thread = threading.Thread(target=self._search_company_thread, args=(company,))
            threads.append(thread)
            thread.start()
            
            # Small delay between starting threads
            time.sleep(0.1)
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Process results from queue
        conn = self.get_db_connection()
        cur = conn.cursor()
        
        while not self.results_queue.empty():
            result = self.results_queue.get()
            try:
                cur.execute("""
                    UPDATE ch_scrape_queue
                    SET found_name = %s,
                        company_number = %s,
                        company_url = %s,
                        search_status = %s,
                        search_timestamp = NOW(),
                        search_error = %s
                    WHERE id = %s
                """, (
                    result.get('found_name'),
                    result.get('company_number'),
                    result.get('company_url'),
                    result.get('status'),
                    result.get('error'),
                    result['id']
                ))
                
                # If company was found, add to overview queue
                if result.get('status') == 'found' and result.get('company_number'):
                    cur.execute("""
                        INSERT INTO ch_scrape_overview (company_number, company_url, scrape_status)
                        VALUES (%s, %s, 'pending')
                        ON CONFLICT (company_number) DO NOTHING
                    """, (result['company_number'], result['company_url']))
                    
            except Exception as e:
                logging.error(f"Error updating result for company {result['id']}: {e}")
        
        conn.commit()
        cur.close()
        conn.close()
    
    def _search_company_thread(self, company):
        """Search for a single company (runs in thread)"""
        try:
            # Random delay to be respectful
            time.sleep(random.uniform(self.delay_min, self.delay_max))
            
            search_url = 'https://find-and-update.company-information.service.gov.uk/search/companies?q=' + quote_plus(company['search_name'])
            
            response = self.session.get(search_url, timeout=15)
            response.raise_for_status()
            
            tree = html.fromstring(response.text)
            
            # Check for results
            results_els = tree.xpath("//ul[@id='results' and @class='results-list']/li[contains(@class, 'type-company')]")
            no_results_el = tree.xpath("//div[@id='no-results']/h2[contains(@id, 'no-results') and contains(text(), 'No results')]")
            
            result = {
                'id': company['id'],
                'search_name': company['search_name']
            }
            
            if no_results_el and not results_els:
                result['status'] = 'not_found'
            elif results_els:
                # Take the first result
                first_result = results_els[0]
                
                # Extract company URL and name
                url_el = first_result.xpath("./h3/a[contains(@href, 'company/')]")
                meta_el = first_result.xpath("./p[@class='meta crumbtrail']")
                
                if url_el and meta_el:
                    result['status'] = 'found'
                    result['company_url'] = urljoin('https://find-and-update.company-information.service.gov.uk/', url_el[0].get('href'))
                    result['found_name'] = url_el[0].text_content().strip()
                    
                    # Extract company number from meta text
                    meta_text = meta_el[0].text_content().strip()
                    if ' - ' in meta_text:
                        result['company_number'] = meta_text.split(' - ')[0].strip()
                else:
                    result['status'] = 'error'
                    result['error'] = 'Could not parse search results'
            else:
                result['status'] = 'error'
                result['error'] = 'Unexpected page format'
            
            self.results_queue.put(result)
            
        except requests.RequestException as e:
            self.results_queue.put({
                'id': company['id'],
                'status': 'error',
                'error': f'Request error: {str(e)}'
            })
        except Exception as e:
            self.results_queue.put({
                'id': company['id'],
                'status': 'error',
                'error': f'Unexpected error: {str(e)}'
            })
    
    def scrape_company_data(self, data_type='overview', limit=None):
        """
        Scrape specific company data type
        
        Args:
            data_type: Type of data to scrape (overview, officers, charges, insolvency)
            limit: Limit number of companies to scrape
        """
        conn = self.get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            if data_type == 'overview':
                table = 'ch_scrape_overview'
                url_suffix = ''
            elif data_type == 'officers':
                table = 'ch_scrape_officers'
                url_suffix = '/officers'
            elif data_type == 'charges':
                table = 'ch_scrape_charges'
                url_suffix = '/charges'
            elif data_type == 'insolvency':
                table = 'ch_scrape_insolvency'
                url_suffix = '/insolvency'
            else:
                logging.error(f"Unknown data type: {data_type}")
                return
            
            # Get companies to scrape
            if data_type == 'overview':
                query = f"""
                    SELECT company_number, company_url
                    FROM {table}
                    WHERE scrape_status = 'pending'
                    ORDER BY company_number
                """
            else:
                # For other data types, get from overview table
                query = f"""
                    SELECT o.company_number, o.company_url
                    FROM ch_scrape_overview o
                    WHERE o.scrape_status = 'parsed'
                    AND NOT EXISTS (
                        SELECT 1 FROM {table} t
                        WHERE t.company_number = o.company_number
                    )
                    ORDER BY o.company_number
                """
            
            if limit:
                query += f" LIMIT {limit}"
            
            cur.execute(query)
            companies = cur.fetchall()
            
            if not companies:
                logging.info(f"No pending companies to scrape for {data_type}")
                return
            
            logging.info(f"Starting {data_type} scrape for {len(companies)} companies")
            
            # Process companies
            for i, company in enumerate(companies):
                try:
                    self._scrape_company_page(company, data_type, url_suffix)
                    
                    if (i + 1) % 10 == 0:
                        logging.info(f"Progress: {i + 1}/{len(companies)} {data_type} pages scraped")
                    
                    # Random delay
                    time.sleep(random.uniform(self.delay_min, self.delay_max))
                    
                except Exception as e:
                    logging.error(f"Error scraping {data_type} for {company['company_number']}: {e}")
            
            logging.info(f"{data_type} scraping completed")
            
        except Exception as e:
            logging.error(f"Error in scrape_company_data: {e}")
        finally:
            cur.close()
            conn.close()
    
    def _scrape_company_page(self, company, data_type, url_suffix):
        """Scrape a specific company page"""
        url = company['company_url'] + url_suffix
        
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            # Compress the HTML for storage
            compressed_html = bz2.compress(response.content)
            
            # Store in database
            conn = self.get_db_connection()
            cur = conn.cursor()
            
            if data_type == 'overview':
                cur.execute("""
                    UPDATE ch_scrape_overview
                    SET raw_html = %s,
                        scrape_status = 'scraped',
                        scrape_timestamp = NOW()
                    WHERE company_number = %s
                """, (compressed_html, company['company_number']))
            else:
                # For other types, we need to handle differently based on structure
                # This is simplified - in reality, officers and charges might have multiple entries
                if data_type == 'officers':
                    # Parse officers list and store each one
                    tree = html.fromstring(response.text)
                    officer_elements = tree.xpath("//div[@class='appointments-list']/div[contains(@class, 'appointment-1')]")
                    
                    for idx, officer_el in enumerate(officer_elements):
                        officer_html = html.tostring(officer_el)
                        compressed_officer = bz2.compress(officer_html)
                        
                        cur.execute("""
                            INSERT INTO ch_scrape_officers (company_number, officer_id, raw_html, scrape_status, scrape_timestamp)
                            VALUES (%s, %s, %s, 'scraped', NOW())
                            ON CONFLICT (company_number, officer_id) DO UPDATE
                            SET raw_html = EXCLUDED.raw_html,
                                scrape_status = 'scraped',
                                scrape_timestamp = NOW()
                        """, (company['company_number'], f"{company['company_number']}_officer_{idx}", compressed_officer))
                
                elif data_type in ['charges', 'insolvency']:
                    table = f'ch_scrape_{data_type}'
                    # For charges, we store the full page HTML with a NULL charge_id
                    if data_type == 'charges':
                        cur.execute(f"""
                            INSERT INTO {table} (company_number, charge_id, raw_html, scrape_status, scrape_timestamp)
                            VALUES (%s, NULL, %s, 'scraped', NOW())
                            ON CONFLICT (company_number, charge_id) DO UPDATE
                            SET raw_html = EXCLUDED.raw_html,
                                scrape_status = 'scraped',
                                scrape_timestamp = NOW()
                        """, (company['company_number'], compressed_html))
                    else:
                        cur.execute(f"""
                            INSERT INTO {table} (company_number, raw_html, scrape_status, scrape_timestamp)
                            VALUES (%s, %s, 'scraped', NOW())
                            ON CONFLICT (company_number) DO UPDATE
                            SET raw_html = EXCLUDED.raw_html,
                                scrape_status = 'scraped',
                                scrape_timestamp = NOW()
                        """, (company['company_number'], compressed_html))
            
            conn.commit()
            cur.close()
            conn.close()
            
        except requests.RequestException as e:
            logging.error(f"Request error for {url}: {e}")
            raise
        except Exception as e:
            logging.error(f"Error scraping {url}: {e}")
            raise
    
    def get_statistics(self):
        """Get current scraping statistics"""
        conn = self.get_db_connection()
        cur = conn.cursor()
        
        try:
            # Queue statistics
            cur.execute("""
                SELECT search_status, COUNT(*) as count
                FROM ch_scrape_queue
                GROUP BY search_status
                ORDER BY search_status
            """)
            
            print("\n=== Scraping Queue Status ===")
            for row in cur.fetchall():
                print(f"{row[0]}: {row[1]:,}")
            
            # Overview statistics
            cur.execute("""
                SELECT scrape_status, COUNT(*) as count
                FROM ch_scrape_overview
                GROUP BY scrape_status
                ORDER BY scrape_status
            """)
            
            print("\n=== Overview Scraping Status ===")
            for row in cur.fetchall():
                print(f"{row[0]}: {row[1]:,}")
            
            # Other table counts
            for table in ['officers', 'charges', 'insolvency']:
                cur.execute(f"SELECT COUNT(*) FROM ch_scrape_{table}")
                count = cur.fetchone()[0]
                print(f"\n{table.capitalize()} records: {count:,}")
            
        except Exception as e:
            logging.error(f"Error getting statistics: {e}")
        finally:
            cur.close()
            conn.close()


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape Companies House data')
    parser.add_argument('--search', action='store_true', help='Search for companies')
    parser.add_argument('--scrape', choices=['overview', 'officers', 'charges', 'insolvency', 'all'],
                       help='Scrape specific data type')
    parser.add_argument('--limit', type=int, help='Limit number of companies to process')
    parser.add_argument('--batch-size', type=int, default=5, help='Number of parallel requests')
    parser.add_argument('--delay-min', type=float, default=1, help='Minimum delay between requests')
    parser.add_argument('--delay-max', type=float, default=3, help='Maximum delay between requests')
    parser.add_argument('--stats', action='store_true', help='Show statistics')
    
    args = parser.parse_args()
    
    scraper = CompaniesHouseScraper(
        batch_size=args.batch_size,
        delay_min=args.delay_min,
        delay_max=args.delay_max
    )
    
    if args.stats:
        scraper.get_statistics()
    elif args.search:
        scraper.search_companies(limit=args.limit)
    elif args.scrape:
        if args.scrape == 'all':
            for data_type in ['overview', 'officers', 'charges', 'insolvency']:
                scraper.scrape_company_data(data_type, limit=args.limit)
        else:
            scraper.scrape_company_data(args.scrape, limit=args.limit)
    else:
        print("Please specify --search, --scrape, or --stats")
        parser.print_help()


if __name__ == '__main__':
    main()