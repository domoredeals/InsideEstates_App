#!/usr/bin/env python3
"""
Large-scale Companies House scraper optimized for 122k+ companies.
Features:
- Progress tracking and resume capability
- Adaptive rate limiting
- Error handling and retry logic
- Batch processing with checkpoints
- Resource monitoring
"""

import os
import sys
import time
import requests
import psycopg2
import bz2
import pickle
from datetime import datetime, timedelta
from urllib.parse import quote_plus, urljoin
from lxml import html
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import random
import threading
from queue import Queue
import logging
import signal
import json

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ch_scraper_large_scale.log'),
        logging.StreamHandler()
    ]
)

class LargeScaleCompaniesHouseScraper:
    def __init__(self, batch_size=5, delay_min=1.5, delay_max=3.5, max_retries=3):
        """
        Initialize the large-scale scraper
        
        Args:
            batch_size: Number of parallel requests (reduced for stability)
            delay_min: Minimum delay between requests in seconds
            delay_max: Maximum delay between requests in seconds
            max_retries: Maximum retry attempts for failed requests
        """
        self.batch_size = batch_size
        self.delay_min = delay_min
        self.delay_max = delay_max
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.lock = threading.Lock()
        self.results_queue = Queue()
        self.stats = {
            'processed': 0,
            'found': 0,
            'not_found': 0,
            'errors': 0,
            'start_time': datetime.now()
        }
        self.running = True
        self.checkpoint_file = 'scraper_checkpoint.json'
        
        # Setup graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
    def _handle_shutdown(self, signum, frame):
        """Handle graceful shutdown"""
        logging.info("Shutdown signal received. Saving progress...")
        self.running = False
        self._save_checkpoint()
        sys.exit(0)
        
    def _save_checkpoint(self):
        """Save current progress to checkpoint file"""
        checkpoint = {
            'stats': self.stats,
            'timestamp': datetime.now().isoformat()
        }
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint, f, default=str)
        logging.info(f"Checkpoint saved: {self.stats['processed']} processed")
        
    def _load_checkpoint(self):
        """Load previous checkpoint if exists"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    checkpoint = json.load(f)
                    logging.info(f"Previous checkpoint loaded: {checkpoint['stats']['processed']} already processed")
                    return checkpoint
            except:
                pass
        return None
        
    def get_db_connection(self):
        """Create database connection with retry logic"""
        retries = 3
        while retries > 0:
            try:
                return psycopg2.connect(
                    host=os.getenv('DB_HOST'),
                    database=os.getenv('DB_NAME'),
                    user=os.getenv('DB_USER'),
                    password=os.getenv('DB_PASSWORD'),
                    connect_timeout=10
                )
            except Exception as e:
                retries -= 1
                if retries == 0:
                    raise
                logging.warning(f"DB connection failed, retrying... ({e})")
                time.sleep(2)
    
    def search_companies(self, resume=True, checkpoint_interval=1000):
        """
        Search for companies on Companies House website with resume capability
        
        Args:
            resume: Whether to resume from last checkpoint
            checkpoint_interval: Save checkpoint every N companies
        """
        # Load checkpoint if resuming
        checkpoint = self._load_checkpoint() if resume else None
        
        conn = self.get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            # Get total pending count
            cur.execute("SELECT COUNT(*) FROM ch_scrape_queue WHERE search_status = 'pending'")
            total_pending = cur.fetchone()['count']
            
            logging.info(f"Total pending companies: {total_pending:,}")
            
            # Process in chunks
            chunk_size = 1000
            offset = 0
            
            while self.running:
                # Get next chunk of companies
                query = """
                    SELECT id, search_name
                    FROM ch_scrape_queue
                    WHERE search_status = 'pending'
                    ORDER BY id
                    LIMIT %s OFFSET %s
                """
                
                cur.execute(query, (chunk_size, offset))
                companies = cur.fetchall()
                
                if not companies:
                    logging.info("No more pending companies")
                    break
                
                logging.info(f"Processing chunk: companies {offset+1} to {offset+len(companies)}")
                
                # Process chunk
                self._process_chunk(companies, checkpoint_interval)
                
                offset += chunk_size
                
                # Show progress
                progress_pct = (self.stats['processed'] / total_pending * 100) if total_pending > 0 else 0
                elapsed = datetime.now() - self.stats['start_time']
                rate = self.stats['processed'] / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
                eta = timedelta(seconds=(total_pending - self.stats['processed']) / rate) if rate > 0 else "Unknown"
                
                logging.info(f"""
Progress: {self.stats['processed']:,}/{total_pending:,} ({progress_pct:.1f}%)
Found: {self.stats['found']:,} | Not Found: {self.stats['not_found']:,} | Errors: {self.stats['errors']:,}
Rate: {rate:.1f} companies/sec | ETA: {eta}
""")
            
            conn.commit()
            logging.info("Company search completed or interrupted")
            
        except Exception as e:
            logging.error(f"Error in search_companies: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()
            self._save_checkpoint()
    
    def _process_chunk(self, companies, checkpoint_interval):
        """Process a chunk of companies"""
        batch = []
        
        for i, company in enumerate(companies):
            if not self.running:
                break
                
            batch.append(company)
            
            if len(batch) == self.batch_size or i == len(companies) - 1:
                self._process_search_batch(batch)
                batch = []
                
                # Save checkpoint periodically
                if self.stats['processed'] % checkpoint_interval == 0:
                    self._save_checkpoint()
    
    def _process_search_batch(self, batch):
        """Process a batch of company searches with improved error handling"""
        threads = []
        
        for company in batch:
            thread = threading.Thread(target=self._search_company_thread, args=(company,))
            threads.append(thread)
            thread.start()
            
            # Small delay between starting threads
            time.sleep(0.1)
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=30)  # Add timeout to prevent hanging
        
        # Process results from queue
        self._process_results()
    
    def _search_company_thread(self, company):
        """Search for a single company with retry logic"""
        retries = 0
        while retries < self.max_retries:
            try:
                # Random delay to be respectful
                time.sleep(random.uniform(self.delay_min, self.delay_max))
                
                search_url = 'https://find-and-update.company-information.service.gov.uk/search/companies?q=' + quote_plus(company['search_name'])
                
                response = self.session.get(search_url, timeout=15)
                
                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logging.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    retries += 1
                    continue
                    
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
                    with self.lock:
                        self.stats['not_found'] += 1
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
                            
                        with self.lock:
                            self.stats['found'] += 1
                    else:
                        result['status'] = 'error'
                        result['error'] = 'Could not parse search results'
                        with self.lock:
                            self.stats['errors'] += 1
                else:
                    result['status'] = 'error'
                    result['error'] = 'Unexpected page format'
                    with self.lock:
                        self.stats['errors'] += 1
                
                self.results_queue.put(result)
                
                with self.lock:
                    self.stats['processed'] += 1
                
                return  # Success, exit retry loop
                
            except requests.RequestException as e:
                retries += 1
                if retries >= self.max_retries:
                    self.results_queue.put({
                        'id': company['id'],
                        'status': 'error',
                        'error': f'Request error after {self.max_retries} retries: {str(e)}'
                    })
                    with self.lock:
                        self.stats['errors'] += 1
                        self.stats['processed'] += 1
                else:
                    logging.warning(f"Retry {retries}/{self.max_retries} for company {company['id']}")
                    time.sleep(2 ** retries)  # Exponential backoff
                    
            except Exception as e:
                self.results_queue.put({
                    'id': company['id'],
                    'status': 'error',
                    'error': f'Unexpected error: {str(e)}'
                })
                with self.lock:
                    self.stats['errors'] += 1
                    self.stats['processed'] += 1
                return
    
    def _process_results(self):
        """Process results from queue with batch updates"""
        results = []
        while not self.results_queue.empty():
            results.append(self.results_queue.get())
        
        if not results:
            return
            
        conn = self.get_db_connection()
        cur = conn.cursor()
        
        try:
            # Batch update queue
            for result in results:
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
            
            conn.commit()
            
        except Exception as e:
            logging.error(f"Error processing results batch: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()
    
    def get_statistics(self):
        """Get comprehensive scraping statistics"""
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
            total = 0
            for row in cur.fetchall():
                print(f"{row[0]}: {row[1]:,}")
                total += row[1]
            print(f"Total: {total:,}")
            
            # Time estimates
            if self.stats['processed'] > 0:
                elapsed = datetime.now() - self.stats['start_time']
                rate = self.stats['processed'] / elapsed.total_seconds()
                
                print(f"\n=== Performance Metrics ===")
                print(f"Processing rate: {rate:.1f} companies/sec")
                print(f"Elapsed time: {elapsed}")
                
                # Calculate remaining time
                cur.execute("SELECT COUNT(*) FROM ch_scrape_queue WHERE search_status = 'pending'")
                remaining = cur.fetchone()[0]
                if rate > 0:
                    eta = timedelta(seconds=remaining / rate)
                    print(f"Estimated time remaining: {eta}")
            
            # Success rate
            if self.stats['processed'] > 0:
                success_rate = (self.stats['found'] / self.stats['processed']) * 100
                print(f"\n=== Success Rate ===")
                print(f"Found: {self.stats['found']:,} ({success_rate:.1f}%)")
                print(f"Not Found: {self.stats['not_found']:,}")
                print(f"Errors: {self.stats['errors']:,}")
            
        except Exception as e:
            logging.error(f"Error getting statistics: {e}")
        finally:
            cur.close()
            conn.close()


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Large-scale Companies House scraper')
    parser.add_argument('--batch-size', type=int, default=5, 
                       help='Number of parallel requests (default: 5, recommended for large scale)')
    parser.add_argument('--delay-min', type=float, default=1.5, 
                       help='Minimum delay between requests (default: 1.5)')
    parser.add_argument('--delay-max', type=float, default=3.5, 
                       help='Maximum delay between requests (default: 3.5)')
    parser.add_argument('--no-resume', action='store_true', 
                       help='Start from beginning, ignore checkpoint')
    parser.add_argument('--checkpoint-interval', type=int, default=1000,
                       help='Save checkpoint every N companies (default: 1000)')
    parser.add_argument('--stats', action='store_true', help='Show statistics only')
    parser.add_argument('--max-retries', type=int, default=3,
                       help='Maximum retry attempts for failed requests (default: 3)')
    
    args = parser.parse_args()
    
    scraper = LargeScaleCompaniesHouseScraper(
        batch_size=args.batch_size,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
        max_retries=args.max_retries
    )
    
    if args.stats:
        scraper.get_statistics()
    else:
        print(f"""
Starting large-scale Companies House scraper
============================================
Batch size: {args.batch_size}
Delay: {args.delay_min}-{args.delay_max} seconds
Resume: {not args.no_resume}
Checkpoint interval: {args.checkpoint_interval}

Press Ctrl+C to gracefully stop and save progress
""")
        scraper.search_companies(
            resume=not args.no_resume,
            checkpoint_interval=args.checkpoint_interval
        )


if __name__ == '__main__':
    main()