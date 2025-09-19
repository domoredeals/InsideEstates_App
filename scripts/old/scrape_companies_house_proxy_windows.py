#!/usr/bin/env python3
"""
Companies House Scraper with Proxy Support for Windows
Uses Bright Data (formerly Luminati) proxy servers to avoid blocking
Supports parallel scraping with CSV input/output
"""

import os
import time
import requests
import csv
import sqlite3
import bz2
import pickle
import threading
from datetime import datetime
from urllib.parse import quote_plus, urljoin
from lxml import html
import random
from queue import Queue

# Proxy Configuration - Update these with your Bright Data credentials
PROXY_USERNAME = "brd-customer-hl_997fefd5-zone-ch30oct22"  # Your Bright Data username
PROXY_PASSWORD = "kikhwzt80akq"  # Your Bright Data password
PROXY_PORT = "22225"  # Your Bright Data port

class CompaniesHouseProxyScraper:
    def __init__(self, input_csv, output_csv, batch_size=100, use_proxy=True):
        """
        Initialize the scraper with proxy support
        
        Args:
            input_csv: Path to input CSV file with company names
            output_csv: Path to output CSV file for results
            batch_size: Number of parallel requests (can be higher with proxy)
            use_proxy: Whether to use proxy servers (True recommended)
        """
        self.input_csv = input_csv
        self.output_csv = output_csv
        self.batch_size = batch_size
        self.use_proxy = use_proxy
        
        # Proxy settings
        self.proxy_username = PROXY_USERNAME
        self.proxy_password = PROXY_PASSWORD
        self.proxy_port = PROXY_PORT
        
        # Thread safety
        self.lock = threading.Lock()
        self.results_queue = Queue()
        
        # Statistics
        self.processed = 0
        self.found = 0
        self.not_found = 0
        self.errors = 0
        self.start_time = datetime.now()
        self.good_count = 0
        
        # Create database for caching (optional)
        self.use_cache = True
        if self.use_cache:
            self.init_database()
    
    def init_database(self):
        """Initialize SQLite database for caching results"""
        self.db_conn = sqlite3.connect('companies_cache.db', check_same_thread=False)
        self.db_cursor = self.db_conn.cursor()
        self.db_cursor.execute("""
            CREATE TABLE IF NOT EXISTS CompaniesCache (
                search_text TEXT NOT NULL PRIMARY KEY,
                found_text TEXT,
                status TEXT,
                company_number TEXT,
                company_url TEXT,
                timestamp REAL
            )
        """)
        self.db_conn.commit()
    
    def get_proxy_url(self, country=None):
        """Get Bright Data proxy URL with session rotation"""
        if not self.use_proxy:
            return None
            
        session_id = str(random.random())
        if country is None:
            return f'http://{self.proxy_username}-session-{session_id}:{self.proxy_password}@zproxy.lum-superproxy.io:{self.proxy_port}'
        else:
            return f'http://{self.proxy_username}-country-{country}-session-{session_id}:{self.proxy_password}@zproxy.lum-superproxy.io:{self.proxy_port}'
    
    def load_companies_from_csv(self):
        """Load company names from input CSV"""
        companies = []
        try:
            with open(self.input_csv, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Try different column names
                    company_name = (row.get('Company Name') or 
                                  row.get('company_name') or 
                                  row.get('name') or 
                                  row.get('Name') or
                                  row.get('search_name') or
                                  list(row.values())[0])  # Use first column if no match
                    
                    if company_name and company_name.strip():
                        companies.append({
                            'text': company_name.strip(),
                            'id': len(companies) + 1
                        })
        except Exception as e:
            print(f"Error reading input CSV: {e}")
            return []
        
        print(f"Loaded {len(companies)} companies from {self.input_csv}")
        return companies
    
    def scrape_all_companies(self):
        """Main method to scrape all companies"""
        companies = self.load_companies_from_csv()
        
        if not companies:
            print("No companies to process!")
            return
        
        print(f"\nStarting scrape of {len(companies):,} companies...")
        print(f"Results will be saved to: {self.output_csv}")
        print(f"Batch size: {self.batch_size}")
        print(f"Using proxy: {self.use_proxy}")
        if self.use_proxy:
            print(f"Proxy service: Bright Data (formerly Luminati)")
        print()
        
        # Create output CSV with headers
        with open(self.output_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Search Name',
                'Found Name', 
                'Company Number',
                'Company URL',
                'Status',
                'Error',
                'Timestamp'
            ])
        
        # Process in batches
        all_thread_items = {}
        
        for i, company in enumerate(companies):
            # Check cache first
            if self.use_cache:
                cached = self.check_cache(company['text'])
                if cached:
                    self._save_cached_result(cached)
                    with self.lock:
                        self.processed += 1
                        if cached['status'] == 'FOUND':
                            self.found += 1
                        elif cached['status'] == 'NOT_FOUND':
                            self.not_found += 1
                    continue
            
            all_thread_items[company['text']] = company
            
            if len(all_thread_items) == self.batch_size or i == len(companies) - 1:
                # Process batch
                self._process_batch(all_thread_items)
                
                # Show progress
                with self.lock:
                    progress_pct = (self.processed / len(companies) * 100) if len(companies) > 0 else 0
                    elapsed = (datetime.now() - self.start_time).total_seconds()
                    rate = self.processed / elapsed if elapsed > 0 else 0
                    
                    print(f"Progress: {self.processed}/{len(companies)} ({progress_pct:.1f}%) | "
                          f"Found: {self.found} | Not Found: {self.not_found} | "
                          f"Errors: {self.errors} | Rate: {rate:.1f}/sec | "
                          f"Good in batch: {self.good_count}/{len(all_thread_items)}")
                
                all_thread_items = {}
                self.good_count = 0
        
        # Final summary
        self._print_summary(len(companies))
    
    def _process_batch(self, batch_items):
        """Process a batch of companies using threads"""
        threads = []
        
        for company_text, company_data in batch_items.items():
            thread = threading.Thread(target=self._search_company_thread, args=(company_data,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads
        for thread in threads:
            thread.join()
        
        # Save results
        self._save_batch_results()
    
    def _search_company_thread(self, company):
        """Search for a single company (thread worker)"""
        good_to_save = False
        result = {
            'search_name': company['text'],
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        try:
            # Get proxy URL
            proxy_url = self.get_proxy_url()
            proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
            
            # Make request
            search_url = 'https://find-and-update.company-information.service.gov.uk/search/companies?q=' + quote_plus(company['text'])
            
            r = requests.get(search_url, proxies=proxies, timeout=15)
            tree = html.fromstring(r.text)
            
            # Check for results
            results_els = tree.xpath("//ul[@id='results' and @class='results-list']/li[contains(@class, 'type-company')]")
            no_results_el = tree.xpath("//div[@id='no-results']/h2[contains(@id, 'no-results') and contains(text(), 'No results')]")
            
            if no_results_el and not results_els:
                # No results found
                good_to_save = True
                result['status'] = 'NOT_FOUND'
                result['found_name'] = ''
                result['company_number'] = ''
                result['company_url'] = ''
                result['error'] = ''
                with self.lock:
                    self.not_found += 1
                    
            elif results_els:
                # Take the first result
                first_result = results_els[0]
                url_el = first_result.xpath("./h3/a[contains(@href, 'company/')]")
                meta_el = first_result.xpath("./p[@class='meta crumbtrail']")
                
                if url_el and meta_el:
                    good_to_save = True
                    result['status'] = 'FOUND'
                    result['company_url'] = urljoin('https://find-and-update.company-information.service.gov.uk/', url_el[0].get('href'))
                    result['found_name'] = url_el[0].text_content().strip()
                    
                    # Extract company number from meta
                    meta_text = meta_el[0].text_content().strip()
                    if '-' in meta_text:
                        result['company_number'] = meta_text.split('-')[0].strip()
                    else:
                        # Extract from URL as fallback
                        result['company_number'] = result['company_url'].split('/company/')[-1].split('/')[0]
                    
                    result['error'] = ''
                    with self.lock:
                        self.found += 1
                else:
                    result['status'] = 'ERROR'
                    result['found_name'] = ''
                    result['company_number'] = ''
                    result['company_url'] = ''
                    result['error'] = 'Could not parse search results'
                    with self.lock:
                        self.errors += 1
            else:
                result['status'] = 'ERROR'
                result['found_name'] = ''
                result['company_number'] = ''
                result['company_url'] = ''
                result['error'] = 'Unexpected page format'
                with self.lock:
                    self.errors += 1
            
        except requests.exceptions.Timeout:
            result = self._create_error_result(company, 'Request timeout')
        except requests.exceptions.RequestException as e:
            result = self._create_error_result(company, f'Request error: {str(e)}')
        except Exception as e:
            result = self._create_error_result(company, f'Unexpected error: {str(e)}')
        
        if good_to_save:
            with self.lock:
                self.good_count += 1
                
            # Save to cache
            if self.use_cache:
                self._save_to_cache(result)
        
        # Add to results queue
        self.results_queue.put(result)
        
        with self.lock:
            self.processed += 1
    
    def _create_error_result(self, company, error_msg):
        """Create an error result"""
        with self.lock:
            self.errors += 1
        
        return {
            'search_name': company['text'],
            'status': 'ERROR',
            'found_name': '',
            'company_number': '',
            'company_url': '',
            'error': error_msg,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    
    def _save_batch_results(self):
        """Save all results from queue to CSV"""
        results = []
        while not self.results_queue.empty():
            results.append(self.results_queue.get())
        
        if results:
            with open(self.output_csv, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                for result in results:
                    writer.writerow([
                        result['search_name'],
                        result['found_name'],
                        result['company_number'],
                        result['company_url'],
                        result['status'],
                        result['error'],
                        result['timestamp']
                    ])
    
    def _save_cached_result(self, cached):
        """Save a cached result to CSV"""
        with open(self.output_csv, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                cached['search_name'],
                cached['found_name'],
                cached['company_number'],
                cached['company_url'],
                cached['status'],
                '',  # No error for cached results
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ])
    
    def check_cache(self, search_text):
        """Check if company is in cache"""
        if not self.use_cache:
            return None
            
        try:
            result = self.db_cursor.execute(
                "SELECT found_text, status, company_number, company_url FROM CompaniesCache WHERE search_text=?",
                (search_text,)
            ).fetchone()
            
            if result:
                return {
                    'search_name': search_text,
                    'found_name': result[0] or '',
                    'status': result[1] or 'NOT_FOUND',
                    'company_number': result[2] or '',
                    'company_url': result[3] or ''
                }
        except:
            pass
        
        return None
    
    def _save_to_cache(self, result):
        """Save result to cache database"""
        if not self.use_cache:
            return
            
        try:
            with self.lock:
                self.db_cursor.execute(
                    "INSERT OR REPLACE INTO CompaniesCache(search_text, found_text, status, company_number, company_url, timestamp) VALUES(?,?,?,?,?,?)",
                    (result['search_name'], result['found_name'], result['status'], 
                     result['company_number'], result['company_url'], datetime.now().timestamp())
                )
                self.db_conn.commit()
        except:
            pass
    
    def _print_summary(self, total):
        """Print final summary"""
        print(f"\n{'='*60}")
        print("SCRAPING COMPLETE!")
        print(f"{'='*60}")
        print(f"Total Processed: {self.processed:,}")
        print(f"Found: {self.found:,} ({self.found/self.processed*100:.1f}%)")
        print(f"Not Found: {self.not_found:,} ({self.not_found/self.processed*100:.1f}%)")
        print(f"Errors: {self.errors:,} ({self.errors/self.processed*100:.1f}%)")
        print(f"Time Taken: {datetime.now() - self.start_time}")
        print(f"Results saved to: {self.output_csv}")
        
        if self.use_cache:
            print(f"\nCache database: companies_cache.db")
            print("(Results are cached for faster re-runs)")


def main():
    """Main function for Windows IDLE"""
    print("Companies House Scraper with Proxy Support")
    print("=" * 60)
    
    # Check proxy credentials
    if PROXY_USERNAME == "your_username_here" or PROXY_PASSWORD == "your_password_here":
        print("\n⚠️  WARNING: Proxy credentials not configured!")
        print("Edit the script and update:")
        print("  PROXY_USERNAME = 'your_bright_data_username'")
        print("  PROXY_PASSWORD = 'your_bright_data_password'")
        print("  PROXY_PORT = 'your_bright_data_port'")
        print("\nYou can get these from your Bright Data dashboard.")
        use_proxy = input("\nContinue without proxy? (y/n): ").lower() == 'y'
        if not use_proxy:
            return
    else:
        use_proxy = True
    
    # Get input and output files
    input_csv = input("\nEnter input CSV filename (default: companies_to_scrape_122k.csv): ").strip()
    if not input_csv:
        input_csv = "companies_to_scrape_122k.csv"
    
    if not os.path.exists(input_csv):
        print(f"\nERROR: Input file '{input_csv}' not found!")
        return
    
    output_csv = input("\nEnter output CSV filename (default: auto-generated): ").strip()
    if not output_csv:
        output_csv = f"companies_house_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    # Get batch size
    if use_proxy:
        print("\nWith proxy, you can use higher batch sizes (50-200)")
        default_batch = 100
    else:
        print("\nWithout proxy, use lower batch sizes (1-10)")
        default_batch = 5
    
    batch_input = input(f"Enter batch size (default: {default_batch}): ").strip()
    batch_size = int(batch_input) if batch_input.isdigit() else default_batch
    
    # Create scraper
    scraper = CompaniesHouseProxyScraper(
        input_csv=input_csv,
        output_csv=output_csv,
        batch_size=batch_size,
        use_proxy=use_proxy
    )
    
    # Start scraping
    try:
        print(f"\nStarting scrape with batch size {batch_size}...")
        if use_proxy:
            print("Using Bright Data proxy servers for parallel scraping")
        else:
            print("WARNING: Without proxy, you may get blocked after many requests")
        
        scraper.scrape_all_companies()
        
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user!")
        print(f"Partial results saved to: {output_csv}")
    except Exception as e:
        print(f"\n\nError during scraping: {e}")
        print(f"Partial results may be saved to: {output_csv}")


if __name__ == '__main__':
    main()