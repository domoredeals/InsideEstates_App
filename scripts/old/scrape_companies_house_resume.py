#!/usr/bin/env python3
"""
Companies House Scraper - Fast Version with Resume Support
Can skip already scraped companies and resume from where it left off
"""

import os
import time
import requests
import csv
from datetime import datetime
from urllib.parse import quote_plus, urljoin
from lxml import html
import random
import threading
from queue import Queue

class CompaniesHouseScraperResume:
    def __init__(self, input_csv, output_csv, batch_size=50, use_proxy=False, 
                 proxy_config=None, delay_min=0.1, delay_max=0.5, resume=True):
        """
        Initialize the scraper for fast CSV operations with resume support
        
        Args:
            input_csv: Path to input CSV file with company names
            output_csv: Path to output CSV file for results
            batch_size: Number of parallel requests (default 50 for speed)
            use_proxy: Whether to use proxy rotation
            proxy_config: Dict with proxy settings (username, password, port)
            delay_min: Minimum delay between requests
            delay_max: Maximum delay between requests
            resume: Whether to skip already scraped companies (default True)
        """
        self.input_csv = input_csv
        self.output_csv = output_csv
        self.batch_size = batch_size
        self.use_proxy = use_proxy
        self.proxy_config = proxy_config or {}
        self.delay_min = delay_min
        self.delay_max = delay_max
        self.resume = resume
        
        self.results_queue = Queue()
        self.progress_lock = threading.Lock()
        self.processed = 0
        self.found = 0
        self.not_found = 0
        self.errors = 0
        self.skipped = 0
        self.start_time = datetime.now()
        
        # Load already scraped companies if resuming
        self.already_scraped = set()
        if self.resume and os.path.exists(self.output_csv):
            self._load_already_scraped()
        
    def _load_already_scraped(self):
        """Load company names that have already been scraped"""
        try:
            with open(self.output_csv, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Add to set of already scraped companies
                    search_name = row.get('Search Name', '')
                    if search_name:
                        self.already_scraped.add(search_name.strip().lower())
            
            print(f"Found {len(self.already_scraped)} already scraped companies in {self.output_csv}")
        except Exception as e:
            print(f"Warning: Could not load existing results: {e}")
            self.already_scraped = set()
    
    def get_proxy_url(self):
        """Get a rotating proxy URL if proxy is enabled"""
        if not self.use_proxy or not self.proxy_config:
            return None
            
        username = self.proxy_config.get('username')
        password = self.proxy_config.get('password')
        port = self.proxy_config.get('port', '22225')
        
        # Luminati/Bright Data format with session rotation
        proxy_url = f'http://{username}-session-{random.random()}:{password}@zproxy.lum-superproxy.io:{port}'
        return {'http': proxy_url, 'https': proxy_url}
        
    def load_companies_from_csv(self):
        """Load company names from input CSV"""
        companies = []
        skipped_count = 0
        
        try:
            with open(self.input_csv, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Try different column names
                    company_name = (row.get('company_name') or 
                                  row.get('Company Name') or 
                                  row.get('name') or 
                                  row.get('Name') or
                                  row.get('search_name') or
                                  list(row.values())[0])  # Use first column if no match
                    
                    if company_name and company_name.strip():
                        # Check if already scraped
                        if self.resume and company_name.strip().lower() in self.already_scraped:
                            skipped_count += 1
                            continue
                            
                        companies.append({
                            'id': len(companies) + 1,
                            'search_name': company_name.strip()
                        })
        except Exception as e:
            print(f"Error reading input CSV: {e}")
            return []
        
        print(f"Loaded {len(companies)} companies from {self.input_csv}")
        if skipped_count > 0:
            print(f"Skipped {skipped_count} already scraped companies")
            self.skipped = skipped_count
        
        return companies
    
    def scrape_all_companies(self):
        """Main method to scrape all companies and save to CSV"""
        companies = self.load_companies_from_csv()
        
        if not companies:
            print("No companies to process!")
            if self.skipped > 0:
                print(f"All {self.skipped} companies were already scraped.")
            return
        
        print(f"\nStarting scrape of {len(companies):,} companies...")
        print(f"Results will be {'appended to' if self.resume and os.path.exists(self.output_csv) else 'saved to'}: {self.output_csv}")
        print(f"Batch size: {self.batch_size}")
        if self.use_proxy:
            print("Using proxy rotation for maximum speed")
        else:
            print("WARNING: Not using proxy - may hit rate limits!")
        print(f"Delay: {self.delay_min}-{self.delay_max} seconds\n")
        
        # Create or append to output CSV
        if not self.resume or not os.path.exists(self.output_csv):
            # Create new file with headers
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
        batch = []
        total = len(companies)
        
        for i, company in enumerate(companies):
            batch.append(company)
            
            if len(batch) == self.batch_size or i == len(companies) - 1:
                self._process_batch(batch)
                batch = []
                
                # Show progress
                with self.progress_lock:
                    progress_pct = (self.processed / total * 100) if total > 0 else 0
                    elapsed = (datetime.now() - self.start_time).total_seconds()
                    rate = self.processed / elapsed if elapsed > 0 else 0
                    
                    print(f"Progress: {self.processed}/{total} ({progress_pct:.1f}%) | "
                          f"Found: {self.found} | Not Found: {self.not_found} | "
                          f"Errors: {self.errors} | Rate: {rate:.1f}/sec | "
                          f"Previously Scraped: {self.skipped}")
        
        # Final summary
        elapsed_total = (datetime.now() - self.start_time).total_seconds()
        print(f"\n{'='*60}")
        print("SCRAPING COMPLETE!")
        print(f"{'='*60}")
        print(f"Total Processed: {self.processed:,}")
        print(f"Found: {self.found:,} ({self.found/self.processed*100:.1f}%)" if self.processed > 0 else "Found: 0")
        print(f"Not Found: {self.not_found:,} ({self.not_found/self.processed*100:.1f}%)" if self.processed > 0 else "Not Found: 0")
        print(f"Errors: {self.errors:,} ({self.errors/self.processed*100:.1f}%)" if self.processed > 0 else "Errors: 0")
        print(f"Previously Scraped (Skipped): {self.skipped:,}")
        print(f"Time Taken: {datetime.now() - self.start_time}")
        print(f"Average Rate: {self.processed/elapsed_total:.1f} companies/second" if elapsed_total > 0 else "N/A")
        print(f"Results saved to: {self.output_csv}")
    
    def _process_batch(self, batch):
        """Process a batch of companies"""
        threads = []
        
        # Launch all threads at once for maximum speed
        for company in batch:
            thread = threading.Thread(target=self._search_company, args=(company,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=30)
        
        # Save results to CSV
        self._save_results()
    
    def _search_company(self, company):
        """Search for a single company"""
        try:
            # Minimal delay if any
            if self.delay_max > 0:
                time.sleep(random.uniform(self.delay_min, self.delay_max))
            
            search_url = 'https://find-and-update.company-information.service.gov.uk/search/companies?q=' + quote_plus(company['search_name'])
            
            # Set up request parameters
            proxies = self.get_proxy_url() if self.use_proxy else None
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            # Make request
            response = requests.get(search_url, 
                                  proxies=proxies, 
                                  headers=headers,
                                  timeout=15)
            response.raise_for_status()
            
            tree = html.fromstring(response.text)
            
            # Check for results
            results_els = tree.xpath("//ul[@id='results']/li[contains(@class, 'type-company')]")
            no_results = tree.xpath("//div[@id='no-results']")
            
            result = {
                'search_name': company['search_name'],
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            if no_results and not results_els:
                result['status'] = 'NOT_FOUND'
                result['found_name'] = ''
                result['company_number'] = ''
                result['company_url'] = ''
                result['error'] = ''
                with self.progress_lock:
                    self.not_found += 1
            elif results_els:
                # Take the first result
                first_result = results_els[0]
                
                # Extract company URL and name
                url_el = first_result.xpath(".//a[contains(@href, 'company/')]")
                
                if url_el:
                    result['status'] = 'FOUND'
                    result['company_url'] = urljoin('https://find-and-update.company-information.service.gov.uk/', url_el[0].get('href'))
                    result['found_name'] = url_el[0].text_content().strip()
                    
                    # Extract company number from URL
                    company_number = result['company_url'].split('/company/')[-1].split('/')[0]
                    result['company_number'] = company_number
                    result['error'] = ''
                    
                    with self.progress_lock:
                        self.found += 1
                else:
                    result['status'] = 'ERROR'
                    result['found_name'] = ''
                    result['company_number'] = ''
                    result['company_url'] = ''
                    result['error'] = 'Could not parse search results'
                    with self.progress_lock:
                        self.errors += 1
            else:
                result['status'] = 'ERROR'
                result['found_name'] = ''
                result['company_number'] = ''
                result['company_url'] = ''
                result['error'] = 'Unexpected page format'
                with self.progress_lock:
                    self.errors += 1
            
            self.results_queue.put(result)
            
            with self.progress_lock:
                self.processed += 1
                
        except requests.exceptions.Timeout:
            self._add_error_result(company, 'Request timeout')
        except requests.exceptions.RequestException as e:
            self._add_error_result(company, f'Request error: {str(e)}')
        except Exception as e:
            self._add_error_result(company, f'Unexpected error: {str(e)}')
    
    def _add_error_result(self, company, error_msg):
        """Add an error result"""
        self.results_queue.put({
            'search_name': company['search_name'],
            'status': 'ERROR',
            'found_name': '',
            'company_number': '',
            'company_url': '',
            'error': error_msg,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        with self.progress_lock:
            self.errors += 1
            self.processed += 1
    
    def _save_results(self):
        """Save results from queue to CSV (append mode)"""
        results = []
        while not self.results_queue.empty():
            results.append(self.results_queue.get())
        
        if results:
            # Always append to maintain resume capability
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


def main():
    """Main function for running the fast scraper with resume support"""
    print("Companies House Scraper - Fast Version with Resume Support")
    print("=" * 60)
    
    # Get input and output file paths
    input_csv = input("Enter path to input CSV file (with company names): ").strip()
    if not input_csv:
        input_csv = "companies_to_search.csv"
        print(f"Using default: {input_csv}")
    
    output_csv = input("Enter path for output CSV file (results): ").strip()
    if not output_csv:
        output_csv = f"companies_house_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        print(f"Using default: {output_csv}")
    
    # Check if input file exists
    if not os.path.exists(input_csv):
        print(f"\nERROR: Input file '{input_csv}' not found!")
        print("Please create a CSV file with company names.")
        return
    
    # Check for existing output file
    resume = True
    if os.path.exists(output_csv):
        resume_input = input(f"\nOutput file '{output_csv}' exists. Resume from where left off? (y/n, default y): ").strip().lower()
        if resume_input == 'n':
            overwrite_input = input("Overwrite existing file? (y/n): ").strip().lower()
            if overwrite_input != 'y':
                print("Exiting without changes.")
                return
            resume = False
    
    # Get batch size
    batch_input = input("\nEnter batch size (10-100, default 50 for speed): ").strip()
    batch_size = int(batch_input) if batch_input.isdigit() else 50
    batch_size = max(10, min(100, batch_size))
    
    # Ask about proxy
    use_proxy_input = input("\nDo you have proxy credentials? (y/n, default n): ").strip().lower()
    use_proxy = use_proxy_input == 'y'
    
    proxy_config = None
    if use_proxy:
        print("\nEnter proxy credentials (Luminati/Bright Data format):")
        proxy_username = input("Proxy username: ").strip()
        proxy_password = input("Proxy password: ").strip()
        proxy_port = input("Proxy port (default 22225): ").strip() or "22225"
        
        proxy_config = {
            'username': proxy_username,
            'password': proxy_password,
            'port': proxy_port
        }
        
        print("\nUsing proxy rotation for maximum speed and reliability!")
        delay_min = 0.0
        delay_max = 0.1
    else:
        print("\nWARNING: Without proxy, you may hit rate limits!")
        print("Recommended: Use smaller batch size (10-20) and add delays")
        
        # Force smaller batch size and delays without proxy
        if batch_size > 20:
            batch_size = 20
            print(f"Reducing batch size to {batch_size} for safety")
        
        delay_min = 0.5
        delay_max = 1.5
    
    print(f"\nConfiguration:")
    print(f"- Batch size: {batch_size}")
    print(f"- Proxy: {'Enabled' if use_proxy else 'Disabled'}")
    print(f"- Delay: {delay_min}-{delay_max} seconds")
    print(f"- Resume: {'Yes - will skip already scraped' if resume else 'No - fresh start'}")
    
    # Create scraper and run
    scraper = CompaniesHouseScraperResume(
        input_csv=input_csv,
        output_csv=output_csv,
        batch_size=batch_size,
        use_proxy=use_proxy,
        proxy_config=proxy_config,
        delay_min=delay_min,
        delay_max=delay_max,
        resume=resume
    )
    
    try:
        scraper.scrape_all_companies()
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user!")
        print(f"Results saved to: {output_csv}")
        print("You can resume from where you left off by running the script again.")
    except Exception as e:
        print(f"\n\nError during scraping: {e}")
        print(f"Partial results saved to: {output_csv}")
        print("You can resume from where you left off by running the script again.")
    
    print("\nPress Enter to exit...")
    input()


if __name__ == '__main__':
    main()