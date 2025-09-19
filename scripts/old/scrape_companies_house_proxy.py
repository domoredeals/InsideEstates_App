#!/usr/bin/env python3
"""
Companies House Scraper with Built-in Proxy Support
Automatically uses proxy and skips already scraped companies
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

# PROXY CONFIGURATION - Update these with your credentials
PROXY_USERNAME = "brd-customer-hl_997fefd5-zone-ch30oct22"
PROXY_PASSWORD = "kikhwzt80akq"
PROXY_PORT = "22225"

class CompaniesHouseScraperProxy:
    def __init__(self, input_csv, output_csv, batch_size=50):
        """
        Initialize the scraper with proxy support and resume capability
        
        Args:
            input_csv: Path to input CSV file with company names
            output_csv: Path to output CSV file for results
            batch_size: Number of parallel requests (default 50 with proxy)
        """
        self.input_csv = input_csv
        self.output_csv = output_csv
        self.batch_size = batch_size
        
        self.results_queue = Queue()
        self.progress_lock = threading.Lock()
        self.processed = 0
        self.found = 0
        self.not_found = 0
        self.errors = 0
        self.skipped = 0
        self.start_time = datetime.now()
        
        # Load already scraped companies
        self.already_scraped = set()
        if os.path.exists(self.output_csv):
            self._load_already_scraped()
    
    def get_proxy_url(self):
        """Get a rotating proxy URL with session rotation"""
        proxy_url = f'http://{PROXY_USERNAME}-session-{random.random()}:{PROXY_PASSWORD}@zproxy.lum-superproxy.io:{PROXY_PORT}'
        return {'http': proxy_url, 'https': proxy_url}
    
    def _load_already_scraped(self):
        """Load company names that have already been scraped"""
        try:
            with open(self.output_csv, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    search_name = row.get('Search Name', '')
                    if search_name:
                        self.already_scraped.add(search_name.strip().lower())
            
            print(f"Found {len(self.already_scraped)} already scraped companies")
        except Exception as e:
            print(f"Warning: Could not load existing results: {e}")
            self.already_scraped = set()
    
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
                                  list(row.values())[0])
                    
                    if company_name and company_name.strip():
                        # Check if already scraped
                        if company_name.strip().lower() in self.already_scraped:
                            skipped_count += 1
                            continue
                            
                        companies.append({
                            'id': len(companies) + 1,
                            'search_name': company_name.strip()
                        })
        except Exception as e:
            print(f"Error reading input CSV: {e}")
            return []
        
        print(f"Loaded {len(companies)} companies to scrape")
        if skipped_count > 0:
            print(f"Skipped {skipped_count} already scraped companies")
            self.skipped = skipped_count
        
        return companies
    
    def scrape_all_companies(self):
        """Main method to scrape all companies and save to CSV"""
        companies = self.load_companies_from_csv()
        
        if not companies:
            print("\nNo new companies to process!")
            if self.skipped > 0:
                print(f"All {self.skipped} companies in the input file were already scraped.")
            return
        
        print(f"\nStarting scrape of {len(companies):,} new companies...")
        print(f"Results will be appended to: {self.output_csv}")
        print(f"Batch size: {self.batch_size}")
        print(f"Using Bright Data proxy rotation\n")
        
        # Create file with headers if it doesn't exist
        if not os.path.exists(self.output_csv):
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
                    
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                          f"Progress: {self.processed}/{total} ({progress_pct:.1f}%) | "
                          f"Found: {self.found} | Not Found: {self.not_found} | "
                          f"Errors: {self.errors} | Rate: {rate:.1f}/sec")
        
        # Final summary
        elapsed_total = (datetime.now() - self.start_time).total_seconds()
        print(f"\n{'='*60}")
        print("SCRAPING COMPLETE!")
        print(f"{'='*60}")
        print(f"New Companies Processed: {self.processed:,}")
        if self.processed > 0:
            print(f"Found: {self.found:,} ({self.found/self.processed*100:.1f}%)")
            print(f"Not Found: {self.not_found:,} ({self.not_found/self.processed*100:.1f}%)")
            print(f"Errors: {self.errors:,} ({self.errors/self.processed*100:.1f}%)")
        print(f"Previously Scraped (Skipped): {self.skipped:,}")
        print(f"Total in Results File: {len(self.already_scraped) + self.processed:,}")
        print(f"Time Taken: {datetime.now() - self.start_time}")
        if elapsed_total > 0:
            print(f"Average Rate: {self.processed/elapsed_total:.1f} companies/second")
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
            # Minimal delay with proxy
            time.sleep(random.uniform(0.0, 0.2))
            
            search_url = 'https://find-and-update.company-information.service.gov.uk/search/companies?q=' + quote_plus(company['search_name'])
            
            # Get proxy for this request
            proxies = self.get_proxy_url()
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            # Make request with proxy
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
    """Main function for running the scraper"""
    print("Companies House Scraper with Proxy")
    print("=" * 60)
    print(f"Proxy: Bright Data ({PROXY_USERNAME})")
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
    
    # Get batch size
    batch_input = input("\nEnter batch size (10-100, default 50): ").strip()
    batch_size = int(batch_input) if batch_input.isdigit() else 50
    batch_size = max(10, min(100, batch_size))
    
    print(f"\n{'='*60}")
    print("CONFIGURATION:")
    print(f"{'='*60}")
    print(f"Input file: {input_csv}")
    print(f"Output file: {output_csv}")
    print(f"Batch size: {batch_size}")
    print(f"Proxy: Enabled (Bright Data)")
    print("\nThe scraper will automatically:")
    print("- Use rotating proxy sessions for each request")
    print("- Skip any companies already in the output file")
    print("- Append new results to the existing file")
    print("- Resume from where it left off if interrupted")
    print(f"{'='*60}\n")
    
    # Create scraper and run
    scraper = CompaniesHouseScraperProxy(
        input_csv=input_csv,
        output_csv=output_csv,
        batch_size=batch_size
    )
    
    try:
        scraper.scrape_all_companies()
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user!")
        print(f"Results saved to: {output_csv}")
        print("Run the script again to automatically resume from where you left off.")
    except Exception as e:
        print(f"\n\nError during scraping: {e}")
        print(f"Partial results saved to: {output_csv}")
        print("Run the script again to automatically resume from where you left off.")
    
    print("\nPress Enter to exit...")
    input()


if __name__ == '__main__':
    main()