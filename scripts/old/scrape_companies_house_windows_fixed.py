#!/usr/bin/env python3
"""
Companies House Scraper for Windows IDLE - Fixed Version
Properly manages connections to prevent network exhaustion
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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class CompaniesHouseScraperCSV:
    def __init__(self, input_csv, output_csv, batch_size=3, delay_min=2, delay_max=4):
        """
        Initialize the scraper for CSV operations
        
        Args:
            input_csv: Path to input CSV file with company names
            output_csv: Path to output CSV file for results
            batch_size: Number of parallel requests (reduced to prevent network issues)
            delay_min: Minimum delay between requests in seconds
            delay_max: Maximum delay between requests in seconds
        """
        self.input_csv = input_csv
        self.output_csv = output_csv
        self.batch_size = batch_size
        self.delay_min = delay_min
        self.delay_max = delay_max
        
        # Thread-local session storage
        self.local = threading.local()
        
        self.results_queue = Queue()
        self.progress_lock = threading.Lock()
        self.processed = 0
        self.found = 0
        self.not_found = 0
        self.errors = 0
        self.start_time = datetime.now()
        
        # Connection semaphore to limit concurrent connections
        self.connection_semaphore = threading.Semaphore(batch_size)
        
    def _get_session(self):
        """Get or create a thread-local session with proper configuration"""
        if not hasattr(self.local, 'session'):
            # Create a new session for this thread
            session = requests.Session()
            
            # Configure retry strategy
            retry_strategy = Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
            )
            
            # Configure adapter with connection pooling
            adapter = HTTPAdapter(
                max_retries=retry_strategy,
                pool_connections=1,
                pool_maxsize=1
            )
            
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            
            # Set headers
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-GB,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            })
            
            self.local.session = session
            
        return self.local.session
        
    def load_companies_from_csv(self):
        """Load company names from input CSV"""
        companies = []
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
                        companies.append({
                            'id': len(companies) + 1,
                            'search_name': company_name.strip()
                        })
        except Exception as e:
            print(f"Error reading input CSV: {e}")
            return []
        
        print(f"Loaded {len(companies)} companies from {self.input_csv}")
        return companies
    
    def scrape_all_companies(self):
        """Main method to scrape all companies and save to CSV"""
        companies = self.load_companies_from_csv()
        
        if not companies:
            print("No companies to process!")
            return
        
        print(f"\nStarting scrape of {len(companies):,} companies...")
        print(f"Results will be saved to: {self.output_csv}")
        print(f"Batch size: {self.batch_size}")
        print(f"Delay: {self.delay_min}-{self.delay_max} seconds\n")
        
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
                          f"Errors: {self.errors} | Rate: {rate:.1f}/sec")
                
                # Add a longer pause between batches to prevent network exhaustion
                if i < len(companies) - 1:
                    time.sleep(random.uniform(1, 2))
        
        # Final summary
        print(f"\n{'='*60}")
        print("SCRAPING COMPLETE!")
        print(f"{'='*60}")
        print(f"Total Processed: {self.processed:,}")
        print(f"Found: {self.found:,} ({self.found/self.processed*100:.1f}%)")
        print(f"Not Found: {self.not_found:,} ({self.not_found/self.processed*100:.1f}%)")
        print(f"Errors: {self.errors:,} ({self.errors/self.processed*100:.1f}%)")
        print(f"Time Taken: {datetime.now() - self.start_time}")
        print(f"Results saved to: {self.output_csv}")
    
    def _process_batch(self, batch):
        """Process a batch of companies"""
        threads = []
        
        for company in batch:
            thread = threading.Thread(target=self._search_company_wrapper, args=(company,))
            threads.append(thread)
            thread.start()
            # Increased delay between thread starts
            time.sleep(0.5)
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join(timeout=60)  # Increased timeout
        
        # Save results to CSV
        self._save_results()
        
        # Clean up any thread-local sessions
        if hasattr(self.local, 'session'):
            try:
                self.local.session.close()
                delattr(self.local, 'session')
            except:
                pass
    
    def _search_company_wrapper(self, company):
        """Wrapper that ensures proper connection management"""
        with self.connection_semaphore:
            self._search_company(company)
    
    def _search_company(self, company):
        """Search for a single company"""
        session = None
        try:
            # Random delay
            time.sleep(random.uniform(self.delay_min, self.delay_max))
            
            # Get thread-local session
            session = self._get_session()
            
            search_url = 'https://find-and-update.company-information.service.gov.uk/search/companies?q=' + quote_plus(company['search_name'])
            
            # Make request with timeout
            response = session.get(search_url, timeout=30)
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
        except requests.exceptions.ConnectionError as e:
            self._add_error_result(company, f'Connection error: {str(e)}')
            # Close and remove the session on connection errors
            if hasattr(self.local, 'session'):
                try:
                    self.local.session.close()
                    delattr(self.local, 'session')
                except:
                    pass
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
        """Save results from queue to CSV"""
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
    """Main function for running in Windows IDLE"""
    print("Companies House Scraper for Windows - Fixed Version")
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
        print("The CSV should have a column with one of these names:")
        print("  - company_name")
        print("  - Company Name")
        print("  - name")
        print("  - Name")
        print("  - search_name")
        return
    
    # Get batch size
    batch_input = input("\nEnter batch size (1-5, default 3, recommended 3 for stability): ").strip()
    batch_size = int(batch_input) if batch_input.isdigit() else 3
    batch_size = max(1, min(5, batch_size))  # Clamp between 1 and 5
    
    print(f"\nUsing batch size: {batch_size}")
    print("Note: Smaller batch sizes are more stable but slower.")
    print("If you experience connection errors, try reducing the batch size to 1 or 2.\n")
    
    # Create scraper and run
    scraper = CompaniesHouseScraperCSV(
        input_csv=input_csv,
        output_csv=output_csv,
        batch_size=batch_size
    )
    
    try:
        scraper.scrape_all_companies()
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user!")
        print(f"Partial results saved to: {output_csv}")
    except Exception as e:
        print(f"\n\nError during scraping: {e}")
        print(f"Partial results may be saved to: {output_csv}")
    
    print("\nPress Enter to exit...")
    input()


if __name__ == '__main__':
    main()