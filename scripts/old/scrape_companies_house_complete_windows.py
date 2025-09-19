#!/usr/bin/env python3
"""
Complete Companies House scraper for Windows - extracts ALL Overview page data
Outputs to CSV file with all the fields from the Overview page
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
import re

# Proxy configuration - Update with your Bright Data credentials
PROXY_USERNAME = "your_username_here"  
PROXY_PASSWORD = "your_password_here"  
PROXY_PORT = "22225"

class CompaniesHouseCompleteScraperWindows:
    def __init__(self, input_csv, output_csv, batch_size=10, use_proxy=False):
        """Initialize the complete scraper"""
        self.input_csv = input_csv
        self.output_csv = output_csv
        self.batch_size = batch_size
        self.use_proxy = use_proxy
        
        # Proxy settings
        self.proxy_username = PROXY_USERNAME
        self.proxy_password = PROXY_PASSWORD
        self.proxy_port = PROXY_PORT
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
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
    
    def get_proxy_url(self):
        """Get proxy URL with session rotation"""
        if not self.use_proxy:
            return None
        
        session_id = str(random.random())
        return f'http://{self.proxy_username}-session-{session_id}:{self.proxy_password}@zproxy.lum-superproxy.io:{self.proxy_port}'
    
    def load_companies(self):
        """Load companies from input CSV"""
        companies = []
        try:
            with open(self.input_csv, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Try different column names
                    company_name = (row.get('Company Name') or 
                                  row.get('company_name') or 
                                  row.get('Search Name') or
                                  row.get('search_name') or
                                  row.get('name') or
                                  row.get('Name') or
                                  list(row.values())[0])
                    
                    if company_name and company_name.strip():
                        companies.append({
                            'search_name': company_name.strip(),
                            'id': len(companies) + 1
                        })
        except Exception as e:
            print(f"Error reading input CSV: {e}")
        
        return companies
    
    def search_company(self, company_name):
        """Search for a company and return its URL"""
        try:
            proxies = None
            if self.use_proxy:
                proxy_url = self.get_proxy_url()
                proxies = {"http": proxy_url, "https": proxy_url}
            
            search_url = f'https://find-and-update.company-information.service.gov.uk/search/companies?q={quote_plus(company_name)}'
            
            response = self.session.get(search_url, timeout=15, proxies=proxies)
            response.raise_for_status()
            
            tree = html.fromstring(response.text)
            
            # Check for results
            results_els = tree.xpath("//ul[@id='results']/li[contains(@class, 'type-company')]")
            no_results = tree.xpath("//div[@id='no-results']")
            
            if no_results and not results_els:
                return None
            
            if results_els:
                # Take first result
                first_result = results_els[0]
                url_el = first_result.xpath("./h3/a[contains(@href, 'company/')]")
                
                if url_el:
                    company_url = urljoin('https://find-and-update.company-information.service.gov.uk/', 
                                        url_el[0].get('href'))
                    found_name = url_el[0].text_content().strip()
                    
                    # Extract company number from URL
                    company_number = company_url.split('/company/')[-1].split('/')[0]
                    
                    # Get meta text for initial status
                    meta_el = first_result.xpath("./p[@class='meta crumbtrail']")
                    meta_text = meta_el[0].text_content().strip() if meta_el else ''
                    
                    return {
                        'company_url': company_url,
                        'company_number': company_number,
                        'found_name': found_name,
                        'meta_text': meta_text
                    }
            
            return None
            
        except Exception as e:
            print(f"Error searching for {company_name}: {e}")
            return None
    
    def extract_overview_data(self, company_url):
        """Extract ALL data from the Overview page"""
        try:
            proxies = None
            if self.use_proxy:
                proxy_url = self.get_proxy_url()
                proxies = {"http": proxy_url, "https": proxy_url}
            
            response = self.session.get(company_url, timeout=15, proxies=proxies)
            response.raise_for_status()
            
            tree = html.fromstring(response.text)
            
            data = {}
            
            # Company name (from page header)
            name_el = tree.xpath("//h1[@class='heading-xlarge' or contains(@class, 'company-name')]")
            if name_el:
                data['Company Name (Official)'] = name_el[0].text_content().strip()
            
            # Registered office address - try multiple approaches
            address_lines = []
            
            # Method 1: Look for dd with address class
            address_el = tree.xpath("//dd[contains(@class, 'address')]")
            
            # Method 2: Look for specific text pattern
            if not address_el:
                address_el = tree.xpath("//dt[contains(text(), 'Registered office address')]/following-sibling::dd[1]")
            
            # Method 3: Look for dd containing postcode pattern
            if not address_el:
                postcode_pattern = r'[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}'
                all_dd = tree.xpath("//dd")
                for dd in all_dd:
                    text = dd.text_content()
                    if re.search(postcode_pattern, text) and 'United Kingdom' in text:
                        address_el = [dd]
                        break
            
            if address_el:
                address_text = address_el[0].text_content().strip()
                # Clean up address - remove extra whitespace
                address_text = re.sub(r'\s+', ' ', address_text)
                address_text = re.sub(r'\s*,\s*', ', ', address_text)
                data['Registered Office Address'] = address_text
                
                # Extract postcode
                postcode_pattern = r'[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}'
                postcode_match = re.search(postcode_pattern, address_text)
                if postcode_match:
                    data['Postcode'] = postcode_match.group(0)
            
            # Company status - multiple approaches
            status_el = tree.xpath("//dd[@id='company-status']")
            if not status_el:
                status_el = tree.xpath("//dt[contains(text(), 'Company status')]/following-sibling::dd[1]")
            if not status_el:
                # Look for Active/Dissolved text in any dd
                status_el = tree.xpath("//dd[contains(text(), 'Active') or contains(text(), 'Dissolved') or contains(text(), 'Liquidation')]")
            
            if status_el:
                data['Company Status'] = status_el[0].text_content().strip()
            
            # Company type
            type_el = tree.xpath("//dd[@id='company-type']")
            if not type_el:
                type_el = tree.xpath("//dt[contains(text(), 'Company type')]/following-sibling::dd[1]")
            if not type_el:
                type_el = tree.xpath("//dd[contains(text(), 'Private') or contains(text(), 'Public') or contains(text(), 'limited')]")
                
            if type_el:
                data['Company Type'] = type_el[0].text_content().strip()
            
            # Incorporation date - look for date pattern
            inc_date_text = None
            
            # Method 1: Look for specific label
            inc_date_el = tree.xpath("//dt[contains(text(), 'Incorporated on')]/following-sibling::dd[1]")
            
            # Method 2: Look for date pattern in dd elements
            if not inc_date_el:
                date_pattern = r'\d{1,2}\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}'
                all_dd = tree.xpath("//dd")
                for dd in all_dd:
                    text = dd.text_content().strip()
                    if re.match(date_pattern, text):
                        inc_date_el = [dd]
                        break
            
            if inc_date_el:
                inc_date_text = inc_date_el[0].text_content().strip()
                data['Incorporated On'] = inc_date_text
            
            # Accounts section
            # Next accounts made up to
            next_accounts_el = tree.xpath("//dt[contains(text(), 'Next accounts made up to')]/following-sibling::dd[1]")
            if next_accounts_el:
                next_accounts_text = next_accounts_el[0].text_content().strip()
                data['Next Accounts Made Up To'] = next_accounts_text
                
                # Due by (usually in the next dt/dd pair)
                due_by_el = tree.xpath("//dt[contains(text(), 'due by')]/following-sibling::dd[1]")
                if due_by_el:
                    data['Accounts Due By'] = due_by_el[0].text_content().strip()
            
            # Last accounts made up to
            last_accounts_el = tree.xpath("//dt[contains(text(), 'Last accounts made up to')]/following-sibling::dd[1]")
            if last_accounts_el:
                data['Last Accounts Made Up To'] = last_accounts_el[0].text_content().strip()
            
            # Confirmation statement section
            # Next statement date
            next_statement_el = tree.xpath("//dt[contains(text(), 'Next statement date')]/following-sibling::dd[1]")
            if next_statement_el:
                next_statement_text = next_statement_el[0].text_content().strip()
                data['Next Statement Date'] = next_statement_text
                
                # Due by
                stmt_due_el = tree.xpath("//h2[contains(text(), 'Confirmation')]/following::dt[contains(text(), 'due by')]/following-sibling::dd[1]")
                if stmt_due_el:
                    data['Statement Due By'] = stmt_due_el[0].text_content().strip()
            
            # Last statement dated
            last_statement_el = tree.xpath("//dt[contains(text(), 'Last statement dated')]/following-sibling::dd[1]")
            if last_statement_el:
                data['Last Statement Dated'] = last_statement_el[0].text_content().strip()
            
            # Nature of business (SIC codes)
            sic_codes = []
            
            # Method 1: Look for list items in SIC section
            sic_els = tree.xpath("//h2[contains(text(), 'Nature of business')]/following::ul[1]/li")
            
            # Method 2: Look for text containing numbers and descriptions
            if not sic_els:
                sic_section = tree.xpath("//h2[contains(text(), 'Nature of business')]/parent::*")
                if sic_section:
                    sic_text = sic_section[0].text_content()
                    # Extract patterns like "41100 - Development of building projects"
                    sic_pattern = r'(\d{5})\s*-\s*([^0-9]+?)(?=\d{5}|$)'
                    matches = re.findall(sic_pattern, sic_text)
                    for code, desc in matches:
                        sic_codes.append(f"{code} - {desc.strip()}")
            
            if sic_els:
                for sic_el in sic_els:
                    sic_text = sic_el.text_content().strip()
                    if sic_text:
                        sic_codes.append(sic_text)
            
            # Store SIC codes
            if sic_codes:
                data['Nature of Business (SIC)'] = ' | '.join(sic_codes)
                # Also store individual codes
                for i, code in enumerate(sic_codes[:4]):  # Max 4 codes
                    data[f'SIC Code {i+1}'] = code
            
            return data
            
        except Exception as e:
            print(f"Error extracting overview data from {company_url}: {e}")
            return None
    
    def process_company_thread(self, company):
        """Thread worker to process a single company"""
        result = {
            'Search Name': company['search_name'],
            'Status': 'ERROR',
            'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        try:
            # Search for company
            print(f"[{company['id']}] Searching for: {company['search_name']}...", end='')
            search_result = self.search_company(company['search_name'])
            
            if not search_result:
                result['Status'] = 'NOT_FOUND'
                with self.lock:
                    self.stats['not_found'] += 1
                print(" NOT FOUND")
            else:
                # Found company
                result['Status'] = 'FOUND'
                result['Found Name'] = search_result['found_name']
                result['Company Number'] = search_result['company_number']
                result['Company URL'] = search_result['company_url']
                result['Search Results Meta'] = search_result['meta_text']
                
                print(f" FOUND ({search_result['company_number']})")
                
                # Extract overview data
                print(f"[{company['id']}] Extracting overview data...", end='')
                overview_data = self.extract_overview_data(search_result['company_url'])
                
                if overview_data:
                    result.update(overview_data)
                    print(" COMPLETE")
                    with self.lock:
                        self.stats['found'] += 1
                else:
                    print(" FAILED")
                    result['Status'] = 'ERROR'
                    result['Error'] = 'Failed to extract overview data'
                    with self.lock:
                        self.stats['errors'] += 1
            
            # Add to results queue
            self.results_queue.put(result)
            
            with self.lock:
                self.stats['processed'] += 1
                
        except Exception as e:
            print(f" ERROR: {e}")
            result['Status'] = 'ERROR'
            result['Error'] = str(e)
            self.results_queue.put(result)
            with self.lock:
                self.stats['errors'] += 1
                self.stats['processed'] += 1
    
    def scrape_all_companies(self):
        """Main method to scrape all companies"""
        companies = self.load_companies()
        
        if not companies:
            print("No companies to process!")
            return
        
        print(f"\nLoaded {len(companies)} companies from {self.input_csv}")
        print(f"Results will be saved to: {self.output_csv}")
        print(f"Batch size: {self.batch_size}")
        print(f"Using proxy: {self.use_proxy}")
        print()
        
        # Prepare output CSV with all columns
        fieldnames = [
            'Search Name',
            'Status',
            'Found Name',
            'Company Number',
            'Company URL',
            'Search Results Meta',
            'Company Name (Official)',
            'Registered Office Address',
            'Postcode',
            'Company Status',
            'Company Type',
            'Incorporated On',
            'Next Accounts Made Up To',
            'Accounts Due By',
            'Last Accounts Made Up To',
            'Next Statement Date',
            'Statement Due By',
            'Last Statement Dated',
            'Nature of Business (SIC)',
            'SIC Code 1',
            'SIC Code 2',
            'SIC Code 3',
            'SIC Code 4',
            'Error',
            'Timestamp'
        ]
        
        # Create output file
        with open(self.output_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
        
        # Process in batches
        for i in range(0, len(companies), self.batch_size):
            batch = companies[i:i + self.batch_size]
            threads = []
            
            # Start threads
            for company in batch:
                thread = threading.Thread(target=self.process_company_thread, args=(company,))
                threads.append(thread)
                thread.start()
            
            # Wait for threads
            for thread in threads:
                thread.join()
            
            # Save results from queue
            self._save_batch_results(fieldnames)
            
            # Progress update
            progress_pct = (self.stats['processed'] / len(companies) * 100) if len(companies) > 0 else 0
            elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
            rate = self.stats['processed'] / elapsed if elapsed > 0 else 0
            
            print(f"\nProgress: {self.stats['processed']}/{len(companies)} ({progress_pct:.1f}%) | "
                  f"Found: {self.stats['found']} | Not Found: {self.stats['not_found']} | "
                  f"Errors: {self.stats['errors']} | Rate: {rate:.1f}/sec")
            
            # Delay between batches if not using proxy
            if not self.use_proxy and i + self.batch_size < len(companies):
                delay = random.uniform(1, 2)
                print(f"Waiting {delay:.1f} seconds before next batch...")
                time.sleep(delay)
        
        # Final summary
        self._print_summary(len(companies))
    
    def _save_batch_results(self, fieldnames):
        """Save results from queue to CSV"""
        results = []
        while not self.results_queue.empty():
            results.append(self.results_queue.get())
        
        if results:
            with open(self.output_csv, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                for result in results:
                    writer.writerow(result)
    
    def _print_summary(self, total):
        """Print final summary"""
        elapsed = datetime.now() - self.stats['start_time']
        
        print(f"\n{'='*60}")
        print("SCRAPING COMPLETE!")
        print(f"{'='*60}")
        print(f"Total Processed: {self.stats['processed']:,}")
        print(f"Found: {self.stats['found']:,} ({self.stats['found']/self.stats['processed']*100:.1f}%)")
        print(f"Not Found: {self.stats['not_found']:,} ({self.stats['not_found']/self.stats['processed']*100:.1f}%)")
        print(f"Errors: {self.stats['errors']:,} ({self.stats['errors']/self.stats['processed']*100:.1f}%)")
        print(f"Time Taken: {elapsed}")
        print(f"Results saved to: {self.output_csv}")


def main():
    """Main function for Windows"""
    print("Complete Companies House Scraper - All Overview Data")
    print("=" * 60)
    print("This extracts ALL fields from the Overview page:")
    print("- Company details (name, number, status, type)")
    print("- Registered office address")
    print("- Incorporation date")
    print("- Accounts information")
    print("- Confirmation statement dates")
    print("- Nature of business (SIC codes)")
    print()
    
    # Get input file
    input_csv = input("Enter input CSV file with company names: ").strip()
    if not input_csv:
        input_csv = "companies_to_scrape.csv"
        print(f"Using default: {input_csv}")
    
    if not os.path.exists(input_csv):
        print(f"\nERROR: Input file '{input_csv}' not found!")
        return
    
    # Get output file
    output_csv = input("Enter output CSV filename (default: auto-generated): ").strip()
    if not output_csv:
        output_csv = f"companies_house_complete_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        print(f"Using: {output_csv}")
    
    # Configuration
    print("\nConfiguration:")
    
    # Batch size
    batch_input = input("Batch size (default 10, max 50 without proxy): ").strip()
    batch_size = int(batch_input) if batch_input.isdigit() else 10
    
    # Proxy option
    use_proxy = False
    if PROXY_USERNAME != "your_username_here":
        use_proxy = input("Use proxy? (y/n, default: n): ").lower() == 'y'
        if use_proxy and batch_size < 50:
            print("With proxy, you can use higher batch sizes (50-100)")
            batch_input = input(f"Update batch size? (current: {batch_size}): ").strip()
            if batch_input.isdigit():
                batch_size = int(batch_input)
    
    # Create scraper
    scraper = CompaniesHouseCompleteScraperWindows(
        input_csv=input_csv,
        output_csv=output_csv,
        batch_size=batch_size,
        use_proxy=use_proxy
    )
    
    # Start scraping
    try:
        print(f"\nStarting scrape...")
        scraper.scrape_all_companies()
        
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user!")
        print(f"Partial results saved to: {output_csv}")
    except Exception as e:
        print(f"\n\nError during scraping: {e}")
        print(f"Partial results may be saved to: {output_csv}")


if __name__ == '__main__':
    main()