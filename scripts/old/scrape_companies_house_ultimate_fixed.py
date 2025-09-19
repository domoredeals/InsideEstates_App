#!/usr/bin/env python3
"""
Ultimate Companies House Scraper - Fixed Version
Proxy details hardcoded like the original script
Just run it - no need to enter proxy details!
"""

import os
import sys
import time
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
import csv
import json
from datetime import datetime, timedelta
from urllib.parse import quote_plus, urljoin
from lxml import html
import random
import threading
from queue import Queue
import logging
from dotenv import load_dotenv
import re

# PROXY CONFIGURATION - HARDCODED LIKE THE ORIGINAL
PROXY_USERNAME = "brd-customer-hl_997fefd5-zone-ch30oct22"
PROXY_PASSWORD = "kikhwzt80akq"
PROXY_PORT = "22225"

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('companies_house_ultimate_scraper.log'),
        logging.StreamHandler()
    ]
)

class CompaniesHouseUltimateScraper:
    def __init__(self, batch_size=100, use_proxy=True, output_mode='csv'):
        """
        Initialize the ultimate scraper
        
        Args:
            batch_size: Number of parallel requests (default 100 like original)
            use_proxy: Whether to use proxy (default True)
            output_mode: 'csv' or 'postgres'
        """
        self.batch_size = batch_size
        self.use_proxy = use_proxy
        self.output_mode = output_mode
        
        # Use the hardcoded proxy details
        self.proxy_username = PROXY_USERNAME
        self.proxy_password = PROXY_PASSWORD
        self.proxy_port = PROXY_PORT
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        self.lock = threading.Lock()
        self.results_queue = Queue()
        
        # Enhanced statistics
        self.stats = {
            'processed': 0,
            'found': 0,
            'not_found': 0,
            'errors': 0,
            'active': 0,
            'dissolved': 0,
            'liquidation': 0,
            'other_status': 0,
            'start_time': datetime.now(),
            'last_checkpoint': 0
        }
        
        # For tracking good requests in batch (like original)
        self.good_count = 0
        
        # Checkpoint file for resume capability
        self.checkpoint_file = 'scraper_checkpoint_ultimate.json'
        self.processed_companies = set()
        
        # Load checkpoint if exists
        self.load_checkpoint()
    
    def get_luminati_proxy_url(self, country=None):
        """Get proxy URL exactly like the original script"""
        if not self.use_proxy:
            return None
            
        if country == None:
            return 'http://' + self.proxy_username + '-session-' + str(random.random()) + ":" + self.proxy_password + '@zproxy.lum-superproxy.io:' + str(self.proxy_port)
        else:
            return 'http://' + self.proxy_username + '-country-' + country + '-session-' + str(random.random()) + ":" + self.proxy_password + '@zproxy.lum-superproxy.io:' + str(self.proxy_port)
    
    def load_checkpoint(self):
        """Load checkpoint for resume capability"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    checkpoint = json.load(f)
                    self.stats.update(checkpoint.get('stats', {}))
                    self.processed_companies = set(checkpoint.get('processed_companies', []))
                    print(f"\n✅ Checkpoint loaded: {len(self.processed_companies)} companies already processed")
                    print(f"   Previous stats: Found={self.stats['found']}, Active={self.stats['active']}, Dissolved={self.stats['dissolved']}")
                    return True
            except Exception as e:
                logging.error(f"Error loading checkpoint: {e}")
        return False
    
    def save_checkpoint(self):
        """Save checkpoint for resume capability"""
        try:
            checkpoint = {
                'stats': self.stats,
                'processed_companies': list(self.processed_companies),
                'timestamp': datetime.now().isoformat()
            }
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint, f)
            self.stats['last_checkpoint'] = self.stats['processed']
        except Exception as e:
            logging.error(f"Error saving checkpoint: {e}")
    
    def get_db_connection(self):
        """Create database connection"""
        if self.output_mode != 'postgres':
            return None
        
        return psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            database=os.getenv('DB_NAME', 'insideestates_app'),
            user=os.getenv('DB_USER', 'insideestates_user'),
            password=os.getenv('DB_PASSWORD', 'InsideEstates2024!')
        )
    
    def search_and_extract_company(self, company_info):
        """Search for company and extract all Overview data"""
        company_name = company_info['search_name']
        company_id = company_info.get('id', 0)
        
        # Skip if already processed
        if company_name in self.processed_companies:
            return None
        
        good_to_save = False
        
        try:
            # STEP 1: Search for company using proxy
            proxy_url = self.get_luminati_proxy_url()
            proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
            
            search_url = f'https://find-and-update.company-information.service.gov.uk/search/companies?q={quote_plus(company_name)}'
            
            response = self.session.get(search_url, timeout=15, proxies=proxies)
            response.raise_for_status()
            
            tree = html.fromstring(response.text)
            
            # Check for results (like original)
            results_els = tree.xpath("//ul[@id='results' and @class='results-list']/li[contains(@class, 'type-company')]")
            no_results = tree.xpath("//div[@id='no-results']/h2[contains(@id, 'no-results') and contains(text(), 'No results')]")
            
            if no_results and not results_els:
                print(f"[{company_id:5d}] {company_name[:40]:40} → NOT FOUND")
                with self.lock:
                    self.stats['not_found'] += 1
                    self.good_count += 1
                return {
                    'search_name': company_name,
                    'status': 'NOT_FOUND'
                }
            
            if not results_els:
                return None
            
            # Get first result (like original)
            first_result = results_els[0]
            url_el = first_result.xpath("./h3/a[contains(@href, 'company/')]")
            meta_el = first_result.xpath("./p[@class='meta crumbtrail']")
            
            if not url_el or not meta_el:
                return None
            
            # Parse like original
            company_url = urljoin('https://find-and-update.company-information.service.gov.uk/', url_el[0].get('href'))
            found_name = url_el[0].text_content().strip()
            meta_text = meta_el[0].text_content().strip()
            
            company_number = None
            if '-' in meta_text:
                company_number = meta_text[0:meta_text.find("-")].strip()
            else:
                company_number = company_url.split('/company/')[-1].split('/')[0]
            
            # STEP 2: Get Overview page
            proxy_url = self.get_luminati_proxy_url()  # New session
            proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
            
            response = self.session.get(company_url, timeout=15, proxies=proxies)
            response.raise_for_status()
            
            tree = html.fromstring(response.text)
            
            # Verify we got a valid page (like original checks for company-header)
            verificator_el = tree.xpath("//div[@class='company-header']/p[@id='company-number']")
            if not verificator_el:
                return None
            
            good_to_save = True
            
            data = {
                'search_name': company_name,
                'status': 'FOUND',
                'company_number': company_number,
                'company_name': found_name,
                'company_url': company_url,
                'search_meta_text': meta_text,
                'data_source': 'SCRAPE',
                'scrape_timestamp': datetime.now().isoformat()
            }
            
            # Extract Company Status (CRITICAL for verification)
            status_el = tree.xpath("//dd[@id='company-status']")
            if not status_el:
                status_el = tree.xpath("//dt[contains(text(), 'Company status')]/following-sibling::dd[1]")
            if not status_el:
                status_el = tree.xpath("//dd[contains(text(), 'Active') or contains(text(), 'Dissolved') or contains(text(), 'Liquidation')]")
            
            if status_el:
                company_status = status_el[0].text_content().strip()
                data['company_status'] = company_status
                
                # Track status statistics
                with self.lock:
                    if 'Active' in company_status:
                        self.stats['active'] += 1
                    elif 'Dissolved' in company_status:
                        self.stats['dissolved'] += 1
                    elif 'Liquidation' in company_status:
                        self.stats['liquidation'] += 1
                    else:
                        self.stats['other_status'] += 1
            else:
                company_status = 'Unknown'
                data['company_status'] = company_status
            
            # Show progress with Company Status
            print(f"[{company_id:5d}] {company_name[:40]:40} → {company_number} | {company_status:20} ✓")
            
            # Extract all other Overview data
            
            # Registered office address
            address_el = tree.xpath("//dd[contains(@class, 'address')]")
            if not address_el:
                address_el = tree.xpath("//dt[contains(text(), 'Registered office address')]/following-sibling::dd[1]")
            if not address_el:
                # Look for postcode pattern
                postcode_pattern = r'[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}'
                all_dd = tree.xpath("//dd")
                for dd in all_dd:
                    if re.search(postcode_pattern, dd.text_content()):
                        address_el = [dd]
                        break
            
            if address_el:
                address_text = address_el[0].text_content().strip()
                address_text = re.sub(r'\s+', ' ', address_text)
                data['registered_office_address'] = address_text
                
                # Extract postcode
                postcode_match = re.search(r'[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}', address_text)
                if postcode_match:
                    data['reg_address_postcode'] = postcode_match.group(0)
                
                # Parse address lines
                lines = [l.strip() for l in address_text.split(',')]
                if lines:
                    data['reg_address_address_line_1'] = lines[0] if len(lines) > 0 else ''
                    data['reg_address_address_line_2'] = lines[1] if len(lines) > 1 else ''
                    data['reg_address_locality'] = lines[2] if len(lines) > 2 else ''
                    data['reg_address_region'] = lines[3] if len(lines) > 3 else ''
            
            # Company type
            type_el = tree.xpath("//dd[@id='company-type']")
            if not type_el:
                type_el = tree.xpath("//dt[contains(text(), 'Company type')]/following-sibling::dd[1]")
            if type_el:
                data['company_category'] = type_el[0].text_content().strip()
            
            # Incorporation date
            inc_date_el = tree.xpath("//dd[@id='company-incorporation-date']")
            if not inc_date_el:
                inc_date_el = tree.xpath("//dt[contains(text(), 'Incorporated on')]/following-sibling::dd[1]")
            if not inc_date_el:
                # Look for date pattern
                date_pattern = r'\d{1,2}\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}'
                all_dd = tree.xpath("//dd")
                for dd in all_dd:
                    if re.match(date_pattern, dd.text_content().strip()):
                        inc_date_el = [dd]
                        break
            
            if inc_date_el:
                inc_date_text = inc_date_el[0].text_content().strip()
                data['incorporation_date_text'] = inc_date_text
                # Try to parse to standard format
                try:
                    inc_date = datetime.strptime(inc_date_text, '%d %B %Y')
                    data['incorporation_date'] = inc_date.strftime('%Y-%m-%d')
                except:
                    pass
            
            # Accounts information
            next_accounts_el = tree.xpath("//dt[contains(text(), 'Next accounts made up to')]/following-sibling::dd[1]")
            if next_accounts_el:
                data['accounts_next_made_up_to'] = next_accounts_el[0].text_content().strip()
            
            accounts_due_el = tree.xpath("//dt[contains(text(), 'due by') and preceding::h2[contains(text(), 'Accounts')]]/following-sibling::dd[1]")
            if accounts_due_el:
                data['accounts_next_due'] = accounts_due_el[0].text_content().strip()
            
            last_accounts_el = tree.xpath("//dt[contains(text(), 'Last accounts made up to')]/following-sibling::dd[1]")
            if last_accounts_el:
                data['accounts_last_made_up_to'] = last_accounts_el[0].text_content().strip()
            
            # Confirmation statement
            next_stmt_el = tree.xpath("//dt[contains(text(), 'Next statement date')]/following-sibling::dd[1]")
            if next_stmt_el:
                data['confirmation_statement_next_made_up_to'] = next_stmt_el[0].text_content().strip()
            
            stmt_due_el = tree.xpath("//dt[contains(text(), 'due by') and preceding::h2[contains(text(), 'Confirmation')]]/following-sibling::dd[1]")
            if stmt_due_el:
                data['confirmation_statement_next_due'] = stmt_due_el[0].text_content().strip()
            
            last_stmt_el = tree.xpath("//dt[contains(text(), 'Last statement dated')]/following-sibling::dd[1]")
            if last_stmt_el:
                data['confirmation_statement_last_made_up_to'] = last_stmt_el[0].text_content().strip()
            
            # SIC codes
            sic_codes = []
            sic_els = tree.xpath("//h2[contains(text(), 'Nature of business')]/following::ul[1]/li")
            if sic_els:
                for sic_el in sic_els:
                    sic_text = sic_el.text_content().strip()
                    if sic_text:
                        sic_codes.append(sic_text)
            
            # Store SIC codes
            for i, code in enumerate(sic_codes[:4]):
                data[f'sic_code_{i+1}'] = code
            
            if good_to_save:
                with self.lock:
                    self.stats['found'] += 1
                    self.good_count += 1
            
            return data
            
        except requests.exceptions.Timeout:
            print(f"[{company_id:5d}] {company_name[:40]:40} → TIMEOUT")
            with self.lock:
                self.stats['errors'] += 1
            return {'search_name': company_name, 'status': 'ERROR', 'error': 'Timeout'}
            
        except Exception as e:
            print(f"[{company_id:5d}] {company_name[:40]:40} → ERROR: {str(e)[:30]}")
            with self.lock:
                self.stats['errors'] += 1
            return {'search_name': company_name, 'status': 'ERROR', 'error': str(e)}
    
    def process_batch(self, batch):
        """Process a batch of companies with threading like original"""
        threads = []
        self.good_count = 0  # Reset for this batch
        
        for company in batch:
            thread = threading.Thread(target=self._process_company_thread, args=(company,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads
        for thread in threads:
            thread.join()
        
        # Return good count for this batch
        return self.good_count
    
    def _process_company_thread(self, company):
        """Thread worker"""
        result = self.search_and_extract_company(company)
        
        if result:
            self.results_queue.put(result)
            
            # Mark as processed
            with self.lock:
                self.processed_companies.add(company['search_name'])
                self.stats['processed'] += 1
    
    def save_results_csv(self, output_file, append=True):
        """Save results to CSV"""
        results = []
        while not self.results_queue.empty():
            results.append(self.results_queue.get())
        
        if not results:
            return
        
        # Determine all fieldnames
        fieldnames = set()
        for result in results:
            fieldnames.update(result.keys())
        
        # Order fieldnames logically
        ordered_fields = [
            'search_name', 'status', 'company_number', 'company_name', 
            'company_status', 'company_category', 'company_url',
            'registered_office_address', 'reg_address_postcode',
            'incorporation_date', 'incorporation_date_text',
            'accounts_next_made_up_to', 'accounts_next_due', 'accounts_last_made_up_to',
            'confirmation_statement_next_made_up_to', 'confirmation_statement_next_due',
            'confirmation_statement_last_made_up_to',
            'sic_code_1', 'sic_code_2', 'sic_code_3', 'sic_code_4',
            'search_meta_text', 'data_source', 'error', 'scrape_timestamp'
        ]
        
        # Add any remaining fields
        for field in sorted(fieldnames):
            if field not in ordered_fields:
                ordered_fields.append(field)
        
        # Write to CSV
        mode = 'a' if append and os.path.exists(output_file) else 'w'
        write_header = mode == 'w' or not os.path.exists(output_file)
        
        with open(output_file, mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=ordered_fields, extrasaction='ignore')
            
            if write_header:
                writer.writeheader()
            
            for result in results:
                writer.writerow(result)
    
    def show_progress(self, total, batch_num, batch_size, good_in_batch):
        """Show detailed progress with ETA and statistics"""
        elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
        
        if self.stats['processed'] > 0:
            rate = self.stats['processed'] / elapsed
            remaining = total - self.stats['processed']
            eta_seconds = remaining / rate if rate > 0 else 0
            eta = datetime.now() + timedelta(seconds=eta_seconds)
            
            print(f"\n{'='*80}")
            print(f"Current item {self.stats['processed']}/{total} - Good requests in this batch: {good_in_batch}/{batch_size}")
            print(f"{'='*80}")
            
            # Overall progress
            progress_pct = (self.stats['processed'] / total * 100) if total > 0 else 0
            print(f"Progress: {self.stats['processed']:,}/{total:,} ({progress_pct:.1f}%)")
            
            # Status breakdown
            print(f"\nStatus Breakdown:")
            print(f"  Found:        {self.stats['found']:6,} ({self.stats['found']/self.stats['processed']*100:5.1f}%)")
            print(f"  Not Found:    {self.stats['not_found']:6,} ({self.stats['not_found']/self.stats['processed']*100:5.1f}%)")
            print(f"  Errors:       {self.stats['errors']:6,} ({self.stats['errors']/self.stats['processed']*100:5.1f}%)")
            
            # Company status breakdown
            if self.stats['found'] > 0:
                print(f"\nCompany Status Breakdown (of {self.stats['found']} found):")
                print(f"  Active:       {self.stats['active']:6,} ({self.stats['active']/self.stats['found']*100:5.1f}%)")
                print(f"  Dissolved:    {self.stats['dissolved']:6,} ({self.stats['dissolved']/self.stats['found']*100:5.1f}%)")
                print(f"  Liquidation:  {self.stats['liquidation']:6,} ({self.stats['liquidation']/self.stats['found']*100:5.1f}%)")
                print(f"  Other:        {self.stats['other_status']:6,} ({self.stats['other_status']/self.stats['found']*100:5.1f}%)")
            
            # Performance metrics
            print(f"\nPerformance:")
            print(f"  Processing rate: {rate:.1f} companies/second")
            print(f"  Time elapsed: {timedelta(seconds=int(elapsed))}")
            print(f"  Time remaining: {timedelta(seconds=int(eta_seconds))}")
            print(f"  ETA: {eta.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Checkpoint info
            if self.stats['processed'] - self.stats['last_checkpoint'] >= 1000:
                print(f"\n💾 Saving checkpoint at {self.stats['processed']} companies...")
                self.save_checkpoint()
            
            print(f"{'='*80}\n")
    
    def process_all_companies(self, companies, output_file='companies_house_results.csv', start_from=0):
        """Process all companies with progress tracking"""
        total = len(companies)
        companies_to_process = []
        
        # Filter out already processed companies
        for company in companies[start_from:]:
            if company['search_name'] not in self.processed_companies:
                companies_to_process.append(company)
        
        print(f"\n🚀 Starting scrape of {len(companies_to_process)} companies")
        print(f"   (Skipping {len(companies) - len(companies_to_process)} already processed)")
        print(f"   Batch size: {self.batch_size}")
        print(f"   Output: {output_file if self.output_mode == 'csv' else 'PostgreSQL database'}")
        print(f"   Using proxy: {self.use_proxy}")
        print()
        
        # Process in batches
        batch_num = 0
        for i in range(0, len(companies_to_process), self.batch_size):
            batch = companies_to_process[i:i + self.batch_size]
            batch_num += 1
            
            print(f"\n📦 Processing batch {batch_num} ({len(batch)} companies)...")
            print("-" * 80)
            
            # Process batch and get good count
            good_in_batch = self.process_batch(batch)
            
            # Save results
            if self.output_mode == 'csv':
                self.save_results_csv(output_file)
            
            # Show progress with good count
            self.show_progress(total, batch_num, len(batch), good_in_batch)
            
            # No delay needed with proxy - it handles rate limiting
        
        # Final checkpoint
        self.save_checkpoint()
        
        # Final summary
        self.print_final_summary()
    
    def print_final_summary(self):
        """Print comprehensive final summary"""
        elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
        
        print(f"\n{'='*80}")
        print(f"🎉 SCRAPING COMPLETE!")
        print(f"{'='*80}")
        
        print(f"\nOverall Statistics:")
        print(f"  Total Processed: {self.stats['processed']:,}")
        print(f"  Successfully Found: {self.stats['found']:,} ({self.stats['found']/self.stats['processed']*100:.1f}%)")
        print(f"  Not Found: {self.stats['not_found']:,}")
        print(f"  Errors: {self.stats['errors']:,}")
        
        if self.stats['found'] > 0:
            print(f"\nCompany Status Distribution:")
            print(f"  Active Companies: {self.stats['active']:,} ({self.stats['active']/self.stats['found']*100:.1f}%)")
            print(f"  Dissolved Companies: {self.stats['dissolved']:,} ({self.stats['dissolved']/self.stats['found']*100:.1f}%)")
            print(f"  In Liquidation: {self.stats['liquidation']:,} ({self.stats['liquidation']/self.stats['found']*100:.1f}%)")
            print(f"  Other Status: {self.stats['other_status']:,} ({self.stats['other_status']/self.stats['found']*100:.1f}%)")
        
        print(f"\nPerformance Summary:")
        print(f"  Total Time: {timedelta(seconds=int(elapsed))}")
        print(f"  Average Rate: {self.stats['processed']/elapsed:.1f} companies/second")
        print(f"  Average Time per Company: {elapsed/self.stats['processed']:.2f} seconds")
        
        print(f"\n✅ Checkpoint saved for easy resume")
        print(f"   Run again to continue from where you left off")
        print(f"{'='*80}")


def main():
    """Main function - simplified like the original"""
    print("🏢 Ultimate Companies House Scraper")
    print("=" * 80)
    print("Features:")
    print("✓ Complete Overview page data extraction")
    print("✓ Real-time Company Status display")
    print("✓ Detailed progress tracking with ETA")
    print("✓ Resume capability with checkpoints")
    print("✓ Proxy settings already configured")
    print()
    
    # Input file (simple like original)
    input_file = input("Enter CSV filename (default: companies_to_scrape_122k.csv): ").strip()
    if not input_file:
        input_file = "companies_to_scrape_122k.csv"
    
    if not os.path.exists(input_file):
        print(f"ERROR: File not found: {input_file}")
        return
    
    # Load companies
    companies = []
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            company_name = (row.get('Company Name') or 
                          row.get('company_name') or 
                          row.get('search_name') or
                          list(row.values())[0])
            
            if company_name and company_name.strip():
                companies.append({
                    'search_name': company_name.strip(),
                    'id': idx + 1
                })
    
    print(f"✅ Loaded {len(companies)} companies from {input_file}")
    
    # Batch size (simple prompt)
    print("\nBatch Size Recommendations:")
    print("  - First run: 1000 (to test everything works)")
    print("  - Normal run: 100 (default, like original script)")
    print("  - Conservative: 50")
    
    batch_input = input("\nEnter batch size (default 100): ").strip()
    batch_size = int(batch_input) if batch_input.isdigit() else 100
    
    # Output file
    output_file = input("\nOutput CSV filename (default: companies_house_results.csv): ").strip()
    if not output_file:
        output_file = f"companies_house_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    # Start from (for resume)
    start_from = 0
    if os.path.exists('scraper_checkpoint_ultimate.json'):
        resume = input("\nCheckpoint found. Resume from last position? (y/n): ").lower()
        if resume != 'y':
            start_input = input("Start from index (default 0): ").strip()
            start_from = int(start_input) if start_input.isdigit() else 0
            # Clear checkpoint if starting fresh
            if start_from == 0:
                try:
                    os.remove('scraper_checkpoint_ultimate.json')
                except:
                    pass
    
    # Create scraper with hardcoded proxy settings
    scraper = CompaniesHouseUltimateScraper(
        batch_size=batch_size,
        use_proxy=True,  # Always use proxy
        output_mode='csv'
    )
    
    # Confirm and start
    print(f"\n📋 Ready to scrape {len(companies)} companies")
    print(f"   Batch size: {batch_size}")
    print(f"   Using proxy: Yes (Bright Data)")
    print(f"   Output: {output_file}")
    print(f"   Starting from: {start_from}")
    
    proceed = input("\n🚀 Start scraping? (y/n): ").lower()
    
    if proceed == 'y':
        try:
            scraper.process_all_companies(companies, output_file, start_from)
        except KeyboardInterrupt:
            print("\n\n⚠️  Scraping interrupted by user!")
            print("💾 Saving checkpoint...")
            scraper.save_checkpoint()
            print("✅ You can resume from where you left off by running the script again")
            scraper.print_final_summary()
    else:
        print("Scraping cancelled.")


if __name__ == '__main__':
    main()