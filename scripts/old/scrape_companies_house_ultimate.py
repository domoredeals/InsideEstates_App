#!/usr/bin/env python3
"""
Ultimate Companies House Scraper with ALL features
- Complete Overview page data extraction
- Detailed progress tracking with ETA
- Shows Company Status in real-time
- Resume capability with checkpoints
- Configurable batch sizes
- CSV and PostgreSQL output options
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
import pickle

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
    def __init__(self, batch_size=10, use_proxy=False, proxy_config=None, output_mode='csv'):
        """
        Initialize the ultimate scraper
        
        Args:
            batch_size: Number of parallel requests
            use_proxy: Whether to use proxy
            proxy_config: Proxy configuration dict
            output_mode: 'csv' or 'postgres'
        """
        self.batch_size = batch_size
        self.use_proxy = use_proxy
        self.proxy_config = proxy_config or {}
        self.output_mode = output_mode
        
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
        
        # Checkpoint file for resume capability
        self.checkpoint_file = 'scraper_checkpoint_ultimate.json'
        self.processed_companies = set()
        
        # Load checkpoint if exists
        self.load_checkpoint()
    
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
    
    def get_proxy_url(self):
        """Get proxy URL if configured"""
        if not self.use_proxy or not self.proxy_config:
            return None
        
        session_id = str(random.random())
        username = self.proxy_config.get('username', '')
        password = self.proxy_config.get('password', '')
        port = self.proxy_config.get('port', '22225')
        
        return f"http://{username}-session-{session_id}:{password}@zproxy.lum-superproxy.io:{port}"
    
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
        
        try:
            # STEP 1: Search for company
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
                print(f"[{company_id:5d}] {company_name[:40]:40} → NOT FOUND")
                with self.lock:
                    self.stats['not_found'] += 1
                return {
                    'search_name': company_name,
                    'status': 'NOT_FOUND'
                }
            
            if not results_els:
                return None
            
            # Get first result
            first_result = results_els[0]
            url_el = first_result.xpath("./h3/a[contains(@href, 'company/')]")
            
            if not url_el:
                return None
            
            company_url = urljoin('https://find-and-update.company-information.service.gov.uk/', 
                                url_el[0].get('href'))
            found_name = url_el[0].text_content().strip()
            company_number = company_url.split('/company/')[-1].split('/')[0]
            
            # Get meta text
            meta_el = first_result.xpath("./p[@class='meta crumbtrail']")
            meta_text = meta_el[0].text_content().strip() if meta_el else ''
            
            # STEP 2: Extract Overview data
            response = self.session.get(company_url, timeout=15, proxies=proxies)
            response.raise_for_status()
            
            tree = html.fromstring(response.text)
            
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
            
            with self.lock:
                self.stats['found'] += 1
            
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
        """Process a batch of companies"""
        threads = []
        
        for company in batch:
            thread = threading.Thread(target=self._process_company_thread, args=(company,))
            threads.append(thread)
            thread.start()
            
            # Small delay between thread starts
            time.sleep(0.05)
        
        # Wait for all threads
        for thread in threads:
            thread.join(timeout=30)
    
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
    
    def save_results_postgres(self):
        """Save results to PostgreSQL"""
        results = []
        while not self.results_queue.empty():
            results.append(self.results_queue.get())
        
        if not results:
            return
        
        conn = self.get_db_connection()
        if not conn:
            return
        
        cur = conn.cursor()
        
        for result in results:
            if result.get('status') != 'FOUND':
                continue
            
            try:
                # Check if exists
                cur.execute("SELECT company_number FROM companies_house_data WHERE company_number = %s", 
                           (result['company_number'],))
                exists = cur.fetchone() is not None
                
                if exists:
                    # Update - build dynamic update query
                    update_fields = []
                    update_values = []
                    
                    for field, value in result.items():
                        if field not in ['company_number', 'search_name', 'status', 'error']:
                            update_fields.append(f"{field} = %s")
                            update_values.append(value)
                    
                    update_values.append(result['company_number'])
                    
                    update_sql = f"""
                        UPDATE companies_house_data 
                        SET {', '.join(update_fields)}
                        WHERE company_number = %s
                    """
                    cur.execute(update_sql, update_values)
                else:
                    # Insert - filter to valid columns only
                    valid_fields = []
                    valid_values = []
                    
                    # Map fields to database columns
                    field_mapping = {
                        'company_number': 'company_number',
                        'company_name': 'company_name',
                        'company_status': 'company_status',
                        'company_category': 'company_category',
                        'registered_office_address': 'registered_office_address',
                        'reg_address_postcode': 'reg_address_postcode',
                        'incorporation_date': 'incorporation_date',
                        'accounts_next_made_up_to': 'accounts_next_made_up_to',
                        'accounts_next_due': 'accounts_next_due',
                        'accounts_last_made_up_to': 'accounts_last_made_up_to',
                        'confirmation_statement_next_made_up_to': 'confirmation_statement_next_made_up_to',
                        'confirmation_statement_next_due': 'confirmation_statement_next_due',
                        'confirmation_statement_last_made_up_to': 'confirmation_statement_last_made_up_to',
                        'sic_code_1': 'sic_code_1',
                        'sic_code_2': 'sic_code_2',
                        'sic_code_3': 'sic_code_3',
                        'sic_code_4': 'sic_code_4',
                        'data_source': 'data_source'
                    }
                    
                    for field, db_column in field_mapping.items():
                        if field in result and result[field]:
                            valid_fields.append(db_column)
                            valid_values.append(result[field])
                    
                    if valid_fields:
                        placeholders = ['%s'] * len(valid_fields)
                        insert_sql = f"""
                            INSERT INTO companies_house_data ({', '.join(valid_fields)})
                            VALUES ({', '.join(placeholders)})
                        """
                        cur.execute(insert_sql, valid_values)
                
                conn.commit()
                
            except Exception as e:
                logging.error(f"Database error for {result.get('company_number')}: {e}")
                conn.rollback()
        
        cur.close()
        conn.close()
    
    def show_progress(self, total, batch_num, batch_size):
        """Show detailed progress with ETA and statistics"""
        elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
        
        if self.stats['processed'] > 0:
            rate = self.stats['processed'] / elapsed
            remaining = total - self.stats['processed']
            eta_seconds = remaining / rate if rate > 0 else 0
            eta = datetime.now() + timedelta(seconds=eta_seconds)
            
            print(f"\n{'='*80}")
            print(f"PROGRESS UPDATE - Batch {batch_num} completed")
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
            
            # Process batch
            self.process_batch(batch)
            
            # Save results
            if self.output_mode == 'csv':
                self.save_results_csv(output_file)
            else:
                self.save_results_postgres()
            
            # Show progress
            self.show_progress(total, batch_num, self.batch_size)
            
            # Delay between batches if not using proxy
            if not self.use_proxy and i + self.batch_size < len(companies_to_process):
                delay = random.uniform(1, 3)
                print(f"⏳ Waiting {delay:.1f} seconds before next batch...")
                time.sleep(delay)
        
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
    """Main function with interactive setup"""
    print("🏢 Ultimate Companies House Scraper")
    print("=" * 80)
    print("Features:")
    print("✓ Complete Overview page data extraction")
    print("✓ Real-time Company Status display")
    print("✓ Detailed progress tracking with ETA")
    print("✓ Resume capability with checkpoints")
    print("✓ Configurable batch sizes")
    print("✓ CSV and PostgreSQL output options")
    print()
    
    # Input source - Default to CSV
    print("Input Options:")
    print("1. CSV file with company names")
    print("2. Load from PostgreSQL ch_scrape_queue")
    
    input_choice = input("\nChoose input source (1 or 2, default 1): ").strip() or '1'
    
    companies = []
    
    if input_choice == '1':
        csv_file = input("Enter CSV filename (default: companies_to_scrape_122k.csv): ").strip()
        if not csv_file:
            csv_file = "companies_to_scrape_122k.csv"
        
        if not os.path.exists(csv_file):
            print(f"ERROR: File not found: {csv_file}")
            return
        
        # Load from CSV
        with open(csv_file, 'r', encoding='utf-8') as f:
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
        
        print(f"✅ Loaded {len(companies)} companies from CSV")
    
    else:
        # Load from database
        scraper = CompaniesHouseUltimateScraper(output_mode='postgres')
        conn = scraper.get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT id, search_name 
            FROM ch_scrape_queue 
            WHERE search_status = 'found'
            ORDER BY id
        """)
        
        for row in cur:
            companies.append({
                'search_name': row['search_name'],
                'id': row['id']
            })
        
        cur.close()
        conn.close()
        
        print(f"✅ Loaded {len(companies)} companies from database")
    
    if not companies:
        print("No companies to process!")
        return
    
    # Configuration
    print("\n⚙️  Configuration:")
    
    # Batch size with recommendation
    print("\nBatch Size Recommendations:")
    print("  - First run: 1000 (to test everything works)")
    print("  - Then: 100 (with proxy for fast processing)")
    
    batch_input = input("\nEnter batch size (default 100): ").strip()
    batch_size = int(batch_input) if batch_input.isdigit() else 100
    
    # Proxy configuration - HARDCODED for easy use
    use_proxy = True  # Always use proxy for speed
    
    proxy_config = {
        'username': "brd-customer-hl_997fefd5-zone-ch30oct22",
        'password': "kikhwzt80akq",
        'port': "22225"
    }
    
    print("\n✅ Using Bright Data proxy for fast parallel processing")
    
    # Output mode - Default to CSV for Windows
    print("\nOutput Options:")
    print("1. CSV file (default)")
    print("2. PostgreSQL database")
    
    output_choice = input("Choose output (1 or 2, default 1): ").strip() or '1'
    output_mode = 'postgres' if output_choice == '2' else 'csv'
    
    output_file = None
    if output_mode == 'csv':
        output_file = input("\nOutput CSV filename (press Enter for auto-generated): ").strip()
        if not output_file:
            output_file = f"companies_house_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    # Start from
    start_from = input("\nStart from index (default 0, use for manual resume): ").strip()
    start_from = int(start_from) if start_from.isdigit() else 0
    
    # Create scraper
    scraper = CompaniesHouseUltimateScraper(
        batch_size=batch_size,
        use_proxy=use_proxy,
        proxy_config=proxy_config,
        output_mode=output_mode
    )
    
    # Confirm and start
    print(f"\n📋 Ready to scrape {len(companies)} companies")
    print(f"   Batch size: {batch_size}")
    print(f"   Using proxy: {use_proxy}")
    print(f"   Output: {output_file if output_mode == 'csv' else 'PostgreSQL'}")
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