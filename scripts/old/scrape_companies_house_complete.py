#!/usr/bin/env python3
"""
Complete Companies House scraper that extracts ALL Overview page data
and stores it directly in the PostgreSQL companies_house_data table
"""

import os
import sys
import time
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
import csv
from datetime import datetime
from urllib.parse import quote_plus, urljoin
from lxml import html
import random
import threading
from queue import Queue
import logging
from dotenv import load_dotenv
import re

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('companies_house_complete_scraper.log'),
        logging.StreamHandler()
    ]
)

class CompaniesHouseCompleteScraper:
    def __init__(self, batch_size=10, use_proxy=False, proxy_config=None):
        """Initialize the scraper"""
        self.batch_size = batch_size
        self.use_proxy = use_proxy
        self.proxy_config = proxy_config
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
            'inserted': 0,
            'updated': 0
        }
    
    def get_db_connection(self):
        """Create database connection"""
        return psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            database=os.getenv('DB_NAME', 'insideestates_app'),
            user=os.getenv('DB_USER', 'insideestates_user'),
            password=os.getenv('DB_PASSWORD', 'InsideEstates2024!')
        )
    
    def get_proxy_url(self):
        """Get proxy URL if configured"""
        if not self.use_proxy or not self.proxy_config:
            return None
        
        session_id = str(random.random())
        return f"http://{self.proxy_config['username']}-session-{session_id}:{self.proxy_config['password']}@zproxy.lum-superproxy.io:{self.proxy_config['port']}"
    
    def search_company(self, company_name):
        """Search for a company and return its URL and basic info"""
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
                    
                    return {
                        'company_url': company_url,
                        'company_number': company_number,
                        'found_name': found_name
                    }
            
            return None
            
        except Exception as e:
            logging.error(f"Error searching for {company_name}: {e}")
            return None
    
    def extract_overview_data(self, company_url, company_number):
        """Extract ALL data from the Overview page"""
        try:
            proxies = None
            if self.use_proxy:
                proxy_url = self.get_proxy_url()
                proxies = {"http": proxy_url, "https": proxy_url}
            
            response = self.session.get(company_url, timeout=15, proxies=proxies)
            response.raise_for_status()
            
            tree = html.fromstring(response.text)
            
            data = {
                'company_number': company_number,
                'data_source': 'SCRAPE'  # Important flag
            }
            
            # Company name
            name_el = tree.xpath("//h1[@class='heading-xlarge' or contains(@class, 'company-name')]")
            if name_el:
                data['company_name'] = name_el[0].text_content().strip()
            
            # Registered office address
            address_el = tree.xpath("//dd[contains(@class, 'address') or @id='reg-address' or contains(., 'United Kingdom')]")
            if not address_el:
                address_el = tree.xpath("//dl[@class='column-two-thirds']//dd[1]")
            
            if address_el:
                address_text = address_el[0].text_content().strip()
                # Clean up address
                address_lines = [line.strip() for line in address_text.split('\n') if line.strip()]
                data['reg_address_address_line_1'] = address_lines[0] if len(address_lines) > 0 else ''
                data['reg_address_address_line_2'] = address_lines[1] if len(address_lines) > 1 else ''
                data['reg_address_locality'] = address_lines[2] if len(address_lines) > 2 else ''
                data['reg_address_region'] = address_lines[3] if len(address_lines) > 3 else ''
                
                # Extract postcode (last line or pattern match)
                postcode_pattern = r'[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}'
                for line in reversed(address_lines):
                    postcode_match = re.search(postcode_pattern, line)
                    if postcode_match:
                        data['reg_address_postcode'] = postcode_match.group(0)
                        break
                
                # Full address for reference
                data['registered_office_address'] = ', '.join(address_lines)
            
            # Company status
            status_el = tree.xpath("//dd[@id='company-status' or contains(text(), 'Active') or contains(text(), 'Dissolved')]")
            if not status_el:
                status_el = tree.xpath("//dt[contains(text(), 'Company status')]/following-sibling::dd[1]")
            if status_el:
                data['company_status'] = status_el[0].text_content().strip()
            
            # Company type
            type_el = tree.xpath("//dd[@id='company-type' or contains(text(), 'Private') or contains(text(), 'Public')]")
            if not type_el:
                type_el = tree.xpath("//dt[contains(text(), 'Company type')]/following-sibling::dd[1]")
            if type_el:
                data['company_category'] = type_el[0].text_content().strip()
            
            # Incorporation date
            inc_date_el = tree.xpath("//dd[@id='company-incorporation-date' or contains(@class, 'incorporation')]")
            if not inc_date_el:
                inc_date_el = tree.xpath("//dt[contains(text(), 'Incorporated on')]/following-sibling::dd[1]")
            if inc_date_el:
                inc_date_text = inc_date_el[0].text_content().strip()
                # Parse date (e.g., "30 November 2020")
                try:
                    inc_date = datetime.strptime(inc_date_text, '%d %B %Y')
                    data['incorporation_date'] = inc_date.strftime('%Y-%m-%d')
                except:
                    data['incorporation_date'] = inc_date_text
            
            # Accounts information
            accounts_section = tree.xpath("//h2[contains(text(), 'Accounts')]/parent::*")
            if accounts_section:
                # Next accounts due
                next_accounts = tree.xpath("//dt[contains(text(), 'Next accounts made up to')]/following-sibling::dd[1]")
                if next_accounts:
                    data['accounts_next_made_up_to'] = next_accounts[0].text_content().strip()
                
                # Due by
                due_by = tree.xpath("//dt[contains(text(), 'due by')]/following-sibling::dd[1]")
                if due_by:
                    data['accounts_next_due'] = due_by[0].text_content().strip()
                
                # Last accounts made up to
                last_accounts = tree.xpath("//dt[contains(text(), 'Last accounts made up to')]/following-sibling::dd[1]")
                if last_accounts:
                    data['accounts_last_made_up_to'] = last_accounts[0].text_content().strip()
            
            # Confirmation statement
            confirm_section = tree.xpath("//h2[contains(text(), 'Confirmation statement')]/parent::*")
            if confirm_section:
                # Next statement due
                next_statement = tree.xpath("//dt[contains(text(), 'Next statement date')]/following-sibling::dd[1]")
                if next_statement:
                    data['confirmation_statement_next_due'] = next_statement[0].text_content().strip()
                
                # Last statement dated
                last_statement = tree.xpath("//dt[contains(text(), 'Last statement dated')]/following-sibling::dd[1]")
                if last_statement:
                    data['confirmation_statement_last_made_up_to'] = last_statement[0].text_content().strip()
            
            # SIC codes
            sic_section = tree.xpath("//h2[contains(text(), 'Nature of business')]/parent::*")
            if sic_section:
                sic_codes = []
                sic_els = tree.xpath("//span[@id='sic-code' or contains(@class, 'sic')]")
                if not sic_els:
                    sic_els = tree.xpath("//ul[contains(@class, 'sic')]/li")
                
                for sic_el in sic_els:
                    sic_text = sic_el.text_content().strip()
                    if sic_text:
                        # Extract just the code if format is "12345 - Description"
                        if ' - ' in sic_text:
                            code = sic_text.split(' - ')[0].strip()
                            sic_codes.append(code)
                        else:
                            sic_codes.append(sic_text)
                
                # Store up to 4 SIC codes
                for i, code in enumerate(sic_codes[:4]):
                    data[f'sic_code_{i+1}'] = code
            
            # Add scrape metadata
            data['scrape_timestamp'] = datetime.now()
            data['scrape_url'] = company_url
            
            return data
            
        except Exception as e:
            logging.error(f"Error extracting overview data from {company_url}: {e}")
            return None
    
    def save_to_database(self, data):
        """Save or update company data in PostgreSQL"""
        conn = self.get_db_connection()
        cur = conn.cursor()
        
        try:
            # Check if company already exists
            cur.execute("SELECT company_number FROM companies_house_data WHERE company_number = %s", 
                       (data['company_number'],))
            exists = cur.fetchone() is not None
            
            if exists:
                # Update existing record
                update_fields = []
                update_values = []
                
                for field, value in data.items():
                    if field not in ['company_number', 'scrape_timestamp']:
                        update_fields.append(f"{field} = %s")
                        update_values.append(value)
                
                update_fields.append("last_updated = %s")
                update_values.append(datetime.now())
                update_values.append(data['company_number'])
                
                update_sql = f"""
                    UPDATE companies_house_data 
                    SET {', '.join(update_fields)}
                    WHERE company_number = %s
                """
                
                cur.execute(update_sql, update_values)
                
                with self.lock:
                    self.stats['updated'] += 1
                    
            else:
                # Insert new record
                fields = list(data.keys())
                placeholders = ['%s'] * len(fields)
                
                insert_sql = f"""
                    INSERT INTO companies_house_data ({', '.join(fields)})
                    VALUES ({', '.join(placeholders)})
                """
                
                cur.execute(insert_sql, list(data.values()))
                
                with self.lock:
                    self.stats['inserted'] += 1
            
            conn.commit()
            return True
            
        except Exception as e:
            logging.error(f"Database error for company {data.get('company_number')}: {e}")
            conn.rollback()
            return False
        finally:
            cur.close()
            conn.close()
    
    def process_company_thread(self, company_info):
        """Thread worker to process a single company"""
        try:
            company_name = company_info['search_name']
            
            # Search for company
            search_result = self.search_company(company_name)
            
            if not search_result:
                with self.lock:
                    self.stats['not_found'] += 1
                    self.stats['processed'] += 1
                logging.info(f"Not found: {company_name}")
                return
            
            # Extract overview data
            overview_data = self.extract_overview_data(
                search_result['company_url'],
                search_result['company_number']
            )
            
            if overview_data:
                # Add search name for reference
                overview_data['search_name'] = company_name
                
                # Save to database
                if self.save_to_database(overview_data):
                    with self.lock:
                        self.stats['found'] += 1
                        self.stats['processed'] += 1
                    logging.info(f"Saved: {overview_data['company_name']} ({overview_data['company_number']})")
                else:
                    with self.lock:
                        self.stats['errors'] += 1
                        self.stats['processed'] += 1
            else:
                with self.lock:
                    self.stats['errors'] += 1
                    self.stats['processed'] += 1
                    
        except Exception as e:
            logging.error(f"Error processing {company_info.get('search_name')}: {e}")
            with self.lock:
                self.stats['errors'] += 1
                self.stats['processed'] += 1
    
    def process_companies(self, companies, start_from=0):
        """Process a list of companies"""
        total = len(companies)
        
        # Skip already processed
        companies = companies[start_from:]
        
        logging.info(f"Processing {len(companies)} companies (starting from index {start_from})")
        
        # Process in batches
        for i in range(0, len(companies), self.batch_size):
            batch = companies[i:i + self.batch_size]
            threads = []
            
            # Start threads for batch
            for company in batch:
                thread = threading.Thread(target=self.process_company_thread, args=(company,))
                threads.append(thread)
                thread.start()
            
            # Wait for batch to complete
            for thread in threads:
                thread.join()
            
            # Progress update
            current_index = start_from + i + len(batch)
            progress_pct = (current_index / total * 100) if total > 0 else 0
            
            logging.info(f"""
Progress: {current_index}/{total} ({progress_pct:.1f}%)
Found: {self.stats['found']} | Not Found: {self.stats['not_found']} | Errors: {self.stats['errors']}
Database: {self.stats['inserted']} inserted, {self.stats['updated']} updated
            """)
            
            # Delay between batches if not using proxy
            if not self.use_proxy:
                time.sleep(random.uniform(1, 2))
    
    def load_companies_from_csv(self, csv_file):
        """Load companies from CSV file"""
        companies = []
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                company_name = (row.get('Company Name') or 
                              row.get('company_name') or 
                              row.get('search_name') or
                              row.get('name') or
                              list(row.values())[0])
                
                if company_name and company_name.strip():
                    companies.append({
                        'search_name': company_name.strip()
                    })
        
        return companies


def main():
    """Main function"""
    print("Complete Companies House Scraper")
    print("=" * 60)
    print("This scraper extracts ALL Overview page data and stores it in PostgreSQL")
    print()
    
    # Input options
    print("Input options:")
    print("1. CSV file with company names")
    print("2. Direct from ch_scrape_queue table")
    
    choice = input("Choose input source (1 or 2): ").strip()
    
    companies = []
    
    if choice == '1':
        csv_file = input("Enter CSV filename: ").strip()
        if not os.path.exists(csv_file):
            print(f"File not found: {csv_file}")
            return
        
        scraper = CompaniesHouseCompleteScraper()
        companies = scraper.load_companies_from_csv(csv_file)
        print(f"Loaded {len(companies)} companies from CSV")
        
    else:
        # Load from database
        scraper = CompaniesHouseCompleteScraper()
        conn = scraper.get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get pending companies
        cur.execute("""
            SELECT search_name 
            FROM ch_scrape_queue 
            WHERE search_status = 'found' 
            AND company_number IS NOT NULL
            ORDER BY id
        """)
        
        for row in cur:
            companies.append({'search_name': row['search_name']})
        
        cur.close()
        conn.close()
        
        print(f"Loaded {len(companies)} companies from database")
    
    if not companies:
        print("No companies to process!")
        return
    
    # Configuration
    batch_size = input("Batch size (default 10): ").strip()
    batch_size = int(batch_size) if batch_size.isdigit() else 10
    
    use_proxy = input("Use proxy? (y/n): ").lower() == 'y'
    
    proxy_config = None
    if use_proxy:
        proxy_config = {
            'username': input("Proxy username: ").strip(),
            'password': input("Proxy password: ").strip(),
            'port': input("Proxy port (default 22225): ").strip() or "22225"
        }
    
    start_from = input("Start from index (default 0): ").strip()
    start_from = int(start_from) if start_from.isdigit() else 0
    
    # Create scraper and process
    scraper = CompaniesHouseCompleteScraper(
        batch_size=batch_size,
        use_proxy=use_proxy,
        proxy_config=proxy_config
    )
    
    try:
        print("\nStarting scrape...")
        scraper.process_companies(companies, start_from)
        
        print("\n" + "=" * 60)
        print("SCRAPING COMPLETE!")
        print("=" * 60)
        print(f"Total processed: {scraper.stats['processed']}")
        print(f"Found: {scraper.stats['found']}")
        print(f"Not found: {scraper.stats['not_found']}")
        print(f"Errors: {scraper.stats['errors']}")
        print(f"Database - Inserted: {scraper.stats['inserted']}, Updated: {scraper.stats['updated']}")
        
    except KeyboardInterrupt:
        print("\n\nScraping interrupted!")
        print(f"Processed {scraper.stats['processed']} companies")
        print("You can resume from where you left off using the start_from parameter")


if __name__ == '__main__':
    main()