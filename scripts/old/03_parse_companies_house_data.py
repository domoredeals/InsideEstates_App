#!/usr/bin/env python3
"""
Parse Companies House scraped HTML data and update database with structured information
"""

import os
import sys
import psycopg2
import bz2
from lxml import html
from datetime import datetime
from dotenv import load_dotenv
import re
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class CompaniesHouseParser:
    def __init__(self):
        pass
    
    def get_db_connection(self):
        """Create database connection"""
        return psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
    
    def parse_overview_pages(self, company_numbers=None, limit=None):
        """Parse overview pages and extract structured data"""
        conn = self.get_db_connection()
        cur = conn.cursor()
        
        try:
            # Get companies to parse
            if company_numbers:
                placeholders = ','.join(['%s'] * len(company_numbers))
                query = f"""
                    SELECT company_number, raw_html
                    FROM ch_scrape_overview
                    WHERE scrape_status = 'scraped'
                    AND company_number IN ({placeholders})
                """
                cur.execute(query, company_numbers)
            else:
                query = """
                    SELECT company_number, raw_html
                    FROM ch_scrape_overview
                    WHERE scrape_status = 'scraped'
                    ORDER BY company_number
                """
                if limit:
                    query += f" LIMIT {limit}"
                cur.execute(query)
            
            companies = cur.fetchall()
            logging.info(f"Parsing {len(companies)} company overview pages")
            
            for i, (company_number, raw_html) in enumerate(companies):
                if raw_html:
                    try:
                        parsed_data = self._parse_overview_html(raw_html)
                        
                        # Update database with parsed data
                        cur.execute("""
                            UPDATE ch_scrape_overview
                            SET company_name = %s,
                                company_status = %s,
                                incorporation_date = %s,
                                company_type = %s,
                                registered_office_address = %s,
                                sic_codes = %s,
                                previous_names = %s,
                                accounts_next_due = %s,
                                confirmation_statement_next_due = %s,
                                parse_timestamp = NOW(),
                                scrape_status = 'parsed'
                            WHERE company_number = %s
                        """, (
                            parsed_data.get('company_name'),
                            parsed_data.get('company_status'),
                            parsed_data.get('incorporation_date'),
                            parsed_data.get('company_type'),
                            parsed_data.get('registered_address'),
                            parsed_data.get('sic_codes'),
                            parsed_data.get('previous_names'),
                            parsed_data.get('accounts_next_due'),
                            parsed_data.get('confirmation_statement_next_due'),
                            company_number
                        ))
                        
                        if (i + 1) % 10 == 0:
                            logging.info(f"Progress: {i + 1}/{len(companies)} parsed")
                        
                    except Exception as e:
                        logging.error(f"Error parsing {company_number}: {e}")
                        cur.execute("""
                            UPDATE ch_scrape_overview
                            SET scrape_status = 'error',
                                scrape_error = %s
                            WHERE company_number = %s
                        """, (str(e), company_number))
            
            conn.commit()
            logging.info("Overview parsing completed")
            
        except Exception as e:
            logging.error(f"Error in parse_overview_pages: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()
    
    def _parse_overview_html(self, raw_html):
        """Parse a single overview HTML page"""
        # Decompress HTML
        html_content = bz2.decompress(raw_html).decode('utf-8')
        tree = html.fromstring(html_content)
        
        data = {}
        
        # Company name - multiple possible locations
        name_selectors = [
            '//p[@class="heading-xlarge"]/text()',
            '//h1[@class="heading-xlarge"]/text()',
            '//div[@class="company-header"]//p[@class="heading-xlarge"]/text()'
        ]
        for selector in name_selectors:
            name = tree.xpath(selector)
            if name:
                data['company_name'] = name[0].strip()
                break
        
        # Parse all dt/dd pairs in the document
        dt_dd_pairs = {}
        all_dts = tree.xpath('//dt')
        
        for dt in all_dts:
            dt_text = dt.text_content().strip().rstrip(':')
            # Get the immediately following dd
            dd = dt.getnext()
            if dd is not None and dd.tag == 'dd':
                dd_text = ' '.join(dd.text_content().split())
                dt_dd_pairs[dt_text.lower()] = dd_text
        
        # Map to our fields
        data['company_status'] = dt_dd_pairs.get('company status', dt_dd_pairs.get('status'))
        data['company_type'] = dt_dd_pairs.get('company type', dt_dd_pairs.get('type'))
        
        # Parse incorporation date
        inc_date_str = dt_dd_pairs.get('incorporated on', dt_dd_pairs.get('incorporated'))
        if inc_date_str:
            try:
                data['incorporation_date'] = datetime.strptime(inc_date_str, '%d %B %Y').date()
            except:
                try:
                    data['incorporation_date'] = datetime.strptime(inc_date_str, '%d %b %Y').date()
                except:
                    pass
            
        # Accounts dates - look for the heading with strong tag
        accounts_section = tree.xpath('//h2[strong[contains(text(), "Accounts")]]/following-sibling::p[1]')
        if accounts_section:
            accounts_text = accounts_section[0].text_content()
            # Look for "due by" date
            due_by_match = re.search(r'due by\s*(\d{1,2}\s+\w+\s+\d{4})', accounts_text, re.IGNORECASE)
            if due_by_match:
                try:
                    data['accounts_next_due'] = datetime.strptime(due_by_match.group(1), '%d %B %Y').date()
                except:
                    pass
        
        # Confirmation statement
        confirmation_section = tree.xpath('//h2[strong[contains(text(), "Confirmation statement")]]/following-sibling::p[1]')
        if confirmation_section:
            confirmation_text = confirmation_section[0].text_content()
            # Look for "due by" date
            due_by_match = re.search(r'due by\s*(\d{1,2}\s+\w+\s+\d{4})', confirmation_text, re.IGNORECASE)
            if due_by_match:
                try:
                    data['confirmation_statement_next_due'] = datetime.strptime(due_by_match.group(1), '%d %B %Y').date()
                except:
                    pass
        
        # Registered office address - try multiple approaches
        # First try: Look for dt with "Registered office address" text
        address_dt = tree.xpath('//dt[contains(text(), "Registered office address")]')
        if address_dt:
            # Get the next dd element
            address_dd = address_dt[0].getnext()
            if address_dd is not None and address_dd.tag == 'dd':
                address_text = address_dd.text_content().strip()
                if address_text:
                    data['registered_address'] = ', '.join(address_text.split())
        
        # Fallback: try the original method
        if not data.get('registered_address'):
            address_section = tree.xpath('//h2[contains(text(), "Registered office address")]/following-sibling::dl[1]')
            if address_section:
                address_parts = address_section[0].xpath('.//dd/text()')
                if address_parts:
                    data['registered_address'] = ', '.join([part.strip() for part in address_parts if part.strip()])
        
        # SIC codes
        sic_section = tree.xpath('//h2[contains(text(), "Nature of business")]/following-sibling::ul[1]')
        if sic_section:
            sic_codes = []
            sic_items = sic_section[0].xpath('.//li/text()')
            for sic in sic_items:
                # Extract just the code if present (format: "01110 - Growing of cereals")
                code_match = re.match(r'(\d+)', sic.strip())
                if code_match:
                    sic_codes.append(code_match.group(1))
            if sic_codes:
                data['sic_codes'] = sic_codes
        
        # Previous names
        prev_names_section = tree.xpath('//h2[contains(text(), "Previous company names")]/following-sibling::table[1]')
        if prev_names_section:
            prev_names = []
            # Get all rows except header
            rows = prev_names_section[0].xpath('.//tr[position()>1]')
            for row in rows:
                cells = row.xpath('.//td/text()')
                if cells and cells[0].strip():
                    prev_names.append(cells[0].strip())
            if prev_names:
                data['previous_names'] = prev_names
        
        return data
    
    def display_parsed_data(self, company_numbers):
        """Display parsed data for specific companies"""
        conn = self.get_db_connection()
        cur = conn.cursor()
        
        try:
            placeholders = ','.join(['%s'] * len(company_numbers))
            cur.execute(f"""
                SELECT o.company_number, o.company_name, o.company_status, o.company_type,
                       o.incorporation_date, o.registered_office_address, o.sic_codes,
                       o.previous_names, o.accounts_next_due, o.confirmation_statement_next_due,
                       q.search_name, q.found_name
                FROM ch_scrape_overview o
                LEFT JOIN ch_scrape_queue q ON o.company_number = q.company_number
                WHERE o.company_number IN ({placeholders})
            """, company_numbers)
            
            for row in cur.fetchall():
                print(f"\n{'='*80}")
                print(f"SEARCHED FOR: {row[10]}")
                print(f"FOUND AS: {row[11]}")
                print(f"{'='*80}")
                print(f"Company Number: {row[0]}")
                print(f"Company Name: {row[1]}")
                print(f"Status: {row[2]}")
                print(f"Type: {row[3]}")
                print(f"Incorporated: {row[4]}")
                print(f"Address: {row[5]}")
                
                if row[6]:  # SIC codes
                    print(f"SIC Codes: {', '.join(row[6])}")
                
                if row[7]:  # Previous names
                    print(f"Previous Names: {', '.join(row[7])}")
                
                if row[8]:  # Accounts due
                    print(f"Accounts Next Due: {row[8]}")
                
                if row[9]:  # Confirmation statement
                    print(f"Confirmation Statement Next Due: {row[9]}")
        
        finally:
            cur.close()
            conn.close()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Parse Companies House scraped data')
    parser.add_argument('--parse', choices=['overview', 'officers', 'charges', 'insolvency'],
                       help='Parse specific data type', default='overview')
    parser.add_argument('--companies', nargs='+', help='Specific company numbers to parse')
    parser.add_argument('--limit', type=int, help='Limit number of companies to parse')
    parser.add_argument('--display', nargs='+', help='Display parsed data for specific companies')
    
    args = parser.parse_args()
    
    parser_obj = CompaniesHouseParser()
    
    if args.display:
        parser_obj.display_parsed_data(args.display)
    else:
        if args.parse == 'overview':
            parser_obj.parse_overview_pages(company_numbers=args.companies, limit=args.limit)


if __name__ == '__main__':
    main()