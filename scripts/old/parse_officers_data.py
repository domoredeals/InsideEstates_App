#!/usr/bin/env python3
"""
Parse Companies House officers data from scraped HTML
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

def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

def parse_officers_for_company(company_number):
    """Parse officers HTML for a specific company"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get all officer records
        cur.execute("""
            SELECT officer_id, raw_html 
            FROM ch_scrape_officers 
            WHERE company_number = %s
            AND scrape_status = 'scraped'
        """, (company_number,))
        
        officers = cur.fetchall()
        if not officers:
            logging.info(f"No officers data found for {company_number}")
            return
        
        logging.info(f"Parsing {len(officers)} officers for {company_number}")
        
        parsed_count = 0
        for officer_id, raw_html in officers:
            if raw_html:
                officer_data = parse_single_officer(raw_html)
                
                if officer_data:
                    # Update the record with parsed data
                    cur.execute("""
                        UPDATE ch_scrape_officers
                        SET officer_name = %s,
                            officer_role = %s,
                            appointed_date = %s,
                            resigned_date = %s,
                            nationality = %s,
                            country_of_residence = %s,
                            occupation = %s,
                            date_of_birth_year = %s,
                            date_of_birth_month = %s,
                            address = %s,
                            scrape_status = 'parsed',
                            parse_timestamp = NOW()
                        WHERE company_number = %s
                        AND officer_id = %s
                    """, (
                        officer_data.get('name'),
                        officer_data.get('role'),
                        officer_data.get('appointed_date'),
                        officer_data.get('resigned_date'),
                        officer_data.get('nationality'),
                        officer_data.get('country_of_residence'),
                        officer_data.get('occupation'),
                        officer_data.get('birth_year'),
                        officer_data.get('birth_month'),
                        officer_data.get('address'),
                        company_number,
                        officer_id
                    ))
                    parsed_count += 1
        
        conn.commit()
        logging.info(f"Successfully parsed {parsed_count} officers for {company_number}")
        
        return parsed_count
        
    except Exception as e:
        logging.error(f"Error parsing officers for {company_number}: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def parse_single_officer(raw_html_bytes):
    """Parse a single officer HTML block"""
    try:
        # Decompress the HTML
        html_content = bz2.decompress(raw_html_bytes)
        if isinstance(html_content, bytes):
            html_content = html_content.decode('utf-8')
        
        tree = html.fromstring(html_content)
        
        officer_data = {}
        
        # Officer name - look for the name in the officer-name span
        # The name is inside an anchor tag within the span
        name_spans = tree.xpath('.//span[contains(@id, "officer-name")]')
        if name_spans:
            # Get all text content from the span, including from child elements
            name_text = name_spans[0].text_content().strip()
            if name_text:
                officer_data['name'] = name_text
        
        # Look for all dl/dt/dd structures
        dl_elements = tree.xpath('.//dl')
        
        for dl in dl_elements:
            dts = dl.xpath('./dt')
            for dt in dts:
                dt_text = dt.text_content().strip().lower()
                dd = dt.getnext()
                
                if dd is not None and dd.tag == 'dd':
                    dd_text = ' '.join(dd.text_content().split())
                    
                    # Map the fields
                    if 'role' in dt_text:
                        officer_data['role'] = dd_text
                    elif 'appointed' in dt_text and 'on' in dt_text:
                        try:
                            officer_data['appointed_date'] = datetime.strptime(dd_text, '%d %B %Y').date()
                        except:
                            try:
                                officer_data['appointed_date'] = datetime.strptime(dd_text, '%d %b %Y').date()
                            except:
                                pass
                    elif 'resigned' in dt_text:
                        try:
                            officer_data['resigned_date'] = datetime.strptime(dd_text, '%d %B %Y').date()
                        except:
                            try:
                                officer_data['resigned_date'] = datetime.strptime(dd_text, '%d %b %Y').date()
                            except:
                                pass
                    elif 'nationality' in dt_text:
                        officer_data['nationality'] = dd_text
                    elif 'country of residence' in dt_text:
                        officer_data['country_of_residence'] = dd_text
                    elif 'occupation' in dt_text:
                        officer_data['occupation'] = dd_text
                    elif 'date of birth' in dt_text:
                        # Parse month and year
                        dob_match = re.search(r'(\w+)\s+(\d{4})', dd_text)
                        if dob_match:
                            month_str = dob_match.group(1)
                            year_str = dob_match.group(2)
                            
                            # Convert month name to number
                            try:
                                month_date = datetime.strptime(month_str, '%B')
                                officer_data['birth_month'] = month_date.month
                            except:
                                try:
                                    month_date = datetime.strptime(month_str, '%b')
                                    officer_data['birth_month'] = month_date.month
                                except:
                                    pass
                            
                            officer_data['birth_year'] = int(year_str)
                    elif 'correspondence address' in dt_text or 'address' in dt_text:
                        # Get all text nodes in the dd
                        address_parts = dd.xpath('.//text()')
                        address = ', '.join([part.strip() for part in address_parts if part.strip()])
                        officer_data['address'] = address
        
        return officer_data
        
    except Exception as e:
        logging.error(f"Error parsing officer HTML: {e}")
        return None

def display_officers(company_number):
    """Display parsed officers for a company"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT officer_name, officer_role, appointed_date, resigned_date,
                   nationality, country_of_residence, occupation,
                   date_of_birth_year, date_of_birth_month, address
            FROM ch_scrape_officers
            WHERE company_number = %s
            ORDER BY appointed_date DESC
        """, (company_number,))
        
        officers = cur.fetchall()
        
        print(f"\nOfficers for company {company_number}:")
        print(f"Total officers: {len(officers)}")
        print("-" * 80)
        
        for officer in officers:
            print(f"\nName: {officer[0]}")
            print(f"Role: {officer[1]}")
            print(f"Appointed: {officer[2]}")
            if officer[3]:
                print(f"Resigned: {officer[3]}")
            if officer[4]:
                print(f"Nationality: {officer[4]}")
            if officer[5]:
                print(f"Country of Residence: {officer[5]}")
            if officer[6]:
                print(f"Occupation: {officer[6]}")
            if officer[7] and officer[8]:
                print(f"Date of Birth: {officer[8]}/{officer[7]}")
            if officer[9]:
                print(f"Address: {officer[9][:100]}...")
                
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Parse Companies House officers data')
    parser.add_argument('company_number', help='Company number to parse officers for')
    parser.add_argument('--display', action='store_true', help='Display parsed officers')
    
    args = parser.parse_args()
    
    if args.display:
        display_officers(args.company_number)
    else:
        parse_officers_for_company(args.company_number)
        display_officers(args.company_number)