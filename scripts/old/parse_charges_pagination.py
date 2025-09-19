#!/usr/bin/env python3
"""
Parse Companies House charges data from multiple pages
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

def parse_charges_for_company(company_number):
    """Parse all charges HTML pages for a specific company"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # First, delete existing parsed charges to avoid duplicates
        cur.execute("""
            DELETE FROM ch_scrape_charges 
            WHERE company_number = %s 
            AND charge_id NOT LIKE 'page_%%'
        """, (company_number,))
        
        # Get all page records
        cur.execute("""
            SELECT charge_id, raw_html 
            FROM ch_scrape_charges 
            WHERE company_number = %s
            AND charge_id LIKE 'page_%%'
            AND scrape_status = 'scraped'
            ORDER BY charge_id
        """, (company_number,))
        
        pages = cur.fetchall()
        if not pages:
            logging.info(f"No charges pages found for {company_number}")
            return
        
        logging.info(f"Parsing {len(pages)} pages of charges for {company_number}")
        
        total_charges = 0
        
        for page_id, raw_html in pages:
            if raw_html:
                page_charges = parse_charges_page(raw_html, company_number, cur)
                total_charges += page_charges
                logging.info(f"Parsed {page_charges} charges from {page_id}")
        
        conn.commit()
        logging.info(f"Successfully parsed {total_charges} charges for {company_number}")
        
        return total_charges
        
    except Exception as e:
        logging.error(f"Error parsing charges for {company_number}: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def parse_charges_page(raw_html_bytes, company_number, cur):
    """Parse a single page of charges HTML"""
    try:
        # Decompress the HTML
        html_content = bz2.decompress(raw_html_bytes)
        if isinstance(html_content, bytes):
            html_content = html_content.decode('utf-8')
        
        tree = html.fromstring(html_content)
        
        # Find all charge container divs by looking for class that equals 'mortgage-X'
        charge_elements = []
        for i in range(1, 100):  # Check up to 100 charges per page
            elements = tree.xpath(f"//div[@class='mortgage-{i}']")
            if not elements:
                break
            charge_elements.extend(elements)
        
        parsed_count = 0
        for charge_el in charge_elements:
            charge_data = parse_single_charge(charge_el)
            
            if charge_data and charge_data.get('charge_id'):
                # Insert the parsed charge
                cur.execute("""
                    INSERT INTO ch_scrape_charges (
                        company_number, charge_id, charge_status, charge_type,
                        delivered_date, created_date, satisfied_date,
                        amount, persons_entitled, brief_description,
                        charge_link, scrape_status, parse_timestamp
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'parsed', NOW())
                    ON CONFLICT (company_number, charge_id) DO UPDATE
                    SET charge_status = EXCLUDED.charge_status,
                        charge_type = EXCLUDED.charge_type,
                        delivered_date = EXCLUDED.delivered_date,
                        created_date = EXCLUDED.created_date,
                        satisfied_date = EXCLUDED.satisfied_date,
                        amount = EXCLUDED.amount,
                        persons_entitled = EXCLUDED.persons_entitled,
                        brief_description = EXCLUDED.brief_description,
                        charge_link = EXCLUDED.charge_link,
                        scrape_status = 'parsed',
                        parse_timestamp = NOW()
                """, (
                    company_number,
                    charge_data.get('charge_id'),
                    charge_data.get('status'),
                    charge_data.get('type'),
                    charge_data.get('delivered_date'),
                    charge_data.get('created_date'),
                    charge_data.get('satisfied_date'),
                    charge_data.get('amount'),
                    charge_data.get('persons_entitled'),
                    charge_data.get('description'),
                    charge_data.get('charge_link')
                ))
                parsed_count += 1
        
        return parsed_count
        
    except Exception as e:
        logging.error(f"Error parsing charges page: {e}")
        return 0

def parse_single_charge(charge_element):
    """Parse a single charge element"""
    charge_data = {}
    
    try:
        # Get charge code from heading - try both with and without border
        heading_link = charge_element.xpath('.//h2[contains(@class, "heading-medium")]/a')
        if heading_link:
            # Extract text and href
            charge_text = heading_link[0].text_content().strip()
            href = heading_link[0].get('href', '')
            
            # Store the link for detail scraping
            if href:
                charge_data['charge_link'] = href
            
            if 'Charge code' in charge_text:
                # Outstanding charges have "Charge code SC00 2116 0067"
                charge_data['charge_id'] = charge_text.replace('Charge code ', '').strip()
            else:
                # Satisfied charges just have the description like "First fixed charge over shares"
                # Use the href to extract the charge ID
                if '/charges/' in href:
                    url_id = href.split('/charges/')[-1]
                    charge_data['charge_id'] = url_id
                else:
                    # Fallback - use description but should not happen
                    charge_data['charge_id'] = charge_text
        
        # Get status from dd element
        status_dd = charge_element.xpath('.//dt[contains(text(), "Status")]/following-sibling::dd[1]/text()')
        if status_dd:
            charge_data['status'] = status_dd[0].strip()
        
        # Parse all dd/dt pairs
        dt_elements = charge_element.xpath('.//dt')
        for dt in dt_elements:
            dt_text = dt.text_content().strip().lower()
            dd = dt.getnext()
            
            if dd is not None and dd.tag == 'dd':
                dd_text = ' '.join(dd.text_content().split())
                
                if 'delivered' in dt_text:
                    try:
                        charge_data['delivered_date'] = datetime.strptime(dd_text, '%d %B %Y').date()
                    except:
                        pass
                elif 'created' in dt_text:
                    try:
                        charge_data['created_date'] = datetime.strptime(dd_text, '%d %B %Y').date()
                    except:
                        pass
                elif 'satisfied' in dt_text:
                    try:
                        charge_data['satisfied_date'] = datetime.strptime(dd_text, '%d %B %Y').date()
                    except:
                        pass
                elif 'amount' in dt_text:
                    charge_data['amount'] = dd_text
        
        # Get persons entitled from the list
        persons_list = charge_element.xpath('.//h3[contains(text(), "Persons entitled")]/following::ul[1]/li/text()')
        if persons_list:
            charge_data['persons_entitled'] = [p.strip() for p in persons_list]
        
        # Get brief description
        desc_elem = charge_element.xpath('.//h3[contains(text(), "Brief description")]/following::p[1]/text()')
        if desc_elem:
            charge_data['description'] = desc_elem[0].strip()
        
        return charge_data
        
    except Exception as e:
        logging.error(f"Error parsing single charge: {e}")
        return None

def display_charges(company_number):
    """Display parsed charges for a company"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT charge_id, charge_status, created_date, satisfied_date,
                   persons_entitled, brief_description
            FROM ch_scrape_charges
            WHERE company_number = %s
            AND charge_id NOT LIKE 'page_%%'
            ORDER BY created_date DESC
        """, (company_number,))
        
        charges = cur.fetchall()
        
        print(f"\nCharges for company {company_number}:")
        print(f"Total charges: {len(charges)}")
        
        outstanding = [c for c in charges if c[1] != 'Satisfied']
        satisfied = [c for c in charges if c[1] == 'Satisfied']
        
        print(f"Outstanding: {len(outstanding)}, Satisfied: {len(satisfied)}")
        print("-" * 80)
        
        if outstanding:
            print("\nOUTSTANDING CHARGES:")
            for charge in outstanding[:5]:
                print(f"\n{charge[0]} - Status: {charge[1]}")
                print(f"Created: {charge[2]}")
                if charge[4]:
                    print(f"Persons Entitled: {', '.join(charge[4])}")
                if charge[5]:
                    print(f"Description: {charge[5][:100]}...")
            
            if len(outstanding) > 5:
                print(f"\n... and {len(outstanding) - 5} more outstanding charges")
        
        if satisfied:
            print(f"\n\nSATISFIED CHARGES: {len(satisfied)} total")
            for charge in satisfied[:3]:
                print(f"\n{charge[0]} - Satisfied on {charge[3]}")
                print(f"Created: {charge[2]}")
                
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Parse Companies House charges data')
    parser.add_argument('company_number', help='Company number to parse charges for')
    parser.add_argument('--display', action='store_true', help='Display parsed charges')
    
    args = parser.parse_args()
    
    if args.display:
        display_charges(args.company_number)
    else:
        parse_charges_for_company(args.company_number)
        display_charges(args.company_number)