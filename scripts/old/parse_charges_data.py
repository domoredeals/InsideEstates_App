#!/usr/bin/env python3
"""
Parse Companies House charges data from scraped HTML
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
    """Parse charges HTML for a specific company"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get the raw HTML from the main record (where charge_id is NULL)
        cur.execute("""
            SELECT raw_html 
            FROM ch_scrape_charges 
            WHERE company_number = %s
            AND charge_id IS NULL
        """, (company_number,))
        
        result = cur.fetchone()
        if not result or not result[0]:
            logging.info(f"No charges data found for {company_number}")
            return
        
        # Decompress and parse
        html_content = bz2.decompress(result[0]).decode('utf-8')
        tree = html.fromstring(html_content)
        
        # Find all charge blocks
        charge_blocks = tree.xpath('//div[starts-with(@class, "mortgage-")]')
        logging.info(f"Found {len(charge_blocks)} charges for {company_number}")
        
        # Clear existing parsed charges (but not the main record with raw HTML)
        cur.execute("""
            DELETE FROM ch_scrape_charges 
            WHERE company_number = %s 
            AND charge_id IS NOT NULL
        """, (company_number,))
        
        charges_data = []
        
        for block in charge_blocks:
            charge_data = parse_single_charge(block)
            if charge_data:
                charges_data.append(charge_data)
        
        # Insert individual charges
        for i, charge in enumerate(charges_data):
            cur.execute("""
                INSERT INTO ch_scrape_charges (
                    company_number, charge_id, charge_status, charge_type,
                    delivered_date, created_date, satisfied_date,
                    amount, persons_entitled, brief_description,
                    scrape_status, parse_timestamp
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'parsed', NOW()
                )
                ON CONFLICT (company_number, charge_id) DO UPDATE SET
                    charge_status = EXCLUDED.charge_status,
                    charge_type = EXCLUDED.charge_type,
                    delivered_date = EXCLUDED.delivered_date,
                    created_date = EXCLUDED.created_date,
                    satisfied_date = EXCLUDED.satisfied_date,
                    amount = EXCLUDED.amount,
                    persons_entitled = EXCLUDED.persons_entitled,
                    brief_description = EXCLUDED.brief_description,
                    parse_timestamp = NOW()
            """, (
                company_number,
                f"{company_number}_charge_{i+1}",
                charge.get('status'),
                charge.get('type'),
                charge.get('delivered_date'),
                charge.get('created_date'),
                charge.get('satisfied_date'),
                charge.get('amount'),
                charge.get('persons_entitled'),
                charge.get('description')
            ))
        
        # Update the main record to show it's been parsed
        cur.execute("""
            UPDATE ch_scrape_charges
            SET scrape_status = 'parsed',
                parse_timestamp = NOW()
            WHERE company_number = %s
            AND charge_id IS NULL
        """, (company_number,))
        
        conn.commit()
        logging.info(f"Successfully parsed {len(charges_data)} charges for {company_number}")
        
        return charges_data
        
    except Exception as e:
        logging.error(f"Error parsing charges for {company_number}: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def parse_single_charge(charge_block):
    """Parse a single charge block"""
    charge_data = {}
    
    # Get charge code from heading
    heading = charge_block.xpath('.//h2/a/text()')
    if heading:
        charge_code_match = re.search(r'Charge code\s+(.+)', heading[0])
        if charge_code_match:
            charge_data['code'] = charge_code_match.group(1).strip()
    
    # Get status
    status = charge_block.xpath('.//dd[contains(@id, "mortgage-status")]/text()')
    if status:
        charge_data['status'] = status[0].strip()
    
    # Get created date
    created = charge_block.xpath('.//dd[contains(@id, "mortgage-created-on")]/text()')
    if created:
        try:
            charge_data['created_date'] = datetime.strptime(created[0].strip(), '%d %B %Y').date()
        except:
            pass
    
    # Get delivered date
    delivered = charge_block.xpath('.//dd[contains(@id, "mortgage-delivered-on")]/text()')
    if delivered:
        try:
            charge_data['delivered_date'] = datetime.strptime(delivered[0].strip(), '%d %B %Y').date()
        except:
            pass
    
    # Get satisfied date if exists
    satisfied = charge_block.xpath('.//dd[contains(@id, "mortgage-satisfied-on")]/text()')
    if satisfied:
        try:
            charge_data['satisfied_date'] = datetime.strptime(satisfied[0].strip(), '%d %B %Y').date()
        except:
            pass
    
    # Get persons entitled
    persons = charge_block.xpath('.//li[contains(@id, "persons-entitled")]/text()')
    if persons:
        charge_data['persons_entitled'] = [p.strip() for p in persons]
    
    # Get brief description
    description = charge_block.xpath('.//p[contains(@id, "mortgage-particulars")]/text()')
    if description:
        charge_data['description'] = description[0].strip()
    
    # Try to determine charge type from description
    if charge_data.get('description'):
        desc_lower = charge_data['description'].lower()
        if 'fixed charge' in desc_lower:
            charge_data['type'] = 'Fixed charge'
        elif 'floating charge' in desc_lower:
            charge_data['type'] = 'Floating charge'
        elif 'fixed and floating' in desc_lower:
            charge_data['type'] = 'Fixed and floating charge'
    
    return charge_data

def display_charges(company_number):
    """Display parsed charges for a company"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT charge_id, charge_status, charge_type, 
                   created_date, delivered_date, satisfied_date,
                   persons_entitled, brief_description
            FROM ch_scrape_charges
            WHERE company_number = %s
            AND charge_id IS NOT NULL
            ORDER BY created_date DESC
        """, (company_number,))
        
        charges = cur.fetchall()
        
        print(f"\nCharges for company {company_number}:")
        print(f"Total charges: {len(charges)}")
        print("-" * 80)
        
        for charge in charges[:5]:  # Show first 5
            print(f"\nCharge ID: {charge[0]}")
            print(f"Status: {charge[1]}")
            print(f"Type: {charge[2]}")
            print(f"Created: {charge[3]}")
            print(f"Delivered: {charge[4]}")
            if charge[5]:
                print(f"Satisfied: {charge[5]}")
            if charge[6]:
                print(f"Persons Entitled: {', '.join(charge[6])}")
            if charge[7]:
                print(f"Description: {charge[7][:100]}...")
                
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