#!/usr/bin/env python3
"""
Parse detailed charge information from Companies House
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

def parse_charge_details(company_number):
    """Parse detailed charge information for a company"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get charges that have been detail scraped
        cur.execute("""
            SELECT charge_id, raw_html
            FROM ch_scrape_charges
            WHERE company_number = %s
            AND scrape_status = 'detail_scraped'
            ORDER BY created_date DESC
        """, (company_number,))
        
        charges = cur.fetchall()
        if not charges:
            logging.info(f"No detail-scraped charges found for {company_number}")
            return
        
        logging.info(f"Parsing {len(charges)} charge details for {company_number}")
        
        parsed_count = 0
        for charge_id, raw_html in charges:
            if raw_html:
                charge_data = parse_single_charge_detail(raw_html)
                
                if charge_data:
                    # Update the charge record with detailed information
                    cur.execute("""
                        UPDATE ch_scrape_charges
                        SET transaction_filed = %s,
                            registration_type = %s,
                            amount_secured = %s,
                            short_particulars = %s,
                            contains_negative_pledge = %s,
                            contains_floating_charge = %s,
                            floating_charge_covers_all = %s,
                            contains_fixed_charge = %s,
                            fixed_charge_description = %s,
                            created_date = COALESCE(%s, created_date),
                            delivered_date = COALESCE(%s, delivered_date),
                            satisfied_date = COALESCE(%s, satisfied_date),
                            charge_status = COALESCE(%s, charge_status),
                            persons_entitled = COALESCE(%s, persons_entitled),
                            scrape_status = 'detail_parsed',
                            parse_timestamp = NOW()
                        WHERE company_number = %s
                        AND charge_id = %s
                    """, (
                        charge_data.get('transaction_filed'),
                        charge_data.get('registration_type'),
                        charge_data.get('amount_secured'),
                        charge_data.get('short_particulars'),
                        charge_data.get('contains_negative_pledge', False),
                        charge_data.get('contains_floating_charge', False),
                        charge_data.get('floating_charge_covers_all', False),
                        charge_data.get('contains_fixed_charge', False),
                        charge_data.get('fixed_charge_description'),
                        charge_data.get('created_date'),
                        charge_data.get('delivered_date'),
                        charge_data.get('satisfied_date'),
                        charge_data.get('status'),
                        charge_data.get('persons_entitled'),
                        company_number,
                        charge_id
                    ))
                    
                    # Store satisfaction transaction details if present
                    if charge_data.get('additional_transactions'):
                        # Usually there's only one satisfaction transaction
                        for transaction in charge_data['additional_transactions']:
                            if 'satisfaction' in transaction.get('description', '').lower():
                                cur.execute("""
                                    UPDATE ch_scrape_charges
                                    SET satisfaction_type = %s,
                                        satisfaction_delivered_date = %s
                                    WHERE company_number = %s
                                    AND charge_id = %s
                                """, (
                                    transaction.get('description'),
                                    transaction.get('delivered'),
                                    company_number,
                                    charge_id
                                ))
                                break  # Only store the first satisfaction transaction
                    
                    parsed_count += 1
        
        conn.commit()
        logging.info(f"Successfully parsed {parsed_count} charge details for {company_number}")
        
        return parsed_count
        
    except Exception as e:
        logging.error(f"Error parsing charge details for {company_number}: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def parse_single_charge_detail(raw_html_bytes):
    """Parse a single charge detail page"""
    try:
        # Decompress the HTML
        html_content = bz2.decompress(raw_html_bytes)
        if isinstance(html_content, bytes):
            html_content = html_content.decode('utf-8')
        
        tree = html.fromstring(html_content)
        
        charge_data = {}
        
        # Look for Created date
        created_date = tree.xpath('//dt[@id="mortgage-created-on-label"]/following-sibling::dd[@id="mortgage-created-on"]/text()')
        if created_date:
            try:
                charge_data['created_date'] = datetime.strptime(created_date[0].strip(), '%d %B %Y').date()
            except:
                pass
        
        # Look for Delivered date
        delivered_date = tree.xpath('//dt[@id="mortgage-delivered-on-label"]/following-sibling::dd[@id="mortgage-delivered-on"]/text()')
        if delivered_date:
            try:
                charge_data['delivered_date'] = datetime.strptime(delivered_date[0].strip(), '%d %B %Y').date()
            except:
                pass
        
        # Look for Status and Satisfied date
        status_dd = tree.xpath('//dd[@id="mortgage-status"]')
        if status_dd:
            status_text = status_dd[0].text_content().strip()
            if 'Satisfied' in status_text:
                charge_data['status'] = 'Satisfied'
                # Extract satisfied date
                date_match = re.search(r'on\s+(\d+\s+\w+\s+\d{4})', status_text)
                if date_match:
                    try:
                        charge_data['satisfied_date'] = datetime.strptime(date_match.group(1), '%d %B %Y').date()
                    except:
                        pass
            else:
                charge_data['status'] = status_text
        
        # Look for Transaction Filed in dt/dd structure
        trans_filed = tree.xpath('//dt[contains(text(), "Transaction Filed")]/following-sibling::dd[1]//span/text()')
        if trans_filed:
            charge_data['transaction_filed'] = trans_filed[0].strip()
            # Extract registration type
            if 'registration of a charge' in trans_filed[0].lower():
                match = re.search(r'\((\d+)\)', trans_filed[0])
                if match:
                    charge_data['registration_type'] = f"Registration of a charge ({match.group(1)})"
        
        # Look for Persons Entitled
        persons = tree.xpath('//ul[@id="persons-entitled"]/li/text()')
        if persons:
            charge_data['persons_entitled'] = [p.strip() for p in persons]
        
        # Look for Amount Secured
        amount = tree.xpath('//h3[@id="mortgage-amount-secured-label"]/following-sibling::p[1]/text()')
        if amount:
            charge_data['amount_secured'] = amount[0].strip()
        
        # Look for Short Particulars
        particulars = tree.xpath('//h3[@id="mortgage-particulars-label"]/following-sibling::p//span[@id="mortgage-particulars"]/text()')
        if particulars:
            charge_data['short_particulars'] = particulars[0].strip()
            # Parse for specific charge types
            particulars_lower = particulars[0].lower()
            if 'negative pledge' in particulars_lower:
                charge_data['contains_negative_pledge'] = True
            if 'floating charge' in particulars_lower:
                charge_data['contains_floating_charge'] = True
                if 'all the property' in particulars_lower or 'whole of the property' in particulars_lower:
                    charge_data['floating_charge_covers_all'] = True
            if 'fixed charge' in particulars_lower:
                charge_data['contains_fixed_charge'] = True
                # Try to extract what the fixed charge covers
                fixed_match = re.search(r'fixed charge[^.]+', particulars_lower)
                if fixed_match:
                    charge_data['fixed_charge_description'] = fixed_match.group(0)
        
        # Look for additional transactions table
        transactions = []
        # Look for the table after "Additional transactions filed against this charge" heading
        table = tree.xpath('//h3[@id="additional-filings-label"]/following-sibling::table[1]')
        
        if table:
            # Skip the header row and process data rows
            transaction_rows = table[0].xpath('.//tr[position() > 1]')
            
            for row in transaction_rows:
                cells = row.xpath('./td')
                if len(cells) >= 2:
                    transaction = {
                        'type': cells[0].text_content().strip() if len(cells) > 0 else '',
                        'delivered': None,
                        'description': ''
                    }
                    
                    # Extract delivered date from second column
                    if len(cells) > 1:
                        date_text = cells[1].text_content().strip()
                        if date_text:
                            try:
                                transaction['delivered'] = datetime.strptime(date_text, '%d %B %Y').date()
                            except:
                                pass
                    
                    # Extract type description
                    type_span = cells[0].xpath('.//span/text()') if len(cells) > 0 else []
                    if type_span:
                        transaction['description'] = type_span[0].strip()
                    else:
                        transaction['description'] = transaction['type']
                    
                    transactions.append(transaction)
        
        if transactions:
            charge_data['additional_transactions'] = transactions
        
        return charge_data
        
    except Exception as e:
        logging.error(f"Error parsing charge detail HTML: {e}")
        return None

def display_charge_details(company_number, charge_id=None):
    """Display parsed charge details"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        if charge_id:
            query = """
                SELECT charge_id, charge_status, created_date, satisfied_date,
                       transaction_filed, registration_type, amount_secured,
                       short_particulars, contains_negative_pledge, 
                       contains_floating_charge, floating_charge_covers_all,
                       contains_fixed_charge, fixed_charge_description
                FROM ch_scrape_charges
                WHERE company_number = %s
                AND charge_id = %s
            """
            cur.execute(query, (company_number, charge_id))
        else:
            query = """
                SELECT charge_id, charge_status, created_date, satisfied_date,
                       transaction_filed, registration_type, amount_secured,
                       short_particulars, contains_negative_pledge, 
                       contains_floating_charge, floating_charge_covers_all,
                       contains_fixed_charge, fixed_charge_description
                FROM ch_scrape_charges
                WHERE company_number = %s
                AND transaction_filed IS NOT NULL
                ORDER BY created_date DESC
                LIMIT 5
            """
            cur.execute(query, (company_number,))
        
        charges = cur.fetchall()
        
        print(f"\nDetailed charge information for {company_number}:")
        print("=" * 100)
        
        for charge in charges:
            print(f"\nCharge ID: {charge[0]}")
            print(f"Status: {charge[1]}")
            print(f"Created: {charge[2]}")
            if charge[3]:
                print(f"Satisfied: {charge[3]}")
            if charge[4]:
                print(f"Transaction: {charge[4]}")
            if charge[6]:
                print(f"Amount Secured: {charge[6]}")
            if charge[7]:
                print(f"Short Particulars: {charge[7][:200]}...")
            
            print("Charge Types:")
            if charge[8]:
                print("  - Contains Negative Pledge")
            if charge[9]:
                print("  - Contains Floating Charge")
                if charge[10]:
                    print("    (Covers all property)")
            if charge[11]:
                print("  - Contains Fixed Charge")
                if charge[12]:
                    print(f"    ({charge[12]})")
            
            # Check for additional transactions
            cur.execute("""
                SELECT transaction_type, transaction_date, description
                FROM ch_scrape_charge_transactions
                WHERE company_number = %s AND charge_id = %s
                ORDER BY transaction_date DESC
            """, (company_number, charge[0]))
            
            transactions = cur.fetchall()
            if transactions:
                print(f"\nAdditional Transactions ({len(transactions)}):")
                for trans in transactions:
                    print(f"  - {trans[0]} on {trans[1]}")
            
            print("-" * 100)
                
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Parse Companies House charge details')
    parser.add_argument('company_number', help='Company number to parse charges for')
    parser.add_argument('--charge-id', help='Specific charge ID to display')
    parser.add_argument('--display', action='store_true', help='Display parsed details')
    
    args = parser.parse_args()
    
    if args.display and args.charge_id:
        display_charge_details(args.company_number, args.charge_id)
    elif args.display:
        display_charge_details(args.company_number)
    else:
        parse_charge_details(args.company_number)
        display_charge_details(args.company_number)