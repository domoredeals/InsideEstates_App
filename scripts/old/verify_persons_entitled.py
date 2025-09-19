#!/usr/bin/env python3
"""
Verify persons_entitled field is being captured and stored properly
"""

import os
import sys
import psycopg2
import bz2
from lxml import html
from datetime import datetime
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

def verify_persons_entitled(company_number='SC002116'):
    """Check if persons_entitled is being captured"""
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Check if we have any records with persons_entitled
        cur.execute("""
            SELECT charge_id, persons_entitled, amount_secured, short_particulars
            FROM ch_scrape_charges
            WHERE company_number = %s
            AND persons_entitled IS NOT NULL
            LIMIT 5
        """, (company_number,))
        
        results = cur.fetchall()
        
        if results:
            print(f"Found {len(results)} charges with persons_entitled for {company_number}:")
            print("=" * 80)
            for charge_id, persons, amount, particulars in results:
                print(f"\nCharge ID: {charge_id}")
                print(f"Persons Entitled: {persons}")
                print(f"Amount Secured: {amount}")
                print(f"Short Particulars: {particulars[:100] if particulars else 'None'}...")
        else:
            print(f"No charges found with persons_entitled for {company_number}")
            
        # Get statistics
        cur.execute("""
            SELECT 
                COUNT(*) as total_charges,
                COUNT(persons_entitled) as has_persons,
                COUNT(amount_secured) as has_amount,
                COUNT(short_particulars) as has_particulars
            FROM ch_scrape_charges
            WHERE company_number = %s
            AND charge_id NOT LIKE 'page_%%'
        """, (company_number,))
        
        stats = cur.fetchone()
        print(f"\n\nStatistics for {company_number}:")
        print(f"Total charges: {stats[0]}")
        print(f"Has persons_entitled: {stats[1]} ({stats[1]/stats[0]*100:.1f}%)")
        print(f"Has amount_secured: {stats[2]} ({stats[2]/stats[0]*100:.1f}%)")
        print(f"Has short_particulars: {stats[3]} ({stats[3]/stats[0]*100:.1f}%)")
        
        # Check a specific charge we know should have the data
        charge_id = 'NRrK3uDqlRdIk5VDyCDHRbqiUlo'
        cur.execute("""
            SELECT charge_id, persons_entitled, amount_secured, short_particulars,
                   scrape_status, raw_html IS NOT NULL as has_html
            FROM ch_scrape_charges
            WHERE company_number = %s
            AND charge_id = %s
        """, (company_number, charge_id))
        
        result = cur.fetchone()
        if result:
            print(f"\n\nSpecific charge {charge_id}:")
            print(f"Persons Entitled: {result[1]}")
            print(f"Amount Secured: {result[2]}")
            print(f"Short Particulars: {result[3]}")
            print(f"Scrape Status: {result[4]}")
            print(f"Has HTML: {result[5]}")
            
            # If we have HTML but no persons_entitled, try parsing it
            if result[5] and not result[1]:
                print("\nCharge has HTML but no persons_entitled. Re-parsing...")
                
                # Get and parse the HTML
                cur.execute("""
                    SELECT raw_html
                    FROM ch_scrape_charges
                    WHERE company_number = %s
                    AND charge_id = %s
                """, (company_number, charge_id))
                
                raw_html = cur.fetchone()[0]
                if raw_html:
                    html_content = bz2.decompress(raw_html)
                    if isinstance(html_content, bytes):
                        html_content = html_content.decode('utf-8')
                    
                    tree = html.fromstring(html_content)
                    
                    # Look for Persons Entitled
                    persons = tree.xpath('//ul[@id="persons-entitled"]/li/text()')
                    if persons:
                        print(f"Found persons_entitled in HTML: {[p.strip() for p in persons]}")
                    else:
                        print("Could not find persons_entitled in HTML with expected selector")
                        
                        # Try alternative selectors
                        alt_selectors = [
                            '//h3[contains(text(), "Persons entitled")]/following-sibling::ul/li/text()',
                            '//dt[contains(text(), "Persons entitled")]/following-sibling::dd//ul/li/text()'
                        ]
                        
                        for selector in alt_selectors:
                            persons = tree.xpath(selector)
                            if persons:
                                print(f"Found with alternative selector: {selector}")
                                print(f"Value: {[p.strip() for p in persons]}")
                                break
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--company', default='SC002116', help='Company number to check')
    args = parser.parse_args()
    
    verify_persons_entitled(args.company)