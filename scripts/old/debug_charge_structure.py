#!/usr/bin/env python3
"""
Debug single charge structure
"""

import os
import sys
import psycopg2
import bz2
from lxml import html
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

conn = get_db_connection()
cur = conn.cursor()

# Get the first page HTML
cur.execute("""
    SELECT raw_html 
    FROM ch_scrape_charges 
    WHERE company_number = 'SC002116' 
    AND charge_id = 'page_1'
    LIMIT 1
""")

result = cur.fetchone()
if result:
    html_content = bz2.decompress(result[0])
    if isinstance(html_content, bytes):
        html_content = html_content.decode('utf-8')
    
    tree = html.fromstring(html_content)
    
    # Look for the actual charge blocks
    charge_blocks = tree.xpath("//div[contains(@id, 'charge-')]")
    if charge_blocks:
        first_charge = charge_blocks[0]
    else:
        # Try alternative structure
        first_charge = tree.xpath("//li[contains(@id, 'charge-')]")[0]
    
    print("First charge HTML structure:")
    print("=" * 80)
    print(html.tostring(first_charge, pretty_print=True).decode('utf-8')[:2000])
    print("=" * 80)
    
    # Try to extract data
    print("\nExtracting data:")
    
    # Charge ID
    charge_id = first_charge.get('id')
    print(f"Charge ID from element: {charge_id}")
    
    # Charge code from heading
    heading = first_charge.xpath('.//h2[@class="heading-medium"]/span[@class="sub-heading"]/a/text()')
    if heading:
        print(f"Charge code: {heading[0]}")
    
    # Status
    status = first_charge.xpath('.//span[@class="status-tag font-xsmall"]/strong/text()')
    if status:
        print(f"Status: {status[0]}")
    
    # Look for all text in the charge
    print("\nAll dt/dd pairs:")
    dts = first_charge.xpath('.//dt')
    for dt in dts:
        dt_text = dt.text_content().strip()
        dd = dt.getnext()
        if dd is not None and dd.tag == 'dd':
            dd_text = dd.text_content().strip()
            print(f"  {dt_text}: {dd_text[:100]}")

cur.close()
conn.close()