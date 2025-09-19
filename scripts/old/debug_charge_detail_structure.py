#!/usr/bin/env python3
"""
Debug charge detail page structure
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

# Get a charge detail HTML
cur.execute("""
    SELECT charge_id, raw_html 
    FROM ch_scrape_charges 
    WHERE company_number = 'SC002116' 
    AND scrape_status = 'detail_scraped'
    LIMIT 1
""")

result = cur.fetchone()
if result:
    charge_id, raw_html = result
    html_content = bz2.decompress(raw_html)
    if isinstance(html_content, bytes):
        html_content = html_content.decode('utf-8')
    
    # Save for inspection
    with open('/tmp/charge_detail.html', 'w') as f:
        f.write(html_content)
    print(f"Saved charge detail HTML to /tmp/charge_detail.html for charge {charge_id}")
    
    tree = html.fromstring(html_content)
    
    # Look for different structures
    print("\nLooking for data structures:")
    
    # Check for dl elements
    dl_elements = tree.xpath('//dl')
    print(f"\nFound {len(dl_elements)} dl elements")
    for i, dl in enumerate(dl_elements[:3]):
        classes = dl.get('class', 'no-class')
        print(f"  dl {i}: class='{classes}'")
    
    # Check for definition terms
    dt_elements = tree.xpath('//dt')
    print(f"\nFound {len(dt_elements)} dt elements:")
    for dt in dt_elements[:10]:
        print(f"  - {dt.text_content().strip()}")
    
    # Check for tables
    tables = tree.xpath('//table')
    print(f"\nFound {len(tables)} tables")
    for i, table in enumerate(tables):
        classes = table.get('class', 'no-class')
        rows = table.xpath('.//tr')
        print(f"  Table {i}: class='{classes}', rows={len(rows)}")
    
    # Check for specific text patterns
    print("\nLooking for key text patterns:")
    
    # Transaction filed
    transaction_elements = tree.xpath("//*[contains(text(), 'Transaction filed')]")
    print(f"Transaction filed elements: {len(transaction_elements)}")
    
    # Amount secured
    amount_elements = tree.xpath("//*[contains(text(), 'Amount secured')]")
    print(f"Amount secured elements: {len(amount_elements)}")
    
    # Short particulars
    particulars_elements = tree.xpath("//*[contains(text(), 'Short particulars')]")
    print(f"Short particulars elements: {len(particulars_elements)}")

cur.close()
conn.close()