#!/usr/bin/env python3
"""
Debug full charge structure
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
    
    # Find the second h2 (first is "Filter charges", second should be first charge)
    headings = tree.xpath("//h2[@class='heading-medium']")
    
    if len(headings) > 1:
        charge_heading = headings[1]
        
        # Get the parent container of the charge
        parent = charge_heading.getparent()
        
        print("Charge container HTML:")
        print("=" * 80)
        print(html.tostring(parent, pretty_print=True).decode('utf-8')[:3000])
        print("=" * 80)
        
        # Now try to extract structured data
        print("\nExtracting charge data:")
        
        # Charge code from h2
        charge_code = charge_heading.text_content().strip()
        print(f"Charge code: {charge_code}")
        
        # Status - look for status tag
        status = parent.xpath('.//strong[@class="status-tag"]/text()')
        if status:
            print(f"Status: {status[0]}")
        
        # Look for all dl elements (definition lists)
        dls = parent.xpath('.//dl')
        print(f"\nFound {len(dls)} dl elements")
        
        for dl in dls:
            dt_elements = dl.xpath('./dt')
            for dt in dt_elements:
                dt_text = dt.text_content().strip()
                dd = dt.getnext()
                if dd is not None and dd.tag == 'dd':
                    dd_text = dd.text_content().strip()
                    print(f"{dt_text}: {dd_text[:100]}")

cur.close()
conn.close()