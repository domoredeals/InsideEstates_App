#!/usr/bin/env python3
"""
Debug why we're not getting all 41 charges
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

total_found = 0
total_parsed = 0

# Process both pages
for page in [1, 2]:
    cur.execute("""
        SELECT raw_html 
        FROM ch_scrape_charges 
        WHERE company_number = 'SC002116' 
        AND charge_id = %s
        LIMIT 1
    """, (f'page_{page}',))
    
    result = cur.fetchone()
    if result:
        html_content = bz2.decompress(result[0])
        if isinstance(html_content, bytes):
            html_content = html_content.decode('utf-8')
        
        tree = html.fromstring(html_content)
        
        # Find all charge divs
        charge_elements = []
        for i in range(1, 100):
            elements = tree.xpath(f"//div[@class='mortgage-{i}']")
            if not elements:
                break
            charge_elements.extend(elements)
        
        print(f"\nPage {page}: Found {len(charge_elements)} mortgage divs")
        total_found += len(charge_elements)
        
        # Check each one
        for i, charge_el in enumerate(charge_elements, 1):
            # Get heading
            heading_link = charge_el.xpath('.//h2[contains(@class, "heading-medium")]/a')
            if heading_link:
                charge_text = heading_link[0].text_content().strip()
                print(f"  {i}: {charge_text[:60]}...")
                total_parsed += 1
            else:
                # No heading found - check what's in this div
                text_content = charge_el.text_content()[:200].strip()
                print(f"  {i}: NO HEADING - Content: {text_content[:60]}...")

print(f"\nSummary:")
print(f"Total divs found: {total_found}")
print(f"Total with headings: {total_parsed}")
print(f"Missing: {total_found - total_parsed}")

cur.close()
conn.close()