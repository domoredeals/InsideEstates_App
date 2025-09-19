#!/usr/bin/env python3
"""
Find the actual charge structure
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
    
    # Search for charge-related structures
    print("Looking for charge structures...")
    
    # Look for headings with charge codes
    headings = tree.xpath("//h2[@class='heading-medium']")
    print(f"\nFound {len(headings)} h2.heading-medium elements")
    if headings:
        for i, h in enumerate(headings[:3]):
            print(f"  Heading {i}: {h.text_content().strip()[:100]}")
    
    # Look for divs with class containing 'mortgage'
    mortgage_divs = tree.xpath("//div[contains(@class, 'mortgage')]")
    print(f"\nFound {len(mortgage_divs)} divs with 'mortgage' in class")
    
    # Look at their classes
    if mortgage_divs:
        print("Classes of mortgage divs:")
        for div in mortgage_divs[:5]:
            print(f"  {div.get('class')}")
    
    # Look for the charge list structure
    charge_list = tree.xpath("//ol[@class='charge-list']")
    print(f"\nFound {len(charge_list)} ol.charge-list elements")
    
    if charge_list:
        # Get charge items within the list
        charge_items = charge_list[0].xpath(".//li[@class='charge-item']")
        print(f"Found {len(charge_items)} charge items")
        
        if charge_items:
            first_item = charge_items[0]
            print("\nFirst charge item HTML:")
            print("=" * 80)
            print(html.tostring(first_item, pretty_print=True).decode('utf-8')[:1500])

cur.close()
conn.close()