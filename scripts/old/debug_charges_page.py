#!/usr/bin/env python3
"""
Debug charges page structure
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

# Get the raw HTML we just scraped
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
    
    # Try different selectors
    print("Looking for charges with different selectors:")
    
    selectors = [
        "//div[@id='mortgage-charges']//ul[@id='full-mortgage-list']/li",
        "//ul[@id='full-mortgage-list']/li",
        "//div[contains(@class, 'charge-')]",
        "//li[contains(@id, 'charge-')]",
        "//h2[contains(@class, 'heading-medium')]"
    ]
    
    for selector in selectors:
        elements = tree.xpath(selector)
        print(f"\n{selector}: Found {len(elements)} elements")
        if elements and len(elements) > 0:
            print(f"  First element text: {elements[0].text_content()[:100]}...")

    # Check for pagination links
    next_links = tree.xpath("//a[contains(@id, 'next')]")
    print(f"\nNext page links found: {len(next_links)}")
    for link in next_links:
        print(f"  Link: {link.get('href')}, Text: {link.text_content().strip()}")

cur.close()
conn.close()