#!/usr/bin/env python3
"""
Debug all charges on page
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
    
    # Save to file for inspection
    with open('/tmp/charges_page1.html', 'w') as f:
        f.write(html_content)
    print("Saved HTML to /tmp/charges_page1.html")
    
    tree = html.fromstring(html_content)
    
    # Count all h2 headings with charge codes
    charge_headings = tree.xpath("//h2[@class='heading-medium' and contains(., 'Charge code')]")
    print(f"\nFound {len(charge_headings)} charge headings")
    for i, h in enumerate(charge_headings[:10]):
        print(f"  {i+1}: {h.text_content().strip()}")
    
    # Try a different approach - look for all divs with numeric mortgage class
    all_divs = tree.xpath("//div[starts-with(@class, 'mortgage-')]")
    print(f"\nAll divs starting with 'mortgage-': {len(all_divs)}")
    
    # Group by class
    classes = {}
    for div in all_divs:
        cls = div.get('class')
        if cls not in classes:
            classes[cls] = 0
        classes[cls] += 1
    
    print("\nDiv class counts:")
    for cls, count in sorted(classes.items()):
        print(f"  {cls}: {count}")

cur.close()
conn.close()