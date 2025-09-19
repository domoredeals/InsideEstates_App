#!/usr/bin/env python3
"""Check scraping progress."""

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    database=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)

cur = conn.cursor()

# Get status counts
cur.execute("""
    SELECT search_status, COUNT(*) as count
    FROM ch_scrape_queue
    GROUP BY search_status
    ORDER BY search_status
""")

print("\n=== Scraping Queue Status ===")
total = 0
for status, count in cur.fetchall():
    print(f"{status}: {count:,}")
    total += count

print(f"\nTotal: {total:,}")

# Get scraped companies count
cur.execute("""
    SELECT COUNT(DISTINCT company_number) 
    FROM ch_scrape_overview 
    WHERE scrape_status = 'parsed'
""")
scraped = cur.fetchone()[0]
print(f"\nCompanies successfully scraped: {scraped:,}")

cur.close()
conn.close()