#!/usr/bin/env python3
import os
import sys
import psycopg2
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    database=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)
cur = conn.cursor()

# Check officer records
cur.execute("""
    SELECT company_number, officer_id, scrape_status, officer_name
    FROM ch_scrape_officers
    ORDER BY company_number, officer_id
""")

results = cur.fetchall()
print(f"Total officer records: {len(results)}")
print("-" * 80)
for row in results:
    print(f"Company: {row[0]}, Officer ID: {row[1]}, Status: {row[2]}, Name: {row[3]}")

cur.close()
conn.close()