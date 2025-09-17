#!/usr/bin/env python3
"""Quick data statistics"""

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'insideestates_app'),
    'user': os.getenv('DB_USER', 'insideestates_user'),
    'password': os.getenv('DB_PASSWORD', 'InsideEstates2024!')
}

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

print("=== Quick Stats ===")

# Companies House
cur.execute("SELECT COUNT(*) FROM companies_house_data")
print(f"Total companies: {cur.fetchone()[0]:,}")

cur.execute("SELECT COUNT(*) FROM companies_house_data WHERE company_status = 'Active'")
print(f"Active companies: {cur.fetchone()[0]:,}")

# Land Registry
cur.execute("SELECT COUNT(DISTINCT title_number) FROM land_registry_data")
print(f"Total properties: {cur.fetchone()[0]:,}")

# Sample join
print("\nSample company with properties:")
cur.execute("""
    SELECT ch.company_name, ch.company_number, ch.company_status
    FROM companies_house_data ch
    WHERE ch.company_number IN (
        SELECT company_1_reg_no FROM land_registry_data 
        WHERE company_1_reg_no IS NOT NULL 
        LIMIT 1
    )
""")
result = cur.fetchone()
if result:
    print(f"  {result[0]} ({result[1]}) - Status: {result[2]}")

cur.close()
conn.close()