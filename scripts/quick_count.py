#!/usr/bin/env python3
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG
import psycopg2

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM companies_house_data")
total = cursor.fetchone()[0]
print(f"Total companies in CH table: {total:,}")

# Check if WARBURTONS is there
cursor.execute("SELECT COUNT(*) FROM companies_house_data WHERE company_number = '00178711'")
warburtons = cursor.fetchone()[0]
print(f"WARBURTONS LIMITED (00178711) in table: {warburtons}")

cursor.close()
conn.close()