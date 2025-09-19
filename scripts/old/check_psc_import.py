#!/usr/bin/env python3
"""Quick script to check PSC import results"""

import sys
import os
import psycopg2

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

# Connect to database
conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

# Get statistics
cursor.execute("SELECT COUNT(*) FROM psc_data")
total_pscs = cursor.fetchone()[0]

cursor.execute("""
    SELECT psc_type, COUNT(*) as count 
    FROM psc_data 
    GROUP BY psc_type 
    ORDER BY count DESC
""")
psc_types = cursor.fetchall()

cursor.execute("""
    SELECT COUNT(DISTINCT company_number) 
    FROM psc_data
""")
unique_companies = cursor.fetchone()[0]

cursor.execute("""
    SELECT COUNT(*) 
    FROM psc_data 
    WHERE ceased_on IS NULL
""")
active_pscs = cursor.fetchone()[0]

print(f"\n{'='*50}")
print("PSC IMPORT RESULTS")
print(f"{'='*50}")
print(f"Total PSC records: {total_pscs:,}")
print(f"Unique companies with PSCs: {unique_companies:,}")
print(f"Active PSCs (not ceased): {active_pscs:,}")
print(f"\nPSC Types:")
for psc_type, count in psc_types:
    print(f"  - {psc_type}: {count:,}")

# Sample query joining with Companies House
cursor.execute("""
    SELECT 
        ch.company_name,
        ch.company_status,
        p.name as psc_name,
        p.natures_of_control
    FROM psc_data p
    JOIN companies_house_data ch ON p.company_number = ch.company_number
    WHERE ch.company_status = 'Active' 
    AND p.ceased_on IS NULL
    LIMIT 5
""")

print(f"\n{'='*50}")
print("SAMPLE: Active Companies with PSCs")
print(f"{'='*50}")
for row in cursor.fetchall():
    print(f"\nCompany: {row[0]} ({row[1]})")
    print(f"PSC: {row[2]}")
    print(f"Control: {', '.join(row[3])}")

cursor.close()
conn.close()