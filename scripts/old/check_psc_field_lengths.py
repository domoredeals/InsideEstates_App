#!/usr/bin/env python3
"""Check maximum field lengths in PSC data"""

import sys
import os
import psycopg2

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

# Connect to database
conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

print("\nChecking maximum field lengths in PSC data...")

# Check name elements
cursor.execute("""
    SELECT 
        MAX(LENGTH(name_elements->>'title')) as max_title,
        MAX(LENGTH(name_elements->>'forename')) as max_forename,
        MAX(LENGTH(name_elements->>'middle_name')) as max_middle_name,
        MAX(LENGTH(name_elements->>'surname')) as max_surname
    FROM psc_data
    WHERE name_elements IS NOT NULL
""")
result = cursor.fetchone()
print(f"\nName Elements:")
print(f"  Title: {result[0] if result[0] else 'NULL'}")
print(f"  Forename: {result[1] if result[1] else 'NULL'}")
print(f"  Middle Name: {result[2] if result[2] else 'NULL'}")
print(f"  Surname: {result[3] if result[3] else 'NULL'}")

# Check address fields
cursor.execute("""
    SELECT 
        MAX(LENGTH(address->>'care_of')) as max_care_of,
        MAX(LENGTH(address->>'po_box')) as max_po_box,
        MAX(LENGTH(address->>'premises')) as max_premises,
        MAX(LENGTH(address->>'address_line_1')) as max_line1,
        MAX(LENGTH(address->>'address_line_2')) as max_line2,
        MAX(LENGTH(address->>'locality')) as max_locality,
        MAX(LENGTH(address->>'region')) as max_region,
        MAX(LENGTH(address->>'country')) as max_country,
        MAX(LENGTH(address->>'postal_code')) as max_postal_code
    FROM psc_data
    WHERE address IS NOT NULL
""")
result = cursor.fetchone()
print(f"\nAddress Fields:")
print(f"  Care Of: {result[0] if result[0] else 'NULL'}")
print(f"  PO Box: {result[1] if result[1] else 'NULL'}")
print(f"  Premises: {result[2] if result[2] else 'NULL'}")
print(f"  Address Line 1: {result[3] if result[3] else 'NULL'}")
print(f"  Address Line 2: {result[4] if result[4] else 'NULL'}")
print(f"  Locality: {result[5] if result[5] else 'NULL'}")
print(f"  Region: {result[6] if result[6] else 'NULL'}")
print(f"  Country: {result[7] if result[7] else 'NULL'}")
print(f"  Postal Code: {result[8] if result[8] else 'NULL'}")

# Check identification fields
cursor.execute("""
    SELECT 
        MAX(LENGTH(identification->>'legal_form')) as max_legal_form,
        MAX(LENGTH(identification->>'legal_authority')) as max_legal_authority,
        MAX(LENGTH(identification->>'place_registered')) as max_place_registered,
        MAX(LENGTH(identification->>'country_registered')) as max_country_registered,
        MAX(LENGTH(identification->>'registration_number')) as max_registration_number
    FROM psc_data
    WHERE identification IS NOT NULL
""")
result = cursor.fetchone()
print(f"\nIdentification Fields:")
print(f"  Legal Form: {result[0] if result[0] else 'NULL'}")
print(f"  Legal Authority: {result[1] if result[1] else 'NULL'}")
print(f"  Place Registered: {result[2] if result[2] else 'NULL'}")
print(f"  Country Registered: {result[3] if result[3] else 'NULL'}")
print(f"  Registration Number: {result[4] if result[4] else 'NULL'}")

# Check samples of longest values
print("\n" + "="*60)
print("SAMPLES OF LONGEST VALUES")
print("="*60)

# Long titles
cursor.execute("""
    SELECT name_elements->>'title' 
    FROM psc_data 
    WHERE LENGTH(name_elements->>'title') > 20
    LIMIT 5
""")
long_titles = cursor.fetchall()
if long_titles:
    print("\nLong Titles:")
    for row in long_titles:
        print(f"  '{row[0]}' (length: {len(row[0])})")

# Long postal codes
cursor.execute("""
    SELECT address->>'postal_code' 
    FROM psc_data 
    WHERE LENGTH(address->>'postal_code') > 20
    LIMIT 5
""")
long_postcodes = cursor.fetchall()
if long_postcodes:
    print("\nLong Postal Codes:")
    for row in long_postcodes:
        print(f"  '{row[0]}' (length: {len(row[0])})")

cursor.close()
conn.close()