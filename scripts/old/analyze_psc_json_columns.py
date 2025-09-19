#!/usr/bin/env python3
"""Analyze JSON columns in PSC data to understand structure"""

import sys
import os
import psycopg2
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

# Connect to database
conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

print("\n" + "="*60)
print("PSC JSON COLUMN ANALYSIS")
print("="*60)

# Analyze name_elements
print("\n1. NAME_ELEMENTS Structure:")
cursor.execute("""
    SELECT name_elements 
    FROM psc_data 
    WHERE name_elements IS NOT NULL 
    LIMIT 5
""")
for row in cursor.fetchall():
    print(f"   {row[0]}")

# Analyze address
print("\n2. ADDRESS Structure:")
cursor.execute("""
    SELECT address 
    FROM psc_data 
    WHERE address IS NOT NULL 
    LIMIT 3
""")
for row in cursor.fetchall():
    print(f"   {row[0]}")

# Analyze identification
print("\n3. IDENTIFICATION Structure (Corporate Entities):")
cursor.execute("""
    SELECT identification 
    FROM psc_data 
    WHERE identification IS NOT NULL 
    LIMIT 3
""")
for row in cursor.fetchall():
    print(f"   {row[0]}")

# Analyze date_of_birth
print("\n4. DATE_OF_BIRTH Structure:")
cursor.execute("""
    SELECT date_of_birth 
    FROM psc_data 
    WHERE date_of_birth IS NOT NULL 
    LIMIT 5
""")
for row in cursor.fetchall():
    print(f"   {row[0]}")

# Count how many records have each type of JSON data
print("\n" + "="*60)
print("JSON COLUMN USAGE STATISTICS")
print("="*60)

cursor.execute("""
    SELECT 
        COUNT(*) as total_records,
        COUNT(name_elements) as has_name_elements,
        COUNT(address) as has_address,
        COUNT(identification) as has_identification,
        COUNT(date_of_birth) as has_date_of_birth,
        COUNT(links) as has_links
    FROM psc_data
""")
stats = cursor.fetchone()
total = stats[0]
print(f"Total records: {total:,}")
print(f"Has name_elements: {stats[1]:,} ({stats[1]/total*100:.1f}%)")
print(f"Has address: {stats[2]:,} ({stats[2]/total*100:.1f}%)")
print(f"Has identification: {stats[3]:,} ({stats[3]/total*100:.1f}%)")
print(f"Has date_of_birth: {stats[4]:,} ({stats[4]/total*100:.1f}%)")
print(f"Has links: {stats[5]:,} ({stats[5]/total*100:.1f}%)")

# Check unique keys in JSON
print("\n" + "="*60)
print("UNIQUE JSON KEYS ANALYSIS")
print("="*60)

# Name elements keys
cursor.execute("""
    SELECT DISTINCT jsonb_object_keys(name_elements) 
    FROM psc_data 
    WHERE name_elements IS NOT NULL
""")
print("\nName Elements Keys:")
for row in cursor.fetchall():
    print(f"  - {row[0]}")

# Address keys
cursor.execute("""
    SELECT DISTINCT jsonb_object_keys(address) 
    FROM psc_data 
    WHERE address IS NOT NULL
""")
print("\nAddress Keys:")
for row in cursor.fetchall():
    print(f"  - {row[0]}")

# Identification keys
cursor.execute("""
    SELECT DISTINCT jsonb_object_keys(identification) 
    FROM psc_data 
    WHERE identification IS NOT NULL
""")
print("\nIdentification Keys:")
for row in cursor.fetchall():
    print(f"  - {row[0]}")

cursor.close()
conn.close()