#!/usr/bin/env python3
"""Test performance comparison between JSON and normalized PSC tables"""

import sys
import os
import psycopg2
from datetime import datetime
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

# Connect to database
conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

print("\n" + "="*70)
print("PSC TABLE PERFORMANCE COMPARISON")
print("="*70)

# Get record counts
cursor.execute("SELECT COUNT(*) FROM psc_data")
json_count = cursor.fetchone()[0]
cursor.execute("SELECT COUNT(*) FROM psc_data_normalized")
norm_count = cursor.fetchone()[0]

print(f"\nOriginal table (JSON): {json_count:,} records")
print(f"Normalized table: {norm_count:,} records")
print(f"Migration progress: {norm_count/json_count*100:.1f}%")

tests = [
    {
        "name": "Search by postal code",
        "json_query": """
            SELECT COUNT(*) 
            FROM psc_data 
            WHERE address->>'postal_code' = 'SW1A 1AA'
        """,
        "norm_query": """
            SELECT COUNT(*) 
            FROM psc_data_normalized 
            WHERE address_postal_code = 'SW1A 1AA'
        """
    },
    {
        "name": "Search by birth year",
        "json_query": """
            SELECT COUNT(*) 
            FROM psc_data 
            WHERE date_of_birth->>'year' = '1970'
        """,
        "norm_query": """
            SELECT COUNT(*) 
            FROM psc_data_normalized 
            WHERE birth_year = 1970
        """
    },
    {
        "name": "Search by surname",
        "json_query": """
            SELECT COUNT(*) 
            FROM psc_data 
            WHERE name_elements->>'surname' = 'Smith'
        """,
        "norm_query": """
            SELECT COUNT(*) 
            FROM psc_data_normalized 
            WHERE name_surname = 'Smith'
        """
    },
    {
        "name": "Geographic analysis (group by region)",
        "json_query": """
            SELECT address->>'region' as region, COUNT(*) 
            FROM psc_data 
            WHERE address->>'region' IS NOT NULL
            GROUP BY address->>'region'
            ORDER BY COUNT(*) DESC
            LIMIT 10
        """,
        "norm_query": """
            SELECT address_region as region, COUNT(*) 
            FROM psc_data_normalized 
            WHERE address_region IS NOT NULL
            GROUP BY address_region
            ORDER BY COUNT(*) DESC
            LIMIT 10
        """
    },
    {
        "name": "Corporate entity search by registration number",
        "json_query": """
            SELECT COUNT(*) 
            FROM psc_data 
            WHERE identification->>'registration_number' = '12345678'
        """,
        "norm_query": """
            SELECT COUNT(*) 
            FROM psc_data_normalized 
            WHERE identification_registration_number = '12345678'
        """
    }
]

print("\n" + "="*70)
print("PERFORMANCE TEST RESULTS")
print("="*70)

for test in tests:
    print(f"\n{test['name']}:")
    
    # Test JSON query
    start = time.time()
    cursor.execute(test['json_query'])
    json_result = cursor.fetchall()
    json_time = time.time() - start
    
    # Test normalized query
    start = time.time()
    cursor.execute(test['norm_query'])
    norm_result = cursor.fetchall()
    norm_time = time.time() - start
    
    # Calculate improvement
    if norm_time > 0:
        improvement = json_time / norm_time
    else:
        improvement = float('inf')
    
    print(f"  Original (JSON): {json_time:.3f} seconds")
    print(f"  Normalized: {norm_time:.3f} seconds")
    print(f"  Speed improvement: {improvement:.1f}x faster")

# Show sample data from normalized table
print("\n" + "="*70)
print("SAMPLE DATA FROM NORMALIZED TABLE")
print("="*70)

cursor.execute("""
    SELECT 
        company_number,
        name,
        name_forename,
        name_surname,
        birth_year,
        address_postal_code,
        address_locality,
        natures_of_control[1] as primary_control
    FROM psc_data_normalized
    WHERE psc_type = 'individual-person-with-significant-control'
    AND ceased_on IS NULL
    AND address_postal_code IS NOT NULL
    LIMIT 5
""")

print("\nSample Individual PSCs:")
for row in cursor.fetchall():
    print(f"\nCompany: {row[0]}")
    print(f"PSC: {row[1]} ({row[2]} {row[3]})")
    print(f"Birth Year: {row[4]}")
    print(f"Location: {row[6]}, {row[5]}")
    print(f"Control: {row[7]}")

cursor.close()
conn.close()