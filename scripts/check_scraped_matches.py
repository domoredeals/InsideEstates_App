#!/usr/bin/env python3
"""
Check how scraped data can help with NO_MATCH records
"""

import psycopg2
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

print("Analyzing scraped data for NO_MATCH improvement potential...")
print("=" * 60)

# Check scraped data stats
cursor.execute("""
    SELECT COUNT(*) as total,
           COUNT(DISTINCT scraped_search_name) as unique_searches,
           COUNT(DISTINCT company_number) as unique_companies
    FROM companies_house_data 
    WHERE scraped_data = TRUE
""")
result = cursor.fetchone()
print(f"\nScraped data statistics:")
print(f"  Total records: {result[0]:,}")
print(f"  Unique search names: {result[1]:,}")
print(f"  Unique company numbers: {result[2]:,}")

# Check Land Registry companies without reg numbers
cursor.execute("""
    SELECT COUNT(DISTINCT company_1)
    FROM land_registry_data
    WHERE company_1 IS NOT NULL 
    AND LENGTH(company_1) > 0
    AND (company_1_reg_no IS NULL OR LENGTH(company_1_reg_no) = 0)
""")
no_reg_count = cursor.fetchone()[0]
print(f"\nLand Registry companies without registration numbers: {no_reg_count:,}")

# Show examples of scraped companies
print("\nExamples of scraped company data:")
cursor.execute("""
    SELECT scraped_search_name, company_number, company_name, scraped_company_status
    FROM companies_house_data
    WHERE scraped_data = TRUE
    ORDER BY last_scraped_date DESC
    LIMIT 10
""")
for row in cursor.fetchall():
    print(f'  Searched: "{row[0]}"')
    print(f'    Found: {row[2]} ({row[1]}) - Status: {row[3]}')

# Check potential matches
print("\nChecking for potential matches...")
cursor.execute("""
    SELECT COUNT(DISTINCT ld.company_1)
    FROM land_registry_data ld
    WHERE ld.company_1 IS NOT NULL 
    AND LENGTH(ld.company_1) > 0
    AND (ld.company_1_reg_no IS NULL OR LENGTH(ld.company_1_reg_no) = 0)
    AND EXISTS (
        SELECT 1 
        FROM companies_house_data ch
        WHERE ch.scraped_data = TRUE
        AND UPPER(TRIM(ch.scraped_search_name)) = UPPER(TRIM(ld.company_1))
    )
""")
potential_matches = cursor.fetchone()[0]
print(f"\nPotential matches found: {potential_matches:,}")

if potential_matches > 0:
    print("\nExample potential matches:")
    cursor.execute("""
        SELECT DISTINCT 
            ld.company_1,
            ch.company_number,
            ch.company_name,
            ch.scraped_company_status
        FROM land_registry_data ld
        JOIN companies_house_data ch ON UPPER(TRIM(ch.scraped_search_name)) = UPPER(TRIM(ld.company_1))
        WHERE ld.company_1 IS NOT NULL 
        AND LENGTH(ld.company_1) > 0
        AND (ld.company_1_reg_no IS NULL OR LENGTH(ld.company_1_reg_no) = 0)
        AND ch.scraped_data = TRUE
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(f'  LR: "{row[0]}" -> CH: {row[2]} ({row[1]}) - {row[3]}')

cursor.close()
conn.close()

print("\nNext step: Run the matching script again to use this enriched data!")