#!/usr/bin/env python3
"""Create indexes on normalized PSC table"""

import sys
import os
import psycopg2
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

# Connect to database
conn = psycopg2.connect(**POSTGRESQL_CONFIG)
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()

print("Creating indexes on psc_data_normalized...")

indexes = [
    # Primary lookups
    ("idx_psc_norm_company_number", "CREATE INDEX IF NOT EXISTS idx_psc_norm_company_number ON psc_data_normalized(company_number);"),
    ("idx_psc_norm_type", "CREATE INDEX IF NOT EXISTS idx_psc_norm_type ON psc_data_normalized(psc_type);"),
    ("idx_psc_norm_name", "CREATE INDEX IF NOT EXISTS idx_psc_norm_name ON psc_data_normalized(name);"),
    ("idx_psc_norm_surname", "CREATE INDEX IF NOT EXISTS idx_psc_norm_surname ON psc_data_normalized(name_surname);"),
    
    # Address searches
    ("idx_psc_norm_postal_code", "CREATE INDEX IF NOT EXISTS idx_psc_norm_postal_code ON psc_data_normalized(address_postal_code);"),
    ("idx_psc_norm_locality", "CREATE INDEX IF NOT EXISTS idx_psc_norm_locality ON psc_data_normalized(address_locality);"),
    ("idx_psc_norm_region", "CREATE INDEX IF NOT EXISTS idx_psc_norm_region ON psc_data_normalized(address_region);"),
    
    # Date filters
    ("idx_psc_norm_birth_year", "CREATE INDEX IF NOT EXISTS idx_psc_norm_birth_year ON psc_data_normalized(birth_year);"),
    ("idx_psc_norm_notified_on", "CREATE INDEX IF NOT EXISTS idx_psc_norm_notified_on ON psc_data_normalized(notified_on);"),
    ("idx_psc_norm_ceased_on", "CREATE INDEX IF NOT EXISTS idx_psc_norm_ceased_on ON psc_data_normalized(ceased_on) WHERE ceased_on IS NULL;"),
    
    # Corporate entity searches
    ("idx_psc_norm_corp_reg_no", "CREATE INDEX IF NOT EXISTS idx_psc_norm_corp_reg_no ON psc_data_normalized(identification_registration_number) WHERE identification_registration_number IS NOT NULL;"),
    
    # Natures of control
    ("idx_psc_norm_natures", "CREATE INDEX IF NOT EXISTS idx_psc_norm_natures ON psc_data_normalized USING GIN (natures_of_control);"),
    
    # Composite indexes for common queries
    ("idx_psc_norm_active_by_postcode", "CREATE INDEX IF NOT EXISTS idx_psc_norm_active_by_postcode ON psc_data_normalized(address_postal_code) WHERE ceased_on IS NULL;"),
    ("idx_psc_norm_company_active", "CREATE INDEX IF NOT EXISTS idx_psc_norm_company_active ON psc_data_normalized(company_number) WHERE ceased_on IS NULL;")
]

for idx_name, idx_sql in indexes:
    print(f"Creating {idx_name}...")
    start = datetime.now()
    try:
        cursor.execute(idx_sql)
        elapsed = (datetime.now() - start).total_seconds()
        print(f"  ✓ Created in {elapsed:.1f} seconds")
    except Exception as e:
        print(f"  ✗ Error: {e}")

print("\nRunning VACUUM ANALYZE on psc_data_normalized...")
cursor.execute("VACUUM ANALYZE psc_data_normalized")

print("\nIndexes created successfully!")

cursor.close()
conn.close()