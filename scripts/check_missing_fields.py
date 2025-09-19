#!/usr/bin/env python3
"""
Check which fields are missing from v_ownership_history
"""

import psycopg2
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

# Fields expected in the view
expected_fields = [
    "title_number",
    "file_month",
    "dataset_type",
    "update_type",
    "change_indicator",
    "change_date",
    "tenure",
    "property_address",
    "district",
    "county",
    "region",
    "postcode",
    "price_paid",
    "date_proprietor_added",
    "date_proprietor_added_yyyy_mm",
    "date_proprietor_added_yyyy",
    "date_proprietor_added_yyyy_q",
    "multiple_address_indicator",
    "additional_proprietor_indicator",
    "source_filename",
    "created_at",
    "ch_match_date",
    "proprietor_sequence",
    "proprietor_name",
    "lr_company_reg_no",
    "proprietorship_category",
    "country_incorporated",
    "proprietor_address_1",
    "proprietor_address_2",
    "proprietor_address_3",
    "ch_matched_name",
    "ch_matched_number",
    "ch_match_type",
    "ch_match_confidence",
    "ch_company_name",
    "ch_company_status",
    "ch_company_category",
    "ch_incorporation_date",
    "ch_dissolution_date",
    "ch_country_of_origin",
    "ch_reg_address_line1",
    "ch_reg_address_line2",
    "ch_reg_address_post_town",
    "ch_reg_address_county",
    "ch_reg_address_country",
    "ch_reg_address_postcode",
    "ch_accounts_next_due_date",
    "ch_accounts_last_made_up_date",
    "ch_accounts_category",
    "ch_sic_code_1",
    "ch_sic_code_2",
    "ch_sic_code_3",
    "ch_sic_code_4",
    "match_quality_description",
    "change_description",
    "ownership_status",
    "ownership_duration_months"
]

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

# Get current fields in view
cursor.execute("""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'v_ownership_history'
    ORDER BY ordinal_position
""")
current_fields = [row[0] for row in cursor.fetchall()]

# Find missing fields
missing_fields = [f for f in expected_fields if f not in current_fields]
extra_fields = [f for f in current_fields if f not in expected_fields]

print("Current fields in v_ownership_history:", len(current_fields))
print("\nMissing fields:")
for field in missing_fields:
    print(f"  - {field}")

print(f"\nTotal missing: {len(missing_fields)}")

if extra_fields:
    print("\nExtra fields not in your list:")
    for field in extra_fields:
        print(f"  + {field}")

# Check columns available in companies_house_data
print("\n\nChecking companies_house_data columns for missing CH fields:")
cursor.execute("""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'companies_house_data'
    AND column_name LIKE '%address%' OR column_name LIKE '%account%' OR column_name LIKE '%country%'
    ORDER BY column_name
""")
print("\nAvailable address/account columns in companies_house_data:")
for row in cursor.fetchall():
    print(f"  - {row[0]}")

cursor.close()
conn.close()