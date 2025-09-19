#!/usr/bin/env python3
"""
Verify all charge fields are being captured
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

conn = get_db_connection()
cur = conn.cursor()

# Get a sample charge with all fields
cur.execute("""
    SELECT charge_id, charge_status, created_date, delivered_date, satisfied_date,
           transaction_filed, registration_type, amount_secured, short_particulars,
           persons_entitled, brief_description, charge_link,
           contains_fixed_charge, contains_floating_charge, contains_negative_pledge
    FROM ch_scrape_charges
    WHERE company_number = 'SC002116'
    AND scrape_status = 'detail_parsed'
    ORDER BY created_date DESC
    LIMIT 1
""")

result = cur.fetchone()
if result:
    print("Sample charge with all fields:")
    print("=" * 80)
    fields = [
        ('Charge ID', 0),
        ('Status', 1),
        ('Created Date', 2),
        ('Delivered Date', 3),
        ('Satisfied Date', 4),
        ('Transaction Filed', 5),
        ('Registration Type', 6),
        ('Amount Secured', 7),
        ('Short Particulars', 8),
        ('Persons Entitled', 9),
        ('Brief Description', 10),
        ('Charge Link', 11),
        ('Contains Fixed Charge', 12),
        ('Contains Floating Charge', 13),
        ('Contains Negative Pledge', 14)
    ]
    
    for field_name, idx in fields:
        value = result[idx]
        if value is not None:
            if isinstance(value, list):
                value = ', '.join(value)
            elif isinstance(value, str) and len(value) > 100:
                value = value[:100] + '...'
            print(f"{field_name}: {value}")

# Check additional transactions
cur.execute("""
    SELECT charge_id, transaction_type, transaction_date, delivered_date, description
    FROM ch_scrape_charge_transactions
    WHERE company_number = 'SC002116'
    LIMIT 5
""")

transactions = cur.fetchall()
if transactions:
    print("\n\nSample additional transactions:")
    print("=" * 80)
    for t in transactions:
        print(f"Charge: {t[0][:30]}...")
        print(f"  Type: {t[1]}")
        print(f"  Transaction Date: {t[2]}")
        print(f"  Delivered Date: {t[3]}")
        print(f"  Description: {t[4]}")
        print()

# Summary statistics
cur.execute("""
    SELECT 
        COUNT(*) as total,
        COUNT(created_date) as has_created,
        COUNT(delivered_date) as has_delivered,
        COUNT(satisfied_date) as has_satisfied,
        COUNT(amount_secured) as has_amount,
        COUNT(short_particulars) as has_particulars,
        COUNT(persons_entitled) as has_persons
    FROM ch_scrape_charges
    WHERE company_number = 'SC002116'
    AND charge_id NOT LIKE 'page_%%'
""")

stats = cur.fetchone()
print("\n\nField completeness statistics:")
print("=" * 80)
print(f"Total charges: {stats[0]}")
print(f"Has created date: {stats[1]} ({stats[1]/stats[0]*100:.0f}%)")
print(f"Has delivered date: {stats[2]} ({stats[2]/stats[0]*100:.0f}%)")
print(f"Has satisfied date: {stats[3]} ({stats[3]/stats[0]*100:.0f}%)")
print(f"Has amount secured: {stats[4]} ({stats[4]/stats[0]*100:.0f}%)")
print(f"Has short particulars: {stats[5]} ({stats[5]/stats[0]*100:.0f}%)")
print(f"Has persons entitled: {stats[6]} ({stats[6]/stats[0]*100:.0f}%)")

cur.close()
conn.close()