#!/usr/bin/env python3
import psycopg2
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

cursor.execute("""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'companies_house_data'
    ORDER BY ordinal_position
""")

print("Companies House table columns:")
for col in cursor.fetchall():
    print(f"  - {col[0]}")

cursor.close()
conn.close()