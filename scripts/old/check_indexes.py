#!/usr/bin/env python3
"""
Check and create necessary indexes for efficient matching
"""

import psycopg2
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

def check_and_create_indexes():
    """Check existing indexes and create missing ones"""
    
    conn = psycopg2.connect(**POSTGRESQL_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor()
    
    print("=== CHECKING EXISTING INDEXES ===\n")
    
    # Check indexes on companies_house_data
    cursor.execute("""
        SELECT 
            schemaname,
            tablename,
            indexname,
            indexdef
        FROM pg_indexes
        WHERE tablename IN ('companies_house_data', 'land_registry_ch_matches')
        ORDER BY tablename, indexname
    """)
    
    indexes = cursor.fetchall()
    current_table = None
    
    for schema, table, index_name, index_def in indexes:
        if table != current_table:
            print(f"\n{table}:")
            current_table = table
        print(f"  - {index_name}")
    
    # Check if we need to create indexes on companies_house_data
    print("\n=== CREATING MISSING INDEXES ===\n")
    
    # Index on company_number (most important for matching)
    try:
        print("Creating index on companies_house_data.company_number...")
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_ch_company_number 
            ON companies_house_data(company_number)
        """)
        print("✅ Index created/verified")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Index on company_name for name-based lookups
    try:
        print("Creating index on companies_house_data.company_name...")
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_ch_company_name 
            ON companies_house_data(company_name)
        """)
        print("✅ Index created/verified")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Index on land_registry_ch_matches for No_Match filtering
    try:
        print("Creating index on land_registry_ch_matches.ch_match_type_1...")
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_ch_matches_type1 
            ON land_registry_ch_matches(ch_match_type_1)
            WHERE ch_match_type_1 = 'No_Match'
        """)
        print("✅ Partial index created/verified for No_Match records")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Check table sizes
    print("\n=== TABLE STATISTICS ===\n")
    cursor.execute("""
        SELECT 
            relname as table_name,
            pg_size_pretty(pg_total_relation_size(oid)) as total_size,
            n_tup_ins as rows_inserted,
            n_tup_upd as rows_updated,
            n_tup_del as rows_deleted
        FROM pg_stat_user_tables
        WHERE relname IN ('companies_house_data', 'land_registry_data', 'land_registry_ch_matches')
        ORDER BY relname
    """)
    
    stats = cursor.fetchall()
    for table, size, ins, upd, del_ in stats:
        print(f"{table}:")
        print(f"  Size: {size}")
        print(f"  Rows: inserted={ins:,}, updated={upd:,}, deleted={del_:,}")
    
    print("\n✅ Index check complete")
    print("\nIndexes should now speed up the matching process significantly.")
    
    cursor.close()
    conn.close()

if __name__ == '__main__':
    check_and_create_indexes()