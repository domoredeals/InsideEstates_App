#!/usr/bin/env python3
"""
Create optimized views for QlikView
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from config.postgresql_config import POSTGRESQL_CONFIG

def create_views():
    conn = psycopg2.connect(**POSTGRESQL_CONFIG)
    cursor = conn.cursor()
    
    try:
        print("Creating QlikView optimized views...")
        
        with open('scripts/create_qlikview_views.sql', 'r') as f:
            sql = f.read()
            
        # Execute the SQL
        cursor.execute(sql)
        conn.commit()
        
        print("✓ Views created successfully!")
        
        # List created views
        cursor.execute("""
            SELECT viewname 
            FROM pg_views 
            WHERE schemaname = 'public' 
            AND viewname LIKE 'qv_%'
            ORDER BY viewname
        """)
        
        views = cursor.fetchall()
        print(f"\nCreated {len(views)} views:")
        for view in views:
            print(f"  - {view[0]}")
            
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    create_views()