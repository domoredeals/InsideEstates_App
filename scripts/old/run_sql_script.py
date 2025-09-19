#!/usr/bin/env python3
"""
Helper script to run SQL files using the configured database connection
"""

import sys
import os
import psycopg2
from pathlib import Path

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

def run_sql_file(sql_file_path):
    """Execute a SQL file using the configured database connection"""
    try:
        # Connect to database
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor()
        print(f"Connected to database successfully")
        
        # Read and execute SQL file
        with open(sql_file_path, 'r') as f:
            sql_content = f.read()
        
        print(f"Executing {sql_file_path}...")
        cursor.execute(sql_content)
        conn.commit()
        
        print(f"✓ SQL script executed successfully!")
        
        # Close connection
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python run_sql_script.py <sql_file>")
        sys.exit(1)
    
    sql_file = sys.argv[1]
    if not os.path.exists(sql_file):
        print(f"Error: File not found: {sql_file}")
        sys.exit(1)
    
    run_sql_file(sql_file)