#!/usr/bin/env python3
"""
Setup PostgreSQL database for InsideEstates App
"""
import sys
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.postgresql_config import POSTGRESQL_CONFIG


def create_database():
    """Create the database if it doesn't exist"""
    # Connect to PostgreSQL server (not to a specific database)
    conn_params = POSTGRESQL_CONFIG.copy()
    db_name = conn_params.pop('database')
    conn_params['database'] = 'postgres'  # Connect to default database
    
    try:
        conn = psycopg2.connect(**conn_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (db_name,)
        )
        exists = cursor.fetchone()
        
        if not exists:
            print(f"Creating database '{db_name}'...")
            cursor.execute(f'CREATE DATABASE "{db_name}"')
            print(f"Database '{db_name}' created successfully!")
        else:
            print(f"Database '{db_name}' already exists.")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"Error creating database: {e}")
        return False
    
    return True


def create_extensions():
    """Create necessary PostgreSQL extensions"""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor()
        
        extensions_sql = """
        -- Create useful extensions
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        CREATE EXTENSION IF NOT EXISTS "pg_trgm"; -- For fuzzy text search
        CREATE EXTENSION IF NOT EXISTS "postgis"; -- For geographical data (if needed)
        """
        
        print("Creating PostgreSQL extensions...")
        cursor.execute(extensions_sql)
        conn.commit()
        print("Extensions created successfully!")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"Error creating extensions: {e}")
        # PostGIS might not be available, that's okay
        if "postgis" not in str(e).lower():
            return False
    
    return True


def main():
    """Main setup function"""
    print("Setting up PostgreSQL database for InsideEstates App...")
    print("-" * 50)
    
    # Step 1: Create database
    if not create_database():
        print("Failed to create database. Exiting.")
        return
    
    # Step 2: Create extensions
    if not create_extensions():
        print("Warning: Some extensions may not have been created.")
        print("This might be okay depending on your PostgreSQL setup.")
    
    print("-" * 50)
    print("Database setup completed successfully!")
    print("\nNext steps:")
    print("1. Update .env file with your database credentials")
    print("2. Define your data sources and schema")
    print("3. Create appropriate tables based on your data")
    print("4. Run data migration scripts to populate the database")


if __name__ == "__main__":
    main()