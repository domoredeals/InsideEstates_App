#!/usr/bin/env python3
"""
Check if ownership_history table exists
"""

import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def main():
    """Check ownership_history table status"""
    conn = None
    try:
        # Connect to database
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432'),
            database=os.getenv('DB_NAME', 'insideestates_app'),
            user=os.getenv('DB_USER', 'insideestates_user'),
            password=os.getenv('DB_PASSWORD', 'InsideEstates2024!')
        )
        
        with conn.cursor() as cur:
            # Check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'ownership_history'
                ) as table_exists
            """)
            table_exists = cur.fetchone()[0]
            print(f"Table ownership_history exists: {table_exists}")
            
            # Check indexes
            cur.execute("""
                SELECT indexname 
                FROM pg_indexes 
                WHERE tablename = 'ownership_history'
                ORDER BY indexname
            """)
            indexes = cur.fetchall()
            
            if indexes:
                print("\nExisting indexes:")
                for idx in indexes:
                    print(f"  - {idx[0]}")
            
            # If table doesn't exist, drop orphan indexes
            if not table_exists:
                print("\n⚠️ Table doesn't exist but indexes do. Cleaning up orphan indexes...")
                
                # Get all indexes that might be orphaned
                cur.execute("""
                    SELECT indexname 
                    FROM pg_indexes 
                    WHERE indexname LIKE 'idx_ownership%'
                """)
                orphan_indexes = cur.fetchall()
                
                for idx in orphan_indexes:
                    try:
                        cur.execute(f"DROP INDEX IF EXISTS {idx[0]}")
                        print(f"  Dropped: {idx[0]}")
                    except Exception as e:
                        print(f"  Failed to drop {idx[0]}: {e}")
                
                conn.commit()
                print("\n✅ Cleanup complete. You can now create the table.")
    
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()