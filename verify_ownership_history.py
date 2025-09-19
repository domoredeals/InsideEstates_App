#!/usr/bin/env python3
"""
Verify ownership_history table after creation
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
from tabulate import tabulate

# Load environment variables
load_dotenv()

def get_db_connection():
    """Get database connection"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        database=os.getenv('DB_NAME', 'insideestates_app'),
        user=os.getenv('DB_USER', 'insideestates_user'),
        password=os.getenv('DB_PASSWORD', 'InsideEstates2024!')
    )

def main():
    """Main verification function"""
    conn = None
    try:
        conn = get_db_connection()
        print("Connected to database\n")
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'ownership_history'
                ) as table_exists
            """)
            
            if not cur.fetchone()['table_exists']:
                print("❌ ownership_history table does not exist!")
                return
            
            print("✅ ownership_history table exists")
            
            # Get total count
            cur.execute("SELECT COUNT(*) as total FROM ownership_history")
            total = cur.fetchone()['total']
            print(f"\n📊 Total records: {total:,}")
            
            # Get ownership status distribution
            cur.execute("""
                SELECT 
                    ownership_status,
                    COUNT(*) as count,
                    ROUND(COUNT(*)::numeric / SUM(COUNT(*)) OVER() * 100, 2) as percentage
                FROM ownership_history
                GROUP BY ownership_status
                ORDER BY count DESC
            """)
            
            status_results = cur.fetchall()
            print("\n📊 Ownership Status Distribution:")
            print(tabulate(status_results, headers='keys', tablefmt='grid'))
            
            # Get ownership type distribution
            cur.execute("""
                SELECT 
                    ownership_type,
                    ownership_status,
                    COUNT(*) as count
                FROM ownership_history
                GROUP BY ownership_type, ownership_status
                ORDER BY ownership_type, ownership_status
            """)
            
            type_results = cur.fetchall()
            print("\n📊 Ownership Type Distribution:")
            print(tabulate(type_results, headers='keys', tablefmt='grid'))
            
            # Check for data quality issues
            cur.execute("""
                SELECT 
                    'Missing end dates (Previous)' as issue,
                    COUNT(*) as count
                FROM ownership_history
                WHERE ownership_status = 'Previous' 
                  AND ownership_end_date IS NULL 
                  AND inferred_disposal_flag = 0
                
                UNION ALL
                
                SELECT 
                    'Missing start dates' as issue,
                    COUNT(*) as count
                FROM ownership_history
                WHERE ownership_start_date IS NULL
                
                UNION ALL
                
                SELECT 
                    'Negative duration' as issue,
                    COUNT(*) as count
                FROM ownership_history
                WHERE ownership_duration_days < 0
            """)
            
            issues = cur.fetchall()
            print("\n🔍 Data Quality Checks:")
            print(tabulate(issues, headers='keys', tablefmt='grid'))
            
            # Sample current ownership
            cur.execute("""
                SELECT 
                    title_number,
                    owner_1,
                    property_address,
                    ownership_start_date,
                    ownership_duration_days,
                    ownership_type
                FROM ownership_history
                WHERE ownership_status = 'Current'
                ORDER BY RANDOM()
                LIMIT 5
            """)
            
            current_sample = cur.fetchall()
            print("\n📋 Sample Current Ownership Records:")
            for row in current_sample:
                print(f"\nTitle: {row['title_number']}")
                print(f"Owner: {row['owner_1']}")
                print(f"Address: {row['property_address'][:60]}...")
                print(f"Since: {row['ownership_start_date']} ({row['ownership_duration_days']} days)")
                print(f"Type: {row['ownership_type']}")
            
            # Sample transactions (Previous with buyer info)
            cur.execute("""
                SELECT 
                    title_number,
                    owner_1,
                    buyer_1,
                    ownership_start_date,
                    ownership_end_date,
                    price_at_acquisition,
                    price_at_disposal
                FROM ownership_history
                WHERE ownership_status = 'Previous'
                  AND buyer_1 IS NOT NULL
                  AND buyer_1 != 'PRIVATE SALE'
                ORDER BY RANDOM()
                LIMIT 3
            """)
            
            transaction_sample = cur.fetchall()
            print("\n📋 Sample Transaction Records (with known buyers):")
            for row in transaction_sample:
                print(f"\nTitle: {row['title_number']}")
                print(f"Seller: {row['owner_1']}")
                print(f"Buyer: {row['buyer_1']}")
                print(f"Period: {row['ownership_start_date']} to {row['ownership_end_date']}")
                if row['price_at_acquisition']:
                    print(f"Acquisition Price: £{row['price_at_acquisition']:,.0f}")
                if row['price_at_disposal']:
                    print(f"Disposal Price: £{row['price_at_disposal']:,.0f}")
            
            # Expected results from SQLite
            print("\n📊 Expected Results (from SQLite):")
            print("  Current: 4,363,780")
            print("  Previous: 1,249,486")
            print("  Total: 5,613,266")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed")

if __name__ == "__main__":
    main()