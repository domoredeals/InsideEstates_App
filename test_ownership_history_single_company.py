#!/usr/bin/env python3
"""
Test ownership_history build with a single company
This allows verification of the logic before processing all data
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
import logging
from tabulate import tabulate

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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

def find_test_company(conn):
    """Find a good test company with multiple properties and transactions"""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Find companies with multiple properties and some that have been sold
        cur.execute("""
            WITH company_properties AS (
                SELECT 
                    proprietor_1_name as company_name,
                    COUNT(DISTINCT title_number) as property_count,
                    COUNT(DISTINCT file_month) as snapshot_count,
                    MIN(file_month) as first_seen,
                    MAX(file_month) as last_seen,
                    COUNT(DISTINCT CASE WHEN file_month = (SELECT MAX(file_month) FROM land_registry_data) THEN title_number END) as current_properties
                FROM land_registry_data
                WHERE proprietor_1_name IS NOT NULL
                  AND proprietor_1_name != ''
                  AND dataset_type = 'CCOD'  -- UK companies only for simplicity
                GROUP BY proprietor_1_name
                HAVING COUNT(DISTINCT title_number) >= 5  -- At least 5 properties
                   AND COUNT(DISTINCT file_month) > 1     -- Seen in multiple snapshots
            )
            SELECT 
                company_name,
                property_count,
                current_properties,
                property_count - current_properties as sold_properties,
                first_seen,
                last_seen
            FROM company_properties
            WHERE property_count - current_properties > 0  -- Has sold some properties
            ORDER BY property_count DESC
            LIMIT 10
        """)
        
        companies = cur.fetchall()
        
        print("\n📊 Test Company Candidates:")
        print(tabulate(companies, headers='keys', tablefmt='grid'))
        
        if companies:
            return companies[0]['company_name']
        else:
            # Fallback - just get any company with multiple properties
            cur.execute("""
                SELECT proprietor_1_name as company_name, COUNT(DISTINCT title_number) as count
                FROM land_registry_data
                WHERE proprietor_1_name IS NOT NULL AND proprietor_1_name != ''
                GROUP BY proprietor_1_name
                HAVING COUNT(DISTINCT title_number) >= 3
                ORDER BY count DESC
                LIMIT 1
            """)
            result = cur.fetchone()
            return result['company_name'] if result else None

def test_single_company(conn, company_name):
    """Test ownership history build for a single company"""
    logger.info(f"🔧 Testing with company: {company_name}")
    
    with conn.cursor() as cur:
        # Create test table
        logger.info("📋 Creating test table...")
        cur.execute("""
            DROP TABLE IF EXISTS ownership_history_test;
            CREATE TABLE ownership_history_test (LIKE ownership_history INCLUDING ALL);
        """)
        
        # Get latest snapshot for this logic
        cur.execute("SELECT MAX(file_month) as latest FROM land_registry_data")
        latest_snapshot = cur.fetchone()[0]
        logger.info(f"📊 Latest snapshot: {latest_snapshot}")
        
        # Create temp table for titles in latest snapshot
        cur.execute("""
            CREATE TEMP TABLE temp_test_latest AS
            SELECT DISTINCT title_number
            FROM land_registry_data
            WHERE file_month = %s
              AND proprietor_1_name = %s
        """, (latest_snapshot, company_name))
        
        # Get all titles ever owned by this company
        cur.execute("""
            CREATE TEMP TABLE test_company_titles AS
            SELECT DISTINCT title_number
            FROM land_registry_data
            WHERE proprietor_1_name = %s
        """, (company_name,))
        
        # Run the main processing query
        logger.info("🔄 Processing ownership history...")
        
        insert_sql = f"""
        INSERT INTO ownership_history_test
        WITH raw_data AS (
            SELECT DISTINCT
                lr.title_number,
                lr.property_address,
                CASE 
                    WHEN lr.date_proprietor_added IS NOT NULL THEN
                        lr.date_proprietor_added::TEXT
                    ELSE (
                        SELECT MIN(sub.file_month)::TEXT
                        FROM land_registry_data sub
                        WHERE sub.title_number = lr.title_number
                          AND sub.proprietor_1_name = lr.proprietor_1_name
                          AND COALESCE(sub.proprietor_2_name, '') = COALESCE(lr.proprietor_2_name, '')
                          AND COALESCE(sub.proprietor_3_name, '') = COALESCE(lr.proprietor_3_name, '')
                          AND COALESCE(sub.proprietor_4_name, '') = COALESCE(lr.proprietor_4_name, '')
                    )
                END AS ownership_start_date,
                CASE 
                    WHEN lr.date_proprietor_added IS NOT NULL THEN
                        'DATED_' || lr.date_proprietor_added::TEXT
                    ELSE 
                        'FIRST_APPEAR_' || lr.proprietor_1_name || '_' || 
                        COALESCE(lr.proprietor_2_name, '') || '_' || 
                        COALESCE(lr.proprietor_3_name, '') || '_' || 
                        COALESCE(lr.proprietor_4_name, '')
                END as dedup_key,
                lr.price_paid::REAL AS price_paid,
                lr.proprietor_1_name AS owner_1,
                lr.proprietor_2_name AS owner_2,
                lr.proprietor_3_name AS owner_3,
                lr.proprietor_4_name AS owner_4,
                lr.source_filename AS source_file,
                lr.file_month,
                lr.dataset_type,
                lr.change_indicator,
                lr.update_type
            FROM land_registry_data lr
            JOIN test_company_titles tct ON lr.title_number = tct.title_number
            WHERE lr.proprietor_1_name IS NOT NULL 
              AND lr.proprietor_1_name != ''
        ),
        deduplicated AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY title_number, dedup_key ORDER BY file_month DESC) as rn
            FROM raw_data
        ),
        unique_records AS (
            SELECT * FROM deduplicated WHERE rn = 1
        ),
        with_sequences AS (
            SELECT *,
                LEAD(ownership_start_date) OVER (PARTITION BY title_number ORDER BY ownership_start_date) AS ownership_end_date,
                LAG(owner_1) OVER (PARTITION BY title_number ORDER BY ownership_start_date) AS seller_1,
                LAG(owner_2) OVER (PARTITION BY title_number ORDER BY ownership_start_date) AS seller_2,
                LAG(owner_3) OVER (PARTITION BY title_number ORDER BY ownership_start_date) AS seller_3,
                LAG(owner_4) OVER (PARTITION BY title_number ORDER BY ownership_start_date) AS seller_4,
                LEAD(owner_1) OVER (PARTITION BY title_number ORDER BY ownership_start_date) AS buyer_1,
                LEAD(owner_2) OVER (PARTITION BY title_number ORDER BY ownership_start_date) AS buyer_2,
                LEAD(owner_3) OVER (PARTITION BY title_number ORDER BY ownership_start_date) AS buyer_3,
                LEAD(owner_4) OVER (PARTITION BY title_number ORDER BY ownership_start_date) AS buyer_4,
                LEAD(price_paid) OVER (PARTITION BY title_number ORDER BY ownership_start_date) AS price_at_disposal
            FROM unique_records
        ),
        with_latest_check AS (
            SELECT 
                ws.*,
                CASE WHEN lt.title_number IS NOT NULL THEN 1 ELSE 0 END as in_latest_snapshot
            FROM with_sequences ws
            LEFT JOIN temp_test_latest lt ON ws.title_number = lt.title_number
        )
        SELECT
            title_number,
            property_address,
            ownership_start_date,
            CASE 
                WHEN ownership_end_date IS NOT NULL THEN ownership_end_date
                WHEN in_latest_snapshot = 0 THEN '{latest_snapshot}'::TEXT
                ELSE NULL
            END AS ownership_end_date,
            owner_1, owner_2, owner_3, owner_4,
            seller_1, seller_2, seller_3, seller_4,
            CASE 
                WHEN ownership_end_date IS NOT NULL AND buyer_1 IS NULL THEN 'PRIVATE SALE'
                WHEN ownership_end_date IS NOT NULL AND buyer_1 = owner_1 THEN NULL
                WHEN in_latest_snapshot = 0 AND ownership_end_date IS NULL THEN 'PRIVATE SALE'
                ELSE buyer_1
            END AS buyer_1,
            buyer_2, buyer_3, buyer_4,
            price_paid AS price_at_acquisition,
            CASE 
                WHEN ownership_end_date IS NOT NULL THEN price_at_disposal 
                ELSE NULL 
            END AS price_at_disposal,
            CASE 
                WHEN ownership_end_date IS NOT NULL THEN 'Previous'
                WHEN in_latest_snapshot = 0 THEN 'Previous'
                ELSE 'Current'
            END AS ownership_status,
            CASE 
                WHEN ownership_start_date IS NOT NULL THEN
                    DATE_PART('day', 
                        COALESCE(
                            ownership_end_date::DATE,
                            CASE WHEN in_latest_snapshot = 0 THEN '{latest_snapshot}'::DATE ELSE CURRENT_DATE END
                        ) - ownership_start_date::DATE
                    )::INTEGER
                ELSE NULL
            END AS ownership_duration_days,
            source_file AS source,
            CASE
                WHEN dataset_type = 'OCOD' THEN 'OVERSEAS COMPANY'
                WHEN dataset_type = 'CCOD' THEN 'UK COMPANY'
                ELSE 'OTHER'
            END AS ownership_type,
            CASE 
                WHEN in_latest_snapshot = 0 AND ownership_end_date IS NULL THEN 1
                WHEN ownership_end_date IS NOT NULL AND buyer_1 = 'PRIVATE SALE' THEN 1
                ELSE 0
            END AS inferred_disposal_flag,
            CASE 
                WHEN in_latest_snapshot = 0 THEN 1
                ELSE 0
            END AS disposal_from_company
        FROM with_latest_check
        WHERE owner_1 = %s  -- Filter to our test company
        """
        
        cur.execute(insert_sql, (company_name,))
        records_created = cur.rowcount
        conn.commit()
        
        logger.info(f"✅ Created {records_created} ownership records")
        
        # Analyze results
        logger.info("\n📊 Analyzing results...")
        
        # Summary stats
        cur.execute("""
            SELECT 
                ownership_status,
                COUNT(*) as count,
                COUNT(DISTINCT title_number) as unique_properties
            FROM ownership_history_test
            GROUP BY ownership_status
            ORDER BY ownership_status
        """)
        
        stats = cur.fetchall()
        print("\n📊 Ownership Status Summary:")
        print(tabulate(stats, headers=['Status', 'Records', 'Properties'], tablefmt='grid'))
        
        # Show some current properties
        cur.execute("""
            SELECT 
                title_number,
                property_address,
                ownership_start_date,
                ownership_duration_days,
                price_at_acquisition
            FROM ownership_history_test
            WHERE ownership_status = 'Current'
            ORDER BY ownership_start_date DESC
            LIMIT 5
        """)
        
        current = cur.fetchall()
        print(f"\n📋 Sample Current Properties for {company_name}:")
        for row in current:
            print(f"\nTitle: {row[0]}")
            print(f"Address: {row[1][:60]}...")
            print(f"Since: {row[2]} ({row[3] or 0} days)")
            if row[4]:
                print(f"Price: £{row[4]:,.0f}")
        
        # Show transactions (sold properties)
        cur.execute("""
            SELECT 
                title_number,
                property_address,
                ownership_start_date,
                ownership_end_date,
                buyer_1,
                price_at_disposal,
                inferred_disposal_flag
            FROM ownership_history_test
            WHERE ownership_status = 'Previous'
            ORDER BY ownership_end_date DESC
            LIMIT 5
        """)
        
        previous = cur.fetchall()
        print(f"\n📋 Sample Sold/Previous Properties for {company_name}:")
        for row in previous:
            print(f"\nTitle: {row[0]}")
            print(f"Address: {row[1][:60]}...")
            print(f"Period: {row[2]} to {row[3]}")
            print(f"Buyer: {row[4] or 'Unknown'}")
            if row[5]:
                print(f"Sale Price: £{row[5]:,.0f}")
            if row[6] == 1:
                print("(Inferred disposal - property disappeared from records)")
        
        # Check data quality
        cur.execute("""
            SELECT 
                'Missing start dates' as issue,
                COUNT(*) as count
            FROM ownership_history_test
            WHERE ownership_start_date IS NULL
            
            UNION ALL
            
            SELECT 
                'Previous without end date' as issue,
                COUNT(*) as count
            FROM ownership_history_test
            WHERE ownership_status = 'Previous' 
              AND ownership_end_date IS NULL
              AND inferred_disposal_flag = 0
              
            UNION ALL
            
            SELECT 
                'Negative duration' as issue,
                COUNT(*) as count
            FROM ownership_history_test
            WHERE ownership_duration_days < 0
        """)
        
        issues = cur.fetchall()
        print("\n🔍 Data Quality Check:")
        print(tabulate(issues, headers=['Issue', 'Count'], tablefmt='grid'))
        
        # Show the raw data timeline for one property
        cur.execute("""
            SELECT title_number
            FROM ownership_history_test
            WHERE ownership_status = 'Previous'
            LIMIT 1
        """)
        
        sample_title = cur.fetchone()
        if sample_title:
            print(f"\n🔍 Raw timeline for title {sample_title[0]}:")
            cur.execute("""
                SELECT 
                    file_month,
                    proprietor_1_name,
                    date_proprietor_added,
                    change_indicator,
                    update_type
                FROM land_registry_data
                WHERE title_number = %s
                ORDER BY file_month
                LIMIT 10
            """, (sample_title[0],))
            
            timeline = cur.fetchall()
            print(tabulate(timeline, 
                         headers=['Snapshot', 'Owner', 'Date Added', 'Change', 'Type'], 
                         tablefmt='grid'))

def main():
    """Main test function"""
    print("🧪 OWNERSHIP HISTORY TEST - SINGLE COMPANY")
    print("=" * 50)
    
    conn = None
    try:
        conn = get_db_connection()
        logger.info("Connected to database")
        
        # Find a good test company
        test_company = find_test_company(conn)
        
        if not test_company:
            print("❌ Could not find a suitable test company")
            return
        
        print(f"\n🎯 Selected test company: {test_company}")
        confirm = input("\nProceed with this company? (yes/no): ")
        
        if confirm.lower() == 'yes':
            test_single_company(conn, test_company)
            
            print("\n✅ Test complete!")
            print("Check the results above. If they look correct, you can run the full build.")
        else:
            print("Test cancelled.")
            
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    main()