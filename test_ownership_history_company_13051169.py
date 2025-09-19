#!/usr/bin/env python3
"""
Test ownership_history build with company 13051169
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

def get_company_info(conn, company_reg_no):
    """Get information about the test company"""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # First check Companies House data
        cur.execute("""
            SELECT 
                company_number,
                company_name,
                company_status,
                incorporation_date,
                reg_address_postcode
            FROM companies_house_data
            WHERE company_number = %s
        """, (company_reg_no,))
        
        ch_info = cur.fetchone()
        if ch_info:
            print("\n🏢 Companies House Information:")
            print(f"Company Number: {ch_info['company_number']}")
            print(f"Company Name: {ch_info['company_name']}")
            print(f"Status: {ch_info['company_status']}")
            print(f"Incorporated: {ch_info['incorporation_date']}")
            print(f"Postcode: {ch_info['reg_address_postcode']}")
        
        # Get all variations of company names in Land Registry data
        cur.execute("""
            SELECT DISTINCT
                proprietor_1_name as company_name,
                company_1_reg_no as reg_no,
                COUNT(DISTINCT title_number) as property_count,
                MIN(file_month) as first_seen,
                MAX(file_month) as last_seen,
                COUNT(DISTINCT file_month) as snapshot_count
            FROM land_registry_data
            WHERE company_1_reg_no = %s
               OR company_2_reg_no = %s
               OR company_3_reg_no = %s
               OR company_4_reg_no = %s
            GROUP BY proprietor_1_name, company_1_reg_no
            ORDER BY property_count DESC
        """, (company_reg_no, company_reg_no, company_reg_no, company_reg_no))
        
        lr_variations = cur.fetchall()
        
        if lr_variations:
            print("\n📋 Land Registry Name Variations:")
            print(tabulate(lr_variations, headers='keys', tablefmt='grid'))
            
            # Return the most common name (with most properties)
            return lr_variations[0]['company_name']
        
        return None

def test_company_13051169(conn):
    """Test ownership history build for company 13051169"""
    company_reg_no = '13051169'
    
    # Get company info and name
    company_name = get_company_info(conn, company_reg_no)
    
    if not company_name:
        print(f"❌ No properties found for company {company_reg_no}")
        return
    
    logger.info(f"🔧 Testing with company: {company_name} (Reg: {company_reg_no})")
    
    with conn.cursor() as cur:
        # Create test table
        logger.info("📋 Creating test table...")
        cur.execute("""
            DROP TABLE IF EXISTS ownership_history_test;
            CREATE TABLE ownership_history_test (LIKE ownership_history INCLUDING ALL);
        """)
        
        # Get latest snapshot
        cur.execute("SELECT MAX(file_month) as latest FROM land_registry_data")
        latest_snapshot = cur.fetchone()[0]
        logger.info(f"📊 Latest snapshot: {latest_snapshot}")
        
        # Create temp table for all titles owned by this company (by reg number)
        cur.execute("""
            CREATE TEMP TABLE test_company_titles AS
            SELECT DISTINCT title_number
            FROM land_registry_data
            WHERE company_1_reg_no = %s
               OR company_2_reg_no = %s
               OR company_3_reg_no = %s
               OR company_4_reg_no = %s
        """, (company_reg_no, company_reg_no, company_reg_no, company_reg_no))
        
        cur.execute("SELECT COUNT(*) FROM test_company_titles")
        title_count = cur.fetchone()[0]
        logger.info(f"📊 Found {title_count} unique titles for company {company_reg_no}")
        
        # Show sample properties
        cur.execute("""
            SELECT 
                lr.title_number,
                lr.property_address,
                lr.postcode,
                lr.date_proprietor_added,
                lr.file_month,
                lr.proprietor_1_name,
                lr.company_1_reg_no
            FROM land_registry_data lr
            JOIN test_company_titles tct ON lr.title_number = tct.title_number
            WHERE lr.company_1_reg_no = %s
            ORDER BY lr.file_month DESC
            LIMIT 5
        """, (company_reg_no,))
        
        sample_props = cur.fetchall()
        print("\n📋 Sample Properties:")
        print(tabulate(sample_props, 
                     headers=['Title', 'Address', 'Postcode', 'Date Added', 'Snapshot', 'Owner Name', 'Reg No'],
                     tablefmt='grid'))
        
        # Check latest snapshot status
        cur.execute("""
            CREATE TEMP TABLE temp_test_latest AS
            SELECT DISTINCT lr.title_number
            FROM land_registry_data lr
            JOIN test_company_titles tct ON lr.title_number = tct.title_number
            WHERE lr.file_month = %s
              AND (lr.company_1_reg_no = %s OR lr.company_2_reg_no = %s 
                   OR lr.company_3_reg_no = %s OR lr.company_4_reg_no = %s)
        """, (latest_snapshot, company_reg_no, company_reg_no, company_reg_no, company_reg_no))
        
        cur.execute("SELECT COUNT(*) FROM temp_test_latest")
        current_count = cur.fetchone()[0]
        logger.info(f"📊 Properties in latest snapshot: {current_count}")
        logger.info(f"📊 Properties that may have been sold: {title_count - current_count}")
        
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
        ),
        final_data AS (
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
                        EXTRACT(EPOCH FROM (
                            COALESCE(
                                ownership_end_date::DATE,
                                CASE WHEN in_latest_snapshot = 0 THEN '{latest_snapshot}'::DATE ELSE CURRENT_DATE END
                            )::TIMESTAMP - ownership_start_date::DATE::TIMESTAMP
                        )) / 86400
                    ELSE NULL
                END::INTEGER AS ownership_duration_days,
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
        )
        SELECT * FROM final_data
        -- Include records for this company even if it's not owner_1
        WHERE EXISTS (
            SELECT 1 FROM land_registry_data lr2
            WHERE lr2.title_number = final_data.title_number
              AND (lr2.company_1_reg_no = %s OR lr2.company_2_reg_no = %s 
                   OR lr2.company_3_reg_no = %s OR lr2.company_4_reg_no = %s)
        )
        """
        
        cur.execute(insert_sql, (company_reg_no, company_reg_no, company_reg_no, company_reg_no))
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
        
        # Show current properties
        cur.execute("""
            SELECT 
                title_number,
                owner_1,
                property_address,
                ownership_start_date,
                ownership_duration_days,
                price_at_acquisition
            FROM ownership_history_test
            WHERE ownership_status = 'Current'
              AND owner_1 = %s
            ORDER BY ownership_start_date DESC
            LIMIT 10
        """, (company_name,))
        
        current = cur.fetchall()
        print(f"\n📋 Current Properties for {company_name}:")
        for row in current:
            print(f"\nTitle: {row[0]}")
            print(f"Owner: {row[1]}")
            print(f"Address: {row[2][:60]}...")
            print(f"Since: {row[3]} ({row[4] or 0} days)")
            if row[5]:
                print(f"Acquisition Price: £{row[5]:,.0f}")
        
        # Show sold properties
        cur.execute("""
            SELECT 
                title_number,
                owner_1,
                property_address,
                ownership_start_date,
                ownership_end_date,
                buyer_1,
                price_at_disposal,
                inferred_disposal_flag
            FROM ownership_history_test
            WHERE ownership_status = 'Previous'
              AND owner_1 = %s
            ORDER BY ownership_end_date DESC
            LIMIT 10
        """, (company_name,))
        
        previous = cur.fetchall()
        print(f"\n📋 Sold/Previous Properties for {company_name}:")
        for row in previous:
            print(f"\nTitle: {row[0]}")
            print(f"Seller: {row[1]}")
            print(f"Address: {row[2][:60]}...")
            print(f"Period: {row[3]} to {row[4]}")
            print(f"Buyer: {row[5] or 'Unknown'}")
            if row[6]:
                print(f"Sale Price: £{row[6]:,.0f}")
            if row[7] == 1:
                print("⚠️ Inferred disposal (property disappeared from records)")
        
        # Check one property's full timeline
        cur.execute("""
            SELECT title_number FROM ownership_history_test LIMIT 1
        """)
        sample_title = cur.fetchone()
        
        if sample_title:
            print(f"\n🔍 Full timeline for property {sample_title[0]}:")
            cur.execute("""
                SELECT 
                    file_month,
                    update_type,
                    change_indicator,
                    proprietor_1_name,
                    company_1_reg_no,
                    date_proprietor_added,
                    price_paid
                FROM land_registry_data
                WHERE title_number = %s
                ORDER BY file_month
                LIMIT 20
            """, (sample_title[0],))
            
            timeline = cur.fetchall()
            print(tabulate(timeline, 
                         headers=['Month', 'Type', 'Change', 'Owner', 'Reg No', 'Date Added', 'Price'],
                         tablefmt='grid'))

def main():
    """Main test function"""
    print("🧪 OWNERSHIP HISTORY TEST - COMPANY 13051169")
    print("=" * 60)
    
    conn = None
    try:
        conn = get_db_connection()
        logger.info("Connected to database")
        
        test_company_13051169(conn)
        
        print("\n✅ Test complete!")
        print("Check the results above. If they look correct, you can run the full build.")
            
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    main()