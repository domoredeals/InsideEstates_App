#!/usr/bin/env python3
"""
Check the results of scraping and matching process.
Shows what companies were found and how many went from No_Match to matched.
"""

import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def connect_to_db():
    """Connect to the PostgreSQL database."""
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

def check_scrape_results(conn):
    """Check what was scraped and matched."""
    cursor = conn.cursor()
    
    print("\n" + "="*80)
    print("SCRAPING RESULTS SUMMARY")
    print("="*80)
    
    # 1. Check scrape queue status
    print("\n1. SCRAPE QUEUE STATUS:")
    cursor.execute("""
        SELECT search_status, COUNT(*) 
        FROM ch_scrape_queue 
        GROUP BY search_status 
        ORDER BY search_status
    """)
    for status, count in cursor.fetchall():
        print(f"   {status}: {count}")
    
    # 2. Check what was successfully scraped
    print("\n2. SUCCESSFULLY SCRAPED COMPANIES:")
    cursor.execute("""
        SELECT 
            COUNT(CASE WHEN scrape_status = 'parsed' THEN 1 END) as parsed,
            COUNT(CASE WHEN scrape_status = 'scraped' THEN 1 END) as scraped_not_parsed,
            COUNT(CASE WHEN scrape_status = 'error' THEN 1 END) as errors,
            COUNT(CASE WHEN scrape_status = 'pending' THEN 1 END) as pending
        FROM ch_scrape_overview
    """)
    result = cursor.fetchone()
    print(f"   Parsed successfully: {result[0]}")
    print(f"   Scraped but not parsed: {result[1]}")
    print(f"   Errors: {result[2]}")
    print(f"   Still pending: {result[3]}")
    
    # 3. Show some examples of scraped companies
    print("\n3. EXAMPLES OF SCRAPED COMPANIES:")
    cursor.execute("""
        SELECT company_name, company_number, company_status, previous_names
        FROM ch_scrape_overview
        WHERE scrape_status = 'parsed'
        ORDER BY scrape_timestamp DESC
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(f"   - {row[0]} ({row[1]}) - Status: {row[2]}")
        if row[3]:
            print(f"     Previous names: {row[3]}")
    
    # 4. Check matches from scraped data
    print("\n4. MATCHES FROM SCRAPED DATA:")
    cursor.execute("""
        SELECT 
            COUNT(DISTINCT id) as total_matches,
            COUNT(DISTINCT CASE WHEN ch_match_type_1 IS NOT NULL THEN id END) as prop1_matches,
            COUNT(DISTINCT CASE WHEN ch_match_type_2 IS NOT NULL THEN id END) as prop2_matches,
            COUNT(DISTINCT CASE WHEN ch_match_type_3 IS NOT NULL THEN id END) as prop3_matches,
            COUNT(DISTINCT CASE WHEN ch_match_type_4 IS NOT NULL THEN id END) as prop4_matches
        FROM land_registry_ch_matches
        WHERE scraped_data = 'Y'
    """)
    result = cursor.fetchone()
    print(f"   Total properties with scraped matches: {result[0]}")
    if result[0] > 0:
        print(f"   - Proprietor 1 matches: {result[1]}")
        print(f"   - Proprietor 2 matches: {result[2]}")
        print(f"   - Proprietor 3 matches: {result[3]}")
        print(f"   - Proprietor 4 matches: {result[4]}")
    
    # 5. Show examples of new matches
    print("\n5. EXAMPLES OF NEW MATCHES (No_Match → Matched):")
    cursor.execute("""
        SELECT 
            lr.proprietor_1_name as land_reg_name,
            lr.company_1_reg_no as land_reg_number,
            m.ch_matched_name_1 as matched_name,
            m.ch_matched_number_1 as matched_number,
            m.ch_match_type_1 as match_type
        FROM land_registry_data lr
        JOIN land_registry_ch_matches m ON lr.id = m.id
        WHERE m.scraped_data = 'Y'
        AND m.ch_match_type_1 != 'No_Match'
        LIMIT 10
    """)
    results = cursor.fetchall()
    if results:
        for row in results:
            print(f"   Land Registry: {row[0]} ({row[1] or 'No number'})")
            print(f"   → Matched to: {row[2]} ({row[3]})")
            print(f"   Match type: {row[4]}")
            print()
    else:
        print("   No new matches found yet.")
    
    # 6. Check if match_scraped_companies.py needs to be run
    print("\n6. UNPROCESSED SCRAPED COMPANIES:")
    cursor.execute("""
        SELECT COUNT(*)
        FROM ch_scrape_overview so
        WHERE so.scrape_status = 'parsed'
        AND NOT EXISTS (
            SELECT 1 
            FROM land_registry_ch_matches m
            JOIN land_registry_data lr ON m.id = lr.id
            WHERE m.scraped_data = 'Y'
            AND (
                (m.ch_matched_number_1 = so.company_number) OR
                (m.ch_matched_number_2 = so.company_number) OR
                (m.ch_matched_number_3 = so.company_number) OR
                (m.ch_matched_number_4 = so.company_number)
            )
        )
    """)
    unprocessed = cursor.fetchone()[0]
    if unprocessed > 0:
        print(f"   ⚠️  {unprocessed} scraped companies haven't been matched yet!")
        print(f"   Run: python scripts/match_scraped_companies.py")
    else:
        print("   ✓ All scraped companies have been processed for matching")
    
    # 7. Check status of newly scraped companies
    print("\n7. NEWLY SCRAPED COMPANIES STATUS:")
    cursor.execute("""
        SELECT 
            sq.search_status,
            COUNT(DISTINCT sq.search_name) as count
        FROM ch_scrape_queue sq
        WHERE sq.created_at >= CURRENT_DATE - INTERVAL '1 day'
        GROUP BY sq.search_status
    """)
    for status, count in cursor.fetchall():
        print(f"   {status}: {count}")
    
    # 8. Show scraped but not parsed companies
    print("\n8. SCRAPED BUT NOT PARSED COMPANIES:")
    cursor.execute("""
        SELECT company_name, company_url, scrape_timestamp
        FROM ch_scrape_overview
        WHERE scrape_status = 'scraped'
        ORDER BY scrape_timestamp DESC
        LIMIT 5
    """)
    results = cursor.fetchall()
    if results:
        print("   These companies were scraped but not parsed yet:")
        for row in results:
            print(f"   - {row[0]} ({row[1]})")
            print(f"     Scraped at: {row[2]}")
    
    # 9. Summary of No_Match reduction
    print("\n9. NO_MATCH REDUCTION SUMMARY:")
    cursor.execute("""
        SELECT COUNT(*) 
        FROM land_registry_ch_matches 
        WHERE ch_match_type_1 = 'No_Match'
    """)
    current_no_match = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT COUNT(DISTINCT id)
        FROM land_registry_ch_matches
        WHERE scraped_data = 'Y'
    """)
    total_scraped_matches = cursor.fetchone()[0]
    
    print(f"   Current No_Match companies: {current_no_match:,}")
    print(f"   Total matches from scraped data: {total_scraped_matches:,}")
    
    cursor.close()

def main():
    """Main function."""
    conn = None
    
    try:
        conn = connect_to_db()
        check_scrape_results(conn)
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()