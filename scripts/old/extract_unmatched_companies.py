#!/usr/bin/env python3
"""
Extract unmatched company names from land_registry_data and populate the ch_scrape_queue table.
Also exports to CSV for reference.
"""

import os
import sys
import csv
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from dotenv import load_dotenv

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

def extract_unmatched_companies(export_csv=True, limit=None):
    """
    Extract unmatched company names and populate scraping queue
    
    Args:
        export_csv: Whether to export to CSV file
        limit: Limit number of companies (for testing)
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        print("Extracting unmatched company names...")
        
        # Collect all unique unmatched company names
        unmatched_companies = set()
        
        for i in range(1, 5):  # Check all 4 proprietor positions
            query = f"""
                SELECT DISTINCT lr.proprietor_{i}_name
                FROM land_registry_data lr
                LEFT JOIN land_registry_ch_matches m ON lr.id = m.id
                WHERE lr.proprietorship_{i}_category IN ('Limited Company or Public Limited Company', 'Limited Liability Partnership')
                AND (m.ch_match_type_{i} = 'No_Match' OR m.ch_match_type_{i} IS NULL)
                AND lr.proprietor_{i}_name IS NOT NULL
                AND lr.proprietor_{i}_name != ''
            """
            if limit:
                query += f" LIMIT {limit // 4}"  # Distribute limit across all positions
            
            cur.execute(query)
            for row in cur.fetchall():
                unmatched_companies.add(row[0])
        
        print(f"Found {len(unmatched_companies):,} unique unmatched company names")
        
        # Apply overall limit if specified
        if limit:
            unmatched_companies = list(unmatched_companies)[:limit]
        
        # Insert into ch_scrape_queue
        print("Populating ch_scrape_queue table...")
        
        # Prepare data for insertion
        insert_data = [(name,) for name in unmatched_companies]
        
        # Batch insert with ON CONFLICT to avoid duplicates
        execute_values(
            cur,
            """
            INSERT INTO ch_scrape_queue (search_name)
            VALUES %s
            ON CONFLICT (search_name) DO NOTHING
            """,
            insert_data,
            template='(%s)',
            page_size=1000
        )
        
        conn.commit()
        
        # Get count of inserted records
        cur.execute("SELECT COUNT(*) FROM ch_scrape_queue WHERE search_status = 'pending'")
        pending_count = cur.fetchone()[0]
        print(f"Successfully populated ch_scrape_queue with {pending_count:,} companies to search")
        
        # Export to CSV if requested
        if export_csv:
            csv_filename = f"unmatched_companies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            if limit:
                csv_filename = f"unmatched_companies_sample_{limit}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            csv_path = os.path.join('data', csv_filename)
            os.makedirs('data', exist_ok=True)
            
            print(f"Exporting to CSV: {csv_path}")
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['company_name'])
                for company_name in sorted(unmatched_companies):
                    writer.writerow([company_name])
            
            print(f"Exported {len(unmatched_companies):,} company names to {csv_path}")
        
        # Show some statistics
        cur.execute("""
            SELECT search_status, COUNT(*) as count
            FROM ch_scrape_queue
            GROUP BY search_status
            ORDER BY search_status
        """)
        
        print("\nCurrent scraping queue status:")
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]:,}")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract unmatched company names for scraping')
    parser.add_argument('--limit', type=int, help='Limit number of companies to extract (for testing)')
    parser.add_argument('--no-csv', action='store_true', help='Skip CSV export')
    
    args = parser.parse_args()
    
    extract_unmatched_companies(
        export_csv=not args.no_csv,
        limit=args.limit
    )

if __name__ == '__main__':
    main()