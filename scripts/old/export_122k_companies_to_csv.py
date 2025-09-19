#!/usr/bin/env python3
"""
Export the 122k pending companies from the database to a CSV file for Windows scraping.
"""

import psycopg2
import csv
import os
from dotenv import load_dotenv

load_dotenv()

def export_pending_companies():
    """Export all pending companies to CSV"""
    
    # Connect to database
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    cur = conn.cursor()
    
    output_file = 'companies_to_scrape_122k.csv'
    
    try:
        # Count total pending
        cur.execute("SELECT COUNT(*) FROM ch_scrape_queue WHERE search_status = 'pending'")
        total = cur.fetchone()[0]
        print(f"Found {total:,} pending companies to export")
        
        # Export to CSV
        print(f"Exporting to {output_file}...")
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Company Name', 'Queue ID'])  # Headers
            
            # Fetch in batches to avoid memory issues
            batch_size = 10000
            offset = 0
            exported = 0
            
            while True:
                cur.execute("""
                    SELECT search_name, id 
                    FROM ch_scrape_queue 
                    WHERE search_status = 'pending'
                    ORDER BY id
                    LIMIT %s OFFSET %s
                """, (batch_size, offset))
                
                rows = cur.fetchall()
                if not rows:
                    break
                
                for search_name, queue_id in rows:
                    writer.writerow([search_name, queue_id])
                    exported += 1
                
                offset += batch_size
                print(f"Exported {exported:,} / {total:,} companies ({exported/total*100:.1f}%)")
        
        print(f"\nExport complete!")
        print(f"File saved as: {output_file}")
        print(f"Total companies: {exported:,}")
        
        # Also create smaller sample files for testing
        print("\nCreating sample files for testing...")
        
        # 100 company sample
        cur.execute("""
            SELECT search_name, id 
            FROM ch_scrape_queue 
            WHERE search_status = 'pending'
            ORDER BY id
            LIMIT 100
        """)
        
        with open('companies_to_scrape_sample_100.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Company Name'])
            for search_name, _ in cur.fetchall():
                writer.writerow([search_name])
        print("Created: companies_to_scrape_sample_100.csv")
        
        # 1000 company sample
        cur.execute("""
            SELECT search_name, id 
            FROM ch_scrape_queue 
            WHERE search_status = 'pending'
            ORDER BY id
            LIMIT 1000
        """)
        
        with open('companies_to_scrape_sample_1000.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Company Name'])
            for search_name, _ in cur.fetchall():
                writer.writerow([search_name])
        print("Created: companies_to_scrape_sample_1000.csv")
        
        # 10000 company sample
        cur.execute("""
            SELECT search_name, id 
            FROM ch_scrape_queue 
            WHERE search_status = 'pending'
            ORDER BY id
            LIMIT 10000
        """)
        
        with open('companies_to_scrape_sample_10000.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Company Name'])
            for search_name, _ in cur.fetchall():
                writer.writerow([search_name])
        print("Created: companies_to_scrape_sample_10000.csv")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    export_pending_companies()