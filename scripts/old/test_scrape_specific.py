#!/usr/bin/env python3
"""
Test scraping specific companies
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrape_companies_house import CompaniesHouseScraper
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def scrape_specific_companies(company_numbers):
    """Scrape specific companies by company number"""
    
    scraper = CompaniesHouseScraper(batch_size=2, delay_min=1, delay_max=2)
    
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    cur = conn.cursor()
    
    try:
        for company_number in company_numbers:
            # Get company URL
            cur.execute("""
                SELECT company_number, company_url
                FROM ch_scrape_overview
                WHERE company_number = %s
            """, (company_number,))
            
            result = cur.fetchone()
            if result:
                print(f"\nScraping {company_number}: {result[1]}")
                
                company_data = {
                    'company_number': result[0],
                    'company_url': result[1]
                }
                
                # Scrape overview
                scraper._scrape_company_page(company_data, 'overview', '')
                print(f"✓ Scraped overview for {company_number}")
                
                # Also scrape officers
                scraper._scrape_company_page(company_data, 'officers', '/officers')
                print(f"✓ Scraped officers for {company_number}")
                
                # And charges
                scraper._scrape_company_page(company_data, 'charges', '/charges')
                print(f"✓ Scraped charges for {company_number}")
                
                # And insolvency
                scraper._scrape_company_page(company_data, 'insolvency', '/insolvency')
                print(f"✓ Scraped insolvency for {company_number}")
                
            else:
                print(f"Company {company_number} not found in overview table")
        
        conn.commit()
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    # Scrape our two test companies
    company_numbers = ['01521211', 'SC002116']  # DOVE BROTHERS LIMITED and CGU INSURANCE PLC
    scrape_specific_companies(company_numbers)