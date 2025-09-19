#!/usr/bin/env python3
"""
Display complete scraped Companies House data for a company
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv
from datetime import datetime

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

def display_company_data(company_number):
    """Display all scraped data for a company"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get overview data
        cur.execute("""
            SELECT company_number, search_name, company_name, company_status, 
                   company_type, incorporation_date,
                   registered_office_address, sic_codes, previous_names,
                   accounts_next_due, confirmation_statement_next_due
            FROM ch_scrape_overview
            WHERE company_number = %s
        """, (company_number,))
        
        overview = cur.fetchone()
        if not overview:
            print(f"No overview data found for company {company_number}")
            return
        
        print("=" * 100)
        print(f"COMPANIES HOUSE DATA FOR: {overview[2]} ({company_number})")
        print("=" * 100)
        
        print("\n=== OVERVIEW ===")
        print(f"Search Name (from Land Registry): {overview[1]}")
        print(f"Current Company Name: {overview[2]}")
        print(f"Company Number: {overview[0]}")
        print(f"Status: {overview[3]}")
        print(f"Type: {overview[4]}")
        print(f"Incorporated: {overview[5]}")
        print(f"Registered Address: {overview[6]}")
        if overview[7]:
            print(f"SIC Codes: {', '.join(overview[7])}")
        if overview[8]:
            print(f"Previous Names: {', '.join(overview[8])}")
        if overview[9]:
            print(f"Accounts Next Due: {overview[9]}")
        if overview[10]:
            print(f"Confirmation Statement Next Due: {overview[10]}")
        
        # Get officers
        cur.execute("""
            SELECT officer_name, officer_role, appointed_date, resigned_date,
                   nationality, country_of_residence, occupation,
                   date_of_birth_year, date_of_birth_month, address
            FROM ch_scrape_officers
            WHERE company_number = %s
            ORDER BY resigned_date IS NULL DESC, appointed_date DESC
        """, (company_number,))
        
        officers = cur.fetchall()
        print(f"\n=== OFFICERS ({len(officers)} total) ===")
        
        current_officers = [o for o in officers if o[3] is None]
        resigned_officers = [o for o in officers if o[3] is not None]
        
        if current_officers:
            print(f"\nCurrent Officers ({len(current_officers)}):")
            for officer in current_officers:
                print(f"\n  • {officer[0]} - {officer[1]}")
                if officer[2]:
                    print(f"    Appointed: {officer[2]}")
                if officer[4]:
                    print(f"    Nationality: {officer[4]}")
                if officer[5]:
                    print(f"    Country of Residence: {officer[5]}")
                if officer[9]:
                    print(f"    Address: {officer[9][:80]}...")
        
        if resigned_officers:
            print(f"\nResigned Officers ({len(resigned_officers)}):")
            for officer in resigned_officers:
                print(f"\n  • {officer[0]} - {officer[1]}")
                if officer[2]:
                    print(f"    Appointed: {officer[2]}")
                print(f"    Resigned: {officer[3]}")
        
        # Get charges
        cur.execute("""
            SELECT charge_id, charge_status, created_date, satisfied_date,
                   persons_entitled, brief_description
            FROM ch_scrape_charges
            WHERE company_number = %s
            AND charge_id IS NOT NULL
            ORDER BY created_date DESC
        """, (company_number,))
        
        charges = cur.fetchall()
        print(f"\n=== CHARGES ({len(charges)} total) ===")
        
        if charges:
            outstanding = [c for c in charges if c[1] != 'Satisfied']
            satisfied = [c for c in charges if c[1] == 'Satisfied']
            
            if outstanding:
                print(f"\nOutstanding Charges ({len(outstanding)}):")
                for charge in outstanding[:5]:  # Show first 5
                    print(f"\n  • {charge[0]} - Created: {charge[2]}")
                    if charge[4]:
                        print(f"    Persons Entitled: {charge[4]}")
                    if charge[5]:
                        print(f"    Particulars: {charge[5][:100]}...")
                if len(outstanding) > 5:
                    print(f"    ... and {len(outstanding) - 5} more outstanding charges")
            
            if satisfied:
                print(f"\nSatisfied Charges ({len(satisfied)}):")
                print(f"  • {len(satisfied)} charges have been satisfied")
        
        print("\n" + "=" * 100)
        
    except Exception as e:
        print(f"Error displaying data: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Display complete Companies House data')
    parser.add_argument('company_numbers', nargs='+', help='Company numbers to display')
    
    args = parser.parse_args()
    
    for company_number in args.company_numbers:
        display_company_data(company_number)
        print("\n")