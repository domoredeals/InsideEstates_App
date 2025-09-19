#!/usr/bin/env python3
"""
Display complete scraped Companies House data including detailed charge information
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

def display_company_data_enhanced(company_number, show_all_charges=False):
    """Display all scraped data for a company including detailed charge info"""
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
        
        print("=" * 120)
        print(f"COMPANIES HOUSE DATA FOR: {overview[2]} ({company_number})")
        print("=" * 120)
        
        print("\n=== COMPANY OVERVIEW ===")
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
        
        # Get charges with details
        cur.execute("""
            SELECT charge_id, charge_status, created_date, satisfied_date,
                   persons_entitled, brief_description,
                   transaction_filed, amount_secured, short_particulars,
                   contains_fixed_charge, contains_floating_charge,
                   contains_negative_pledge, charge_link
            FROM ch_scrape_charges
            WHERE company_number = %s
            AND charge_id NOT LIKE 'page_%%'
            ORDER BY created_date DESC
        """, (company_number,))
        
        charges = cur.fetchall()
        print(f"\n=== CHARGES ({len(charges)} total) ===")
        
        outstanding = [c for c in charges if c[1] == 'Outstanding']
        satisfied = [c for c in charges if c[1] == 'Satisfied']
        
        print(f"Status: {len(outstanding)} Outstanding, {len(satisfied)} Satisfied")
        
        # Show detailed info for outstanding charges
        if outstanding:
            print(f"\n--- OUTSTANDING CHARGES ({len(outstanding)}) ---")
            for charge in outstanding[:10 if not show_all_charges else None]:
                print(f"\n• Charge ID: {charge[0]}")
                print(f"  Created: {charge[2]}")
                if charge[4]:
                    print(f"  Persons Entitled: {', '.join(charge[4])}")
                if charge[6]:
                    print(f"  Transaction: {charge[6]}")
                if charge[7]:
                    print(f"  Amount Secured: {charge[7]}")
                if charge[8]:
                    print(f"  Particulars: {charge[8][:150]}...")
                if charge[5]:
                    print(f"  Brief Description: {charge[5][:100]}...")
                
                # Charge types
                types = []
                if charge[9]:
                    types.append("Fixed Charge")
                if charge[10]:
                    types.append("Floating Charge")
                if charge[11]:
                    types.append("Negative Pledge")
                if types:
                    print(f"  Types: {', '.join(types)}")
            
            if len(outstanding) > 10 and not show_all_charges:
                print(f"\n  ... and {len(outstanding) - 10} more outstanding charges")
        
        # Summary for satisfied charges
        if satisfied:
            print(f"\n--- SATISFIED CHARGES ({len(satisfied)}) ---")
            # Show first few with details
            for charge in satisfied[:5 if not show_all_charges else None]:
                print(f"\n• {charge[0][:30]}... - Satisfied: {charge[3]}")
                print(f"  Created: {charge[2]}")
                if charge[7]:
                    print(f"  Amount Secured: {charge[7][:80]}...")
                if charge[8]:
                    print(f"  Particulars: {charge[8][:100]}...")
            
            if len(satisfied) > 5 and not show_all_charges:
                print(f"\n  ... and {len(satisfied) - 5} more satisfied charges")
        
        # Get officers summary
        cur.execute("""
            SELECT COUNT(*), 
                   COUNT(CASE WHEN resigned_date IS NULL THEN 1 END) as current,
                   COUNT(CASE WHEN resigned_date IS NOT NULL THEN 1 END) as resigned
            FROM ch_scrape_officers
            WHERE company_number = %s
        """, (company_number,))
        
        officer_counts = cur.fetchone()
        if officer_counts and officer_counts[0] > 0:
            print(f"\n=== OFFICERS ===")
            print(f"Total: {officer_counts[0]} ({officer_counts[1]} current, {officer_counts[2]} resigned)")
            
            # Show current officers
            cur.execute("""
                SELECT officer_name, officer_role, appointed_date
                FROM ch_scrape_officers
                WHERE company_number = %s
                AND resigned_date IS NULL
                ORDER BY appointed_date DESC
                LIMIT 5
            """, (company_number,))
            
            current_officers = cur.fetchall()
            if current_officers:
                print("\nCurrent Officers:")
                for officer in current_officers:
                    print(f"  • {officer[0]} - {officer[1]} (appointed {officer[2]})")
        
        print("\n" + "=" * 120)
        
    except Exception as e:
        print(f"Error displaying data: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Display complete Companies House data')
    parser.add_argument('company_numbers', nargs='+', help='Company numbers to display')
    parser.add_argument('--all-charges', action='store_true', help='Show all charges (not just first 10)')
    
    args = parser.parse_args()
    
    for company_number in args.company_numbers:
        display_company_data_enhanced(company_number, args.all_charges)
        print("\n")