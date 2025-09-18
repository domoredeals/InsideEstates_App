#!/usr/bin/env python3
"""
Example queries for the ownership history views
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

def connect():
    """Connect to PostgreSQL database"""
    return psycopg2.connect(**POSTGRESQL_CONFIG)

def query_ownership_history(title_number):
    """Get complete ownership history for a specific property"""
    conn = connect()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    print(f"\n=== Ownership History for Title Number: {title_number} ===\n")
    
    cursor.execute("""
        SELECT 
            file_month,
            proprietor_name,
            lr_company_reg_no,
            ch_company_name,
            ch_company_status,
            ch_match_confidence,
            match_quality_description,
            ownership_status,
            change_description,
            ownership_duration_months
        FROM v_ownership_history 
        WHERE title_number = %s 
        ORDER BY file_month DESC, proprietor_sequence
    """, (title_number,))
    
    results = cursor.fetchall()
    if results:
        for row in results:
            print(f"Date: {row['file_month']}")
            print(f"Owner: {row['proprietor_name']}")
            print(f"Company Reg: {row['lr_company_reg_no'] or 'Not provided'}")
            print(f"CH Match: {row['ch_company_name'] or 'No match'}")
            print(f"Status: {row['ch_company_status'] or 'N/A'}")
            print(f"Match Quality: {row['match_quality_description']} (Confidence: {row['ch_match_confidence'] or 0:.2f})")
            print(f"Ownership Status: {row['ownership_status']}")
            print(f"Duration: {row['ownership_duration_months'] or 0} months")
            print("-" * 50)
    else:
        print("No ownership records found for this title number")
    
    cursor.close()
    conn.close()

def find_properties_by_company(company_name):
    """Find all properties owned by a company"""
    conn = connect()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    print(f"\n=== Properties owned by companies matching: {company_name} ===\n")
    
    cursor.execute("""
        SELECT DISTINCT 
            title_number, 
            property_address, 
            postcode,
            ownership_status, 
            ch_company_name,
            ch_company_status
        FROM v_ownership_history 
        WHERE ch_company_name ILIKE %s
        ORDER BY title_number
        LIMIT 20
    """, (f'%{company_name}%',))
    
    results = cursor.fetchall()
    if results:
        for row in results:
            print(f"Title: {row['title_number']}")
            print(f"Address: {row['property_address']}")
            print(f"Postcode: {row['postcode']}")
            print(f"Company: {row['ch_company_name']}")
            print(f"Status: {row['ownership_status']}")
            print("-" * 50)
        print(f"\nTotal properties shown: {len(results)} (limited to 20)")
    else:
        print("No properties found for this company")
    
    cursor.close()
    conn.close()

def get_ownership_summary(postcode_prefix):
    """Get ownership summary for properties in a postcode area"""
    conn = connect()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    print(f"\n=== Ownership Summary for Postcode: {postcode_prefix}% ===\n")
    
    cursor.execute("""
        SELECT 
            title_number,
            property_address,
            current_owners,
            total_unique_owners,
            ownership_records,
            high_confidence_matches,
            medium_confidence_matches,
            no_matches
        FROM v_ownership_summary 
        WHERE postcode LIKE %s
        ORDER BY total_unique_owners DESC
        LIMIT 10
    """, (f'{postcode_prefix}%',))
    
    results = cursor.fetchall()
    if results:
        for row in results:
            print(f"Title: {row['title_number']}")
            print(f"Address: {row['property_address']}")
            print(f"Current Owners: {row['current_owners'] or 'None listed'}")
            print(f"Total Unique Owners: {row['total_unique_owners']}")
            print(f"Ownership Records: {row['ownership_records']}")
            print(f"Match Quality - High: {row['high_confidence_matches']}, "
                  f"Medium: {row['medium_confidence_matches']}, "
                  f"No Match: {row['no_matches']}")
            print("-" * 50)
    else:
        print("No properties found in this postcode area")
    
    cursor.close()
    conn.close()

def main():
    """Main function to demonstrate queries"""
    # Example 1: Get a sample title number to query
    conn = connect()
    cursor = conn.cursor()
    
    # Get a sample title number that has ownership data
    cursor.execute("""
        SELECT DISTINCT title_number 
        FROM land_registry_data 
        WHERE proprietor_1_name IS NOT NULL 
        LIMIT 1
    """)
    sample_title = cursor.fetchone()
    
    if sample_title:
        # Query ownership history for this property
        query_ownership_history(sample_title[0])
    
    # Example 2: Find properties by a company name
    # Let's look for properties owned by companies with "LIMITED" in the name
    cursor.execute("""
        SELECT DISTINCT ch_company_name 
        FROM v_ownership_history 
        WHERE ch_company_name IS NOT NULL 
        LIMIT 1
    """)
    sample_company = cursor.fetchone()
    
    if sample_company:
        # Use just the first word of the company name for search
        company_search = sample_company[0].split()[0] if sample_company[0] else "LIMITED"
        find_properties_by_company(company_search)
    
    # Example 3: Get ownership summary for a postcode
    # Let's get a sample postcode
    cursor.execute("""
        SELECT DISTINCT LEFT(postcode, 3) as postcode_prefix
        FROM land_registry_data 
        WHERE postcode IS NOT NULL 
        LIMIT 1
    """)
    sample_postcode = cursor.fetchone()
    
    if sample_postcode:
        get_ownership_summary(sample_postcode[0])
    
    cursor.close()
    conn.close()

if __name__ == '__main__':
    main()