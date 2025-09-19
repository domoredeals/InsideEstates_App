#!/usr/bin/env python3
"""
Fix missing status column in scraped CSV by extracting it from the search results
This is faster than visiting each company page
"""

import csv
import requests
from lxml import html
import time
import random
from datetime import datetime
import os

def extract_status_from_search(search_name, session=None):
    """
    Extract the status/meta text from Companies House search results
    This is what should have been in the 'status' column
    """
    
    if not session:
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    try:
        from urllib.parse import quote_plus
        search_url = f'https://find-and-update.company-information.service.gov.uk/search/companies?q={quote_plus(search_name)}'
        
        response = session.get(search_url, timeout=15)
        response.raise_for_status()
        
        tree = html.fromstring(response.text)
        
        # Find search results
        results_els = tree.xpath("//ul[@id='results']/li[contains(@class, 'type-company')]")
        
        if results_els:
            # Get first result's meta text
            first_result = results_els[0]
            meta_el = first_result.xpath("./p[@class='meta crumbtrail']")
            
            if meta_el:
                meta_text = meta_el[0].text_content().strip()
                
                # Parse the meta text
                # Format is usually: "12345678 - Private limited company, Active"
                # or: "12345678 - Dissolved on 12 January 2020"
                
                parts = {}
                
                if ' - ' in meta_text:
                    number_part, status_part = meta_text.split(' - ', 1)
                    parts['full_meta'] = meta_text
                    parts['company_number_from_meta'] = number_part.strip()
                    
                    # Parse status part
                    if ', ' in status_part:
                        # Format: "Private limited company, Active"
                        type_part, status = status_part.rsplit(', ', 1)
                        parts['company_type'] = type_part.strip()
                        parts['status'] = status.strip()
                    elif 'Dissolved' in status_part or 'Liquidation' in status_part:
                        # Format: "Dissolved on 12 January 2020"
                        parts['status'] = status_part.strip()
                        parts['company_type'] = 'Company'  # Default
                    else:
                        parts['status'] = status_part.strip()
                else:
                    parts['full_meta'] = meta_text
                
                return parts
        
        return {}
        
    except Exception as e:
        print(f"Error searching for {search_name}: {e}")
        return {}


def fix_csv_status(input_csv, output_csv, limit=None):
    """
    Add missing status information to CSV
    """
    
    print(f"Reading input file: {input_csv}")
    
    # Read existing data
    rows = []
    with open(input_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        original_fieldnames = reader.fieldnames
        for row in reader:
            rows.append(row)
    
    print(f"Found {len(rows)} total rows")
    
    # Check what we have
    found_companies = [r for r in rows if r.get('Status') == 'FOUND']
    print(f"Found {len(found_companies)} companies marked as FOUND")
    
    if limit:
        rows = rows[:limit]
        print(f"Processing first {limit} rows only")
    
    # Determine new fieldnames
    new_fieldnames = list(original_fieldnames)
    if 'Meta Text' not in new_fieldnames:
        new_fieldnames.insert(new_fieldnames.index('Status') + 1, 'Meta Text')
    if 'Company Type' not in new_fieldnames:
        new_fieldnames.insert(new_fieldnames.index('Meta Text') + 1, 'Company Type')
    if 'Company Status' not in new_fieldnames:
        new_fieldnames.insert(new_fieldnames.index('Company Type') + 1, 'Company Status')
    
    # Create output file
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=new_fieldnames, extrasaction='ignore')
        writer.writeheader()
        
        session = requests.Session()
        processed = 0
        updated = 0
        
        for i, row in enumerate(rows):
            try:
                # Only process FOUND companies that don't have meta text
                if row.get('Status') == 'FOUND' and not row.get('Meta Text'):
                    print(f"[{i+1}/{len(rows)}] Updating {row.get('Search Name')}...", end='')
                    
                    # Get status from search
                    status_info = extract_status_from_search(row['Search Name'], session)
                    
                    if status_info:
                        row['Meta Text'] = status_info.get('full_meta', '')
                        row['Company Type'] = status_info.get('company_type', '')
                        row['Company Status'] = status_info.get('status', '')
                        updated += 1
                        print(f" {status_info.get('status', 'Updated')}")
                    else:
                        print(" No status found")
                    
                    # Delay to be respectful
                    time.sleep(random.uniform(1, 2))
                
                writer.writerow(row)
                processed += 1
                    
            except Exception as e:
                print(f" ERROR: {e}")
                writer.writerow(row)  # Write original row
    
    print(f"\nProcessed {processed} rows")
    print(f"Updated {updated} companies with status information")
    print(f"Results saved to: {output_csv}")


def analyze_csv(csv_file):
    """
    Analyze what columns and data we have
    """
    print(f"\nAnalyzing: {csv_file}")
    print("-" * 60)
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        
        print(f"Columns found: {', '.join(fieldnames)}")
        
        # Count statistics
        total = 0
        found = 0
        not_found = 0
        errors = 0
        has_meta = 0
        
        sample_found = None
        
        for row in reader:
            total += 1
            status = row.get('Status', '')
            
            if status == 'FOUND':
                found += 1
                if not sample_found:
                    sample_found = row
            elif status == 'NOT_FOUND':
                not_found += 1
            elif status == 'ERROR':
                errors += 1
            
            if row.get('Meta Text') or row.get('Company Status'):
                has_meta += 1
        
        print(f"\nStatistics:")
        print(f"Total rows: {total}")
        print(f"Found: {found}")
        print(f"Not found: {not_found}")
        print(f"Errors: {errors}")
        print(f"Has meta/status info: {has_meta}")
        
        if sample_found:
            print(f"\nSample FOUND company:")
            for key, value in sample_found.items():
                if value:
                    print(f"  {key}: {value}")


def main():
    """Main function"""
    print("Fix Missing Status Information")
    print("=" * 60)
    
    # Get input file
    input_csv = input("Enter your scraped CSV file: ").strip()
    if not input_csv:
        print("No input file specified!")
        return
    
    if not os.path.exists(input_csv):
        print(f"File not found: {input_csv}")
        return
    
    # Analyze the file first
    analyze_csv(input_csv)
    
    # Ask what to do
    print("\nOptions:")
    print("1. Add missing status/meta information from search results")
    print("2. Extract detailed status from company pages (slower)")
    print("3. Just analyze, don't modify")
    
    choice = input("\nChoose option (1-3): ").strip()
    
    if choice == '1':
        # Fix with search results
        output_csv = input(f"\nOutput filename (default: {input_csv.replace('.csv', '_fixed.csv')}): ").strip()
        if not output_csv:
            output_csv = input_csv.replace('.csv', '_fixed.csv')
        
        limit_str = input("Process how many rows? (blank for all): ").strip()
        limit = int(limit_str) if limit_str.isdigit() else None
        
        print(f"\nFixing missing status information...")
        fix_csv_status(input_csv, output_csv, limit)
        
    elif choice == '2':
        # Use the other script
        print("\nUse extract_company_status.py for detailed extraction")
        
    else:
        print("\nAnalysis complete.")


if __name__ == '__main__':
    main()