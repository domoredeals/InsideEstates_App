#!/usr/bin/env python3
"""
Extract company status and additional information from Companies House URLs
This script visits each company page to get the full status information
"""

import csv
import requests
from lxml import html
import time
import random
from datetime import datetime
import os
from urllib.parse import urljoin

def extract_company_details(company_url, session=None, use_proxy=False, proxy_config=None):
    """
    Extract detailed information from a company page
    
    Returns dict with:
    - company_status (Active, Dissolved, Liquidation, etc.)
    - company_type (Ltd, Limited, PLC, etc.)
    - incorporation_date
    - sic_codes
    - registered_address
    - previous_names
    """
    
    if not session:
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    try:
        # Add proxy if configured
        proxies = None
        if use_proxy and proxy_config:
            proxy_url = f"http://{proxy_config['username']}-session-{random.random()}:{proxy_config['password']}@zproxy.lum-superproxy.io:{proxy_config['port']}"
            proxies = {"http": proxy_url, "https": proxy_url}
        
        # Request the company page
        response = session.get(company_url, timeout=15, proxies=proxies)
        response.raise_for_status()
        
        tree = html.fromstring(response.text)
        
        details = {}
        
        # Extract company status
        status_el = tree.xpath("//dd[@id='company-status' or contains(@class, 'company-status')]")
        if status_el:
            details['company_status'] = status_el[0].text_content().strip()
        else:
            # Try alternative xpath
            status_el = tree.xpath("//p[contains(text(), 'Company status')]/following-sibling::*[1]")
            if status_el:
                details['company_status'] = status_el[0].text_content().strip()
        
        # Extract company type
        type_el = tree.xpath("//dd[@id='company-type' or contains(@class, 'company-type')]")
        if type_el:
            details['company_type'] = type_el[0].text_content().strip()
        else:
            # Try from header
            header_el = tree.xpath("//p[@class='heading-xlarge' or contains(@class, 'company-name')]")
            if header_el:
                full_name = header_el[0].text_content().strip()
                # Extract type from name (LIMITED, LTD, PLC, etc.)
                if 'LIMITED' in full_name.upper():
                    details['company_type'] = 'Private limited company'
                elif 'PLC' in full_name.upper():
                    details['company_type'] = 'Public limited company'
                elif 'LLP' in full_name.upper():
                    details['company_type'] = 'Limited liability partnership'
        
        # Extract incorporation date
        inc_date_el = tree.xpath("//dd[@id='company-incorporation-date' or contains(@class, 'incorporation-date')]")
        if inc_date_el:
            details['incorporation_date'] = inc_date_el[0].text_content().strip()
        
        # Extract SIC codes
        sic_el = tree.xpath("//span[@id='sic-code' or contains(@class, 'sic-code')]")
        if sic_el:
            sic_codes = []
            for el in sic_el:
                code = el.text_content().strip()
                if code and code not in sic_codes:
                    sic_codes.append(code)
            details['sic_codes'] = ', '.join(sic_codes)
        
        # Extract registered address
        address_el = tree.xpath("//dd[contains(@class, 'address') or @id='reg-address']")
        if address_el:
            address_text = address_el[0].text_content().strip()
            # Clean up address
            address_text = ' '.join(address_text.split())
            details['registered_address'] = address_text
        
        # Extract previous names count
        prev_names_el = tree.xpath("//a[contains(@href, 'previous-company-names')]")
        if prev_names_el:
            prev_text = prev_names_el[0].text_content().strip()
            # Extract number from text like "View 3 previous company names"
            import re
            match = re.search(r'(\d+)\s+previous', prev_text)
            if match:
                details['previous_names_count'] = match.group(1)
        
        # Meta description for summary
        meta_el = tree.xpath("//p[@class='meta' or contains(@class, 'company-meta')]")
        if meta_el:
            details['meta_description'] = meta_el[0].text_content().strip()
        
        return details
        
    except Exception as e:
        print(f"Error extracting details from {company_url}: {e}")
        return {}


def update_csv_with_status(input_csv, output_csv, use_proxy=False, proxy_config=None, limit=None):
    """
    Read the scraped CSV and add status information
    """
    
    print(f"Reading input file: {input_csv}")
    
    # Read existing data
    companies = []
    with open(input_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            if row.get('Status') == 'FOUND' and row.get('Company URL'):
                companies.append(row)
    
    print(f"Found {len(companies)} companies to process")
    
    if limit:
        companies = companies[:limit]
        print(f"Limited to {limit} companies")
    
    # Add new fields
    new_fieldnames = fieldnames + [
        'Company Status',
        'Company Type', 
        'Incorporation Date',
        'SIC Codes',
        'Registered Address',
        'Previous Names Count',
        'Meta Description'
    ]
    
    # Create output file
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=new_fieldnames)
        writer.writeheader()
        
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        processed = 0
        for i, row in enumerate(companies):
            try:
                print(f"[{i+1}/{len(companies)}] Processing {row.get('Found Name', row.get('Search Name'))}...", end='')
                
                # Get company details
                details = extract_company_details(
                    row['Company URL'], 
                    session=session,
                    use_proxy=use_proxy,
                    proxy_config=proxy_config
                )
                
                # Add details to row
                row.update({
                    'Company Status': details.get('company_status', ''),
                    'Company Type': details.get('company_type', ''),
                    'Incorporation Date': details.get('incorporation_date', ''),
                    'SIC Codes': details.get('sic_codes', ''),
                    'Registered Address': details.get('registered_address', ''),
                    'Previous Names Count': details.get('previous_names_count', ''),
                    'Meta Description': details.get('meta_description', '')
                })
                
                writer.writerow(row)
                processed += 1
                
                if details.get('company_status'):
                    print(f" {details['company_status']}")
                else:
                    print(" No status found")
                
                # Delay to be respectful
                if not use_proxy:
                    time.sleep(random.uniform(1, 2))
                else:
                    time.sleep(random.uniform(0.1, 0.3))
                    
            except Exception as e:
                print(f" ERROR: {e}")
                writer.writerow(row)  # Write original row without updates
    
    print(f"\nProcessed {processed} companies")
    print(f"Results saved to: {output_csv}")


def main():
    """Main function"""
    print("Companies House Status Extractor")
    print("=" * 60)
    
    # Get input file
    input_csv = input("Enter your scraped CSV file (with Company URLs): ").strip()
    if not input_csv:
        print("No input file specified!")
        return
    
    if not os.path.exists(input_csv):
        print(f"File not found: {input_csv}")
        return
    
    # Output file
    output_csv = input(f"Enter output filename (default: {input_csv.replace('.csv', '_with_status.csv')}): ").strip()
    if not output_csv:
        output_csv = input_csv.replace('.csv', '_with_status.csv')
    
    # Proxy option
    use_proxy = input("Use proxy? (y/n, default: n): ").lower() == 'y'
    
    proxy_config = None
    if use_proxy:
        print("\nEnter Bright Data proxy credentials:")
        proxy_config = {
            'username': input("Username: ").strip(),
            'password': input("Password: ").strip(),
            'port': input("Port (default: 22225): ").strip() or "22225"
        }
    
    # Limit option for testing
    limit_str = input("\nProcess how many companies? (blank for all): ").strip()
    limit = int(limit_str) if limit_str.isdigit() else None
    
    print(f"\nStarting extraction...")
    print(f"Input: {input_csv}")
    print(f"Output: {output_csv}")
    print(f"Using proxy: {use_proxy}")
    if limit:
        print(f"Limit: {limit} companies")
    print()
    
    try:
        update_csv_with_status(input_csv, output_csv, use_proxy, proxy_config, limit)
    except KeyboardInterrupt:
        print("\n\nInterrupted by user!")
    except Exception as e:
        print(f"\n\nError: {e}")


if __name__ == '__main__':
    main()