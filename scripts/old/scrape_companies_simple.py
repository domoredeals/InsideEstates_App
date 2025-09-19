"""
Simple Companies House Scraper for Windows
Just put company names in companies_to_search.csv and run!
"""

import time
import requests
import csv
from datetime import datetime
from urllib.parse import quote_plus
import random

def scrape_companies_house(input_file='companies_to_search.csv', 
                          output_file=None,
                          delay_seconds=2):
    """
    Scrape Companies House for company information
    
    Args:
        input_file: CSV file with company names (default: companies_to_search.csv)
        output_file: Where to save results (default: auto-generated with timestamp)
        delay_seconds: Delay between requests to be respectful (default: 2 seconds)
    """
    
    # Set output filename if not provided
    if output_file is None:
        output_file = f'companies_house_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    
    # Try to read input file
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            # Skip header if present
            first_row = next(reader, None)
            if first_row and first_row[0].lower() in ['company', 'company name', 'name', 'search_name']:
                companies = [row[0].strip() for row in reader if row and row[0].strip()]
            else:
                # No header, include first row
                companies = [first_row[0].strip()] if first_row else []
                companies.extend([row[0].strip() for row in reader if row and row[0].strip()])
    except FileNotFoundError:
        print(f"ERROR: Could not find '{input_file}'")
        print("\nPlease create a CSV file with company names, one per line.")
        print("Example content:")
        print("Company Name")
        print("Apple Inc")
        print("Microsoft Corporation")
        print("Google LLC")
        return
    except Exception as e:
        print(f"ERROR reading input file: {e}")
        return
    
    if not companies:
        print("No companies found in the input file!")
        return
    
    print(f"Found {len(companies)} companies to search")
    print(f"Results will be saved to: {output_file}")
    print(f"Delay between requests: {delay_seconds} seconds\n")
    
    # Prepare output file
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Search Name', 'Found Name', 'Company Number', 'Status', 'URL'])
    
    # Setup for requests
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    
    # Statistics
    found = 0
    not_found = 0
    errors = 0
    start_time = datetime.now()
    
    # Search each company
    for i, company_name in enumerate(companies, 1):
        try:
            # Show progress
            print(f"[{i}/{len(companies)}] Searching for: {company_name}...", end=' ')
            
            # Search URL
            search_url = 'https://find-and-update.company-information.service.gov.uk/search/companies?q=' + quote_plus(company_name)
            
            # Make request
            response = session.get(search_url, timeout=10)
            
            # Parse response
            if 'no-results' in response.text and 'No results found' in response.text:
                # No results found
                print("NOT FOUND")
                not_found += 1
                result = [company_name, '', '', 'NOT_FOUND', '']
            elif 'results-list' in response.text:
                # Try to extract first result
                try:
                    # Find company link
                    start = response.text.find('/company/')
                    if start > 0:
                        end = response.text.find('"', start)
                        company_url = 'https://find-and-update.company-information.service.gov.uk' + response.text[start:end]
                        company_number = company_url.split('/company/')[-1].split('/')[0]
                        
                        # Try to find company name
                        name_start = response.text.find('>', end) + 1
                        name_end = response.text.find('<', name_start)
                        found_name = response.text[name_start:name_end].strip()
                        
                        print(f"FOUND: {found_name} ({company_number})")
                        found += 1
                        result = [company_name, found_name, company_number, 'FOUND', company_url]
                    else:
                        print("ERROR: Could not parse results")
                        errors += 1
                        result = [company_name, '', '', 'ERROR', 'Could not parse results']
                except Exception as e:
                    print(f"ERROR: {e}")
                    errors += 1
                    result = [company_name, '', '', 'ERROR', str(e)]
            else:
                print("ERROR: Unexpected response")
                errors += 1
                result = [company_name, '', '', 'ERROR', 'Unexpected response']
            
            # Save result
            with open(output_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(result)
            
            # Delay before next request (be respectful!)
            if i < len(companies):  # Don't delay after last company
                time.sleep(delay_seconds + random.uniform(0, 1))  # Add small random delay
                
        except requests.exceptions.Timeout:
            print("TIMEOUT")
            errors += 1
            with open(output_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([company_name, '', '', 'ERROR', 'Request timeout'])
            time.sleep(delay_seconds * 2)  # Longer delay after timeout
            
        except Exception as e:
            print(f"ERROR: {e}")
            errors += 1
            with open(output_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([company_name, '', '', 'ERROR', str(e)])
            time.sleep(delay_seconds)
    
    # Final summary
    elapsed = datetime.now() - start_time
    print(f"\n{'='*60}")
    print("SCRAPING COMPLETE!")
    print(f"{'='*60}")
    print(f"Total Companies: {len(companies)}")
    print(f"Found: {found} ({found/len(companies)*100:.1f}%)")
    print(f"Not Found: {not_found} ({not_found/len(companies)*100:.1f}%)")
    print(f"Errors: {errors} ({errors/len(companies)*100:.1f}%)")
    print(f"Time Taken: {elapsed}")
    print(f"\nResults saved to: {output_file}")


# Run the scraper when script is executed
if __name__ == '__main__':
    print("Companies House Simple Scraper")
    print("="*60)
    print("This will search for companies on Companies House website")
    print("\nMake sure you have:")
    print("1. A file named 'companies_to_search.csv' with company names")
    print("2. Internet connection")
    print("3. Python packages: requests, lxml (install with: pip install requests lxml)")
    print()
    
    # Check if default input file exists
    import os
    if not os.path.exists('companies_to_search.csv'):
        print("WARNING: 'companies_to_search.csv' not found!")
        print("\nCreating example file...")
        with open('companies_to_search.csv', 'w', encoding='utf-8') as f:
            f.write("Company Name\n")
            f.write("Example Company Ltd\n")
            f.write("Test Corporation\n")
        print("Created 'companies_to_search.csv' with examples.")
        print("Please edit this file with your company names and run again.")
        input("\nPress Enter to exit...")
    else:
        # Ask if user wants to proceed
        proceed = input("Ready to start scraping? (y/n): ").lower()
        if proceed == 'y':
            scrape_companies_house()
            input("\nPress Enter to exit...")
        else:
            print("Scraping cancelled.")