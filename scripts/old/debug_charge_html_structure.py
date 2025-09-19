#!/usr/bin/env python3
"""
Debug the HTML structure of a specific charge page to find correct selectors
"""

import os
import sys
import requests
from lxml import html
from dotenv import load_dotenv
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

def debug_charge_page(company_number, charge_id):
    """Debug the HTML structure of a charge page"""
    
    url = f"https://find-and-update.company-information.service.gov.uk/company/{company_number}/charges/{charge_id}"
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    
    print(f"Fetching charge page: {url}")
    
    try:
        response = session.get(url, timeout=15)
        response.raise_for_status()
        
        tree = html.fromstring(response.content)
        
        print("\n=== CHARGE DETAILS STRUCTURE ===\n")
        
        # Look for Transaction Filed
        print("1. Transaction Filed:")
        trans_selectors = [
            '//dt[contains(text(), "Transaction Filed")]/following-sibling::dd[1]//span/text()',
            '//dt[contains(., "Transaction Filed")]/following-sibling::dd[1]//text()',
            '//dt[@id="mortgage-transaction-filed-label"]/following-sibling::dd//text()',
            '//dt[text()="Transaction Filed"]/following-sibling::dd[1]//text()'
        ]
        for selector in trans_selectors:
            result = tree.xpath(selector)
            if result:
                print(f"  Found with selector: {selector}")
                print(f"  Value: {[r.strip() for r in result if r.strip()]}")
                break
        
        # Look for Persons Entitled
        print("\n2. Persons Entitled:")
        person_selectors = [
            '//ul[@id="persons-entitled"]/li/text()',
            '//dt[contains(text(), "Persons entitled")]/following-sibling::dd//ul/li/text()',
            '//dt[contains(., "Persons entitled")]/following-sibling::dd[1]//text()',
            '//dt[@id="mortgage-persons-entitled-label"]/following-sibling::dd//text()',
            '//h3[contains(text(), "Persons entitled")]/following-sibling::ul/li/text()',
            '//h3[@id="mortgage-persons-entitled-label"]/following-sibling::ul/li/text()',
            '//dt[text()="Persons entitled"]/following-sibling::dd[1]//ul/li/text()'
        ]
        for selector in person_selectors:
            result = tree.xpath(selector)
            if result:
                print(f"  Found with selector: {selector}")
                print(f"  Value: {[r.strip() for r in result if r.strip()]}")
                break
        
        # Look for Amount Secured
        print("\n3. Amount Secured:")
        amount_selectors = [
            '//h3[@id="mortgage-amount-secured-label"]/following-sibling::p[1]/text()',
            '//h3[contains(text(), "Amount secured")]/following-sibling::p[1]/text()',
            '//dt[contains(text(), "Amount secured")]/following-sibling::dd[1]//text()',
            '//dt[@id="mortgage-amount-secured-label"]/following-sibling::dd//text()'
        ]
        for selector in amount_selectors:
            result = tree.xpath(selector)
            if result:
                print(f"  Found with selector: {selector}")
                print(f"  Value: {[r.strip() for r in result if r.strip()]}")
                break
        
        # Look for Short Particulars
        print("\n4. Short Particulars:")
        particulars_selectors = [
            '//h3[@id="mortgage-particulars-label"]/following-sibling::p//span[@id="mortgage-particulars"]/text()',
            '//h3[contains(text(), "Short particulars")]/following-sibling::p//text()',
            '//dt[contains(text(), "Short particulars")]/following-sibling::dd[1]//text()',
            '//dt[@id="mortgage-short-particulars-label"]/following-sibling::dd//text()',
            '//span[@id="mortgage-particulars"]/text()'
        ]
        for selector in particulars_selectors:
            result = tree.xpath(selector)
            if result:
                print(f"  Found with selector: {selector}")
                print(f"  Value: {[r.strip() for r in result if r.strip()][:200]}...")
                break
        
        # Debug: Show all dt/dd pairs
        print("\n5. All dt/dd pairs found on page:")
        dts = tree.xpath('//dt')
        for dt in dts[:10]:  # Limit to first 10
            dt_text = dt.text_content().strip()
            if dt_text:
                dd = dt.xpath('./following-sibling::dd[1]')
                if dd:
                    dd_text = dd[0].text_content().strip()[:100]
                    print(f"  {dt_text}: {dd_text}")
        
        # Debug: Show all h3 headings
        print("\n6. All h3 headings found on page:")
        h3s = tree.xpath('//h3')
        for h3 in h3s[:10]:  # Limit to first 10
            h3_text = h3.text_content().strip()
            if h3_text:
                print(f"  {h3_text}")
                # Check for following content
                following = h3.xpath('./following-sibling::*[1]')
                if following:
                    tag = following[0].tag
                    content = following[0].text_content().strip()[:100]
                    print(f"    Followed by <{tag}>: {content}")
        
        # Save HTML for manual inspection
        with open(f'charge_debug_{charge_id}.html', 'w', encoding='utf-8') as f:
            f.write(response.text)
        print(f"\nFull HTML saved to charge_debug_{charge_id}.html")
        
    except Exception as e:
        print(f"Error debugging charge page: {e}")

if __name__ == '__main__':
    # Debug the specific charge mentioned
    debug_charge_page('SC002116', 'NRrK3uDqlRdIk5VDyCDHRbqiUlo')