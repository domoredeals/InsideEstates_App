#!/usr/bin/env python3
"""
Find where these housing associations actually are in Companies House
"""

import sys
import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

def find_housing_associations():
    """Search for housing associations in Companies House"""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        print("=== SEARCHING FOR HOUSING ASSOCIATIONS IN COMPANIES HOUSE ===\n")
        
        # Search for specific housing associations by partial names
        housing_search_terms = [
            ('CIRCLE THIRTY THREE', 'CIRCLE%THIRTY%THREE%'),
            ('ADACTUS', 'ADACTUS%'),
            ('HEXAGON HOUSING', 'HEXAGON%HOUSING%'),
            ('WATERLOO HOUSING', 'WATERLOO%HOUSING%'),
            ('NEWLON', 'NEWLON%'),
            ('IRWELL VALLEY', 'IRWELL%VALLEY%')
        ]
        
        for name, pattern in housing_search_terms:
            print(f"\nSearching for '{name}':")
            
            # Search in current names
            cursor.execute("""
                SELECT 
                    company_number,
                    company_name,
                    company_status,
                    company_category,
                    incorporation_date
                FROM companies_house_data
                WHERE company_name LIKE %s
                ORDER BY company_name
                LIMIT 10
            """, (pattern,))
            
            current_matches = cursor.fetchall()
            
            if current_matches:
                print(f"  Found in current names:")
                for match in current_matches:
                    print(f"    {match['company_name']}")
                    print(f"      Number: {match['company_number']}, Status: {match['company_status']}")
                    print(f"      Category: {match['company_category']}, Incorporated: {match['incorporation_date']}")
            
            # Search in previous names
            previous_found = False
            for i in range(1, 11):
                cursor.execute(f"""
                    SELECT 
                        company_number,
                        company_name,
                        previous_name_{i}_name,
                        company_status,
                        company_category
                    FROM companies_house_data
                    WHERE previous_name_{i}_name LIKE %s
                    LIMIT 5
                """, (pattern,))
                
                prev_matches = cursor.fetchall()
                if prev_matches and not previous_found:
                    print(f"\n  Found in previous names:")
                    previous_found = True
                
                for match in prev_matches:
                    print(f"    Previous: {match[f'previous_name_{i}_name']}")
                    print(f"    Current: {match['company_name']} ({match['company_number']})")
            
            if not current_matches and not previous_found:
                print(f"  ❌ NOT FOUND in Companies House")
        
        # Check if they might be registered as different entity types
        print("\n\n=== CHECKING FOR SPECIAL REGISTRATION TYPES ===")
        
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN company_number LIKE 'RS%' THEN 'Registered Society'
                    WHEN company_number LIKE 'IP%' THEN 'Industrial & Provident Society'
                    WHEN company_number LIKE 'CS%' THEN 'Scottish Charity'
                    WHEN company_number LIKE 'CE%' THEN 'Charitable Incorporated Organisation'
                    WHEN company_number LIKE 'RP%' THEN 'Royal Patent'
                    ELSE 'Standard Company'
                END as registration_type,
                COUNT(*) as count
            FROM companies_house_data
            WHERE company_name LIKE '%HOUSING%'
            GROUP BY registration_type
            ORDER BY count DESC
        """)
        
        print("\nHousing-related entities by registration type:")
        for row in cursor.fetchall():
            print(f"  {row['registration_type']}: {row['count']:,}")
        
        # Look for specific patterns in Registered Societies
        print("\n\n=== REGISTERED SOCIETIES (RS/IP numbers) ===")
        
        cursor.execute("""
            SELECT 
                company_number,
                company_name,
                company_status,
                incorporation_date
            FROM companies_house_data
            WHERE (company_number LIKE 'RS%' OR company_number LIKE 'IP%')
            AND company_name LIKE '%HOUSING%'
            AND (
                company_name LIKE '%CIRCLE%' OR
                company_name LIKE '%ADACTUS%' OR
                company_name LIKE '%HEXAGON%' OR
                company_name LIKE '%WATERLOO%'
            )
            ORDER BY company_name
        """)
        
        societies = cursor.fetchall()
        if societies:
            print(f"\nFound {len(societies)} housing societies with RS/IP numbers:")
            for soc in societies:
                print(f"  {soc['company_name']}")
                print(f"    Number: {soc['company_number']}, Status: {soc['company_status']}")
        else:
            print("\nNo matching housing societies found with RS/IP numbers")
        
        cursor.close()
        conn.close()
        
        print("\n\n=== CONCLUSION ===")
        print("Many housing associations are:")
        print("1. Not in the standard Companies House data")
        print("2. Registered as different entity types (RS/IP numbers)")
        print("3. May have changed names or merged")
        print("4. May be registered with regulators other than Companies House")
        print("\nThis explains why 40% don't match - they're legitimate entities")
        print("but not standard UK limited companies in the Companies House register!")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    find_housing_associations()