#!/usr/bin/env python3
"""
Script 05: Merge scraped companies into companies_house_data table.
Also updates land_registry_ch_matches with the new matches.
"""

import psycopg2
import os
from dotenv import load_dotenv
import logging
from decimal import Decimal

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('05_merge_scraped.log'),
        logging.StreamHandler()
    ]
)

def get_connection():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

def main():
    """Main function - merge scraped data."""
    conn = get_connection()
    cur = conn.cursor()
    
    logging.info("Starting merge of scraped companies...")
    
    # 1. Add scraped companies to companies_house_data
    logging.info("Adding scraped companies to companies_house_data...")
    cur.execute("""
        INSERT INTO companies_house_data (
            company_number, company_name, company_status, 
            company_category, incorporation_date,
            reg_address_line1, reg_address_postcode
        )
        SELECT 
            so.company_number,
            so.company_name,
            so.company_status,
            COALESCE(so.company_type, 'Limited Company'),
            so.incorporation_date,
            so.registered_office_address,
            CASE 
                WHEN so.registered_office_address LIKE '%, __ ___' 
                THEN SUBSTRING(so.registered_office_address FROM '.*, (..)$')
                ELSE NULL
            END
        FROM ch_scrape_overview so
        WHERE so.scrape_status = 'parsed'
        AND NOT EXISTS (
            SELECT 1 FROM companies_house_data ch 
            WHERE ch.company_number = so.company_number
        )
    """)
    companies_added = cur.rowcount
    logging.info(f"Added {companies_added} companies to companies_house_data")
    
    # 2. Update land_registry_ch_matches for scraped companies
    logging.info("Updating matches for scraped companies...")
    
    # Add scraped_data column if it doesn't exist
    cur.execute("""
        DO $$ BEGIN
            ALTER TABLE land_registry_ch_matches ADD COLUMN scraped_data CHAR(1) DEFAULT 'N';
        EXCEPTION
            WHEN duplicate_column THEN NULL;
        END $$;
    """)
    
    # Match by company number
    cur.execute("""
        WITH scraped_matches AS (
            SELECT 
                lr.id,
                so.company_name,
                so.company_number,
                'Number' as match_type,
                1.00 as confidence,
                1 as prop_num
            FROM land_registry_data lr
            JOIN ch_scrape_overview so ON (
                lr.company_1_reg_no = so.company_number OR
                lr.company_1_reg_no = REGEXP_REPLACE(so.company_number, '^[A-Z]+', '') OR
                lr.company_1_reg_no = LPAD(REGEXP_REPLACE(so.company_number, '^[A-Z]+0*', ''), 8, '0')
            )
            JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE m.ch_match_type_1 = 'No_Match'
            AND so.scrape_status = 'parsed'
            
            UNION ALL
            
            -- Repeat for proprietors 2, 3, 4...
            SELECT lr.id, so.company_name, so.company_number, 'Number', 1.00, 2
            FROM land_registry_data lr
            JOIN ch_scrape_overview so ON (
                lr.company_2_reg_no = so.company_number OR
                lr.company_2_reg_no = REGEXP_REPLACE(so.company_number, '^[A-Z]+', '') OR
                lr.company_2_reg_no = LPAD(REGEXP_REPLACE(so.company_number, '^[A-Z]+0*', ''), 8, '0')
            )
            JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE m.ch_match_type_2 = 'No_Match'
            AND so.scrape_status = 'parsed'
        )
        UPDATE land_registry_ch_matches m
        SET 
            ch_matched_name_1 = sm.company_name,
            ch_matched_number_1 = sm.company_number,
            ch_match_type_1 = sm.match_type,
            ch_match_confidence_1 = sm.confidence,
            scraped_data = 'Y',
            updated_at = CURRENT_TIMESTAMP
        FROM scraped_matches sm
        WHERE m.id = sm.id AND sm.prop_num = 1
    """)
    matches_updated = cur.rowcount
    
    # Match by company name (including previous names)
    cur.execute("""
        WITH name_matches AS (
            SELECT 
                lr.id,
                so.company_name,
                so.company_number,
                CASE 
                    WHEN UPPER(TRIM(lr.proprietor_1_name)) = UPPER(TRIM(so.company_name)) THEN 'Name'
                    ELSE 'PrevName'
                END as match_type,
                0.90 as confidence,
                1 as prop_num
            FROM land_registry_data lr
            JOIN ch_scrape_overview so ON (
                UPPER(TRIM(lr.proprietor_1_name)) = UPPER(TRIM(so.company_name)) OR
                UPPER(TRIM(lr.proprietor_1_name)) = ANY(
                    SELECT UPPER(TRIM(unnest(so.previous_names)))
                )
            )
            JOIN land_registry_ch_matches m ON lr.id = m.id
            WHERE m.ch_match_type_1 = 'No_Match'
            AND so.scrape_status = 'parsed'
            AND NOT EXISTS (
                SELECT 1 FROM land_registry_ch_matches
                WHERE id = lr.id AND ch_matched_number_1 = so.company_number
            )
        )
        UPDATE land_registry_ch_matches m
        SET 
            ch_matched_name_1 = nm.company_name,
            ch_matched_number_1 = nm.company_number,
            ch_match_type_1 = nm.match_type,
            ch_match_confidence_1 = nm.confidence,
            scraped_data = 'Y',
            updated_at = CURRENT_TIMESTAMP
        FROM name_matches nm
        WHERE m.id = nm.id AND nm.prop_num = 1
    """)
    name_matches_updated = cur.rowcount
    
    conn.commit()
    
    # Show results
    logging.info(f"Total matches updated: {matches_updated + name_matches_updated}")
    
    # Summary statistics
    cur.execute("""
        SELECT 
            COUNT(*) as total_scraped,
            COUNT(CASE WHEN company_status = 'Dissolved' THEN 1 END) as dissolved,
            COUNT(CASE WHEN company_status = 'Active' THEN 1 END) as active
        FROM ch_scrape_overview
        WHERE scrape_status = 'parsed'
    """)
    stats = cur.fetchone()
    logging.info(f"Scraped companies: {stats[0]} total ({stats[1]} dissolved, {stats[2]} active)")
    
    cur.execute("""
        SELECT COUNT(*) 
        FROM land_registry_ch_matches 
        WHERE scraped_data = 'Y'
    """)
    total_scraped_matches = cur.fetchone()[0]
    logging.info(f"Total properties matched via scraping: {total_scraped_matches}")
    
    cur.close()
    conn.close()
    logging.info("Merge complete!")

if __name__ == "__main__":
    main()