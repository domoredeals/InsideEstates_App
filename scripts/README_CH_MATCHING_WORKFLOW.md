# Companies House Matching Workflow

This document describes the complete workflow for matching Land Registry proprietors to Companies House data, including scraping for missing companies.

## Overview

The workflow consists of 4 main steps:
1. Import Companies House basic data file
2. Match Land Registry proprietors to Companies House
3. Scrape Companies House website for missing companies
4. Update database with scraped data

## Step 1: Import Companies House Data

```bash
python scripts/02_import_companies_house_production.py
```

This imports the monthly Companies House basic data snapshot file.

## Step 2: Match Land Registry to Companies House

```bash
python scripts/03_match_lr_to_ch_production.py
```

**Key Features (v4.0):**
- Only processes Limited Companies/PLCs and LLPs
- 4-tier matching: Name+Number (1.0), Number (0.9), Name (0.7), Previous Name (0.5)
- For unmatched companies, populates CH fields with Land Registry data
- Uses 'Land_Registry' match type with 0.3 confidence for unmatched
- Exports list of companies not found in CH data for scraping

**Match Types:**
- `Name+Number` (1.0) - Perfect match on both name and registration number
- `Number` (0.9) - Match on registration number only
- `Name` (0.7) - Match on normalized company name only  
- `Previous_Name` (0.5) - Match on previous company names
- `Land_Registry` (0.3) - No CH match found, using Land Registry data
- `Not_Company` (0.0) - Not a limited company or LLP (individuals, charities, etc.)
- `Scraped` (0.8) - Updated with data from CH website scraping

## Step 3: Scrape Missing Companies (Windows)

Run on Windows machine with proxy access:

```bash
python 04_scrape_companies_house_proxy_with_overview.py
```

**Input:** CSV file exported from step 2 (no_match_companies_*.csv)
**Output:** CSV with scraped company data including:
- Company name, number, status
- Incorporation date
- Registered address
- SIC codes
- Previous names

## Step 4: Update Database with Scraped Data

```bash
python scripts/05_update_from_scraping.py scraped_results.csv
```

**Updates:**
- Companies table: Adds full CH data (status, dates, SIC codes, etc.)
- Match table: Changes match_type from 'Land_Registry' to 'Scraped' (0.8 confidence)

## Frontend Display

The frontend can now always display company names from `ch_matched_name_*` fields:
- High confidence (>0.7): CH verified data
- Medium confidence (0.5-0.7): Name or previous name matches
- Low confidence (<0.5): Land Registry data only

Match quality is indicated by:
- `ch_match_type_*`: Shows data source
- `ch_match_confidence_*`: Numeric confidence score

## Data Quality Indicators

| Match Type | Confidence | Description |
|------------|------------|-------------|
| Name+Number | 1.0 | Perfect match - highest quality |
| Number | 0.9 | Registration number match - very reliable |
| Scraped | 0.8 | Found via web scraping - good quality |
| Name | 0.7 | Name-only match - verify manually |
| Previous_Name | 0.5 | Historical name match - needs verification |
| Land_Registry | 0.3 | No CH data - lowest quality |
| Not_Company | 0.0 | Not a limited company/LLP |

## Automation

The complete workflow can be automated with a cron job:
```bash
# Run monthly after CH data release
0 3 5 * * cd /home/adc/Projects/InsideEstates_App && ./scripts/run_ch_matching_workflow.sh
```

## Notes

- Companies House data is updated monthly (usually first week)
- Scraping should be done from Windows with proxy access
- The system progressively improves data quality through scraping
- All match types preserve data for frontend display