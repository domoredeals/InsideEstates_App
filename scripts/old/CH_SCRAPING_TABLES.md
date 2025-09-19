# Companies House Scraping Tables Structure

## Overview
The scraping system uses 5 PostgreSQL tables to store Companies House data for the 122,799 unmatched companies.

## Table Details

### 1. ch_scrape_queue
**Purpose**: Controls which companies to search for and tracks search results

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| search_name | TEXT | Company name we're searching for (e.g., "CGU INSURANCE PLC") |
| found_name | TEXT | Actual company name found (e.g., "AVIVA INSURANCE LIMITED") |
| company_number | TEXT | Companies House number if found |
| company_url | TEXT | Full URL to company page |
| search_status | TEXT | pending, searching, found, not_found, error |
| search_timestamp | TIMESTAMPTZ | When the search was performed |
| search_error | TEXT | Error message if search failed |

### 2. ch_scrape_overview
**Purpose**: Stores main company information including previous names

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| company_number | TEXT | Companies House number (unique) |
| company_url | TEXT | URL to company page |
| raw_html | BYTEA | Compressed HTML of overview page |
| company_name | TEXT | Current company name |
| company_status | TEXT | Active, Dissolved, etc. |
| incorporation_date | DATE | When company was incorporated |
| company_type | TEXT | Private limited Company, PLC, etc. |
| registered_office_address | TEXT | Current registered address |
| sic_codes | TEXT[] | Array of SIC codes |
| previous_names | TEXT[] | **Array of all previous company names** |
| accounts_next_due | DATE | Next accounts filing date |
| confirmation_statement_next_due | DATE | Next confirmation statement date |
| scrape_status | TEXT | pending, scraped, parsed, error |
| scrape_timestamp | TIMESTAMPTZ | When scraped |
| parse_timestamp | TIMESTAMPTZ | When parsed |

### 3. ch_scrape_officers
**Purpose**: Officer/director information (not yet populated)

| Column | Type | Description |
|--------|------|-------------|
| company_number | TEXT | Links to company |
| officer_name | TEXT | Name of officer/director |
| officer_role | TEXT | Director, Secretary, etc. |
| appointed_date | DATE | When appointed |
| resigned_date | DATE | When resigned (if applicable) |
| ... | ... | Additional officer details |

### 4. ch_scrape_charges
**Purpose**: Mortgage/charge information (not yet populated)

### 5. ch_scrape_insolvency
**Purpose**: Insolvency history (not yet populated)

## Key Features for Matching

1. **Previous Names Captured**: The `previous_names` array in `ch_scrape_overview` stores all historical company names
2. **Company Status**: The `company_status` field shows if a company is Dissolved, Active, etc.
3. **Name Changes Tracked**: `ch_scrape_queue` shows what was searched vs. what was found

## Example Data

### Example 1: Name Change
- **Searched for**: "CGU INSURANCE PLC"
- **Found as**: "AVIVA INSURANCE LIMITED" 
- **Previous names**: ['CGU INSURANCE PLC', 'GENERAL ACCIDENT FIRE AND LIFE ASSURANCE CORPORATION PUBLIC LIMITED COMPANY']
- **Result**: Can match on previous name

### Example 2: Dissolved Company
- **Searched for**: "DOVE BROTHERS LIMITED"
- **Found as**: "DOVE BROTHERS LIMITED"
- **Status**: "Dissolved"
- **Previous names**: ["O'ROURKE BUILDING LIMITED", 'METRO FLOORS LIMITED', 'PASTVANT LIMITED']
- **Result**: Found but marked as dissolved

## Usage for Matching

To update the Land Registry matching system with this data:

```sql
-- Find companies that match on previous names
SELECT DISTINCT lr.id, lr.proprietor_1_name, ch.company_number, ch.company_name, ch.company_status
FROM land_registry_data lr
JOIN ch_scrape_queue q ON lr.proprietor_1_name = q.search_name
JOIN ch_scrape_overview ch ON q.company_number = ch.company_number
WHERE q.search_status = 'found'
AND lr.proprietor_1_name = ANY(ch.previous_names);

-- Find dissolved companies
SELECT DISTINCT lr.id, lr.proprietor_1_name, ch.company_number, ch.company_status
FROM land_registry_data lr
JOIN ch_scrape_queue q ON lr.proprietor_1_name = q.search_name  
JOIN ch_scrape_overview ch ON q.company_number = ch.company_number
WHERE ch.company_status = 'Dissolved';
```