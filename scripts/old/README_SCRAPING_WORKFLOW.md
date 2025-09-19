# Companies House Scraping Workflow

This workflow finds and matches companies that are missing from the bulk Companies House data, particularly dissolved companies and those with name changes.

## Scripts Overview (in order of execution)

### 01_queue_no_match_companies.py
**Purpose**: Adds "No_Match" Limited Companies and LLPs to the scraping queue  
**Usage**: `python 01_queue_no_match_companies.py [--limit 100]`  
**What it does**:
- Finds companies in Land Registry marked as "No_Match"
- Filters to only Limited Companies and LLPs (not Local Authorities, etc.)
- Adds them to `ch_scrape_queue` table with status 'pending'

### 02_scrape_companies_house.py
**Purpose**: Searches for and scrapes company data from Companies House website  
**Usage**:
```bash
# Step 1: Search for companies
python 02_scrape_companies_house.py --search [--limit 100]

# Step 2: Scrape company overview pages
python 02_scrape_companies_house.py --scrape overview [--limit 100]

# Optional: Check statistics
python 02_scrape_companies_house.py --stats
```
**What it does**:
- Search: Finds companies on CH website, updates queue with URLs
- Scrape: Downloads HTML from company pages, stores in `ch_scrape_overview`

### 03_parse_companies_house_data.py
**Purpose**: Extracts structured data from scraped HTML  
**Usage**: `python 03_parse_companies_house_data.py --parse overview [--limit 100]`  
**What it does**:
- Parses HTML to extract company details (name, status, dates, etc.)
- Handles dissolved companies and previous names
- Updates `ch_scrape_overview` with parsed data

### 04_match_scraped_companies.py
**Purpose**: Matches scraped companies with Land Registry "No_Match" records  
**Usage**: `python 04_match_scraped_companies.py`  
**What it does**:
- Finds companies in `ch_scrape_overview` that can match Land Registry records
- Handles name changes (e.g., CGU INSURANCE → AVIVA)
- Handles registration number variations (e.g., "2116" → "SC002116")
- Updates `land_registry_ch_matches` with new matches
- Marks matches with `scraped_data = 'Y'`

### 05_add_scraped_to_companies_house.py
**Purpose**: Adds scraped companies to main `companies_house_data` table  
**Usage**: `python 05_add_scraped_to_companies_house.py [--dry-run]`  
**What it does**:
- Takes parsed companies from `ch_scrape_overview`
- Adds them to `companies_house_data` (including dissolved companies)
- Makes all companies searchable in the main table

### 06_check_scrape_results.py
**Purpose**: Shows comprehensive results of the scraping and matching process  
**Usage**: `python 06_check_scrape_results.py`  
**What it does**:
- Shows scrape queue status
- Lists successfully scraped and parsed companies
- Shows how many matches were made
- Identifies any companies that still need processing

## Complete Workflow Example

```bash
# 1. Queue 100 companies for scraping
python 01_queue_no_match_companies.py --limit 100

# 2. Search for them on Companies House
python 02_scrape_companies_house.py --search --limit 100

# 3. Scrape their overview pages
python 02_scrape_companies_house.py --scrape overview --limit 100

# 4. Parse the HTML into structured data
python 03_parse_companies_house_data.py --parse overview --limit 100

# 5. Match them against Land Registry records
python 04_match_scraped_companies.py

# 6. Add scraped companies to main table
python 05_add_scraped_to_companies_house.py

# 7. Check the results
python 06_check_scrape_results.py
```

## What Gets Scraped

The workflow specifically targets:
- **Dissolved companies** not in bulk CH data
- **Companies with name changes** (previous names are captured)
- **Companies with registration number variations**
- Only **Limited Companies and LLPs** (not councils, etc.)

## Database Tables Used

- `ch_scrape_queue` - Queue of companies to search/scrape
- `ch_scrape_overview` - Scraped company data
- `land_registry_ch_matches` - Matching results
- `companies_house_data` - Main company database

## Success Metrics

From a test of 100 companies:
- Found 102 companies on CH website
- Successfully parsed 100 companies
- Made 66 new matches (reducing "No_Match" count)
- Added 75 dissolved companies to main database