# Companies House Web Scraping System

## Overview
This system scrapes Companies House website data for companies that couldn't be matched in our existing database. We have 122,799 unique company names to search for.

## Database Tables Created

1. **ch_scrape_queue** - Controls what companies to search for
   - Tracks search status (pending, searching, found, not_found, error)
   - Stores found company numbers and URLs

2. **ch_scrape_overview** - Company overview pages
3. **ch_scrape_officers** - Officer information
4. **ch_scrape_charges** - Charges/mortgage data
5. **ch_scrape_insolvency** - Insolvency information

## Scripts

### 1. extract_unmatched_companies.py
Extracts unmatched company names from land_registry_data and populates the scraping queue.

```bash
# Extract all 122,799 companies
python scripts/extract_unmatched_companies.py

# Extract limited sample for testing
python scripts/extract_unmatched_companies.py --limit 100
```

### 2. scrape_companies_house.py
Main scraping script with polite delays and error handling.

```bash
# Check statistics
python scripts/scrape_companies_house.py --stats

# Search for companies (finds company numbers and URLs)
python scripts/scrape_companies_house.py --search --limit 100

# Scrape overview pages
python scripts/scrape_companies_house.py --scrape overview --limit 100

# Scrape all data types
python scripts/scrape_companies_house.py --scrape all --limit 100

# Full process for all companies (use with caution!)
python scripts/scrape_companies_house.py --search
python scripts/scrape_companies_house.py --scrape all
```

## Usage Instructions

### Step 1: Initial Setup
```bash
# Already completed - tables created
python scripts/create_ch_scrape_tables.sql
```

### Step 2: Load Companies to Search
```bash
# Load sample for testing
python scripts/extract_unmatched_companies.py --limit 1000

# Or load all companies (122,799)
python scripts/extract_unmatched_companies.py
```

### Step 3: Search Companies House
```bash
# Test with small batch first
python scripts/scrape_companies_house.py --search --limit 100 --batch-size 5

# Adjust parameters based on results
# --batch-size: Number of parallel requests (default 5, max 10)
# --delay-min: Minimum delay between requests (default 1 second)
# --delay-max: Maximum delay between requests (default 3 seconds)
```

### Step 4: Scrape Company Data
```bash
# Scrape overview pages first
python scripts/scrape_companies_house.py --scrape overview --limit 100

# Then scrape additional data
python scripts/scrape_companies_house.py --scrape officers --limit 100
python scripts/scrape_companies_house.py --scrape charges --limit 100
python scripts/scrape_companies_house.py --scrape insolvency --limit 100
```

### Step 5: Monitor Progress
```bash
# Check statistics at any time
python scripts/scrape_companies_house.py --stats

# View logs
tail -f ch_scraper.log
```

## Test Results

Initial test with 10 companies:
- All 10 companies were successfully found
- Overview pages scraped successfully
- Compressed HTML stored (8-9KB compressed, ~35KB uncompressed)
- No blocking or rate limiting observed with current delays

## Next Steps

1. **Parse HTML Data**: Create parsers to extract structured data from HTML
2. **Match Back to Land Registry**: Update land_registry_ch_matches with found companies
3. **Scale Up**: Gradually increase batch sizes and reduce delays if no issues
4. **Full Run**: Process all 122,799 companies (estimate 34-68 hours with current settings)

## Important Notes

- **Be Respectful**: Current settings use 1-3 second delays between requests
- **Monitor for Blocking**: Watch for 429 errors or captcha pages
- **Incremental Processing**: Script supports resuming from where it left off
- **Data Storage**: HTML is compressed with bz2 to save space

## Database Queries

```sql
-- Check queue status
SELECT search_status, COUNT(*) FROM ch_scrape_queue GROUP BY search_status;

-- View found companies
SELECT search_name, found_name, company_number 
FROM ch_scrape_queue 
WHERE search_status = 'found' 
LIMIT 10;

-- Check scraping progress
SELECT scrape_status, COUNT(*) 
FROM ch_scrape_overview 
GROUP BY scrape_status;
```