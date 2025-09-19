# Companies House Scraper for Windows

## Quick Start Guide

### 1. Simple Version (Recommended for beginners)

#### Files Needed:
- `scrape_companies_simple.py` - The main scraper script
- `companies_to_search.csv` - Your list of company names
- `RUN_SCRAPER.bat` - Double-click to run (optional)

#### Steps:

1. **Install Required Python Packages**
   Open Command Prompt or PowerShell and run:
   ```
   pip install requests lxml
   ```

2. **Create Your Company List**
   Edit `companies_to_search.csv` with your company names:
   ```
   Company Name
   Apple Inc
   Microsoft Corporation
   Amazon.com Inc
   ```

3. **Run the Scraper**
   
   **Option A: Double-click Method**
   - Double-click `RUN_SCRAPER.bat`
   
   **Option B: Python IDLE**
   - Open `scrape_companies_simple.py` in IDLE
   - Press F5 to run
   
   **Option C: Command Line**
   ```
   python scrape_companies_simple.py
   ```

4. **Get Your Results**
   Results will be saved to: `companies_house_results_YYYYMMDD_HHMMSS.csv`

### 2. Advanced Version (For larger batches)

Use `scrape_companies_house_windows.py` for:
- Batch processing with multiple threads
- Better error handling
- Progress tracking
- Custom input/output files

#### Running Advanced Version in IDLE:
```python
# Open scrape_companies_house_windows.py in IDLE
# Press F5 to run
# Follow the prompts:
# - Enter input CSV filename
# - Enter output CSV filename
# - Choose batch size (1-10)
```

## Input File Format

Your `companies_to_search.csv` should look like:
```
Company Name
TESCO PLC
SAINSBURY'S SUPERMARKETS LTD
ASDA STORES LIMITED
```

The scraper accepts these column names:
- Company Name
- company_name
- name
- Name
- search_name

## Output File Format

The results CSV will contain:
```
Search Name,Found Name,Company Number,Status,URL
TESCO PLC,TESCO PLC,00445790,FOUND,https://find-and-update.company-information.service.gov.uk/company/00445790
```

### Output Columns:
- **Search Name**: The name you searched for
- **Found Name**: The official company name found
- **Company Number**: UK company registration number
- **Status**: FOUND, NOT_FOUND, or ERROR
- **URL**: Link to the company page

## Performance Settings

### Simple Version
- Fixed 2-3 second delay between requests
- Sequential processing (one at a time)
- Good for up to 1,000 companies

### Advanced Version
- Configurable batch size (1-10 parallel requests)
- Adjustable delays (1.5-3.5 seconds)
- Good for 1,000-10,000 companies

### Time Estimates
- Simple version: ~3-4 seconds per company
- Advanced version: ~1-2 seconds per company (with batch size 5)

Examples:
- 100 companies: ~5-6 minutes
- 1,000 companies: ~50-60 minutes
- 10,000 companies: ~5-8 hours

## Troubleshooting

### Common Issues:

1. **"ModuleNotFoundError: No module named 'requests'"**
   - Install required packages: `pip install requests lxml`

2. **"FileNotFoundError: companies_to_search.csv"**
   - Make sure the CSV file is in the same folder as the script

3. **Timeout Errors**
   - Internet connection issue
   - Companies House website may be slow
   - Script will retry automatically

4. **Rate Limiting**
   - If you get many errors, reduce batch size
   - Add longer delays between requests

### For 122k Companies

If you need to scrape 122k companies:

1. **Split into batches**: Process 10,000 at a time
2. **Run overnight**: Less traffic on Companies House
3. **Use advanced version**: Better error recovery
4. **Monitor progress**: Check output file periodically

Example approach for large batches:
```python
# Split your input file into chunks
# Run each chunk separately
# Combine results at the end
```

## Tips for Best Results

1. **Clean Company Names**
   - Remove extra spaces
   - Keep official suffixes (Ltd, PLC, Limited)
   - Remove special characters if possible

2. **Monitor Progress**
   - Check the console output
   - Look at partial results in output CSV
   - Script saves after each company

3. **Be Respectful**
   - Don't run multiple instances
   - Keep delays between requests
   - Run during off-peak hours if possible

## Example Usage

### Basic Example:
1. Put company names in `companies_to_search.csv`
2. Double-click `RUN_SCRAPER.bat`
3. Wait for completion
4. Open results CSV in Excel

### IDLE Example:
```python
# In Python IDLE:
>>> exec(open('scrape_companies_simple.py').read())
```

### Custom Files Example:
```python
# In Python IDLE:
from scrape_companies_simple import scrape_companies_house

# Custom input and output files
scrape_companies_house(
    input_file='my_companies.csv',
    output_file='my_results.csv',
    delay_seconds=3
)
```

## Support

- Results are saved after each company (partial results available)
- Script can be stopped with Ctrl+C
- Re-run to process remaining companies
- Check the Status column to see what happened to each company