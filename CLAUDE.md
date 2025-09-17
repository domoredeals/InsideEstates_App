# InsideEstates App - PostgreSQL Version

## 🎯 PROJECT OVERVIEW
First live version of InsideEstates using PostgreSQL database. Imports and processes UK Land Registry CCOD (Commercial and Corporate Ownership Data) and OCOD (Overseas Companies Ownership Data) to provide property ownership intelligence.

## 📊 DATA SOURCES

### Land Registry Data Files
**Location**: `/home/adc/Projects/InsideEstates_App/DATA/SOURCE/LR/`
- **CCOD/** - UK companies that own property in England and Wales
  - 88 monthly files (2018-01 to 2025-08)
  - Format: `CCOD_FULL_YYYY_MM.csv` (complete monthly snapshot)
  - Format: `CCOD_COU_YYYY_MM.csv` (change only updates)
- **OCOD/** - Overseas companies that own property in England and Wales  
  - 88 monthly files (2018-01 to 2025-08)
  - Format: `OCOD_FULL_YYYY_MM.csv` (complete monthly snapshot)
  - Format: `OCOD_COU_YYYY_MM.csv` (change only updates)

### Technical Specification
Based on official Land Registry spec: https://use-land-property-data.service.gov.uk/datasets/ccod/tech-spec

**Key Fields**:
- Title Number (unique property identifier)
- Tenure (Freehold/Leasehold)
- Property Address, District, County, Region, Postcode
- Up to 4 Proprietors per property with:
  - Company name and registration number
  - Proprietorship category
  - Country incorporated (OCOD only)
  - Correspondence addresses
- Price Paid (last sale price from Price Paid Data)
- Date Proprietor Added
- Change Indicator & Date (for COU files)

## 🏗️ DATABASE ARCHITECTURE

### PostgreSQL Schema - Single Table Design
- **land_registry_data** - All property and proprietor information in one denormalized table
  - Property fields (title_number, address, postcode, price, etc.)
  - Up to 4 proprietors with all their details inline
  - Metadata tracking (dataset_type, file_month, update_type)
  - Automatic timestamps and deduplication

### Key Features
- Single table design - no joins required
- Handles both CCOD and OCOD datasets
- Tracks changes over time (Change Indicator)
- Supports both FULL and COU (Change Only Update) files
- Automatic deduplication using title_number + file_month

## 💻 DEVELOPMENT WORKFLOW

### Initial Setup
```bash
cd /home/adc/Projects/InsideEstates_App
source venv/bin/activate

# Database credentials already configured in .env:
# Database: insideestates_app
# User: insideestates_user  
# Password: InsideEstates2024!
```

### Import Land Registry Data
```bash
# Create the single table schema
python scripts/import_to_single_table.py --create-table

# Import all CCOD and OCOD files
python scripts/import_to_single_table.py

# Or import specific directories
python scripts/import_to_single_table.py --ccod-dir /path/to/ccod --ocod-dir /path/to/ocod

# Monitor progress in land_registry_single_table_import.log
```

### Data Processing Features
- Batch processing (5,000 records at a time)
- Progress tracking with visual progress bars
- Automatic skip of already imported files
- Error handling and recovery
- All data stored in single denormalized table for simplicity

## 🚀 PERFORMANCE OPTIMIZATIONS

### PostgreSQL Configuration
- Optimized for 128GB RAM system
- 32GB shared buffers
- Parallel processing enabled (16 workers)
- SSD-optimized settings
- Bulk loading optimizations in scripts

### Import Performance
- COPY command support for maximum speed
- Batch inserts with execute_values
- Transaction optimization
- Automatic index creation
- Smart duplicate handling with ON CONFLICT

## 📈 DATA CHARACTERISTICS

### Expected Volume
- ~4-5 million property records
- ~3-4 million unique companies
- Monthly updates add ~50-100k changes

### Data Quality Notes
- ~2% of titles have missing Date Proprietor Added
- Company Registration Numbers not mandatory pre-1997
- Some typographic errors in company numbers
- Addresses may have inconsistent formatting

## 🔧 KEY COMMANDS

### Database Management
```bash
# View data statistics
psql -U insideestates_user -d insideestates_app -c "SELECT COUNT(DISTINCT title_number) as properties FROM land_registry_data;"

# Count by dataset type
psql -U insideestates_user -d insideestates_app -c "SELECT dataset_type, COUNT(DISTINCT title_number) FROM land_registry_data GROUP BY dataset_type;"

# See latest imported files
psql -U insideestates_user -d insideestates_app -c "SELECT DISTINCT file_month, dataset_type, update_type, COUNT(*) FROM land_registry_data GROUP BY file_month, dataset_type, update_type ORDER BY file_month DESC LIMIT 10;"
```

### Monitoring Queries
```sql
-- Properties by dataset type
SELECT dataset_type, COUNT(DISTINCT title_number) FROM land_registry_data GROUP BY dataset_type;

-- Find properties by company registration number
SELECT * FROM land_registry_data 
WHERE company_1_reg_no = '12345678' 
   OR company_2_reg_no = '12345678'
   OR company_3_reg_no = '12345678'
   OR company_4_reg_no = '12345678';

-- Latest data for a specific property
SELECT DISTINCT ON (title_number) *
FROM land_registry_data
WHERE title_number = 'ABC123'
ORDER BY title_number, file_month DESC;
```

## 🏢 COMPANIES HOUSE DATA

### Data Source
**Location**: `/home/adc/Projects/InsideEstates_App/DATA/SOURCE/CH/`
- Basic Company Data snapshot files (5.6M+ companies, 2.6GB)
- Format: `BasicCompanyDataAsOneFile-YYYY-MM-DD.csv`
- Monthly snapshots from Companies House Public Data Product

### Import Companies House Data
```bash
# Create the companies_house_data table
python scripts/import_companies_house.py --create-table

# Import the data file (takes ~30-45 minutes)
python scripts/import_companies_house.py --file DATA/SOURCE/CH/BasicCompanyDataAsOneFile-2025-09-01.csv

# Or auto-detect and import all files in CH directory
python scripts/import_companies_house.py

# Monitor progress in companies_house_import.log
```

### Companies House Schema
- **companies_house_data** - All UK company information
  - Company details (number, name, status, incorporation date)
  - Registered address with postcode
  - SIC codes (up to 4)
  - Accounts and returns filing dates
  - Mortgage charges information
  - Previous names (up to 10)
  - Confirmation statement dates

### Key Queries
```sql
-- Find companies by registration number
SELECT * FROM companies_house_data WHERE company_number = '12345678';

-- Find companies at a postcode
SELECT * FROM companies_house_data WHERE reg_address_postcode = 'SW1A 1AA';

-- Active companies incorporated in last year
SELECT * FROM companies_house_data 
WHERE company_status = 'Active' 
AND incorporation_date > CURRENT_DATE - INTERVAL '1 year';

-- Join with Land Registry to find property ownership
SELECT DISTINCT 
    ch.company_name,
    ch.company_number,
    ch.company_status,
    ch.reg_address_postcode,
    COUNT(DISTINCT lr.title_number) as property_count
FROM companies_house_data ch
JOIN land_registry_data lr ON (
    ch.company_number = lr.company_1_reg_no OR
    ch.company_number = lr.company_2_reg_no OR
    ch.company_number = lr.company_3_reg_no OR
    ch.company_number = lr.company_4_reg_no
)
WHERE ch.company_status = 'Active'
GROUP BY ch.company_name, ch.company_number, ch.company_status, ch.reg_address_postcode
ORDER BY property_count DESC;
```

## 🚨 IMPORTANT NOTES

1. **Change Only Updates (COU)**: The Change Indicator shows A (Added) or D (Deleted) - handle appropriately
2. **Multiple Addresses**: Check multiple_address_indicator = 'Y' for properties with additional addresses
3. **Additional Proprietors**: Check additional_proprietor_indicator = 'Y' for excluded proprietor types
4. **Date Format**: All dates in DD-MM-YYYY format in source files
5. **UTF-8 Encoding**: Source files use UTF-8 with comma separators and quoted fields
6. **Companies House Updates**: Company data changes daily - consider regular updates

## 📋 NEXT STEPS

1. Build Flask web application to serve the data
2. Create API endpoints for property/company searches combining both datasets
3. Implement analytics dashboards showing property ownership patterns
4. Set up automated monthly updates for both Land Registry and Companies House
5. Add property valuation data (Zoopla/Rightmove APIs)