# Single Table Import for Land Registry Data

## Overview
This simplified import process creates a single denormalized table containing all property and proprietor data from the Land Registry CCOD and OCOD files.

## Table Structure
The `land_registry_data` table contains:
- All property fields (title_number, address, postcode, etc.)
- All proprietor fields for up to 4 proprietors per property
- Metadata fields (dataset_type, file_month, update_type)
- Automatic timestamps

## Usage

### 1. Create the Table
```bash
cd /home/adc/Projects/InsideEstates_App
source venv/bin/activate
python scripts/import_to_single_table.py --create-table
```

### 2. Import All Data
```bash
# Import all CCOD and OCOD files from default directories
python scripts/import_to_single_table.py
```

### 3. Import Specific Directories
```bash
# Import only CCOD files
python scripts/import_to_single_table.py --ccod-dir /path/to/ccod

# Import only OCOD files  
python scripts/import_to_single_table.py --ocod-dir /path/to/ocod

# Import both from custom locations
python scripts/import_to_single_table.py --ccod-dir /path/to/ccod --ocod-dir /path/to/ocod
```

### 4. Adjust Batch Size
```bash
# Use larger batches for faster import (default is 5000)
python scripts/import_to_single_table.py --batch-size 10000
```

## Features
- Single table design - no joins needed
- Handles both FULL and COU (Change Only Update) files
- Automatic deduplication using title_number + file_month
- Progress bars for each file
- Comprehensive error logging
- Optimized for PostgreSQL bulk loading

## Sample Queries

### Find all properties owned by a company
```sql
SELECT * FROM land_registry_data 
WHERE company_1_reg_no = '12345678' 
   OR company_2_reg_no = '12345678'
   OR company_3_reg_no = '12345678' 
   OR company_4_reg_no = '12345678';
```

### Get latest data for a property
```sql
SELECT DISTINCT ON (title_number) *
FROM land_registry_data
WHERE title_number = 'ABC123'
ORDER BY title_number, file_month DESC;
```

### Count properties by postcode area
```sql
SELECT LEFT(postcode, 4) as postcode_area, 
       COUNT(DISTINCT title_number) as property_count
FROM land_registry_data
WHERE postcode IS NOT NULL
GROUP BY postcode_area
ORDER BY property_count DESC;
```

### Find overseas-owned properties
```sql
SELECT DISTINCT title_number, property_address, proprietor_1_name, country_1_incorporated
FROM land_registry_data
WHERE dataset_type = 'OCOD'
  AND country_1_incorporated IS NOT NULL;
```

## Notes
- The import process will skip rows where title_number is empty
- Dates are parsed from DD-MM-YYYY format
- Price paid values are converted to numeric (NULL if invalid)
- All text fields are cleaned of NUL characters
- Progress is logged to `land_registry_single_table_import.log`