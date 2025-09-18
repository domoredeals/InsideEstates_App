# Land Registry to Companies House Matching

This set of scripts implements a 4-tier matching system to match Land Registry property ownership data with official Companies House company information.

## Overview

The matching system enriches the `land_registry_data` table with official Companies House information for each proprietor, using a sophisticated 4-tier matching algorithm that handles variations in company names and registration numbers.

## Files

1. **add_ch_match_columns.sql** - SQL script to add Companies House matching columns to the land_registry_data table
2. **match_lr_to_ch.py** - Main Python script that performs the 4-tier matching
3. **create_ch_matched_view.sql** - SQL views for easy access to matched data
4. **test_matching.py** - Test script to verify the setup and matching process

## 4-Tier Matching Logic

The matching algorithm uses the following hierarchy (from highest to lowest confidence):

1. **Tier 1: Name + Number Match (Confidence: 1.0)**
   - Matches on both normalized company name AND registration number
   - Highest confidence - almost certain to be correct

2. **Tier 2: Number Only Match (Confidence: 0.9)**
   - Matches on company registration number only
   - Very high confidence - registration numbers are unique

3. **Tier 3: Current Name Match (Confidence: 0.7)**
   - Matches on normalized current company name only
   - Medium confidence - names can be similar across companies

4. **Tier 4: Previous Name Match (Confidence: 0.5)**
   - Matches on previous company names (from CH historical data)
   - Lower confidence - but valuable for tracking company name changes

5. **Tier 5: No Match (Confidence: 0.0)**
   - No match found in Companies House data

## Setup Instructions

### 1. Add Companies House Match Columns

First, add the necessary columns to store the matched data:

```bash
psql -U insideestates_user -d insideestates_app -f add_ch_match_columns.sql
```

This adds the following columns for each proprietor (1-4):
- `ch_matched_name_X` - The official Companies House name
- `ch_matched_number_X` - The Companies House registration number
- `ch_match_type_X` - The type of match (Name+Number, Number, Name, Previous_Name, No_Match)
- `ch_match_confidence_X` - Confidence score (0.0 to 1.0)

### 2. Test the Matching Process

Run the test script to verify everything is set up correctly:

```bash
python test_matching.py
```

### 3. Run Test Matching

Test the matching on a small subset of data:

```bash
# Test on 1,000 records
python match_lr_to_ch.py --test 1000
```

### 4. Run Full Matching

Run the complete matching process:

```bash
# Run with default batch size (10,000)
python match_lr_to_ch.py

# Or with custom batch size
python match_lr_to_ch.py --batch-size 20000
```

The matching process:
- Loads all Companies House data into memory (~5.6M companies)
- Processes Land Registry records in batches
- Updates each record with matched CH information
- Provides statistics on match rates

### 5. Create Views

Create the views for easy data access:

```bash
psql -U insideestates_user -d insideestates_app -f create_ch_matched_view.sql
```

## Using the Matched Data

### Direct Queries

Find all properties owned by a specific company:
```sql
SELECT * FROM land_registry_data 
WHERE ch_matched_number_1 = '12345678' 
   OR ch_matched_number_2 = '12345678'
   OR ch_matched_number_3 = '12345678' 
   OR ch_matched_number_4 = '12345678';
```

### Using Views

**v_properties_with_companies** - Full property details with company information:
```sql
-- Find properties with low confidence matches
SELECT * FROM v_properties_with_companies 
WHERE total_match_confidence > 0 AND total_match_confidence < 2.0
ORDER BY total_match_confidence ASC;
```

**v_company_properties** - Summary of properties by company:
```sql
-- Find companies with most properties
SELECT * FROM v_company_properties 
ORDER BY property_count DESC 
LIMIT 100;
```

**v_match_quality_by_region** - Match statistics by region:
```sql
SELECT * FROM v_match_quality_by_region;
```

## Performance Considerations

- The matching script processes ~50,000 records per minute
- Full matching of ~5M properties takes approximately 2 hours
- Companies House data is loaded into memory for fast lookups (~2GB RAM)
- Batch updates minimize database round trips
- Indexes are automatically created on matched columns

## Name Normalization

The matching process normalizes company names by:
- Converting to uppercase
- Removing punctuation
- Standardizing "AND" / "&" 
- Removing common suffixes (LIMITED, LTD, PLC, etc.)
- Keeping only alphanumeric characters

This handles variations like:
- "ABC LIMITED" matches "ABC LTD"
- "SMITH & JONES" matches "SMITH AND JONES"
- "XYZ LTD." matches "XYZ LIMITED"

## Match Statistics

After running the matching, check the statistics:

```sql
SELECT * FROM v_ch_match_summary;
```

Typical match rates:
- ~60-70% of proprietors have some match
- ~40% match on Name+Number (highest confidence)
- ~20% match on Number only
- ~15% match on Name only
- ~5% match on Previous Names