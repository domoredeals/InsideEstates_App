# InsideEstates Production Data Pipeline Guide

## Overview

The InsideEstates data pipeline consists of three main stages:

1. **Land Registry Import** - Import CCOD/OCOD property ownership data
2. **Companies House Import** - Import UK company registration data  
3. **Matching** - Match property owners to company records

## 🚀 Quick Start

### Run Complete Pipeline
```bash
# Activate virtual environment
source venv/bin/activate

# Run full pipeline (all 3 steps)
python scripts/run_full_pipeline.py
```

### Run Individual Steps
```bash
# Import only Land Registry data
python scripts/run_full_pipeline.py --steps lr

# Import only Companies House data  
python scripts/run_full_pipeline.py --steps ch

# Run only matching
python scripts/run_full_pipeline.py --steps match
```

## 📁 Production Scripts

### 1. Land Registry Import (`01_import_land_registry_production.py`)

Imports CCOD (UK companies) and OCOD (overseas companies) property ownership data.

**Features:**
- Handles both FULL updates and Change Only Updates (COU)
- Automatic deduplication using (title_number, file_month) unique constraint
- Progress tracking with visual progress bars
- Resume capability - skips already imported files
- Comprehensive error handling and logging

**Usage:**
```bash
python scripts/01_import_land_registry_production.py \
    --ccod-dir DATA/SOURCE/LR/CCOD \
    --ocod-dir DATA/SOURCE/LR/OCOD \
    --batch-size 5000
```

### 2. Companies House Import (`02_import_companies_house_production.py`)

Imports Companies House Basic Company Data (5.6M+ UK companies).

**Features:**
- Efficient batch processing for large files (2.6GB+)
- Handles all company types and statuses
- Imports up to 10 previous company names per company
- Memory-optimized with periodic garbage collection
- Progress tracking and comprehensive logging

**Usage:**
```bash
python scripts/02_import_companies_house_production.py \
    --ch-dir DATA/SOURCE/CH \
    --batch-size 10000
```

### 3. Matching Script (`03_match_lr_to_ch_production.py`)

Matches Land Registry proprietors to Companies House records using 4-tier logic.

**Features:**
- **Proven Normalization**: REMOVES company suffixes (LIMITED, LTD, etc.) to maximize matches - based on extensive testing
- **4-Tier Matching**:
  - Tier 1: Name+Number match (confidence 1.0)
  - Tier 2: Number only match (confidence 0.9)
  - Tier 3: Current name match (confidence 0.7)
  - Tier 4: Previous name match (confidence 0.5)
  - Tier 5: No match (confidence 0.0)
- **Multiple Modes**:
  - `full` - Process all records
  - `no_match_only` - Re-process only No_Match records
  - `missing_only` - Process records not in match table
  - `date_range` - Process specific date ranges
- **Resume Capability**: Can be interrupted and resumed
- **Progress Tracking**: Real-time statistics and ETA

**Usage:**
```bash
# Full matching
python scripts/03_match_lr_to_ch_production.py --mode full

# Re-process only failed matches
python scripts/03_match_lr_to_ch_production.py --mode no_match_only

# Test with limited records
python scripts/03_match_lr_to_ch_production.py --mode full --test 1000
```

## 📊 Database Schema

### Tables Created

1. **`land_registry_data`** - Property ownership records
   - ~8.2M records across 88 monthly snapshots
   - Tracks up to 4 proprietors per property
   - Includes property details, prices, dates

2. **`companies_house_data`** - UK company registration data
   - ~5.6M company records
   - Current and historical company names
   - Registration details, status, addresses

3. **`land_registry_ch_matches`** - Matching results
   - Links LR records to CH companies
   - Stores match confidence and type
   - Enables ownership history tracking

### Key Views

1. **`v_ownership_history`** - Complete ownership timeline
   ```sql
   SELECT * FROM v_ownership_history 
   WHERE title_number = 'ABC123'
   ORDER BY file_month DESC;
   ```

2. **`v_ownership_summary`** - Aggregated ownership statistics
   ```sql
   SELECT * FROM v_ownership_summary
   WHERE postcode LIKE 'SW1%';
   ```

## 🔄 Regular Updates

### Monthly Update Process

1. **Download new data files**:
   - Land Registry: [CCOD/OCOD from GOV.UK](https://use-land-property-data.service.gov.uk/)
   - Companies House: [Basic company data](http://download.companieshouse.gov.uk/en_output.html)

2. **Place files in correct directories**:
   - `DATA/SOURCE/LR/CCOD/` - CCOD monthly files
   - `DATA/SOURCE/LR/OCOD/` - OCOD monthly files  
   - `DATA/SOURCE/CH/` - Companies House snapshot

3. **Run the pipeline**:
   ```bash
   python scripts/run_full_pipeline.py
   ```

The pipeline will automatically:
- Skip already imported LR files
- Update existing CH records
- Match only new/changed records

## 🐛 Troubleshooting

### Check Import Status
```bash
python scripts/run_full_pipeline.py --steps none
```
This runs only the database status check.

### Fix Failed Matches
```bash
# Re-run matching for records marked as No_Match
python scripts/03_match_lr_to_ch_production.py --mode no_match_only
```

### Test Normalization
```bash
python scripts/test_production_normalization.py
```

### View Logs
All scripts create detailed logs:
- `lr_import_production_YYYYMMDD_HHMMSS.log`
- `ch_import_production_YYYYMMDD_HHMMSS.log`
- `lr_ch_matching_production_YYYYMMDD_HHMMSS.log`
- `pipeline_run_YYYYMMDD_HHMMSS.log`

## ⚡ Performance Tips

1. **Run overnight**: Full pipeline takes 4-8 hours
2. **Use screen/tmux**: Allows disconnecting while running
3. **Monitor progress**: All scripts show real-time progress
4. **Check resources**: Ensure sufficient disk space (50GB+) and RAM (8GB+)

## 🎯 Expected Results

After successful pipeline completion:
- **Match Rate**: 50-60% of proprietors matched to Companies House
- **High Confidence**: ~70% of matches are Tier 1 (Name+Number)
- **Processing Time**: 
  - LR Import: 1-2 hours
  - CH Import: 30-45 minutes
  - Matching: 2-4 hours

## 📝 SQL Examples

### Find ownership history for a property
```sql
SELECT * FROM v_ownership_history 
WHERE title_number = 'MX418571'
ORDER BY file_month DESC;
```

### Find all properties owned by a company
```sql
SELECT DISTINCT title_number, property_address, ownership_status
FROM v_ownership_history
WHERE ch_company_name = 'TESCO PLC'
ORDER BY property_address;
```

### Analyze match quality
```sql
SELECT 
    ch_match_type_1 as match_type,
    COUNT(*) as count,
    ROUND(AVG(ch_match_confidence_1)::numeric, 2) as avg_confidence
FROM land_registry_ch_matches
WHERE ch_match_type_1 IS NOT NULL
GROUP BY ch_match_type_1
ORDER BY count DESC;
```