# Production Land Registry to Companies House Matcher

This production-ready script matches Land Registry proprietors to Companies House data using a robust 4-tier matching system.

## Features

✅ **Proven Normalization** - REMOVES company suffixes (LIMITED, LTD, PLC, etc.) to increase matches  
✅ **4-Tier Matching Logic** - From highest to lowest confidence  
✅ **Progress Tracking** - Can be resumed if interrupted  
✅ **Multiple Modes** - Full, re-match, missing records, date ranges  
✅ **Comprehensive Logging** - Detailed logs and error tracking  
✅ **Memory Efficient** - Optimized for large datasets  
✅ **Production Ready** - Error handling, checkpoints, graceful shutdown

## Prerequisites

1. **Database Setup**: Ensure these tables exist:
   - `land_registry_data` (imported data)
   - `companies_house_data` (imported data)
   - `land_registry_ch_matches` (match results)

2. **Create Match Table**:
   ```bash
   python scripts/run_sql_script.py scripts/create_match_table.sql
   ```

## Usage Examples

### 1. Full Matching (All Records)
```bash
python scripts/03_match_lr_to_ch_production.py --mode full
```

### 2. Re-process Only Unmatched Records
```bash
python scripts/03_match_lr_to_ch_production.py --mode no_match_only
```

### 3. Process Missing Records Only
```bash
python scripts/03_match_lr_to_ch_production.py --mode missing_only
```

### 4. Process Date Range
```bash
python scripts/03_match_lr_to_ch_production.py --mode date_range --date-from 2024-01-01 --date-to 2024-12-31
```

### 5. Test Mode (Limited Records)
```bash
python scripts/03_match_lr_to_ch_production.py --mode full --test 10000
```

### 6. Resume from Checkpoint
```bash
python scripts/03_match_lr_to_ch_production.py --mode full --resume
```

### 7. Start Fresh (Ignore Checkpoint)
```bash
python scripts/03_match_lr_to_ch_production.py --mode full --no-resume
```

## 4-Tier Matching System

The script uses a sophisticated matching system with confidence scores:

| Tier | Match Type | Confidence | Description |
|------|------------|------------|-------------|
| 1 | Name+Number | 1.0 | Both company name AND registration number match exactly |
| 2 | Number | 0.9 | Registration number matches (high confidence) |
| 3 | Name | 0.7 | Company name matches current name (medium confidence) |
| 4 | Previous_Name | 0.5 | Company name matches a previous company name (low confidence) |
| 5 | No_Match | 0.0 | No match found |

## Key Improvements Over Previous Scripts

### Fixed Normalization Issues
- **Company Names**: Don't remove suffixes like LIMITED, LTD, PLC
- **Company Numbers**: Proper padding with leading zeros (e.g., "178711" → "00178711")
- **Standardization**: LTD → LIMITED, CO → COMPANY, PLC → PUBLIC LIMITED COMPANY

### Robust Error Handling  
- Database connection retries with exponential backoff
- Batch insert retries for transient failures
- Graceful handling of corrupt data records
- Comprehensive error logging

### Performance Optimizations
- Memory-efficient Companies House data loading
- Optimized batch sizes (5,000 records default)
- Periodic garbage collection
- Progress checkpointing every 50,000 records

### Resume Capability
- Automatic checkpoint saving in `matching_state.json`
- Resume from last processed record ID
- Statistics preservation across restarts
- Graceful shutdown handling (Ctrl+C)

## Command Line Options

```
--mode {full,no_match_only,missing_only,date_range}
                      Processing mode (default: full)

--batch-size BATCH_SIZE
                      Batch size for processing (default: 5000)

--checkpoint-interval CHECKPOINT_INTERVAL
                      Save checkpoint every N records (default: 50000)

--date-from DATE_FROM
                      Start date for date_range mode (YYYY-MM-DD)

--date-to DATE_TO     End date for date_range mode (YYYY-MM-DD)

--test TEST           Test mode - process only N records

--resume              Resume from checkpoint if available (default)

--no-resume           Start fresh, ignore any checkpoint
```

## Output and Results

### Match Results Table
Results are stored in `land_registry_ch_matches` table with fields:
- `ch_matched_name_1` to `ch_matched_name_4` - Matched company names
- `ch_matched_number_1` to `ch_matched_number_4` - Matched company numbers  
- `ch_match_type_1` to `ch_match_type_4` - Match type (Name+Number, Number, Name, Previous_Name, No_Match)
- `ch_match_confidence_1` to `ch_match_confidence_4` - Confidence scores

### Query Matched Data
Use the convenient view to query results:
```sql
-- Find all properties owned by a specific company
SELECT * FROM v_land_registry_with_ch 
WHERE ch_matched_number_1 = '12345678';

-- Get match quality statistics
SELECT 
    ch_match_type_1,
    COUNT(*) as count,
    AVG(ch_match_confidence_1) as avg_confidence
FROM land_registry_ch_matches 
GROUP BY ch_match_type_1 
ORDER BY count DESC;
```

### Log Files
The script creates detailed log files:
- `lr_ch_matching_production_YYYYMMDD_HHMMSS.log` - Main log
- `lr_ch_matching_errors_YYYYMMDD_HHMMSS.log` - Error-only log
- `matching_state.json` - Checkpoint file (deleted on successful completion)

## Expected Performance

On a typical system:
- **Loading CH Data**: ~5-10 minutes (5.6M companies)
- **Matching Rate**: ~5,000-10,000 records/second  
- **Memory Usage**: ~2-4 GB for CH lookup tables
- **Total Time**: 2-6 hours for full dataset (depending on hardware)

## Monitoring Progress

The script provides:
- Real-time progress bars with tqdm
- Match rate statistics every 100k records
- Memory usage monitoring
- Checkpoint saves every 50k records
- Comprehensive final statistics

## Troubleshooting

### Common Issues

**"Required table not found"**
```bash
# Create the match table
python scripts/run_sql_script.py scripts/create_match_table.sql
```

**"Database connection failed"**  
- Check PostgreSQL is running
- Verify credentials in `config/postgresql_config.py`
- Ensure sufficient connections available

**"Memory issues"**  
- Reduce batch size: `--batch-size 2000`
- Reduce checkpoint interval: `--checkpoint-interval 25000`
- Monitor system memory usage

**"Checkpoint corruption"**  
- Delete `matching_state.json` and restart with `--no-resume`

### Getting Help

View detailed usage:
```bash
python scripts/03_match_lr_to_ch_production.py --help
```

Test normalization functions:
```bash
python scripts/test_production_normalization.py
```

## Migration from Old Scripts

To migrate from previous matching scripts:

1. **Backup existing match data** (if any):
   ```sql
   CREATE TABLE land_registry_ch_matches_backup AS 
   SELECT * FROM land_registry_ch_matches;
   ```

2. **Clear match table**:
   ```sql
   TRUNCATE land_registry_ch_matches;
   ```

3. **Run production script**:
   ```bash
   python scripts/03_match_lr_to_ch_production.py --mode full
   ```

The new script will provide significantly better match rates due to the fixed normalization logic.