# Large-Scale Companies House Scraping Guide

## Overview
This guide explains how to scrape 122,000+ companies from Companies House efficiently and respectfully.

## Key Features
- **Resume Capability**: Automatically saves progress and can resume from where it left off
- **Rate Limiting**: Respects Companies House servers with configurable delays
- **Error Recovery**: Automatic retry logic with exponential backoff
- **Real-time Monitoring**: Live progress tracking and performance metrics
- **Resource Management**: Optimized for long-running operations

## Scripts

### 1. Main Scraper: `02_scrape_companies_house_large_scale.py`
The enhanced scraper designed for 122k+ companies.

**Basic Usage:**
```bash
# Start scraping with default settings
python scripts/02_scrape_companies_house_large_scale.py

# Start fresh (ignore previous checkpoint)
python scripts/02_scrape_companies_house_large_scale.py --no-resume

# Adjust performance settings
python scripts/02_scrape_companies_house_large_scale.py --batch-size 3 --delay-min 2 --delay-max 5

# Show statistics only
python scripts/02_scrape_companies_house_large_scale.py --stats
```

**Recommended Settings for 122k Companies:**
```bash
# Conservative (slower but safer)
python scripts/02_scrape_companies_house_large_scale.py --batch-size 3 --delay-min 2 --delay-max 4

# Balanced (default)
python scripts/02_scrape_companies_house_large_scale.py --batch-size 5 --delay-min 1.5 --delay-max 3.5

# Aggressive (faster but monitor for rate limits)
python scripts/02_scrape_companies_house_large_scale.py --batch-size 10 --delay-min 1 --delay-max 2
```

### 2. Real-time Monitor: `monitor_large_scale_scrape.py`
Shows live progress and performance metrics.

```bash
# Monitor with 5-second refresh (default)
python scripts/monitor_large_scale_scrape.py

# Monitor with custom refresh rate
python scripts/monitor_large_scale_scrape.py --refresh 10
```

**Monitor shows:**
- Overall progress with visual progress bar
- Status breakdown (found, not found, errors)
- Processing rate and time estimates
- Recent activity and error analysis
- Recently processed companies

### 3. Management Tool: `manage_large_scale_scrape.py`
Utilities for managing the scraping process.

```bash
# Get detailed time estimates
python scripts/manage_large_scale_scrape.py time-estimates

# Analyze errors
python scripts/manage_large_scale_scrape.py analyze-errors

# Reset errors for retry
python scripts/manage_large_scale_scrape.py reset-errors
python scripts/manage_large_scale_scrape.py reset-errors --pattern "timeout" --limit 100

# Export results to CSV
python scripts/manage_large_scale_scrape.py export --output found_companies.csv

# Pause/resume scraping
python scripts/manage_large_scale_scrape.py pause
python scripts/manage_large_scale_scrape.py resume
```

## Running the Complete Process

### Step 1: Start the Scraper
In one terminal:
```bash
cd /home/adc/Projects/InsideEstates_App
source venv/bin/activate
python scripts/02_scrape_companies_house_large_scale.py
```

### Step 2: Monitor Progress
In another terminal:
```bash
cd /home/adc/Projects/InsideEstates_App
source venv/bin/activate
python scripts/monitor_large_scale_scrape.py
```

### Step 3: Check Time Estimates
Periodically check detailed estimates:
```bash
python scripts/manage_large_scale_scrape.py time-estimates
```

## Time Estimates

Based on conservative settings (5 req/sec with delays):
- **Processing Rate**: ~2-3 companies/second
- **Total Time**: ~11-17 hours for 122k companies
- **Daily Capacity**: ~170k-260k companies

## Best Practices

### 1. Start Conservative
Begin with conservative settings and increase speed if no issues:
```bash
# Start with
--batch-size 3 --delay-min 2 --delay-max 4

# If running well, increase to
--batch-size 5 --delay-min 1.5 --delay-max 3.5
```

### 2. Run During Off-Peak Hours
Consider running during UK night time (11 PM - 7 AM GMT) for better performance.

### 3. Monitor for Rate Limiting
Watch for 429 errors in the monitor. The scraper handles them automatically but frequent occurrences mean you should slow down.

### 4. Regular Checkpoints
The scraper saves progress every 1,000 companies by default. You can adjust:
```bash
--checkpoint-interval 500  # More frequent saves
--checkpoint-interval 5000  # Less frequent saves
```

### 5. Handle Interruptions
The scraper handles interruptions gracefully:
- **Ctrl+C**: Saves progress and exits cleanly
- **System crash**: Resume from last checkpoint
- **Network issues**: Automatic retry with backoff

## Error Recovery

### Common Errors and Solutions

1. **Request Timeouts**
   ```bash
   # Reset timeout errors
   python scripts/manage_large_scale_scrape.py reset-errors --pattern "timeout"
   ```

2. **Rate Limiting (429 errors)**
   - Scraper handles automatically
   - Consider reducing batch size or increasing delays

3. **Connection Errors**
   ```bash
   # Reset connection errors for retry
   python scripts/manage_large_scale_scrape.py reset-errors --pattern "connection"
   ```

## Performance Optimization

### For Faster Scraping
1. Run multiple instances on different subsets (advanced)
2. Use higher batch sizes during off-peak hours
3. Reduce delays gradually while monitoring errors

### For Stability
1. Use conservative settings
2. Enable verbose logging
3. Set up automated monitoring alerts

## Database Impact

The scraper is optimized for PostgreSQL:
- Uses connection pooling
- Batch updates to reduce load
- Efficient indexing on search columns
- Minimal locking for concurrent access

## Completion

Once scraping is complete:

1. **Export results**:
   ```bash
   python scripts/manage_large_scale_scrape.py export --output all_scraped_companies.csv
   ```

2. **Parse the data**:
   ```bash
   python scripts/03_parse_companies_house_data.py
   ```

3. **Update matches**:
   ```bash
   python scripts/04_match_scraped_companies.py
   ```

## Support

- Logs are saved to `ch_scraper_large_scale.log`
- Checkpoint saved to `scraper_checkpoint.json`
- Database queries are optimized for 122k+ scale
- Process is resumable at any point