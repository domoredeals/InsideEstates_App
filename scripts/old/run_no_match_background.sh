#!/bin/bash

echo "Starting No_Match reprocessing in background..."
echo "This will process all ~3.7M No_Match records with the fixed normalization."
echo

# Run in background with nohup
nohup python scripts/03_match_lr_to_ch_production.py --mode no_match_only --no-resume > no_match_reprocessing.log 2>&1 &

# Get the process ID
PID=$!

echo "Process started with PID: $PID"
echo "Monitor progress with: tail -f no_match_reprocessing.log"
echo "Check if still running with: ps -p $PID"
echo
echo "The script will:"
echo "1. Load 5.6M Companies House records into memory (~2-3 minutes)"
echo "2. Process all ~3.7M No_Match records"
echo "3. Apply the fixed normalization (removes suffixes properly)"
echo "4. ST181927 should get a Name match since 'S NOTARO LIMITED' → 'SNOTARO'"