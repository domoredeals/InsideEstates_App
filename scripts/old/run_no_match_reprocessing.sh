#!/bin/bash

echo "=== Reprocessing ALL No_Match Records ==="
echo
echo "The script was resuming from ID 11067187 due to checkpoint/resume feature."
echo "To process ALL No_Match records including ST181927, use --no-resume flag."
echo
echo "Running with --no-resume to start fresh..."
echo

# Run the script without resume to process ALL No_Match records
python 03_match_lr_to_ch_production.py --mode no_match_only --no-resume

echo
echo "This will process all ~3.7M No_Match records from the beginning,"
echo "including ST181927 which should now get a Name match with the fixed normalization."