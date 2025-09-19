#!/bin/bash

echo "=== Processing REMAINING No_Match records ==="
echo
echo "The previous run processed IDs 0-11,067,187 (only 5,000 No_Match records)."
echo "Now processing the remaining 3.7M No_Match records above ID 11,067,187."
echo "This includes ST181927 (ID 15,971,641)."
echo

# Run WITHOUT --no-resume to continue from checkpoint
python scripts/03_match_lr_to_ch_production.py --mode no_match_only

echo
echo "This will continue from ID 11,067,187 and process all remaining No_Match records."