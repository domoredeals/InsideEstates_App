#!/bin/bash

echo "=== FRESH FULL MATCHING RUN ==="
echo
echo "This will:"
echo "1. Clear all existing matches"
echo "2. Run the matching script with the FIXED normalization"
echo "3. Process ALL 22M+ Land Registry records from scratch"
echo "4. Apply the correct matching logic that removes suffixes"
echo

read -p "This will take 30-40 minutes. Continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    exit 1
fi

echo "Clearing existing matches..."
psql -U insideestates_user -d insideestates_app -c "TRUNCATE TABLE land_registry_ch_matches;"

echo
echo "Starting fresh full matching run..."
python scripts/03_match_lr_to_ch_production.py --mode full --no-resume

echo
echo "This will ensure ST181927 and ALL other records get properly matched with the fixed normalization."