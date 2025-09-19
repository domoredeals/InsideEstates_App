#!/usr/bin/env python3
"""
Merge successfully rescraped results back into the main results file
"""

import csv
import os
from datetime import datetime
from collections import defaultdict

def merge_results():
    print("Companies House Results Merger")
    print("=" * 60)
    
    # File paths
    main_file = "/mnt/c/Users/adcus/OneDrive/Desktop/companies_house_overview_results_20250919_191457.csv"
    rescraped_file = "/home/adc/Projects/InsideEstates_App/DATA/SOURCE/CH/companies_house_errors_rescraped.csv"
    output_file = f"/home/adc/Projects/InsideEstates_App/DATA/SOURCE/CH/companies_house_overview_results_merged_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    # Check files exist
    if not os.path.exists(main_file):
        main_file = "/home/adc/Projects/InsideEstates_App/DATA/SOURCE/CH/companies_house_overview_results_20250919_191457.csv"
    if not os.path.exists(rescraped_file):
        rescraped_file = "/mnt/c/Users/adcus/OneDrive/Desktop/companies_house_errors_rescraped.csv"
    
    if not os.path.exists(main_file):
        print(f"ERROR: Main file not found: {main_file}")
        return
    if not os.path.exists(rescraped_file):
        print(f"ERROR: Rescraped file not found: {rescraped_file}")
        return
    
    print(f"Main file: {main_file}")
    print(f"Rescraped file: {rescraped_file}")
    print(f"Output file: {output_file}")
    print()
    
    # Load rescraped results into a dictionary
    rescraped_data = {}
    rescraped_stats = defaultdict(int)
    
    with open(rescraped_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            search_name = row['Search Name'].strip()
            status = row['Status'].strip()
            rescraped_data[search_name.lower()] = row
            rescraped_stats[status] += 1
    
    print(f"Loaded {len(rescraped_data)} rescraped results:")
    for status, count in rescraped_stats.items():
        print(f"  - {status}: {count}")
    print()
    
    # Process main file and merge
    merged_count = 0
    kept_original = 0
    total_rows = 0
    final_stats = defaultdict(int)
    
    # Read headers from rescraped file (excluding 'Original Error' column)
    with open(rescraped_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        if 'Original Error' in headers:
            headers.remove('Original Error')
    
    with open(main_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=headers)
        writer.writeheader()
        
        for row in reader:
            total_rows += 1
            search_name = row.get('Search Name', '').strip()
            original_status = row.get('Status', '').strip()
            
            # Check if we have a rescraped version
            if search_name.lower() in rescraped_data and original_status == 'ERROR':
                # Use the rescraped data
                rescraped_row = rescraped_data[search_name.lower()]
                # Remove 'Original Error' field if present
                output_row = {k: v for k, v in rescraped_row.items() if k != 'Original Error'}
                writer.writerow(output_row)
                merged_count += 1
                final_stats[rescraped_row['Status']] += 1
            else:
                # Keep original row
                writer.writerow(row)
                kept_original += 1
                final_stats[original_status] += 1
            
            # Progress indicator
            if total_rows % 10000 == 0:
                print(f"Processed {total_rows:,} rows...")
    
    print(f"\nMerge Complete!")
    print(f"{'='*60}")
    print(f"Total rows processed: {total_rows:,}")
    print(f"Rows updated from rescrape: {merged_count:,}")
    print(f"Rows kept original: {kept_original:,}")
    print(f"\nFinal Status Distribution:")
    for status, count in sorted(final_stats.items()):
        percentage = (count / total_rows * 100) if total_rows > 0 else 0
        print(f"  - {status}: {count:,} ({percentage:.1f}%)")
    print(f"\nOutput saved to: {output_file}")
    
    # Create summary report
    summary_file = f"merge_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(summary_file, 'w') as f:
        f.write("Companies House Merge Summary\n")
        f.write("="*60 + "\n")
        f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Main file: {main_file}\n")
        f.write(f"Rescraped file: {rescraped_file}\n")
        f.write(f"Output file: {output_file}\n\n")
        f.write(f"Total rows: {total_rows:,}\n")
        f.write(f"Updated from rescrape: {merged_count:,}\n")
        f.write(f"Kept original: {kept_original:,}\n\n")
        f.write("Final Status Distribution:\n")
        for status, count in sorted(final_stats.items()):
            percentage = (count / total_rows * 100) if total_rows > 0 else 0
            f.write(f"  - {status}: {count:,} ({percentage:.1f}%)\n")
        f.write(f"\nERROR reduction: {16824 - final_stats.get('ERROR', 0):,}\n")
    
    print(f"\nSummary saved to: {summary_file}")

if __name__ == "__main__":
    try:
        merge_results()
    except Exception as e:
        print(f"\nError during merge: {e}")
        import traceback
        traceback.print_exc()
    
    input("\nPress Enter to exit...")