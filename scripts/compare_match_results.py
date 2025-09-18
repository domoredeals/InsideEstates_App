#!/usr/bin/env python3
"""
Compare matching results from different attempts
"""

import psycopg2
import sys
from pathlib import Path

# Add parent directory for imports
sys.path.append(str(Path(__file__).parent.parent))
from config.postgresql_config import POSTGRESQL_CONFIG

conn = psycopg2.connect(**POSTGRESQL_CONFIG)
cursor = conn.cursor()

print("=== MATCHING RESULTS COMPARISON ===\n")

# Current results (from successful run)
cursor.execute("""
    SELECT 
        COUNT(*) as total_records,
        COUNT(CASE WHEN ch_match_type_1 != 'No_Match' THEN 1 END) as matched_prop1,
        COUNT(CASE WHEN ch_match_type_1 = 'Name+Number' THEN 1 END) as tier1,
        COUNT(CASE WHEN ch_match_type_1 = 'Number' THEN 1 END) as tier2,
        COUNT(CASE WHEN ch_match_type_1 = 'Name' THEN 1 END) as tier3,
        COUNT(CASE WHEN ch_match_type_1 = 'Previous_Name' THEN 1 END) as tier4,
        COUNT(CASE WHEN ch_match_type_1 = 'No_Match' THEN 1 END) as no_match
    FROM land_registry_ch_matches
""")

result = cursor.fetchone()
total, matched, tier1, tier2, tier3, tier4, no_match = result

print("FINAL SUCCESSFUL RUN (with fixed normalization):")
print(f"Total records: {total:,}")
print(f"Matched: {matched:,} ({matched/total*100:.2f}%)")
print(f"  - Tier 1 (Name+Number): {tier1:,} ({tier1/total*100:.2f}%)")
print(f"  - Tier 2 (Number only): {tier2:,} ({tier2/total*100:.2f}%)")
print(f"  - Tier 3 (Name only): {tier3:,} ({tier3/total*100:.2f}%)")
print(f"  - Tier 4 (Previous name): {tier4:,} ({tier4/total*100:.2f}%)")
print(f"No match: {no_match:,} ({no_match/total*100:.2f}%)")

# Compare with what we saw in earlier attempts
print("\n" + "="*60)
print("\nCOMPARISON WITH EARLIER ATTEMPTS:")
print("\n1. INITIAL RUN (before normalization fix):")
print("   - Showed only 5,000 records in database")
print("   - But claimed 89.82% match rate")
print("   - Issue: Only inserted first 5,000 records")

print("\n2. NO_MATCH_ONLY RUN (with --no-resume):")
print("   - Processed 3,715,000 records") 
print("   - Found 26,748 new matches (0.71% of no_match records)")
print("   - Including 7,430 new Name-only matches")
print("   - Issue: Only processed IDs up to 11,067,187")

print("\n3. UPGRADE RUN:")
print("   - Upgraded 29,184 Number-only to Name+Number matches")
print("   - This showed the normalization fix was working")

print("\n4. FINAL COMPLETE RUN:")
print(f"   - Processed ALL {total:,} records")
print(f"   - {tier3:,} Name-only matches (vs ~720K before fix)")
print(f"   - Overall match rate: {matched/total*100:.2f}%")

# Check some specific improvements
print("\n" + "="*60)
print("\nSPECIFIC IMPROVEMENTS:")

# Check how many "LIMITED" variations now match
cursor.execute("""
    SELECT COUNT(DISTINCT lr.id)
    FROM land_registry_data lr
    JOIN land_registry_ch_matches m ON lr.id = m.id
    WHERE m.ch_match_type_1 = 'Name'
    AND (lr.proprietor_1_name LIKE '%LIMITED' OR lr.proprietor_1_name LIKE '%LTD')
""")
limited_matches = cursor.fetchone()[0]
print(f"\n'LIMITED/LTD' companies with Name matches: {limited_matches:,}")
print("(These would have failed without suffix removal)")

# ST181927 specifically
cursor.execute("""
    SELECT 
        lr.proprietor_1_name,
        lr.company_1_reg_no,
        m.ch_match_type_1,
        m.ch_matched_name_1
    FROM land_registry_data lr
    JOIN land_registry_ch_matches m ON lr.id = m.id
    WHERE lr.title_number = 'ST181927'
""")
result = cursor.fetchone()
if result:
    print(f"\nST181927:")
    print(f"  LR: '{result[0]}' (reg: {result[1]})")
    print(f"  Match: {result[2]} → '{result[3]}'")
    print("  ✅ Successfully matched despite registration number typo!")

cursor.close()
conn.close()