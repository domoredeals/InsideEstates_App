#!/usr/bin/env python3
"""
Add Companies House matching columns to land_registry_data table
Runs commands individually to avoid timeouts
"""

import sys
import os
import psycopg2
import time

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

def run_commands():
    """Execute SQL commands one at a time"""
    commands = [
        # Add columns for proprietor 1
        "ALTER TABLE land_registry_data ADD COLUMN IF NOT EXISTS ch_matched_name_1 TEXT",
        "ALTER TABLE land_registry_data ADD COLUMN IF NOT EXISTS ch_matched_number_1 VARCHAR(20)",
        "ALTER TABLE land_registry_data ADD COLUMN IF NOT EXISTS ch_match_type_1 VARCHAR(20)",
        "ALTER TABLE land_registry_data ADD COLUMN IF NOT EXISTS ch_match_confidence_1 DECIMAL(3,2)",
        
        # Add columns for proprietor 2
        "ALTER TABLE land_registry_data ADD COLUMN IF NOT EXISTS ch_matched_name_2 TEXT",
        "ALTER TABLE land_registry_data ADD COLUMN IF NOT EXISTS ch_matched_number_2 VARCHAR(20)",
        "ALTER TABLE land_registry_data ADD COLUMN IF NOT EXISTS ch_match_type_2 VARCHAR(20)",
        "ALTER TABLE land_registry_data ADD COLUMN IF NOT EXISTS ch_match_confidence_2 DECIMAL(3,2)",
        
        # Add columns for proprietor 3
        "ALTER TABLE land_registry_data ADD COLUMN IF NOT EXISTS ch_matched_name_3 TEXT",
        "ALTER TABLE land_registry_data ADD COLUMN IF NOT EXISTS ch_matched_number_3 VARCHAR(20)",
        "ALTER TABLE land_registry_data ADD COLUMN IF NOT EXISTS ch_match_type_3 VARCHAR(20)",
        "ALTER TABLE land_registry_data ADD COLUMN IF NOT EXISTS ch_match_confidence_3 DECIMAL(3,2)",
        
        # Add columns for proprietor 4
        "ALTER TABLE land_registry_data ADD COLUMN IF NOT EXISTS ch_matched_name_4 TEXT",
        "ALTER TABLE land_registry_data ADD COLUMN IF NOT EXISTS ch_matched_number_4 VARCHAR(20)",
        "ALTER TABLE land_registry_data ADD COLUMN IF NOT EXISTS ch_match_type_4 VARCHAR(20)",
        "ALTER TABLE land_registry_data ADD COLUMN IF NOT EXISTS ch_match_confidence_4 DECIMAL(3,2)",
        
        # Add match date column
        "ALTER TABLE land_registry_data ADD COLUMN IF NOT EXISTS ch_match_date TIMESTAMP"
    ]
    
    # Indexes to create
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_lr_ch_matched_number_1 ON land_registry_data(ch_matched_number_1) WHERE ch_matched_number_1 IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS idx_lr_ch_matched_number_2 ON land_registry_data(ch_matched_number_2) WHERE ch_matched_number_2 IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS idx_lr_ch_matched_number_3 ON land_registry_data(ch_matched_number_3) WHERE ch_matched_number_3 IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS idx_lr_ch_matched_number_4 ON land_registry_data(ch_matched_number_4) WHERE ch_matched_number_4 IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS idx_lr_ch_match_types ON land_registry_data(ch_match_type_1, ch_match_type_2, ch_match_type_3, ch_match_type_4)"
    ]
    
    try:
        # Connect to database
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor()
        print("Connected to database successfully")
        
        # Run ALTER TABLE commands
        print("\nAdding columns...")
        for i, cmd in enumerate(commands, 1):
            print(f"  [{i}/{len(commands)}] {cmd[:60]}...")
            cursor.execute(cmd)
            conn.commit()
            print(f"  ✓ Done")
        
        # Create indexes (these might take longer)
        print("\nCreating indexes (this may take a few minutes)...")
        for i, cmd in enumerate(indexes, 1):
            print(f"  [{i}/{len(indexes)}] Creating index {i}...")
            start_time = time.time()
            cursor.execute(cmd)
            conn.commit()
            elapsed = time.time() - start_time
            print(f"  ✓ Done in {elapsed:.1f}s")
        
        # Add summary view
        print("\nCreating summary view...")
        cursor.execute("""
            CREATE OR REPLACE VIEW v_ch_match_summary AS
            SELECT 
                COUNT(*) as total_properties,
                COUNT(DISTINCT CASE WHEN ch_match_type_1 != 'No_Match' OR ch_match_type_2 != 'No_Match' 
                                        OR ch_match_type_3 != 'No_Match' OR ch_match_type_4 != 'No_Match' 
                                   THEN title_number END) as properties_with_matches,
                COUNT(CASE WHEN ch_match_type_1 = 'Name+Number' THEN 1 END) as prop1_name_number_matches,
                COUNT(CASE WHEN ch_match_type_1 = 'Number' THEN 1 END) as prop1_number_matches,
                COUNT(CASE WHEN ch_match_type_1 = 'Name' THEN 1 END) as prop1_name_matches,
                COUNT(CASE WHEN ch_match_type_1 = 'Previous_Name' THEN 1 END) as prop1_previous_name_matches,
                COUNT(CASE WHEN ch_match_type_1 = 'No_Match' THEN 1 END) as prop1_no_matches
            FROM land_registry_data
            WHERE ch_match_date IS NOT NULL
        """)
        conn.commit()
        print("  ✓ Summary view created")
        
        # Verify columns were added
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'land_registry_data' 
            AND column_name LIKE 'ch_%'
            ORDER BY column_name
        """)
        columns = cursor.fetchall()
        
        print(f"\n✅ Setup complete! Added {len(columns)} CH columns:")
        for col in columns:
            print(f"  - {col[0]}")
        
        # Close connection
        cursor.close()
        conn.close()
        
        print("\nNext step: Run the matching script")
        print("  python scripts/match_lr_to_ch.py --test 1000")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    run_commands()