#!/usr/bin/env python3
"""
Build ownership_history table from land_registry_data
Adapted from SQLite version to work with PostgreSQL and FULL+COU data model
"""

import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
import os
from datetime import datetime
from dotenv import load_dotenv
import logging
from tqdm import tqdm
import time

# Setup logging
log_filename = f'build_ownership_history_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class OwnershipHistoryBuilder:
    def __init__(self, chunk_size=10000):
        self.chunk_size = chunk_size
        self.conn = None
        self.cursor = None
        self.latest_snapshot = None
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'insideestates_app'),
            'user': os.getenv('DB_USER', 'insideestates_user'),
            'password': os.getenv('DB_PASSWORD', 'InsideEstates2024!')
        }
        
    def connect(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            logger.info("Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
            
    def disconnect(self):
        """Disconnect from database"""
        if self.conn:
            self.conn.close()
            
    def setup_optimizations(self):
        """Apply PostgreSQL optimizations"""
        logger.info("🔧 Applying PostgreSQL optimizations...")
        with self.conn.cursor() as cur:
            cur.execute("SET work_mem = '256MB'")
            cur.execute("SET maintenance_work_mem = '512MB'")
            self.conn.commit()
        
    def get_latest_snapshot(self):
        """Get latest snapshot date"""
        with self.conn.cursor() as cur:
            cur.execute("SELECT MAX(file_month) as latest FROM land_registry_data")
            self.latest_snapshot = cur.fetchone()[0]
            logger.info(f"📊 Latest snapshot: {self.latest_snapshot}")
            return self.latest_snapshot
    
    def create_temp_latest_titles_table(self):
        """Create temporary table of titles in latest snapshot"""
        logger.info("📋 Creating latest titles lookup table...")
        
        with self.conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS temp_latest_titles")
            cur.execute(f"""
            CREATE TEMPORARY TABLE temp_latest_titles AS
            SELECT DISTINCT title_number
            FROM land_registry_data
            WHERE file_month = %s
            """, (self.latest_snapshot,))
            
            # Index for fast lookups
            cur.execute("CREATE INDEX idx_temp_latest ON temp_latest_titles(title_number)")
            self.conn.commit()
            
            cur.execute("SELECT COUNT(*) FROM temp_latest_titles")
            count = cur.fetchone()[0]
            logger.info(f"📋 {count:,} titles in latest snapshot")
    
    def create_ownership_history_table(self):
        """Create ownership_history table"""
        logger.info("⚙️ Creating ownership_history table...")
        
        with open('create_ownership_history_table.sql', 'r') as f:
            sql = f.read()
        
        with self.conn.cursor() as cur:
            cur.execute(sql)
            self.conn.commit()
        
        logger.info("✅ Table and indexes created successfully")
    
    def get_all_titles_chunked(self):
        """Get all unique titles in chunks"""
        logger.info("📋 Getting all unique titles...")
        
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(DISTINCT title_number) FROM land_registry_data")
            total_titles = cur.fetchone()[0]
            
        logger.info(f"📊 Found {total_titles:,} unique titles")
        
        # Process in chunks
        chunks = []
        offset = 0
        
        with self.conn.cursor('title_cursor') as cur:
            cur.execute("""
                SELECT DISTINCT title_number 
                FROM land_registry_data 
                ORDER BY title_number
            """)
            
            chunk = []
            for row in cur:
                chunk.append(row[0])
                if len(chunk) >= self.chunk_size:
                    chunks.append(chunk)
                    chunk = []
            
            if chunk:  # Add remaining titles
                chunks.append(chunk)
        
        logger.info(f"📋 Split into {len(chunks)} chunks of up to {self.chunk_size:,} titles")
        return chunks, total_titles
    
    def process_title_chunk(self, title_chunk, chunk_num, total_chunks):
        """Process a single chunk of titles"""
        chunk_start = time.time()
        
        # Use COPY for better performance
        with self.conn.cursor() as cur:
            # Create temp table for this chunk
            cur.execute("CREATE TEMP TABLE chunk_titles (title_number TEXT)")
            
            # Insert chunk titles
            execute_values(cur, 
                "INSERT INTO chunk_titles (title_number) VALUES %s",
                [(t,) for t in title_chunk]
            )
            
            # Main processing query adapted for PostgreSQL
            insert_sql = f"""
            INSERT INTO ownership_history
            WITH raw_data AS (
                SELECT DISTINCT
                    lr.title_number,
                    lr.property_address,
                    -- Handle date conversion and missing dates
                    CASE 
                        WHEN lr.date_proprietor_added IS NOT NULL THEN
                            lr.date_proprietor_added::TEXT
                        ELSE (
                            -- Use first appearance in snapshots
                            SELECT MIN(sub.file_month)::TEXT
                            FROM land_registry_data sub
                            WHERE sub.title_number = lr.title_number
                              AND sub.proprietor_1_name = lr.proprietor_1_name
                              AND COALESCE(sub.proprietor_2_name, '') = COALESCE(lr.proprietor_2_name, '')
                              AND COALESCE(sub.proprietor_3_name, '') = COALESCE(lr.proprietor_3_name, '')
                              AND COALESCE(sub.proprietor_4_name, '') = COALESCE(lr.proprietor_4_name, '')
                        )
                    END AS ownership_start_date,
                    -- Create unique key for deduplication
                    CASE 
                        WHEN lr.date_proprietor_added IS NOT NULL THEN
                            'DATED_' || lr.date_proprietor_added::TEXT
                        ELSE 
                            'FIRST_APPEAR_' || lr.proprietor_1_name || '_' || 
                            COALESCE(lr.proprietor_2_name, '') || '_' || 
                            COALESCE(lr.proprietor_3_name, '') || '_' || 
                            COALESCE(lr.proprietor_4_name, '')
                    END as dedup_key,
                    lr.price_paid::REAL AS price_paid,
                    lr.proprietor_1_name AS owner_1,
                    lr.proprietor_2_name AS owner_2,
                    lr.proprietor_3_name AS owner_3,
                    lr.proprietor_4_name AS owner_4,
                    lr.source_filename AS source_file,
                    lr.file_month,
                    lr.dataset_type,
                    lr.update_type
                FROM land_registry_data lr
                JOIN chunk_titles ct ON lr.title_number = ct.title_number
                WHERE lr.proprietor_1_name IS NOT NULL 
                  AND lr.proprietor_1_name != ''
            ),
            deduplicated AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY title_number, dedup_key ORDER BY file_month DESC) as rn
                FROM raw_data
            ),
            unique_records AS (
                SELECT * FROM deduplicated WHERE rn = 1
            ),
            with_sequences AS (
                SELECT *,
                    LEAD(ownership_start_date) OVER (PARTITION BY title_number ORDER BY ownership_start_date) AS ownership_end_date,
                    LAG(owner_1) OVER (PARTITION BY title_number ORDER BY ownership_start_date) AS seller_1,
                    LAG(owner_2) OVER (PARTITION BY title_number ORDER BY ownership_start_date) AS seller_2,
                    LAG(owner_3) OVER (PARTITION BY title_number ORDER BY ownership_start_date) AS seller_3,
                    LAG(owner_4) OVER (PARTITION BY title_number ORDER BY ownership_start_date) AS seller_4,
                    LEAD(owner_1) OVER (PARTITION BY title_number ORDER BY ownership_start_date) AS buyer_1,
                    LEAD(owner_2) OVER (PARTITION BY title_number ORDER BY ownership_start_date) AS buyer_2,
                    LEAD(owner_3) OVER (PARTITION BY title_number ORDER BY ownership_start_date) AS buyer_3,
                    LEAD(owner_4) OVER (PARTITION BY title_number ORDER BY ownership_start_date) AS buyer_4,
                    LEAD(price_paid) OVER (PARTITION BY title_number ORDER BY ownership_start_date) AS price_at_disposal
                FROM unique_records
            ),
            with_latest_check AS (
                SELECT 
                    ws.*,
                    -- Check if title exists in latest snapshot
                    CASE WHEN lt.title_number IS NOT NULL THEN 1 ELSE 0 END as in_latest_snapshot
                FROM with_sequences ws
                LEFT JOIN temp_latest_titles lt ON ws.title_number = lt.title_number
            )
            SELECT
                title_number,
                property_address,
                ownership_start_date,
                -- Proper end date logic
                CASE 
                    WHEN ownership_end_date IS NOT NULL THEN ownership_end_date
                    WHEN in_latest_snapshot = 0 THEN '{self.latest_snapshot}'::TEXT  -- Inferred disposal
                    ELSE NULL  -- Current ownership
                END AS ownership_end_date,
                owner_1, owner_2, owner_3, owner_4,
                seller_1, seller_2, seller_3, seller_4,
                -- Proper buyer logic
                CASE 
                    WHEN ownership_end_date IS NOT NULL AND buyer_1 IS NULL THEN 'PRIVATE SALE'
                    WHEN ownership_end_date IS NOT NULL AND buyer_1 = owner_1 THEN NULL
                    WHEN in_latest_snapshot = 0 AND ownership_end_date IS NULL THEN 'PRIVATE SALE'  -- Inferred disposal
                    ELSE buyer_1
                END AS buyer_1,
                buyer_2, buyer_3, buyer_4,
                price_paid AS price_at_acquisition,
                CASE 
                    WHEN ownership_end_date IS NOT NULL THEN price_at_disposal 
                    ELSE NULL 
                END AS price_at_disposal,
                -- Ownership status
                CASE 
                    WHEN ownership_end_date IS NOT NULL THEN 'Previous'
                    WHEN in_latest_snapshot = 0 THEN 'Previous'  -- Disappeared = Previous
                    ELSE 'Current'  -- In latest snapshot = Current
                END AS ownership_status,
                -- Duration calculation
                CASE 
                    WHEN ownership_start_date IS NOT NULL THEN
                        DATE_PART('day', 
                            COALESCE(
                                ownership_end_date::DATE,
                                CASE WHEN in_latest_snapshot = 0 THEN '{self.latest_snapshot}'::DATE ELSE CURRENT_DATE END
                            ) - ownership_start_date::DATE
                        )::INTEGER
                    ELSE NULL
                END AS ownership_duration_days,
                source_file AS source,
                CASE
                    WHEN dataset_type = 'OCOD' THEN 'OVERSEAS COMPANY'
                    WHEN dataset_type = 'CCOD' THEN 'UK COMPANY'
                    ELSE 'OTHER'
                END AS ownership_type,
                -- Inferred disposal flag
                CASE 
                    WHEN in_latest_snapshot = 0 AND ownership_end_date IS NULL THEN 1
                    WHEN ownership_end_date IS NOT NULL AND buyer_1 = 'PRIVATE SALE' THEN 1
                    ELSE 0
                END AS inferred_disposal_flag,
                -- Disposal from company flag
                CASE 
                    WHEN in_latest_snapshot = 0 THEN 1
                    ELSE 0
                END AS disposal_from_company
            FROM with_latest_check
            """
            
            cur.execute(insert_sql)
            records_inserted = cur.rowcount
            self.conn.commit()
            
            # Clean up temp table
            cur.execute("DROP TABLE chunk_titles")
            
        chunk_time = time.time() - chunk_start
        
        # Get current total count
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ownership_history")
            total_count = cur.fetchone()[0]
        
        logger.info(f"✅ Chunk {chunk_num}/{total_chunks} complete ({len(title_chunk):,} titles, "
                   f"{records_inserted:,} records) - {chunk_time:.1f}s - Total: {total_count:,}")
    
    def run_processing(self):
        """Main execution method"""
        start_time = time.time()
        logger.info("🚀 Starting ownership history processing...")
        
        self.connect()
        
        try:
            # Setup
            self.setup_optimizations()
            self.create_ownership_history_table()
            self.get_latest_snapshot()
            self.create_temp_latest_titles_table()
            
            # Get title chunks
            title_chunks, total_titles = self.get_all_titles_chunked()
            
            logger.info(f"🔄 Processing {len(title_chunks)} chunks...")
            
            # Process each chunk with progress bar
            for i, chunk in enumerate(tqdm(title_chunks, desc="Processing chunks"), 1):
                self.process_title_chunk(chunk, i, len(title_chunks))
                
                # Progress feedback every 10 chunks
                if i % 10 == 0:
                    logger.info(f"📊 Processed {i}/{len(title_chunks)} chunks ({(i/len(title_chunks)*100):.1f}%)")
            
            # Final validation
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get final counts
                cur.execute("""
                    SELECT 
                        ownership_status,
                        COUNT(*) as count
                    FROM ownership_history
                    GROUP BY ownership_status
                    ORDER BY ownership_status
                """)
                
                status_counts = cur.fetchall()
                
                logger.info("📊 Final ownership distribution:")
                for row in status_counts:
                    logger.info(f"  {row['ownership_status']}: {row['count']:,}")
                
                # Check for problematic cases
                cur.execute("""
                    SELECT COUNT(*) as count
                    FROM ownership_history
                    WHERE ownership_status = 'Previous' 
                      AND ownership_end_date IS NULL 
                      AND inferred_disposal_flag = 0
                """)
                
                missing_end_dates = cur.fetchone()['count']
                logger.info(f"🔍 Validation: {missing_end_dates:,} Previous records missing end dates (should be 0)")
                
        except Exception as e:
            logger.error(f"Error during processing: {e}")
            raise
        finally:
            self.disconnect()
        
        end_time = time.time()
        total_time = end_time - start_time
        
        logger.info("=" * 60)
        logger.info("🎉 PROCESSING COMPLETE")
        logger.info(f"⏱️  Total time: {total_time:.2f} seconds ({total_time/60:.1f} minutes)")
        logger.info("=" * 60)

def main():
    """Main entry point"""
    print("🔧 OWNERSHIP HISTORY BUILDER FOR POSTGRESQL")
    print("=" * 50)
    print("📊 This will create the ownership_history table")
    print("📊 Processing FULL+COU data model")
    print("=" * 50)
    
    chunk_size = input("Enter chunk size (default 10000): ").strip()
    chunk_size = int(chunk_size) if chunk_size.isdigit() else 10000
    
    confirm = input("\nThis will DROP and recreate the ownership_history table. Continue? (yes/no): ")
    if confirm.lower() != 'yes':
        print("Cancelled.")
        return
    
    processor = OwnershipHistoryBuilder(chunk_size=chunk_size)
    processor.run_processing()

if __name__ == "__main__":
    main()