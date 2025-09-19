#!/usr/bin/env python3
"""
Bulk data loader with PostgreSQL optimizations
"""
import sys
import os
import psycopg2
from psycopg2.extras import execute_values
from contextlib import contextmanager
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class BulkLoader:
    """Optimized bulk data loader for PostgreSQL"""
    
    def __init__(self, table_name, columns, batch_size=10000):
        self.table_name = table_name
        self.columns = columns
        self.batch_size = batch_size
        self.conn = None
        self.cursor = None
        
    @contextmanager
    def bulk_load_context(self):
        """Context manager for bulk loading with optimizations"""
        try:
            # Connect to database
            self.conn = psycopg2.connect(**POSTGRESQL_CONFIG)
            self.cursor = self.conn.cursor()
            
            logger.info("Applying bulk load optimizations...")
            
            # Apply bulk loading optimizations
            optimizations = [
                "SET synchronous_commit = OFF",
                "SET work_mem = '1GB'",
                "SET maintenance_work_mem = '16GB'",
                "SET max_parallel_maintenance_workers = 16",
                # Disable triggers and foreign key checks
                f"ALTER TABLE {self.table_name} DISABLE TRIGGER ALL",
            ]
            
            for opt in optimizations:
                try:
                    self.cursor.execute(opt)
                except Exception as e:
                    logger.warning(f"Could not apply: {opt} - {e}")
            
            # Start transaction
            self.conn.commit()
            
            yield self
            
            # Re-enable triggers
            self.cursor.execute(f"ALTER TABLE {self.table_name} ENABLE TRIGGER ALL")
            self.conn.commit()
            
            logger.info("Analyzing table for query optimizer...")
            self.cursor.execute(f"ANALYZE {self.table_name}")
            self.conn.commit()
            
        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
    
    def insert_batch(self, data):
        """Insert a batch of data using execute_values (fastest method)"""
        if not data:
            return
            
        query = f"""
            INSERT INTO {self.table_name} ({', '.join(self.columns)})
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        
        try:
            execute_values(
                self.cursor,
                query,
                data,
                template=None,
                page_size=self.batch_size
            )
            self.conn.commit()
            logger.info(f"Inserted batch of {len(data)} records")
        except Exception as e:
            logger.error(f"Error inserting batch: {e}")
            self.conn.rollback()
            raise
    
    def copy_from_csv(self, file_path, delimiter=',', null_string=''):
        """Use COPY for maximum performance with CSV files"""
        try:
            with open(file_path, 'r') as f:
                self.cursor.copy_expert(
                    f"""
                    COPY {self.table_name} ({', '.join(self.columns)})
                    FROM STDIN
                    WITH (FORMAT CSV, DELIMITER '{delimiter}', NULL '{null_string}', HEADER true)
                    """,
                    f
                )
                self.conn.commit()
                logger.info(f"Copied data from {file_path}")
        except Exception as e:
            logger.error(f"Error copying from CSV: {e}")
            self.conn.rollback()
            raise
    
    def create_indexes(self, index_definitions):
        """Create indexes in parallel after bulk load"""
        logger.info("Creating indexes...")
        
        for index_name, index_def in index_definitions.items():
            try:
                # Create index concurrently to avoid locking
                create_sql = f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {index_name} {index_def}"
                self.cursor.execute(create_sql)
                self.conn.commit()
                logger.info(f"Created index: {index_name}")
            except Exception as e:
                logger.error(f"Error creating index {index_name}: {e}")
                self.conn.rollback()


def optimize_table_storage(table_name):
    """Optimize table storage after bulk load"""
    conn = psycopg2.connect(**POSTGRESQL_CONFIG)
    cursor = conn.cursor()
    
    try:
        logger.info(f"Optimizing storage for {table_name}...")
        
        # VACUUM to reclaim space
        cursor.execute(f"VACUUM ANALYZE {table_name}")
        
        # Cluster table on primary key if exists
        cursor.execute(f"""
            SELECT i.indexname 
            FROM pg_indexes i
            JOIN pg_class c ON c.relname = i.indexname
            WHERE i.tablename = %s
            AND c.relisprimary = true
        """, (table_name,))
        
        pk_index = cursor.fetchone()
        if pk_index:
            logger.info(f"Clustering table on {pk_index[0]}...")
            cursor.execute(f"CLUSTER {table_name} USING {pk_index[0]}")
        
        conn.commit()
        logger.info("Table optimization complete")
        
    except Exception as e:
        logger.error(f"Error optimizing table: {e}")
    finally:
        cursor.close()
        conn.close()


# Example usage
if __name__ == "__main__":
    # Example: Load company data
    loader = BulkLoader(
        table_name='companies',
        columns=['company_number', 'company_name', 'incorporation_date', 'status'],
        batch_size=10000
    )
    
    with loader.bulk_load_context():
        # Example batch insert
        sample_data = [
            ('12345678', 'Example Ltd', '2020-01-01', 'Active'),
            ('87654321', 'Test Corp', '2021-01-01', 'Active'),
        ]
        loader.insert_batch(sample_data)
        
        # Example CSV copy (fastest method)
        # loader.copy_from_csv('/path/to/companies.csv')
        
        # Create indexes after load
        indexes = {
            'idx_companies_number': 'ON companies(company_number)',
            'idx_companies_name': 'ON companies USING gin(company_name gin_trgm_ops)',
        }
        loader.create_indexes(indexes)
    
    # Optimize storage
    optimize_table_storage('companies')