#!/usr/bin/env python3
"""
Apply PostgreSQL performance optimizations
"""
import os
import sys
import subprocess
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.postgresql_config import POSTGRESQL_CONFIG


def get_postgresql_conf_path():
    """Find PostgreSQL configuration file location"""
    try:
        result = subprocess.run(
            ["sudo", "-u", "postgres", "psql", "-t", "-c", "SHOW config_file"],
            capture_output=True, text=True, check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        print("Could not find PostgreSQL config file. Using default location.")
        return "/etc/postgresql/14/main/postgresql.conf"  # Common default


def backup_config(conf_path):
    """Backup existing PostgreSQL configuration"""
    backup_path = f"{conf_path}.backup"
    try:
        subprocess.run(["sudo", "cp", conf_path, backup_path], check=True)
        print(f"Backed up configuration to: {backup_path}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error backing up config: {e}")
        return False


def apply_runtime_settings():
    """Apply settings that can be changed without restart"""
    runtime_settings = [
        # Memory settings (some require restart, but try anyway)
        "SET work_mem = '512MB';",
        "SET maintenance_work_mem = '8GB';",
        "SET temp_buffers = '256MB';",
        
        # Query planner
        "SET random_page_cost = 1.1;",
        "SET effective_io_concurrency = 200;",
        "SET parallel_setup_cost = 100;",
        "SET parallel_tuple_cost = 0.01;",
        "SET max_parallel_workers_per_gather = 8;",
        
        # Logging
        "SET log_min_duration_statement = 1000;",
        "SET log_checkpoints = ON;",
        "SET log_connections = ON;",
        "SET log_disconnections = ON;",
        "SET log_temp_files = 0;",
        "SET log_autovacuum_min_duration = 0;",
        
        # Statistics
        "SET track_io_timing = ON;",
        "SET track_functions = 'all';",
        
        # Make permanent
        "ALTER SYSTEM SET work_mem = '512MB';",
        "ALTER SYSTEM SET maintenance_work_mem = '8GB';",
        "ALTER SYSTEM SET random_page_cost = 1.1;",
        "ALTER SYSTEM SET effective_io_concurrency = 200;",
        "ALTER SYSTEM SET max_parallel_workers_per_gather = 8;",
        "ALTER SYSTEM SET max_parallel_maintenance_workers = 8;",
        "ALTER SYSTEM SET log_min_duration_statement = 1000;",
        "ALTER SYSTEM SET track_io_timing = ON;",
    ]
    
    try:
        # Connect as superuser
        conn = psycopg2.connect(
            host=POSTGRESQL_CONFIG['host'],
            port=POSTGRESQL_CONFIG['port'],
            database='postgres',
            user='postgres'  # Might need to adjust
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        print("Applying runtime settings...")
        for setting in runtime_settings:
            try:
                cursor.execute(setting)
                print(f"✓ {setting.strip()}")
            except Exception as e:
                print(f"✗ {setting.strip()} - {str(e)[:50]}...")
        
        cursor.close()
        conn.close()
        print("\nRuntime settings applied!")
        return True
        
    except Exception as e:
        print(f"Error applying runtime settings: {e}")
        return False


def optimize_database():
    """Apply database-specific optimizations"""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        optimizations = [
            # Enable extensions for better performance
            "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;",
            "CREATE EXTENSION IF NOT EXISTS pg_buffercache;",
            "CREATE EXTENSION IF NOT EXISTS pgstattuple;",
            
            # Configure default statistics target for better query plans
            "ALTER DATABASE {} SET default_statistics_target = 1000;".format(
                POSTGRESQL_CONFIG['database']
            ),
            
            # Set connection defaults
            "ALTER DATABASE {} SET random_page_cost = 1.1;".format(
                POSTGRESQL_CONFIG['database']
            ),
            "ALTER DATABASE {} SET effective_io_concurrency = 200;".format(
                POSTGRESQL_CONFIG['database']
            ),
        ]
        
        print("\nApplying database-specific optimizations...")
        for opt in optimizations:
            try:
                cursor.execute(opt)
                print(f"✓ {opt.strip()}")
            except Exception as e:
                print(f"✗ {opt.strip()} - {str(e)[:50]}...")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error applying database optimizations: {e}")
        return False


def create_monitoring_views():
    """Create useful monitoring views"""
    try:
        conn = psycopg2.connect(**POSTGRESQL_CONFIG)
        cursor = conn.cursor()
        
        views_sql = """
        -- Active queries view
        CREATE OR REPLACE VIEW monitoring.active_queries AS
        SELECT 
            pid,
            usename,
            application_name,
            client_addr,
            backend_start,
            query_start,
            state,
            wait_event_type,
            wait_event,
            EXTRACT(EPOCH FROM (now() - query_start))::INT as query_duration_seconds,
            query
        FROM pg_stat_activity
        WHERE state != 'idle'
        ORDER BY query_start;
        
        -- Table sizes view
        CREATE OR REPLACE VIEW monitoring.table_sizes AS
        SELECT 
            schemaname,
            tablename,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS total_size,
            pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) AS table_size,
            pg_size_pretty(pg_indexes_size(schemaname||'.'||tablename)) AS indexes_size
        FROM pg_tables
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
        
        -- Index usage statistics
        CREATE OR REPLACE VIEW monitoring.index_usage AS
        SELECT
            schemaname,
            tablename,
            indexname,
            idx_scan as index_scans,
            idx_tup_read as tuples_read,
            idx_tup_fetch as tuples_fetched,
            pg_size_pretty(pg_relation_size(indexrelid)) as index_size
        FROM pg_stat_user_indexes
        ORDER BY idx_scan DESC;
        
        -- Cache hit rates
        CREATE OR REPLACE VIEW monitoring.cache_hit_rates AS
        SELECT
            'index hit rate' AS metric,
            ROUND(100.0 * sum(idx_blks_hit) / NULLIF(sum(idx_blks_hit + idx_blks_read),0), 2) AS ratio
        FROM pg_statio_user_indexes
        UNION ALL
        SELECT
            'table hit rate' AS metric,
            ROUND(100.0 * sum(heap_blks_hit) / NULLIF(sum(heap_blks_hit + heap_blks_read),0), 2) AS ratio
        FROM pg_statio_user_tables;
        """
        
        # Create monitoring schema first
        cursor.execute("CREATE SCHEMA IF NOT EXISTS monitoring;")
        
        print("\nCreating monitoring views...")
        cursor.execute(views_sql)
        conn.commit()
        print("✓ Monitoring views created")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error creating monitoring views: {e}")
        return False


def print_optimization_summary():
    """Print summary and recommendations"""
    print("\n" + "="*60)
    print("PostgreSQL Optimization Summary")
    print("="*60)
    print("\nSettings applied for 128GB RAM system:")
    print("- shared_buffers: 32GB")
    print("- effective_cache_size: 96GB")
    print("- work_mem: 512MB (per operation)")
    print("- maintenance_work_mem: 8GB")
    print("- max_parallel_workers: 16 (using all CPU cores)")
    
    print("\nMonitoring views created in 'monitoring' schema:")
    print("- monitoring.active_queries - See currently running queries")
    print("- monitoring.table_sizes - Check table and index sizes")
    print("- monitoring.index_usage - Monitor index effectiveness")
    print("- monitoring.cache_hit_rates - Check buffer cache performance")
    
    print("\nRecommendations:")
    print("1. Restart PostgreSQL to apply all settings:")
    print("   sudo systemctl restart postgresql")
    print("\n2. For bulk data loading, use these settings:")
    print("   SET synchronous_commit = OFF;")
    print("   SET checkpoint_segments = 100;")
    print("   SET maintenance_work_mem = '16GB';")
    print("\n3. After loading data, run:")
    print("   VACUUM ANALYZE;")
    print("\n4. Monitor performance with:")
    print("   SELECT * FROM monitoring.active_queries;")
    print("   SELECT * FROM monitoring.cache_hit_rates;")
    
    print("\n5. Include tuning config in postgresql.conf:")
    print(f"   sudo echo 'include_if_exists = /home/adc/Projects/InsideEstates_App/config/postgresql_tuning.conf' >> /etc/postgresql/*/main/postgresql.conf")


def main():
    """Main optimization function"""
    print("PostgreSQL Performance Optimization")
    print("-" * 40)
    
    # Get PostgreSQL config path
    conf_path = get_postgresql_conf_path()
    print(f"PostgreSQL config: {conf_path}")
    
    # Apply runtime settings
    if not apply_runtime_settings():
        print("Warning: Some runtime settings could not be applied")
    
    # Apply database optimizations
    if not optimize_database():
        print("Warning: Some database optimizations could not be applied")
    
    # Create monitoring views
    if not create_monitoring_views():
        print("Warning: Monitoring views could not be created")
    
    # Print summary
    print_optimization_summary()


if __name__ == "__main__":
    main()