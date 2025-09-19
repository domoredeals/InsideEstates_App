#!/usr/bin/env python3
"""
Manage large-scale scraping operations.
Provides tools for monitoring, controlling, and analyzing the scraping process.
"""

import psycopg2
import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
import json

load_dotenv()

def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

def reset_errors(error_pattern=None, limit=None):
    """Reset error status back to pending for retry"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        if error_pattern:
            query = """
                UPDATE ch_scrape_queue
                SET search_status = 'pending',
                    search_error = NULL,
                    search_timestamp = NULL
                WHERE search_status = 'error'
                AND search_error LIKE %s
            """
            params = [f'%{error_pattern}%']
        else:
            query = """
                UPDATE ch_scrape_queue
                SET search_status = 'pending',
                    search_error = NULL,
                    search_timestamp = NULL
                WHERE search_status = 'error'
            """
            params = []
        
        if limit:
            query += f" LIMIT {limit}"
        
        cur.execute(query, params)
        affected = cur.rowcount
        conn.commit()
        
        print(f"Reset {affected} error entries back to pending")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def analyze_errors():
    """Analyze error patterns"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get error summary
        cur.execute("""
            SELECT search_error, COUNT(*) as count
            FROM ch_scrape_queue
            WHERE search_status = 'error'
            AND search_error IS NOT NULL
            GROUP BY search_error
            ORDER BY count DESC
        """)
        
        print("\n=== ERROR ANALYSIS ===")
        print("-" * 80)
        print(f"{'Error Message':60} {'Count':>10}")
        print("-" * 80)
        
        total_errors = 0
        for error, count in cur.fetchall():
            total_errors += count
            error_msg = error[:57] + "..." if len(error) > 60 else error
            print(f"{error_msg:60} {count:10,}")
        
        print("-" * 80)
        print(f"{'TOTAL ERRORS':60} {total_errors:10,}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

def export_results(output_file='scraped_companies.csv'):
    """Export successfully scraped companies to CSV"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        import csv
        
        cur.execute("""
            SELECT search_name, found_name, company_number, company_url, search_timestamp
            FROM ch_scrape_queue
            WHERE search_status = 'found'
            ORDER BY search_timestamp DESC
        """)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Search Name', 'Found Name', 'Company Number', 'Company URL', 'Timestamp'])
            
            count = 0
            for row in cur:
                writer.writerow(row)
                count += 1
        
        print(f"Exported {count:,} companies to {output_file}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

def get_time_estimates():
    """Get detailed time estimates"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Get counts
        cur.execute("""
            SELECT 
                SUM(CASE WHEN search_status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN search_status != 'pending' THEN 1 ELSE 0 END) as processed,
                COUNT(*) as total
            FROM ch_scrape_queue
        """)
        
        pending, processed, total = cur.fetchone()
        
        # Get processing times
        cur.execute("""
            SELECT 
                MIN(search_timestamp) as start_time,
                MAX(search_timestamp) as last_time,
                COUNT(*) as count
            FROM ch_scrape_queue
            WHERE search_timestamp IS NOT NULL
        """)
        
        start_time, last_time, count = cur.fetchone()
        
        if start_time and last_time and count > 0:
            elapsed = (last_time - start_time).total_seconds()
            rate = count / elapsed if elapsed > 0 else 0
            
            print("\n=== TIME ESTIMATES ===")
            print(f"Total Companies: {total:,}")
            print(f"Processed: {processed:,} ({processed/total*100:.1f}%)")
            print(f"Pending: {pending:,}")
            print()
            print(f"Processing Started: {start_time}")
            print(f"Last Activity: {last_time}")
            print(f"Elapsed Time: {timedelta(seconds=int(elapsed))}")
            print(f"Average Rate: {rate:.2f} companies/second")
            print()
            
            if rate > 0 and pending > 0:
                eta_seconds = pending / rate
                eta = datetime.now() + timedelta(seconds=eta_seconds)
                print(f"Estimated Time Remaining: {timedelta(seconds=int(eta_seconds))}")
                print(f"Estimated Completion: {eta.strftime('%Y-%m-%d %H:%M:%S')}")
                
                # Calculate different scenarios
                print("\n=== SCENARIO ANALYSIS ===")
                scenarios = [
                    ("Current rate", rate),
                    ("50% slower", rate * 0.5),
                    ("50% faster", rate * 1.5),
                    ("2x faster", rate * 2),
                    ("Night rate (3x)", rate * 3)
                ]
                
                for name, scenario_rate in scenarios:
                    if scenario_rate > 0:
                        scenario_eta = pending / scenario_rate
                        scenario_date = datetime.now() + timedelta(seconds=scenario_eta)
                        print(f"{name:15} ({scenario_rate:.2f}/sec): {timedelta(seconds=int(scenario_eta))} → {scenario_date.strftime('%Y-%m-%d %H:%M')}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

def pause_scraping():
    """Mark all pending as 'paused' to temporarily stop scraping"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            UPDATE ch_scrape_queue
            SET search_status = 'paused'
            WHERE search_status = 'pending'
        """)
        affected = cur.rowcount
        conn.commit()
        
        print(f"Paused {affected} pending companies")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def resume_scraping():
    """Resume paused scraping"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            UPDATE ch_scrape_queue
            SET search_status = 'pending'
            WHERE search_status = 'paused'
        """)
        affected = cur.rowcount
        conn.commit()
        
        print(f"Resumed {affected} paused companies")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Manage large-scale scraping operations')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Reset errors command
    reset_parser = subparsers.add_parser('reset-errors', help='Reset errors back to pending')
    reset_parser.add_argument('--pattern', help='Only reset errors matching pattern')
    reset_parser.add_argument('--limit', type=int, help='Limit number to reset')
    
    # Analyze errors command
    analyze_parser = subparsers.add_parser('analyze-errors', help='Analyze error patterns')
    
    # Export results command
    export_parser = subparsers.add_parser('export', help='Export results to CSV')
    export_parser.add_argument('--output', default='scraped_companies.csv', help='Output filename')
    
    # Time estimates command
    time_parser = subparsers.add_parser('time-estimates', help='Get detailed time estimates')
    
    # Pause command
    pause_parser = subparsers.add_parser('pause', help='Pause scraping')
    
    # Resume command
    resume_parser = subparsers.add_parser('resume', help='Resume paused scraping')
    
    args = parser.parse_args()
    
    if args.command == 'reset-errors':
        reset_errors(args.pattern, args.limit)
    elif args.command == 'analyze-errors':
        analyze_errors()
    elif args.command == 'export':
        export_results(args.output)
    elif args.command == 'time-estimates':
        get_time_estimates()
    elif args.command == 'pause':
        pause_scraping()
    elif args.command == 'resume':
        resume_scraping()
    else:
        parser.print_help()

if __name__ == '__main__':
    main()