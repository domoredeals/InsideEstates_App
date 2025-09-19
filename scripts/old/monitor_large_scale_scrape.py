#!/usr/bin/env python3
"""Monitor large-scale scraping progress in real-time."""

import psycopg2
import os
import time
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
import json

load_dotenv()

def clear_screen():
    """Clear the terminal screen"""
    os.system('clear' if os.name == 'posix' else 'cls')

def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

def format_duration(seconds):
    """Format duration in human readable format"""
    if seconds < 0:
        return "N/A"
    
    days = int(seconds // 86400)
    hours = int((seconds % 86400) // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    
    if days > 0:
        return f"{days}d {hours}h {minutes}m"
    elif hours > 0:
        return f"{hours}h {minutes}m {secs}s"
    elif minutes > 0:
        return f"{minutes}m {secs}s"
    else:
        return f"{secs}s"

def monitor_progress(refresh_interval=5):
    """Monitor scraping progress"""
    
    # Load checkpoint for additional stats
    checkpoint_file = 'scraper_checkpoint.json'
    
    while True:
        try:
            clear_screen()
            
            conn = get_db_connection()
            cur = conn.cursor()
            
            # Get overall statistics
            cur.execute("""
                SELECT search_status, COUNT(*) as count
                FROM ch_scrape_queue
                GROUP BY search_status
                ORDER BY search_status
            """)
            
            status_counts = {}
            total = 0
            for status, count in cur.fetchall():
                status_counts[status] = count
                total += count
            
            # Calculate progress
            processed = total - status_counts.get('pending', 0)
            progress_pct = (processed / total * 100) if total > 0 else 0
            
            # Get recent activity
            cur.execute("""
                SELECT COUNT(*) as recent_count
                FROM ch_scrape_queue
                WHERE search_timestamp > NOW() - INTERVAL '5 minutes'
                AND search_status != 'pending'
            """)
            recent_count = cur.fetchone()[0]
            
            # Get error details
            cur.execute("""
                SELECT search_error, COUNT(*) as count
                FROM ch_scrape_queue
                WHERE search_status = 'error'
                AND search_error IS NOT NULL
                GROUP BY search_error
                ORDER BY count DESC
                LIMIT 5
            """)
            error_details = cur.fetchall()
            
            # Get success rate
            found = status_counts.get('found', 0)
            not_found = status_counts.get('not_found', 0)
            errors = status_counts.get('error', 0)
            success_rate = (found / processed * 100) if processed > 0 else 0
            
            # Load checkpoint if exists
            checkpoint_data = None
            if os.path.exists(checkpoint_file):
                try:
                    with open(checkpoint_file, 'r') as f:
                        checkpoint_data = json.load(f)
                except:
                    pass
            
            # Display header
            print("=" * 80)
            print("COMPANIES HOUSE LARGE-SCALE SCRAPING MONITOR".center(80))
            print("=" * 80)
            print(f"Last Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print()
            
            # Display overall progress
            print("OVERALL PROGRESS")
            print("-" * 80)
            print(f"Total Companies: {total:,}")
            print(f"Processed: {processed:,} ({progress_pct:.2f}%)")
            print(f"Remaining: {status_counts.get('pending', 0):,}")
            print()
            
            # Progress bar
            bar_width = 50
            filled = int(bar_width * progress_pct / 100)
            bar = "█" * filled + "░" * (bar_width - filled)
            print(f"[{bar}] {progress_pct:.1f}%")
            print()
            
            # Display status breakdown
            print("STATUS BREAKDOWN")
            print("-" * 80)
            print(f"Found:     {found:,} ({success_rate:.1f}%)")
            print(f"Not Found: {not_found:,} ({(not_found/processed*100) if processed > 0 else 0:.1f}%)")
            print(f"Errors:    {errors:,} ({(errors/processed*100) if processed > 0 else 0:.1f}%)")
            print(f"Pending:   {status_counts.get('pending', 0):,}")
            print()
            
            # Display performance metrics
            print("PERFORMANCE METRICS")
            print("-" * 80)
            
            if checkpoint_data and 'stats' in checkpoint_data:
                start_time = datetime.fromisoformat(checkpoint_data['stats']['start_time'])
                elapsed = datetime.now() - start_time
                elapsed_seconds = elapsed.total_seconds()
                
                if elapsed_seconds > 0:
                    rate = processed / elapsed_seconds
                    print(f"Processing Rate: {rate:.2f} companies/second")
                    print(f"Recent Activity: {recent_count} in last 5 minutes ({recent_count/5:.1f}/min)")
                    print(f"Elapsed Time: {format_duration(elapsed_seconds)}")
                    
                    # Calculate ETA
                    remaining = status_counts.get('pending', 0)
                    if rate > 0:
                        eta_seconds = remaining / rate
                        print(f"Estimated Time Remaining: {format_duration(eta_seconds)}")
                        eta_datetime = datetime.now() + timedelta(seconds=eta_seconds)
                        print(f"Estimated Completion: {eta_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                print("Performance data not available yet...")
            
            print()
            
            # Display recent errors if any
            if error_details:
                print("TOP ERRORS")
                print("-" * 80)
                for error, count in error_details[:3]:
                    error_msg = error[:60] + "..." if len(error) > 60 else error
                    print(f"{count:4d} - {error_msg}")
                print()
            
            # Display recently processed companies
            cur.execute("""
                SELECT search_name, found_name, company_number, search_status
                FROM ch_scrape_queue
                WHERE search_timestamp IS NOT NULL
                ORDER BY search_timestamp DESC
                LIMIT 5
            """)
            recent_companies = cur.fetchall()
            
            if recent_companies:
                print("RECENTLY PROCESSED")
                print("-" * 80)
                for search_name, found_name, company_number, status in recent_companies:
                    if status == 'found':
                        print(f"✓ {search_name[:30]:30} → {found_name[:30]:30} [{company_number}]")
                    elif status == 'not_found':
                        print(f"✗ {search_name[:30]:30} → NOT FOUND")
                    else:
                        print(f"⚠ {search_name[:30]:30} → ERROR")
            
            print()
            print(f"Refreshing in {refresh_interval} seconds... (Press Ctrl+C to exit)")
            
            cur.close()
            conn.close()
            
            time.sleep(refresh_interval)
            
        except KeyboardInterrupt:
            print("\nMonitoring stopped.")
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(refresh_interval)

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Monitor large-scale scraping progress')
    parser.add_argument('--refresh', type=int, default=5, 
                       help='Refresh interval in seconds (default: 5)')
    
    args = parser.parse_args()
    
    monitor_progress(refresh_interval=args.refresh)

if __name__ == '__main__':
    main()