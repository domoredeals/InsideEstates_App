#!/usr/bin/env python3
"""
InsideEstates Data Pipeline Orchestrator

This script runs the complete data pipeline:
1. Import Land Registry data (CCOD/OCOD)
2. Import Companies House data
3. Match LR records to CH records

Can run the full pipeline or individual steps.
"""

import os
import sys
import subprocess
import argparse
import logging
from datetime import datetime
from pathlib import Path

# Configure logging
log_filename = f'pipeline_run_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PipelineOrchestrator:
    def __init__(self, venv_path='venv'):
        self.venv_path = venv_path
        self.python_path = os.path.join(venv_path, 'bin', 'python')
        self.scripts_dir = Path(__file__).parent
        
        # Check if virtual environment exists
        if not os.path.exists(self.python_path):
            logger.error(f"Virtual environment not found at {self.python_path}")
            logger.error(f"Please activate your virtual environment or specify correct path")
            sys.exit(1)
            
    def run_script(self, script_name, args=None, description=None):
        """Run a Python script with arguments"""
        script_path = self.scripts_dir / script_name
        
        if not script_path.exists():
            logger.error(f"Script not found: {script_path}")
            return False
            
        cmd = [self.python_path, str(script_path)]
        if args:
            cmd.extend(args)
            
        logger.info("="*70)
        logger.info(f"Running: {description or script_name}")
        logger.info(f"Command: {' '.join(cmd)}")
        logger.info("="*70)
        
        try:
            start_time = datetime.now()
            result = subprocess.run(cmd, check=True)
            elapsed = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"✅ Completed {script_name} in {elapsed/60:.1f} minutes")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Failed to run {script_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected error running {script_name}: {e}")
            return False
            
    def run_land_registry_import(self, ccod_dir=None, ocod_dir=None):
        """Step 1: Import Land Registry data"""
        args = []
        if ccod_dir:
            args.extend(['--ccod-dir', ccod_dir])
        if ocod_dir:
            args.extend(['--ocod-dir', ocod_dir])
            
        return self.run_script(
            '01_import_land_registry_production.py',
            args=args,
            description="Step 1: Import Land Registry CCOD/OCOD data"
        )
        
    def run_companies_house_import(self, ch_dir=None, ch_file=None):
        """Step 2: Import Companies House data"""
        args = []
        if ch_file:
            args.extend(['--file', ch_file])
        elif ch_dir:
            args.extend(['--ch-dir', ch_dir])
            
        return self.run_script(
            '02_import_companies_house_production.py',
            args=args,
            description="Step 2: Import Companies House Basic Company Data"
        )
        
    def run_matching(self, mode='full', test_limit=None):
        """Step 3: Run matching algorithm"""
        args = ['--mode', mode]
        if test_limit:
            args.extend(['--test', str(test_limit)])
            
        return self.run_script(
            '03_match_lr_to_ch_production.py',
            args=args,
            description=f"Step 3: Match LR to CH records (mode: {mode})"
        )
        
    def check_database_status(self):
        """Check current database status"""
        logger.info("\n" + "="*70)
        logger.info("DATABASE STATUS CHECK")
        logger.info("="*70)
        
        try:
            # Add parent directory to path for imports
            sys.path.append(str(self.scripts_dir.parent))
            import psycopg2
            from config.postgresql_config import POSTGRESQL_CONFIG
            
            conn = psycopg2.connect(**POSTGRESQL_CONFIG)
            cursor = conn.cursor()
            
            # Check Land Registry data
            cursor.execute("SELECT COUNT(*), COUNT(DISTINCT title_number), COUNT(DISTINCT file_month) FROM land_registry_data")
            lr_total, lr_titles, lr_months = cursor.fetchone()
            logger.info(f"Land Registry: {lr_total:,} records, {lr_titles:,} unique titles, {lr_months} months")
            
            # Check Companies House data
            cursor.execute("SELECT COUNT(*), COUNT(CASE WHEN company_status = 'Active' THEN 1 END) FROM companies_house_data")
            ch_total, ch_active = cursor.fetchone()
            logger.info(f"Companies House: {ch_total:,} companies ({ch_active:,} active)")
            
            # Check matches
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN ch_match_type_1 != 'No_Match' OR ch_match_type_2 != 'No_Match' 
                               OR ch_match_type_3 != 'No_Match' OR ch_match_type_4 != 'No_Match' THEN 1 END) as matched
                FROM land_registry_ch_matches
            """)
            match_result = cursor.fetchone()
            if match_result and match_result[0] > 0:
                match_total, matched = match_result
                match_rate = (matched / match_total * 100) if match_total > 0 else 0
                logger.info(f"Matches: {match_total:,} records processed, {matched:,} matched ({match_rate:.1f}%)")
            else:
                logger.info("Matches: No matches found (table may be empty)")
                
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error checking database status: {e}")
            
    def run_pipeline(self, steps=None, **kwargs):
        """Run the complete pipeline or specific steps"""
        start_time = datetime.now()
        
        logger.info("\n" + "#"*70)
        logger.info("# INSIDEESTATES DATA PIPELINE")
        logger.info("#"*70)
        
        # Check database status
        self.check_database_status()
        
        # Determine which steps to run
        if steps is None:
            steps = ['lr', 'ch', 'match']  # Run all steps
            
        success = True
        
        # Step 1: Land Registry Import
        if 'lr' in steps:
            success = self.run_land_registry_import(
                ccod_dir=kwargs.get('ccod_dir'),
                ocod_dir=kwargs.get('ocod_dir')
            )
            if not success and not kwargs.get('continue_on_error'):
                logger.error("Land Registry import failed. Stopping pipeline.")
                return False
                
        # Step 2: Companies House Import
        if 'ch' in steps:
            success = self.run_companies_house_import(
                ch_dir=kwargs.get('ch_dir'),
                ch_file=kwargs.get('ch_file')
            )
            if not success and not kwargs.get('continue_on_error'):
                logger.error("Companies House import failed. Stopping pipeline.")
                return False
                
        # Step 3: Matching
        if 'match' in steps:
            success = self.run_matching(
                mode=kwargs.get('match_mode', 'full'),
                test_limit=kwargs.get('test_limit')
            )
            if not success and not kwargs.get('continue_on_error'):
                logger.error("Matching failed. Stopping pipeline.")
                return False
                
        # Final status check
        self.check_database_status()
        
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info("\n" + "#"*70)
        logger.info(f"# PIPELINE COMPLETE - Total time: {elapsed/60:.1f} minutes")
        logger.info("#"*70)
        logger.info(f"Log file: {log_filename}")
        
        return success

def main():
    parser = argparse.ArgumentParser(
        description='InsideEstates Data Pipeline Orchestrator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run complete pipeline
  python run_full_pipeline.py
  
  # Run only Land Registry import
  python run_full_pipeline.py --steps lr
  
  # Run only matching with test limit
  python run_full_pipeline.py --steps match --test-limit 1000
  
  # Run matching to fix No_Match records only
  python run_full_pipeline.py --steps match --match-mode no_match_only
  
  # Run with custom data directories
  python run_full_pipeline.py --ccod-dir /path/to/ccod --ocod-dir /path/to/ocod
        """
    )
    
    # Pipeline control
    parser.add_argument('--steps', nargs='+', 
                       choices=['lr', 'ch', 'match'],
                       help='Which steps to run (default: all)')
    parser.add_argument('--continue-on-error', action='store_true',
                       help='Continue pipeline even if a step fails')
    
    # Land Registry options
    parser.add_argument('--ccod-dir', type=str,
                       default='DATA/SOURCE/LR/CCOD',
                       help='Directory containing CCOD files')
    parser.add_argument('--ocod-dir', type=str,
                       default='DATA/SOURCE/LR/OCOD',
                       help='Directory containing OCOD files')
    
    # Companies House options
    parser.add_argument('--ch-dir', type=str,
                       default='DATA/SOURCE/CH',
                       help='Directory containing CH files')
    parser.add_argument('--ch-file', type=str,
                       help='Specific CH file to import')
    
    # Matching options
    parser.add_argument('--match-mode', type=str,
                       choices=['full', 'no_match_only', 'missing_only', 'date_range'],
                       default='full',
                       help='Matching mode (default: full)')
    parser.add_argument('--test-limit', type=int,
                       help='Limit matching to N records for testing')
    
    # Virtual environment
    parser.add_argument('--venv-path', type=str, default='venv',
                       help='Path to virtual environment')
    
    args = parser.parse_args()
    
    # Create orchestrator
    orchestrator = PipelineOrchestrator(venv_path=args.venv_path)
    
    # Run pipeline
    success = orchestrator.run_pipeline(
        steps=args.steps,
        continue_on_error=args.continue_on_error,
        ccod_dir=args.ccod_dir,
        ocod_dir=args.ocod_dir,
        ch_dir=args.ch_dir,
        ch_file=args.ch_file,
        match_mode=args.match_mode,
        test_limit=args.test_limit
    )
    
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()