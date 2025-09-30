#!/usr/bin/env python3
"""
Force a fresh run of Be Pharm ETL to get all available data
"""

import subprocess
import sys
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_fresh_etl():
    """Run Be Pharm ETL with force flag to reprocess all files"""
    logger.info("Starting FRESH Be Pharm ETL run")
    logger.info("This will fetch ALL available files from the Shufersal transparency portal")

    # Run the fixed script with force flag
    cmd = [
        sys.executable,
        "/Users/noa/Desktop/PriceComparisonApp/01_data_scraping_pipeline/be_good_pharm_etl_FIXED.py",
        "--force"  # Force reprocessing even if files were seen before
    ]

    logger.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=False, text=True)

    if result.returncode == 0:
        logger.info("Be Pharm ETL completed successfully")
    else:
        logger.error(f"Be Pharm ETL failed with return code {result.returncode}")

    return result.returncode

if __name__ == "__main__":
    sys.exit(run_fresh_etl())