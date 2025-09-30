#!/usr/bin/env python3
"""
Force a fresh run of Super-Pharm ETL to get all available data
"""

import subprocess
import sys
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_fresh_etl():
    """Run Super-Pharm ETL with force flag to reprocess all files"""
    logger.info("Starting FRESH Super-Pharm ETL run")
    logger.info("This will fetch ALL available files from the transparency portal")

    # Run the fixed script
    cmd = [
        sys.executable,
        "/Users/noa/Desktop/PriceComparisonApp/01_data_scraping_pipeline/super_pharm_transparency_etl_FIXED.py",
        "--force"  # Force reprocessing even if files were seen before
    ]

    logger.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=False, text=True)

    if result.returncode == 0:
        logger.info("Super-Pharm ETL completed successfully")
    else:
        logger.error(f"Super-Pharm ETL failed with return code {result.returncode}")

    return result.returncode

if __name__ == "__main__":
    sys.exit(run_fresh_etl())