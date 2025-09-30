#!/usr/bin/env python3
"""
Force Be Pharm ETL to reprocess all files by temporarily modifying the timestamp check
"""

import os
import sys
import tempfile
import subprocess
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_force_etl():
    """Create a modified version of the script that ignores timestamp checks"""

    # Read the original script
    original_script = "/Users/noa/Desktop/PriceComparisonApp/01_data_scraping_pipeline/be_good_pharm_etl_FIXED.py"

    with open(original_script, 'r') as f:
        content = f.read()

    # Modify to always download files regardless of timestamps
    # Replace the date check with a condition that's always true
    modified_content = content.replace(
        "datetime.strptime(file_date, '%Y%m%d').date() >= cutoff_date",
        "True  # FORCE: Always process files"
    )

    # Create temporary file with modifications
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as tmp:
        tmp.write(modified_content)
        temp_script = tmp.name

    try:
        logger.info("Running Be Pharm ETL with forced reprocessing...")

        # Run the modified script
        cmd = [sys.executable, temp_script]
        result = subprocess.run(cmd, capture_output=False, text=True)

        if result.returncode == 0:
            logger.info("Be Pharm ETL completed successfully!")
        else:
            logger.error(f"Be Pharm ETL failed with code {result.returncode}")

        return result.returncode

    finally:
        # Clean up temp file
        if os.path.exists(temp_script):
            os.unlink(temp_script)

if __name__ == "__main__":
    sys.exit(run_force_etl())