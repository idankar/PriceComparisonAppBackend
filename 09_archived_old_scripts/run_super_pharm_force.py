#!/usr/bin/env python3
"""
Force Super-Pharm ETL to reprocess all files by temporarily modifying the timestamp check
"""

import os
import sys
import tempfile
import shutil
import subprocess
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_force_etl():
    """Create a modified version of the script that ignores timestamp checks"""

    # Read the original script
    original_script = "/Users/noa/Desktop/PriceComparisonApp/01_data_scraping_pipeline/super_pharm_transparency_etl_FIXED.py"

    with open(original_script, 'r') as f:
        content = f.read()

    # Modify the timestamp check - comment it out and always process
    # Look for lines that check if file was already processed
    modified_content = content.replace(
        "if file_timestamp and file_timestamp <= last_update:",
        "if False:  # FORCE: Always process files"
    )

    # Also modify the part that checks processed files
    modified_content = modified_content.replace(
        "if filename in self.processed_files:",
        "if False:  # FORCE: Always process files"
    )

    # Create temporary file with modifications
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as tmp:
        tmp.write(modified_content)
        temp_script = tmp.name

    try:
        logger.info("Running Super-Pharm ETL with forced reprocessing...")

        # Run the modified script
        cmd = [sys.executable, temp_script]
        result = subprocess.run(cmd, capture_output=False, text=True)

        if result.returncode == 0:
            logger.info("Super-Pharm ETL completed successfully!")
        else:
            logger.error(f"Super-Pharm ETL failed with code {result.returncode}")

        return result.returncode

    finally:
        # Clean up temp file
        if os.path.exists(temp_script):
            os.unlink(temp_script)

if __name__ == "__main__":
    sys.exit(run_force_etl())