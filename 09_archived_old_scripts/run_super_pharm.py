#!/usr/bin/env python3
"""
Run Super-Pharm ETL Pipeline - Schema Compliant Version
"""
import sys
import subprocess
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("STARTING SUPER-PHARM ETL PIPELINE")
    logger.info("=" * 80)

    script_path = "/Users/noa/Desktop/PriceComparisonApp/01_data_scraping_pipeline/super_pharm_etl_schema_compliant.py"
    python_path = "/Users/noa/Desktop/PriceComparisonApp/fresh_env/bin/python"

    try:
        result = subprocess.run([python_path, script_path],
                              capture_output=False,
                              text=True,
                              check=True)
        logger.info("Super-Pharm ETL completed successfully")
    except subprocess.CalledProcessError as e:
        logger.error(f"Super-Pharm ETL failed with exit code {e.returncode}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error running Super-Pharm ETL: {e}")
        sys.exit(1)