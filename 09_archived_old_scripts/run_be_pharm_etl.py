#!/usr/bin/env python3
"""
Run Be Pharm ETL only
"""
import sys
sys.path.insert(0, '01_data_scraping_pipeline')

from be_good_pharm_etl import PharmacyETL
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("STARTING BE PHARM FULL ETL")
    logger.info("=" * 80)

    etl = PharmacyETL()

    # Process only Be Pharm
    logger.info("\nProcessing Be Pharm (Chain ID: 7290027600007)...")
    be_pharm_config = etl.pharmacies['Be Pharm']
    etl.process_pharmacy('Be Pharm', be_pharm_config)

    etl.print_summary()

    logger.info("\n" + "=" * 80)
    logger.info("BE PHARM ETL COMPLETE")
    logger.info("=" * 80)