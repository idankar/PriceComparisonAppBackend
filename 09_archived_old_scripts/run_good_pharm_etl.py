#!/usr/bin/env python3
"""
Run Good Pharm ETL only
"""
import sys
sys.path.insert(0, '01_data_scraping_pipeline')

from be_good_pharm_etl import PharmacyETL
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("STARTING GOOD PHARM FULL ETL")
    logger.info("=" * 80)

    etl = PharmacyETL()

    # Process only Good Pharm
    logger.info("\nProcessing Good Pharm...")
    good_pharm_config = etl.pharmacies['Good Pharm']
    etl.process_pharmacy('Good Pharm', good_pharm_config)

    etl.print_summary()

    logger.info("\n" + "=" * 80)
    logger.info("GOOD PHARM ETL COMPLETE")
    logger.info("=" * 80)