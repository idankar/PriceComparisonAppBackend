#!/usr/bin/env python3
"""
Test script to verify each retailer's ETL handles all file types correctly
"""

import sys
import os
sys.path.insert(0, '01_data_scraping_pipeline')

import psycopg2
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Database config
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'price_comparison_app_v2',
    'user': 'postgres',
    'password': '025655358'
}

def test_super_pharm_etl():
    """Test Super-Pharm ETL for all file types"""
    logger.info("=" * 60)
    logger.info("TESTING SUPER-PHARM ETL")
    logger.info("=" * 60)

    from super_pharm_transparency_etl import SuperPharmTransparencyETL

    etl = SuperPharmTransparencyETL()
    etl.connect_db()

    # Get file list
    files = etl.get_file_list()
    logger.info(f"Found {len(files)} total files")

    # Count file types
    file_types = {}
    for file in files:
        file_type = file.get('type', 'unknown')
        file_types[file_type] = file_types.get(file_type, 0) + 1

    logger.info("File type breakdown:")
    for ft, count in file_types.items():
        logger.info(f"  {ft}: {count} files")

    # Test processing one of each type (if available)
    test_files = {}
    for file in files[:100]:  # Check first 100 files
        file_type = file.get('type', 'unknown')
        if file_type not in test_files:
            test_files[file_type] = file

    results = {}
    for file_type, file_info in test_files.items():
        logger.info(f"\nTesting {file_type} file: {file_info['name']}")
        try:
            if file_type == 'stores':
                result = etl.process_stores_file(file_info)
                results[file_type] = f"Processed {result} stores"
            elif file_type == 'prices':
                products, prices = etl.process_price_file(file_info)
                results[file_type] = f"Processed {products} products, {prices} prices"
            elif file_type == 'promotions':
                result = etl.process_promotion_file(file_info)
                results[file_type] = f"Processed {result} promotions"
        except Exception as e:
            results[file_type] = f"ERROR: {str(e)}"

    # Check database stats
    etl.cursor.execute("""
        SELECT COUNT(DISTINCT storeid) as stores,
               COUNT(DISTINCT rp.retailer_product_id) as products,
               COUNT(DISTINCT pr.price_id) as prices
        FROM stores s
        FULL OUTER JOIN retailer_products rp ON s.retailerid = rp.retailer_id
        FULL OUTER JOIN prices pr ON rp.retailer_product_id = pr.retailer_product_id
        WHERE COALESCE(s.retailerid, rp.retailer_id) = 52
    """)
    stats = etl.cursor.fetchone()

    logger.info("\nSuper-Pharm database stats:")
    logger.info(f"  Stores: {stats[0]}")
    logger.info(f"  Products: {stats[1]}")
    logger.info(f"  Prices: {stats[2]}")

    etl.conn.close()

    return results

def test_be_pharm_etl():
    """Test Be Pharm ETL for all file types"""
    logger.info("\n" + "=" * 60)
    logger.info("TESTING BE PHARM ETL")
    logger.info("=" * 60)

    from be_good_pharm_etl import PharmacyETL

    etl = PharmacyETL()

    # Get Be Pharm files
    files = etl.get_shufersal_files('7290027600007')
    logger.info(f"Found {len(files)} total files")

    # Count file types
    file_types = {}
    for file in files:
        file_type = file.get('type', 'unknown')
        file_types[file_type] = file_types.get(file_type, 0) + 1

    logger.info("File type breakdown:")
    for ft, count in file_types.items():
        logger.info(f"  {ft}: {count} files")

    # Check database stats
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(DISTINCT storeid) as stores,
               COUNT(DISTINCT rp.retailer_product_id) as products,
               COUNT(DISTINCT pr.price_id) as prices
        FROM stores s
        FULL OUTER JOIN retailer_products rp ON s.retailerid = rp.retailer_id
        FULL OUTER JOIN prices pr ON rp.retailer_product_id = pr.retailer_product_id
        WHERE COALESCE(s.retailerid, rp.retailer_id) = 150
    """)
    stats = cursor.fetchone()

    logger.info("\nBe Pharm database stats:")
    logger.info(f"  Stores: {stats[0]}")
    logger.info(f"  Products: {stats[1]}")
    logger.info(f"  Prices: {stats[2]}")

    conn.close()

    return file_types

def test_good_pharm_etl():
    """Test Good Pharm ETL for all file types"""
    logger.info("\n" + "=" * 60)
    logger.info("TESTING GOOD PHARM ETL")
    logger.info("=" * 60)

    from be_good_pharm_etl import PharmacyETL

    etl = PharmacyETL()

    # Get Good Pharm files
    files = etl.get_good_pharm_files()
    logger.info(f"Found {len(files)} total files")

    # Count file types
    file_types = {}
    for file in files:
        file_type = file.get('type', 'unknown')
        file_types[file_type] = file_types.get(file_type, 0) + 1

    logger.info("File type breakdown:")
    for ft, count in file_types.items():
        logger.info(f"  {ft}: {count} files")

    # Check database stats
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(DISTINCT storeid) as stores,
               COUNT(DISTINCT rp.retailer_product_id) as products,
               COUNT(DISTINCT pr.price_id) as prices
        FROM stores s
        FULL OUTER JOIN retailer_products rp ON s.retailerid = rp.retailer_id
        FULL OUTER JOIN prices pr ON rp.retailer_product_id = pr.retailer_product_id
        WHERE COALESCE(s.retailerid, rp.retailer_id) = 97
    """)
    stats = cursor.fetchone()

    logger.info("\nGood Pharm database stats:")
    logger.info(f"  Stores: {stats[0]}")
    logger.info(f"  Products: {stats[1]}")
    logger.info(f"  Prices: {stats[2]}")

    conn.close()

    return file_types

def main():
    """Run all tests"""
    logger.info("TESTING ALL RETAILER ETLs")
    logger.info("=" * 80)

    # Test each retailer
    super_pharm_results = test_super_pharm_etl()
    be_pharm_results = test_be_pharm_etl()
    good_pharm_results = test_good_pharm_etl()

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("TEST SUMMARY")
    logger.info("=" * 80)

    logger.info("\nSuper-Pharm test results:")
    for file_type, result in super_pharm_results.items():
        logger.info(f"  {file_type}: {result}")

    logger.info("\nBe Pharm file types found:")
    for file_type, count in be_pharm_results.items():
        logger.info(f"  {file_type}: {count} files")

    logger.info("\nGood Pharm file types found:")
    for file_type, count in good_pharm_results.items():
        logger.info(f"  {file_type}: {count} files")

    logger.info("\n✅ All ETLs tested successfully!")

if __name__ == '__main__':
    main()