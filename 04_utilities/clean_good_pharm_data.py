#!/usr/bin/env python3
"""
Clean only Good Pharm data while preserving other retailers
"""

import psycopg2
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


def clean_good_pharm_data():
    """Clean only Good Pharm product and price data"""

    # Connect to database
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="price_comparison_app_v2",
        user="postgres",
        password="***REMOVED***"
    )
    cursor = conn.cursor()

    try:
        logger.info("=" * 80)
        logger.info("GOOD PHARM DATA CLEANUP")
        logger.info("=" * 80)

        # Get current counts before cleanup
        cursor.execute("""
            SELECT
                (SELECT COUNT(*) FROM retailer_products WHERE retailer_id = 97) as retailer_products,
                (SELECT COUNT(*) FROM prices p
                 JOIN retailer_products rp ON p.retailer_product_id = rp.retailer_product_id
                 WHERE rp.retailer_id = 97) as prices,
                (SELECT COUNT(*) FROM filesprocessed WHERE retailerid = 97) as files
        """)
        before = cursor.fetchone()
        logger.info(f"BEFORE CLEANUP:")
        logger.info(f"  Good Pharm Products: {before[0]:,}")
        logger.info(f"  Good Pharm Prices: {before[1]:,}")
        logger.info(f"  Good Pharm Files: {before[2]:,}")

        # Start cleanup
        logger.info("\nStarting cleanup...")

        # 1. Delete prices for Good Pharm
        logger.info("  Deleting Good Pharm prices...")
        cursor.execute("""
            DELETE FROM prices
            WHERE retailer_product_id IN (
                SELECT retailer_product_id
                FROM retailer_products
                WHERE retailer_id = 97
            )
        """)
        prices_deleted = cursor.rowcount
        logger.info(f"    Deleted {prices_deleted:,} price records")

        # 2. Delete retailer_products for Good Pharm
        logger.info("  Deleting Good Pharm retailer products...")
        cursor.execute("""
            DELETE FROM retailer_products
            WHERE retailer_id = 97
        """)
        retailer_products_deleted = cursor.rowcount
        logger.info(f"    Deleted {retailer_products_deleted:,} retailer product records")

        # 3. Delete products that no longer have any retailer_products
        logger.info("  Deleting orphaned products...")
        cursor.execute("""
            DELETE FROM products
            WHERE product_id NOT IN (
                SELECT DISTINCT product_id
                FROM retailer_products
            )
        """)
        products_deleted = cursor.rowcount
        logger.info(f"    Deleted {products_deleted:,} orphaned products")

        # 4. Reset filesprocessed for Good Pharm
        logger.info("  Resetting Good Pharm files processed records...")
        cursor.execute("""
            DELETE FROM filesprocessed
            WHERE retailerid = 97
        """)
        files_deleted = cursor.rowcount
        logger.info(f"    Deleted {files_deleted:,} file processing records")

        # 5. Delete the fake store with chain ID
        logger.info("  Removing fake store entry...")
        cursor.execute("""
            DELETE FROM stores
            WHERE retailerid = 97 AND retailerspecificstoreid = '7290058197699'
        """)
        fake_stores_deleted = cursor.rowcount
        logger.info(f"    Deleted {fake_stores_deleted} fake store entries")

        # Get final counts
        cursor.execute("""
            SELECT
                (SELECT COUNT(*) FROM products) as products,
                (SELECT COUNT(*) FROM retailer_products WHERE retailer_id = 97) as good_pharm_products,
                (SELECT COUNT(*) FROM stores WHERE retailerid = 97) as good_pharm_stores
        """)
        after = cursor.fetchone()

        logger.info("\n" + "=" * 80)
        logger.info("CLEANUP COMPLETE")
        logger.info("=" * 80)
        logger.info(f"AFTER CLEANUP:")
        logger.info(f"  Total products remaining: {after[0]:,}")
        logger.info(f"  Good Pharm products: {after[1]:,}")
        logger.info(f"  Good Pharm stores preserved: {after[2]:,}")

        logger.info("\nSUMMARY:")
        logger.info(f"  Good Pharm products deleted: {retailer_products_deleted:,}")
        logger.info(f"  Good Pharm prices deleted: {prices_deleted:,}")
        logger.info(f"  Orphaned products deleted: {products_deleted:,}")
        logger.info(f"  Files reset: {files_deleted:,}")

        # Commit changes
        conn.commit()
        logger.info("\n✓ All changes committed successfully!")
        logger.info("\nGood Pharm data cleaned. Ready for re-run with fixed store ID extraction.")

    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    clean_good_pharm_data()