#!/usr/bin/env python3
"""
Clean slate script for pharmacy data
Removes old product and price data while preserving store and retailer information
"""

import psycopg2
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


def clean_pharmacy_data():
    """Clean old pharmacy product and price data"""

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
        logger.info("PHARMACY DATA CLEAN SLATE OPERATION")
        logger.info("=" * 80)

        # Get current counts before cleanup
        cursor.execute("""
            SELECT
                (SELECT COUNT(*) FROM products) as products,
                (SELECT COUNT(*) FROM retailer_products) as retailer_products,
                (SELECT COUNT(*) FROM prices) as prices,
                (SELECT COUNT(*) FROM filesprocessed WHERE retailerid IN (52, 97, 150)) as files
        """)
        before = cursor.fetchone()
        logger.info(f"BEFORE CLEANUP:")
        logger.info(f"  Products: {before[0]:,}")
        logger.info(f"  Retailer Products: {before[1]:,}")
        logger.info(f"  Prices: {before[2]:,}")
        logger.info(f"  Files Processed: {before[3]:,}")

        # Start cleanup
        logger.info("\nStarting cleanup...")

        # 1. Delete prices for pharmacy retailers (Be Pharm=150, Good Pharm=97, Super-Pharm=52)
        logger.info("  Deleting pharmacy prices...")
        cursor.execute("""
            DELETE FROM prices
            WHERE retailer_product_id IN (
                SELECT retailer_product_id
                FROM retailer_products
                WHERE retailer_id IN (52, 97, 150)
            )
        """)
        prices_deleted = cursor.rowcount
        logger.info(f"    Deleted {prices_deleted:,} price records")

        # 2. Delete retailer_products for pharmacy retailers
        logger.info("  Deleting pharmacy retailer products...")
        cursor.execute("""
            DELETE FROM retailer_products
            WHERE retailer_id IN (52, 97, 150)
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

        # 4. Reset filesprocessed for pharmacy retailers
        logger.info("  Resetting files processed records...")
        cursor.execute("""
            DELETE FROM filesprocessed
            WHERE retailerid IN (52, 97, 150)
        """)
        files_deleted = cursor.rowcount
        logger.info(f"    Deleted {files_deleted:,} file processing records")

        # 5. Keep stores but ensure they're active
        logger.info("  Ensuring pharmacy stores are active...")
        cursor.execute("""
            UPDATE stores
            SET isactive = true, updatedat = NOW()
            WHERE retailerid IN (52, 97, 150)
        """)
        stores_updated = cursor.rowcount
        logger.info(f"    Updated {stores_updated} stores to active status")

        # Get final counts
        cursor.execute("""
            SELECT
                (SELECT COUNT(*) FROM products) as products,
                (SELECT COUNT(*) FROM retailer_products) as retailer_products,
                (SELECT COUNT(*) FROM prices) as prices,
                (SELECT COUNT(*) FROM filesprocessed WHERE retailerid IN (52, 97, 150)) as files,
                (SELECT COUNT(*) FROM stores WHERE retailerid IN (52, 97, 150)) as stores
        """)
        after = cursor.fetchone()

        logger.info("\n" + "=" * 80)
        logger.info("CLEANUP COMPLETE")
        logger.info("=" * 80)
        logger.info(f"AFTER CLEANUP:")
        logger.info(f"  Products: {after[0]:,}")
        logger.info(f"  Retailer Products: {after[1]:,}")
        logger.info(f"  Prices: {after[2]:,}")
        logger.info(f"  Files Processed: {after[3]:,}")
        logger.info(f"  Pharmacy Stores (preserved): {after[4]:,}")

        logger.info("\nSUMMARY:")
        logger.info(f"  Products deleted: {products_deleted:,}")
        logger.info(f"  Retailer products deleted: {retailer_products_deleted:,}")
        logger.info(f"  Prices deleted: {prices_deleted:,}")
        logger.info(f"  Files reset: {files_deleted:,}")

        # Commit changes
        conn.commit()
        logger.info("\n✓ All changes committed successfully!")
        logger.info("\nThe database is now ready for fresh ETL runs with barcode-first matching.")

    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    import sys

    print("\n" + "="*80)
    print("WARNING: This will DELETE all pharmacy product and price data!")
    print("Stores and retailer information will be preserved.")
    print("="*80)
    response = input("\nAre you sure you want to proceed? (yes/no): ")

    if response.lower() == 'yes':
        clean_pharmacy_data()
    else:
        print("Cleanup cancelled.")
        sys.exit(0)