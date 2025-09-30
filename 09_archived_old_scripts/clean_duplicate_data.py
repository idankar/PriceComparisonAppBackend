#!/usr/bin/env python3
"""
Clean duplicate price data from database
"""

import psycopg2
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_duplicates():
    """Remove duplicate prices and clean up data"""
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="price_comparison_app_v2",
        user="postgres",
        password="***REMOVED***"
    )
    cursor = conn.cursor()

    try:
        logger.info("Checking current data...")

        # Check current counts
        cursor.execute("""
            SELECT r.retailername,
                   COUNT(DISTINCT rp.retailer_product_id) as products,
                   COUNT(p.price_id) as prices,
                   COUNT(DISTINCT p.store_id) as stores
            FROM retailers r
            JOIN retailer_products rp ON r.retailerid = rp.retailer_id
            LEFT JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
            WHERE r.retailerid IN (52, 150, 97)
            GROUP BY r.retailername
        """)

        logger.info("Current data:")
        for row in cursor.fetchall():
            logger.info(f"  {row[0]}: {row[1]:,} products, {row[2]:,} prices, {row[3]} stores")

        # Remove duplicate prices for Super-Pharm (keep only latest per product/store)
        logger.info("\nRemoving duplicate Super-Pharm prices...")
        cursor.execute("""
            DELETE FROM prices p1
            WHERE p1.retailer_product_id IN (
                SELECT retailer_product_id
                FROM retailer_products
                WHERE retailer_id = 52
            )
            AND EXISTS (
                SELECT 1
                FROM prices p2
                WHERE p2.retailer_product_id = p1.retailer_product_id
                AND p2.store_id = p1.store_id
                AND p2.price_timestamp > p1.price_timestamp
            )
        """)
        deleted = cursor.rowcount
        logger.info(f"  Deleted {deleted:,} duplicate Super-Pharm prices")

        # Check if Be Pharm has proper store associations
        logger.info("\nChecking Be Pharm store associations...")
        cursor.execute("""
            SELECT COUNT(DISTINCT p.store_id) as stores_with_prices,
                   COUNT(*) as total_prices
            FROM prices p
            JOIN retailer_products rp ON p.retailer_product_id = rp.retailer_product_id
            WHERE rp.retailer_id = 150
        """)
        result = cursor.fetchone()
        logger.info(f"  Be Pharm: {result[1]:,} prices across {result[0]} stores")

        # Final counts
        logger.info("\nFinal counts after cleanup:")
        cursor.execute("""
            SELECT r.retailername,
                   COUNT(DISTINCT rp.retailer_product_id) as products,
                   COUNT(p.price_id) as prices,
                   COUNT(DISTINCT p.store_id) as stores
            FROM retailers r
            JOIN retailer_products rp ON r.retailerid = rp.retailer_id
            LEFT JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
            WHERE r.retailerid IN (52, 150, 97)
            GROUP BY r.retailername
        """)

        for row in cursor.fetchall():
            logger.info(f"  {row[0]}: {row[1]:,} products, {row[2]:,} prices, {row[3]} stores")

        conn.commit()
        logger.info("\nCleanup complete!")

    except Exception as e:
        logger.error(f"Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    clean_duplicates()