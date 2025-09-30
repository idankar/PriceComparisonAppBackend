#!/usr/bin/env python3
"""
Fix duplicate stores caused by different ID formats (with/without leading zeros)
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fix_duplicate_stores():
    """Clean up duplicate stores and consolidate prices"""

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="price_comparison_app_v2",
        user="postgres",
        password="***REMOVED***"
    )
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # Start transaction
        logger.info("Starting duplicate stores cleanup...")

        # 1. Find all duplicate stores for each retailer
        retailers = [
            (52, 'Super-Pharm'),
            (97, 'Good Pharm')
        ]

        for retailer_id, retailer_name in retailers:
            logger.info(f"\nProcessing {retailer_name} (retailer_id={retailer_id})...")

            # Find groups of duplicate stores
            cursor.execute("""
                WITH normalized_stores AS (
                    SELECT
                        storeid,
                        retailerspecificstoreid,
                        LPAD(retailerspecificstoreid::text, 3, '0') as normalized_id
                    FROM stores
                    WHERE retailerid = %s
                )
                SELECT
                    normalized_id,
                    ARRAY_AGG(storeid ORDER BY LENGTH(retailerspecificstoreid), retailerspecificstoreid) as store_ids,
                    ARRAY_AGG(retailerspecificstoreid ORDER BY LENGTH(retailerspecificstoreid), retailerspecificstoreid) as original_ids,
                    COUNT(*) as duplicate_count
                FROM normalized_stores
                GROUP BY normalized_id
                HAVING COUNT(*) > 1
                ORDER BY normalized_id::int
            """, (retailer_id,))

            duplicates = cursor.fetchall()
            logger.info(f"Found {len(duplicates)} groups of duplicate stores")

            for dup in duplicates:
                # Keep the first store (shortest ID format, e.g., "1" instead of "001")
                keep_store_id = dup['store_ids'][0]
                remove_store_ids = dup['store_ids'][1:]

                logger.info(f"  Store {dup['normalized_id']}: Keeping storeid={keep_store_id} ({dup['original_ids'][0]}), removing {len(remove_store_ids)} duplicates")

                # Move all prices from duplicate stores to the keeper store
                for remove_id in remove_store_ids:
                    # First, check if there are any prices for this store
                    cursor.execute("""
                        SELECT COUNT(*) as price_count
                        FROM prices
                        WHERE store_id = %s
                    """, (remove_id,))
                    price_count = cursor.fetchone()['price_count']

                    if price_count > 0:
                        logger.info(f"    Moving {price_count} prices from storeid={remove_id} to storeid={keep_store_id}")

                        # Update prices to point to the keeper store
                        # Handle potential conflicts by using ON CONFLICT
                        cursor.execute("""
                            UPDATE prices
                            SET store_id = %s
                            WHERE store_id = %s
                            AND NOT EXISTS (
                                SELECT 1 FROM prices p2
                                WHERE p2.retailer_product_id = prices.retailer_product_id
                                AND p2.store_id = %s
                                AND p2.price_timestamp = prices.price_timestamp
                            )
                        """, (keep_store_id, remove_id, keep_store_id))

                        # Delete any remaining prices that would cause conflicts
                        cursor.execute("""
                            DELETE FROM prices
                            WHERE store_id = %s
                        """, (remove_id,))

                    # Now delete the duplicate store
                    cursor.execute("""
                        DELETE FROM stores
                        WHERE storeid = %s
                    """, (remove_id,))

            conn.commit()
            logger.info(f"Completed cleanup for {retailer_name}")

        # 2. Verify the results
        logger.info("\n" + "="*80)
        logger.info("VERIFICATION:")

        cursor.execute("""
            SELECT
                r.retailername,
                COUNT(DISTINCT s.storeid) as store_count,
                COUNT(DISTINCT LPAD(s.retailerspecificstoreid::text, 3, '0')) as unique_stores
            FROM retailers r
            LEFT JOIN stores s ON r.retailerid = s.retailerid
            WHERE r.retailerid IN (52, 97, 150)
            GROUP BY r.retailerid, r.retailername
            ORDER BY r.retailername
        """)

        results = cursor.fetchall()
        for row in results:
            logger.info(f"  {row['retailername']}: {row['store_count']} stores (unique: {row['unique_stores']})")

        # 3. Check price distribution
        logger.info("\nPrice distribution check:")
        cursor.execute("""
            SELECT
                r.retailername,
                COUNT(DISTINCT p.store_id) as stores_with_prices,
                COUNT(p.price_id) as total_prices
            FROM retailers r
            LEFT JOIN retailer_products rp ON r.retailerid = rp.retailer_id
            LEFT JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
            WHERE r.retailerid IN (52, 97, 150)
            GROUP BY r.retailerid, r.retailername
            ORDER BY r.retailername
        """)

        results = cursor.fetchall()
        for row in results:
            logger.info(f"  {row['retailername']}: {row['stores_with_prices']} stores with prices, {row['total_prices']:,} total prices")

        logger.info("\n" + "="*80)
        logger.info("Duplicate stores cleanup completed successfully!")

    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    fix_duplicate_stores()