#!/usr/bin/env python3
"""
Clean up invalid stores that don't exist in the official XML files
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def cleanup_invalid_stores():
    """Remove stores that don't exist in official XML files"""

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="price_comparison_app_v2",
        user="postgres",
        password="025655358"
    )
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        logger.info("Starting invalid stores cleanup...")

        # Good Pharm: Remove stores not in XML (252, 701, and any text-appended store IDs)
        logger.info("\nCleaning Good Pharm stores...")

        # Valid Good Pharm store IDs from XML
        valid_good_pharm = [48, 51, 70, 73, 75, 83, 85, 86, 102, 115, 124, 129, 130, 131, 137, 141,
                            145, 146, 149, 150, 151, 250, 251, 253, 255, 256, 257, 300, 301, 350,
                            351, 352, 400, 480, 481, 482, 501, 502, 550, 601, 605, 650, 702, 750,
                            751, 752, 753, 754, 755, 756, 757, 804, 850, 851, 852, 890, 891, 903,
                            905, 907, 908, 909, 910, 911, 912, 913, 914, 950, 951, 952, 953, 970]

        # Find invalid stores
        cursor.execute("""
            SELECT storeid, retailerspecificstoreid, storename
            FROM stores
            WHERE retailerid = 97
        """)

        all_stores = cursor.fetchall()
        stores_to_remove = []

        for store in all_stores:
            try:
                # Extract numeric store ID
                store_id_str = store['retailerspecificstoreid'].strip()
                if ' ' in store_id_str:
                    # Has text appended - extract the number part
                    store_id = int(store_id_str.split(' ')[0])
                else:
                    store_id = int(store_id_str)

                if store_id not in valid_good_pharm:
                    stores_to_remove.append((store['storeid'], store_id_str, store['storename']))
            except:
                # Can't parse - mark for removal
                stores_to_remove.append((store['storeid'], store['retailerspecificstoreid'], store['storename']))

        if stores_to_remove:
            logger.info(f"Found {len(stores_to_remove)} invalid Good Pharm stores to remove:")
            for store_id, spec_id, name in stores_to_remove:
                logger.info(f"  - Store {spec_id}: {name} (storeid={store_id})")

                # Check for prices
                cursor.execute("SELECT COUNT(*) as count FROM prices WHERE store_id = %s", (store_id,))
                price_count = cursor.fetchone()['count']

                if price_count > 0:
                    logger.info(f"    Removing {price_count} prices")
                    cursor.execute("DELETE FROM prices WHERE store_id = %s", (store_id,))

                # Delete the store
                cursor.execute("DELETE FROM stores WHERE storeid = %s", (store_id,))

        # Also remove stores with text-appended IDs (they're duplicates)
        logger.info("\nRemoving duplicate stores with text-appended IDs...")
        cursor.execute("""
            SELECT storeid, retailerspecificstoreid, storename
            FROM stores
            WHERE retailerid = 97
            AND retailerspecificstoreid LIKE '% %'
        """)
        text_appended = cursor.fetchall()

        if text_appended:
            logger.info(f"Found {len(text_appended)} stores with text-appended IDs to remove:")
            for store in text_appended:
                logger.info(f"  - Removing duplicate: {store['retailerspecificstoreid'][:50]} (storeid={store['storeid']})")

                # Check for prices
                cursor.execute("SELECT COUNT(*) as count FROM prices WHERE store_id = %s", (store['storeid'],))
                price_count = cursor.fetchone()['count']

                if price_count > 0:
                    logger.info(f"    Removing {price_count} prices")
                    cursor.execute("DELETE FROM prices WHERE store_id = %s", (store['storeid'],))

                # Delete the store
                cursor.execute("DELETE FROM stores WHERE storeid = %s", (store['storeid'],))

        conn.commit()

        # Verify final counts
        logger.info("\n" + "="*80)
        logger.info("FINAL VERIFICATION:")

        cursor.execute("""
            SELECT
                r.retailername,
                COUNT(DISTINCT s.storeid) as store_count,
                CASE
                    WHEN r.retailername = 'Super-Pharm' THEN 306
                    WHEN r.retailername = 'Good Pharm' THEN 72
                    WHEN r.retailername = 'Be Pharm' THEN 136
                END as expected_stores
            FROM retailers r
            LEFT JOIN stores s ON r.retailerid = s.retailerid
            WHERE r.retailerid IN (52, 97, 150)
            GROUP BY r.retailerid, r.retailername
            ORDER BY r.retailername
        """)

        results = cursor.fetchall()
        for row in results:
            status = "✅" if row['store_count'] <= row['expected_stores'] else "⚠️"
            logger.info(f"  {status} {row['retailername']}: {row['store_count']} stores (expected: {row['expected_stores']})")

        # Check price distribution
        logger.info("\nPrice distribution:")
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
        logger.info("Invalid stores cleanup completed!")

    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    cleanup_invalid_stores()