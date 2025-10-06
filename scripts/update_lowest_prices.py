#!/usr/bin/env python3
"""
Background Price Update Script

This script calculates and updates the lowest_price for all active products
in the canonical_products table by querying the prices table.

Schedule this to run every 15-30 minutes via cron job:
    */15 * * * * /path/to/python /path/to/update_lowest_prices.py
"""

import os
import sys
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_NAME = os.getenv("DB_NAME", "price_comparison_app_v2")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "***REMOVED***")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")


def update_lowest_prices():
    """
    Updates the lowest_price column for all products based on current prices.
    """
    print(f"[{datetime.now().isoformat()}] Starting price update job...")

    try:
        # Connect to database
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()

        # Update lowest_price for all products
        # This query finds the minimum price for each product from the prices table
        update_query = """
            UPDATE canonical_products cp
            SET lowest_price = sub.min_price
            FROM (
                SELECT rp.barcode, MIN(p.price) as min_price
                FROM retailer_products rp
                JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
                JOIN stores s ON p.store_id = s.storeid
                WHERE p.price > 0
                  AND s.isactive = true
                  AND p.price_timestamp = (
                      SELECT MAX(p2.price_timestamp)
                      FROM prices p2
                      WHERE p2.retailer_product_id = p.retailer_product_id
                        AND p2.store_id = p.store_id
                  )
                GROUP BY rp.barcode
            ) AS sub
            WHERE cp.barcode = sub.barcode
              AND cp.is_active = true;
        """

        print(f"[{datetime.now().isoformat()}] Executing price update query...")
        cur.execute(update_query)
        rows_updated = cur.rowcount

        # Commit the transaction
        conn.commit()

        print(f"[{datetime.now().isoformat()}] ✓ Successfully updated {rows_updated} products")

        # Get statistics
        cur.execute("""
            SELECT
                COUNT(*) as total_products,
                COUNT(lowest_price) as products_with_prices,
                MIN(lowest_price) as min_price,
                MAX(lowest_price) as max_price,
                AVG(lowest_price) as avg_price
            FROM canonical_products
            WHERE is_active = true;
        """)

        stats = cur.fetchone()
        if stats:
            print(f"[{datetime.now().isoformat()}] Statistics:")
            print(f"  - Total active products: {stats[0]}")
            print(f"  - Products with prices: {stats[1]}")
            print(f"  - Min price: ₪{stats[2]:.2f}" if stats[2] else "  - Min price: N/A")
            print(f"  - Max price: ₪{stats[3]:.2f}" if stats[3] else "  - Max price: N/A")
            print(f"  - Avg price: ₪{stats[4]:.2f}" if stats[4] else "  - Avg price: N/A")

        cur.close()
        conn.close()

        print(f"[{datetime.now().isoformat()}] Price update job completed successfully")
        return True

    except Exception as e:
        print(f"[{datetime.now().isoformat()}] ✗ Error updating prices: {str(e)}", file=sys.stderr)
        if 'conn' in locals():
            conn.rollback()
        return False


if __name__ == "__main__":
    success = update_lowest_prices()
    sys.exit(0 if success else 1)
