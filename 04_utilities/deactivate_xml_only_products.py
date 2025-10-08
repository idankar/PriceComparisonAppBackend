#!/usr/bin/env python3
"""
Deactivate XML-Only Products

This script marks all products that were created by government XML parsers
(source_retailer_id IS NULL) as inactive. These products should only be
activated when commercial scrapers find and enrich them with images and categories.

Strategy:
1. Mark all NULL-source products as inactive
2. Report statistics on what was deactivated
3. Provide recommendations for ETL script updates
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('deactivate_xml_only_products.log'),
        logging.StreamHandler()
    ]
)

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'price_comparison_app_v2',
    'user': 'postgres',
    'password': '025655358'
}

def analyze_before_deactivation(cursor):
    """Analyze products before deactivation"""
    logging.info("\n" + "="*80)
    logging.info("PRE-DEACTIVATION ANALYSIS")
    logging.info("="*80)

    # Total NULL-source products
    cursor.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN is_active = true THEN 1 END) as active,
            COUNT(CASE WHEN is_active = false THEN 1 END) as inactive
        FROM canonical_products
        WHERE source_retailer_id IS NULL
    """)
    result = cursor.fetchone()
    logging.info(f"NULL-source products: {result['total']} total, {result['active']} active, {result['inactive']} inactive")

    # Breakdown by retailer availability
    cursor.execute("""
        SELECT
            r.retailername,
            COUNT(DISTINCT cp.barcode) as product_count
        FROM canonical_products cp
        JOIN retailer_products rp ON cp.barcode = rp.barcode
        JOIN retailers r ON rp.retailer_id = r.retailerid
        WHERE cp.source_retailer_id IS NULL
          AND cp.is_active = true
          AND rp.retailer_id IN (52, 97, 150)
        GROUP BY r.retailername
        ORDER BY product_count DESC
    """)
    retailers = cursor.fetchall()
    logging.info("\nBreakdown by retailer:")
    for row in retailers:
        logging.info(f"  {row['retailername']}: {row['product_count']} products")

    # Check if any have prices that would be lost
    cursor.execute("""
        SELECT COUNT(DISTINCT cp.barcode) as products_with_prices
        FROM canonical_products cp
        WHERE cp.source_retailer_id IS NULL
          AND cp.is_active = true
          AND cp.lowest_price IS NOT NULL
    """)
    result = cursor.fetchone()
    logging.info(f"\nProducts with cached prices that will be hidden: {result['products_with_prices']}")

    # Sample products to be deactivated
    cursor.execute("""
        SELECT barcode, name, brand, lowest_price
        FROM canonical_products
        WHERE source_retailer_id IS NULL
          AND is_active = true
        ORDER BY RANDOM()
        LIMIT 10
    """)
    samples = cursor.fetchall()
    logging.info("\nSample products to be deactivated:")
    for product in samples:
        logging.info(f"  {product['barcode']}: {product['name']} (Brand: {product['brand']}, Price: {product['lowest_price']})")

def deactivate_xml_only_products(cursor, dry_run=True):
    """Mark all NULL-source products as inactive"""
    logging.info("\n" + "="*80)
    logging.info("DEACTIVATION PROCESS")
    logging.info("="*80)

    if dry_run:
        logging.info("🧪 DRY RUN MODE - No changes will be made")
    else:
        logging.info("⚠️  LIVE MODE - Products will be deactivated")

    # Count products to be deactivated
    cursor.execute("""
        SELECT COUNT(*) as count
        FROM canonical_products
        WHERE source_retailer_id IS NULL
          AND is_active = true
    """)
    count = cursor.fetchone()['count']
    logging.info(f"\nProducts to deactivate: {count}")

    if not dry_run:
        # Perform deactivation
        cursor.execute("""
            UPDATE canonical_products
            SET is_active = false,
                last_scraped_at = NOW()
            WHERE source_retailer_id IS NULL
              AND is_active = true
        """)

        updated_count = cursor.rowcount
        logging.info(f"✅ Deactivated {updated_count} products")
        return updated_count
    else:
        logging.info("ℹ️  Dry run complete - use --execute to perform actual deactivation")
        return 0

def analyze_after_deactivation(cursor):
    """Analyze products after deactivation"""
    logging.info("\n" + "="*80)
    logging.info("POST-DEACTIVATION ANALYSIS")
    logging.info("="*80)

    # Total active products remaining
    cursor.execute("""
        SELECT
            COUNT(*) as total_active,
            COUNT(CASE WHEN source_retailer_id IS NOT NULL THEN 1 END) as with_source,
            COUNT(CASE WHEN source_retailer_id IS NULL THEN 1 END) as without_source,
            COUNT(CASE WHEN image_url IS NOT NULL THEN 1 END) as with_images,
            COUNT(CASE WHEN category IS NOT NULL THEN 1 END) as with_categories
        FROM canonical_products
        WHERE is_active = true
    """)
    result = cursor.fetchone()
    logging.info(f"Active products after cleanup: {result['total_active']}")
    logging.info(f"  - With source retailer: {result['with_source']}")
    logging.info(f"  - Without source (should be 0): {result['without_source']}")
    logging.info(f"  - With images: {result['with_images']} ({100.0 * result['with_images'] / result['total_active']:.1f}%)")
    logging.info(f"  - With categories: {result['with_categories']} ({100.0 * result['with_categories'] / result['total_active']:.1f}%)")

    # Products with prices
    cursor.execute("""
        SELECT COUNT(*) as count
        FROM canonical_products
        WHERE is_active = true
          AND lowest_price IS NOT NULL
    """)
    result = cursor.fetchone()
    logging.info(f"\nActive products with prices (displayable): {result['count']}")

def provide_recommendations():
    """Provide recommendations for ETL and scraper updates"""
    logging.info("\n" + "="*80)
    logging.info("RECOMMENDATIONS FOR CODE UPDATES")
    logging.info("="*80)

    recommendations = """
1. UPDATE GOVERNMENT ETL SCRIPTS:
   - When creating new canonical_products entries, set is_active = FALSE
   - Only set is_active = TRUE if the product already exists
   - This prevents non-pharma items from appearing in the app

   Example code change in ETL scripts:
   ```python
   INSERT INTO canonical_products (barcode, name, brand, is_active)
   VALUES (%s, %s, %s, FALSE)  -- Changed from TRUE to FALSE
   ON CONFLICT (barcode) DO NOTHING
   ```

2. UPDATE COMMERCIAL SCRAPERS:
   - When scraping products, ALWAYS set is_active = TRUE
   - This activates products when commercial data is found
   - Already implemented in Super-Pharm scraper (verify others)

   Example:
   ```python
   INSERT INTO canonical_products (barcode, name, brand, image_url, category, source_retailer_id, is_active)
   VALUES (%s, %s, %s, %s, %s, %s, TRUE)
   ON CONFLICT (barcode) DO UPDATE SET
       image_url = EXCLUDED.image_url,
       category = EXCLUDED.category,
       source_retailer_id = EXCLUDED.source_retailer_id,
       is_active = TRUE  -- Activate when commercial data is found
   ```

3. VERIFY PRICE CALCULATION:
   - Ensure lowest_price calculation only considers active products
   - Check that API endpoints filter by is_active = TRUE (already implemented)

4. CONSIDER FUTURE BACKFILL:
   - Once commercial scrapers are updated, you can run image/category backfill
   - Backfill script should set is_active = TRUE when it successfully finds data
"""

    logging.info(recommendations)

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Deactivate products that were created by XML parsers without commercial data'
    )
    parser.add_argument(
        '--execute',
        action='store_true',
        help='Execute the deactivation (default is dry-run)'
    )

    args = parser.parse_args()

    logging.info("="*80)
    logging.info("XML-ONLY PRODUCTS DEACTIVATION SCRIPT")
    logging.info("="*80)
    logging.info(f"Started at: {datetime.now()}")
    logging.info(f"Mode: {'EXECUTE' if args.execute else 'DRY RUN'}")

    conn = None
    try:
        # Connect to database
        conn = psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)
        cursor = conn.cursor()

        # Analyze before
        analyze_before_deactivation(cursor)

        # Deactivate products
        deactivated_count = deactivate_xml_only_products(cursor, dry_run=not args.execute)

        if args.execute:
            # Commit changes
            conn.commit()
            logging.info("\n✅ Changes committed to database")

            # Analyze after
            analyze_after_deactivation(cursor)

        # Provide recommendations
        provide_recommendations()

        logging.info("\n" + "="*80)
        logging.info("SCRIPT COMPLETE")
        logging.info("="*80)

        if not args.execute:
            logging.info("\n⚠️  This was a DRY RUN - no changes were made")
            logging.info("Run with --execute flag to perform actual deactivation")
        else:
            logging.info(f"\n✅ Successfully deactivated {deactivated_count} XML-only products")

    except Exception as e:
        logging.error(f"❌ Error: {str(e)}", exc_info=True)
        if conn:
            conn.rollback()
            logging.info("Database changes rolled back")
        raise

    finally:
        if conn:
            cursor.close()
            conn.close()
            logging.info("Database connection closed")

if __name__ == "__main__":
    main()
