#!/usr/bin/env python3
"""
Deactivate Products Without Images

Policy: Only products with commercial scraper data (images) should be active.
Products from XML-only (no images) should be deactivated until commercial scrapers find them.

This ensures the canonical product list only shows products with full data:
- Name, brand, category, description (from XML or scraper)
- Image URL (from commercial scraper ONLY)
- Prices (from price XML updates)

Products without images are "dormant" - they have pricing data in retailer_products
but are not displayed in the app until a commercial scraper enriches them.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from datetime import datetime
import argparse

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('deactivate_products_without_images.log'),
        logging.StreamHandler()
    ]
)

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'price_comparison_app_v2',
    'user': 'postgres',
    'password': '***REMOVED***'
}

def analyze_before_deactivation(cursor):
    """Analyze products before deactivation"""
    logging.info("\n" + "="*80)
    logging.info("PRE-DEACTIVATION ANALYSIS")
    logging.info("="*80)

    # Overall statistics
    cursor.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN is_active = true THEN 1 END) as active,
            COUNT(CASE WHEN is_active = false THEN 1 END) as inactive
        FROM canonical_products
    """)
    result = cursor.fetchone()
    logging.info(f"Total products: {result['total']}")
    logging.info(f"  Active: {result['active']}")
    logging.info(f"  Inactive: {result['inactive']}")

    # Breakdown by retailer and image status
    cursor.execute("""
        SELECT
            r.retailername,
            COUNT(*) as total_active,
            COUNT(cp.image_url) as with_images,
            COUNT(*) - COUNT(cp.image_url) as without_images
        FROM canonical_products cp
        LEFT JOIN retailers r ON cp.source_retailer_id = r.retailerid
        WHERE cp.is_active = true
        GROUP BY r.retailername
        ORDER BY without_images DESC
    """)
    retailers = cursor.fetchall()
    logging.info("\nActive products by retailer:")
    for row in retailers:
        retailer = row['retailername'] or 'NULL source'
        logging.info(f"  {retailer}:")
        logging.info(f"    Total: {row['total_active']}")
        logging.info(f"    With images: {row['with_images']}")
        logging.info(f"    Without images: {row['without_images']}")

    # Products to be deactivated
    cursor.execute("""
        SELECT COUNT(*) as count
        FROM canonical_products
        WHERE is_active = true
          AND image_url IS NULL
    """)
    result = cursor.fetchone()
    logging.info(f"\n⚠️  Products to be deactivated (active but no images): {result['count']}")

    # Check if these products have prices
    cursor.execute("""
        SELECT COUNT(DISTINCT cp.barcode) as products_with_prices
        FROM canonical_products cp
        JOIN retailer_products rp ON cp.barcode = rp.barcode
        WHERE cp.is_active = true
          AND cp.image_url IS NULL
    """)
    result = cursor.fetchone()
    logging.info(f"    (of which {result['products_with_prices']} have pricing data in retailer_products)")

    # Sample products to be deactivated
    cursor.execute("""
        SELECT cp.barcode, cp.name, cp.brand, r.retailername as source
        FROM canonical_products cp
        LEFT JOIN retailers r ON cp.source_retailer_id = r.retailerid
        WHERE cp.is_active = true
          AND cp.image_url IS NULL
        ORDER BY RANDOM()
        LIMIT 10
    """)
    samples = cursor.fetchall()
    logging.info("\nSample products to be deactivated:")
    for product in samples:
        source = product['source'] or 'NULL'
        logging.info(f"  {product['barcode']}: {product['name'][:60]} (Source: {source})")

def deactivate_products_without_images(cursor, dry_run=True):
    """Deactivate all products without images"""
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
        WHERE is_active = true
          AND image_url IS NULL
    """)
    count = cursor.fetchone()['count']
    logging.info(f"\nProducts to deactivate: {count}")

    if not dry_run:
        # Perform deactivation
        cursor.execute("""
            UPDATE canonical_products
            SET is_active = false,
                last_scraped_at = NOW()
            WHERE image_url IS NULL
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

    # Overall active products
    cursor.execute("""
        SELECT
            COUNT(*) as total_active,
            COUNT(image_url) as with_images,
            COUNT(*) - COUNT(image_url) as without_images,
            COUNT(category) as with_categories
        FROM canonical_products
        WHERE is_active = true
    """)
    result = cursor.fetchone()
    logging.info(f"Active products after deactivation: {result['total_active']}")
    logging.info(f"  - With images: {result['with_images']} ({100.0 * result['with_images'] / result['total_active']:.1f}%)")
    logging.info(f"  - Without images (should be 0): {result['without_images']}")
    logging.info(f"  - With categories: {result['with_categories']} ({100.0 * result['with_categories'] / result['total_active']:.1f}%)")

    # By retailer
    cursor.execute("""
        SELECT
            r.retailername,
            COUNT(*) as total_active,
            COUNT(cp.image_url) as with_images
        FROM canonical_products cp
        LEFT JOIN retailers r ON cp.source_retailer_id = r.retailerid
        WHERE cp.is_active = true
        GROUP BY r.retailername
        ORDER BY total_active DESC
    """)
    retailers = cursor.fetchall()
    logging.info("\nActive products by retailer:")
    for row in retailers:
        retailer = row['retailername'] or 'NULL source'
        pct = 100.0 * row['with_images'] / row['total_active'] if row['total_active'] > 0 else 0
        logging.info(f"  {retailer}: {row['total_active']} products ({row['with_images']} with images, {pct:.1f}%)")

    # Inactive products stats
    cursor.execute("""
        SELECT COUNT(*) as count
        FROM canonical_products
        WHERE is_active = false
    """)
    result = cursor.fetchone()
    logging.info(f"\nTotal inactive products (dormant, awaiting commercial scraper): {result['count']}")

def provide_policy_reminder():
    """Remind about the canonical product policy"""
    logging.info("\n" + "="*80)
    logging.info("CANONICAL PRODUCT POLICY REMINDER")
    logging.info("="*80)

    policy = """
CANONICAL PRODUCT ACTIVATION POLICY:
====================================

A product in canonical_products should ONLY be active (is_active = TRUE) if:
✅ It has an image_url (from commercial scraper)
✅ It has a source_retailer_id (indicating which pharmacy sells it)
✅ It has basic data (name, brand, category)

Products WITHOUT images are kept INACTIVE until:
1. A commercial scraper visits the product page
2. The scraper extracts the image URL
3. The scraper updates canonical_products with is_active = TRUE

WHY THIS POLICY:
- XML files contain pricing data but NO images or detailed info
- Users expect to see product images in the app
- Showing products without images degrades user experience
- Dormant products still have pricing data in retailer_products table

COMMERCIAL SCRAPER RESPONSIBILITIES:
1. Super-Pharm scraper: ALWAYS set is_active = TRUE when updating
2. Be Pharm scraper: ALWAYS set is_active = TRUE when updating
3. Good Pharm scraper: ALWAYS set is_active = TRUE when updating

XML ETL RESPONSIBILITIES:
1. Create new products with is_active = FALSE (dormant)
2. Update prices in retailer_products table
3. Let commercial scrapers activate products when they find them

IMAGE BACKFILL:
- When backfilling images from Azure, also set is_active = TRUE
- This activates products that have verified images
"""

    logging.info(policy)

def main():
    parser = argparse.ArgumentParser(
        description='Deactivate products without images (XML-only products)'
    )
    parser.add_argument(
        '--execute',
        action='store_true',
        help='Execute the deactivation (default is dry-run)'
    )

    args = parser.parse_args()

    logging.info("="*80)
    logging.info("DEACTIVATE PRODUCTS WITHOUT IMAGES")
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
        deactivated_count = deactivate_products_without_images(cursor, dry_run=not args.execute)

        if args.execute:
            # Commit changes
            conn.commit()
            logging.info("\n✅ Changes committed to database")

            # Analyze after
            analyze_after_deactivation(cursor)

        # Provide policy reminder
        provide_policy_reminder()

        logging.info("\n" + "="*80)
        logging.info("SCRIPT COMPLETE")
        logging.info("="*80)

        if not args.execute:
            logging.info("\n⚠️  This was a DRY RUN - no changes were made")
            logging.info("Run with --execute flag to perform actual deactivation")
        else:
            logging.info(f"\n✅ Successfully deactivated {deactivated_count} products without images")

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
