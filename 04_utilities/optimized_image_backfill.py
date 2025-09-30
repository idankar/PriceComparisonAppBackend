#!/usr/bin/env python3
"""
OPTIMIZED Image Backfill Script

This script uses the existing retailer_products table to populate images
for canonical products, instead of web scraping. This is 100x faster.

Strategy:
1. Find canonical products with NULL source and no images
2. Match them with retailer_products by barcode
3. Prioritize Super-Pharm and Be Pharm (most reliable)
4. Batch update canonical_products table
"""

import psycopg2
from psycopg2.extras import execute_batch
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('optimized_image_backfill.log'),
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

# Retailer priority (most reliable first)
RETAILER_PRIORITY = [52, 150]  # Super-Pharm, Be Pharm

class OptimizedImageBackfiller:
    def __init__(self, batch_size=1000):
        self.batch_size = batch_size
        self.stats = {
            'total_processed': 0,
            'images_found': 0,
            'super_pharm': 0,
            'be_pharm': 0,
            'not_found': 0
        }

    def get_image_mappings(self):
        """
        Get barcode -> image mappings from commercial_government_matches
        Prioritizes Super-Pharm, then Be Pharm based on commercial products
        """
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        logging.info("🔍 Fetching image mappings from commercial products...")

        query = """
            WITH product_images AS (
                SELECT DISTINCT
                    cgm.commercial_product_id as barcode,
                    cgm.commercial_image_url,
                    -- Infer retailer from the commercial match data
                    CASE
                        WHEN cgm.commercial_name ILIKE '%super%pharm%' THEN 52
                        WHEN cgm.commercial_name ILIKE '%be%pharm%' OR cgm.commercial_name ILIKE '%bestore%' THEN 150
                        ELSE 52  -- Default to Super-Pharm
                    END as retailer_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY cgm.commercial_product_id
                        ORDER BY cgm.created_at DESC
                    ) as rank
                FROM commercial_government_matches cgm
                WHERE cgm.commercial_image_url IS NOT NULL
                    AND cgm.commercial_image_url != ''
                    AND cgm.commercial_product_id IS NOT NULL
            )
            SELECT barcode, commercial_image_url, retailer_id, 'Inferred' as retailername
            FROM product_images
            WHERE rank = 1
        """

        cur.execute(query)
        results = cur.fetchall()

        # Convert to dict for fast lookup
        mappings = {}
        for barcode, image_url, retailer_id, retailer_name in results:
            mappings[barcode] = {
                'image_url': image_url,
                'retailer_id': retailer_id,
                'retailer_name': retailer_name
            }

        cur.close()
        conn.close()

        logging.info(f"✅ Found {len(mappings):,} barcode -> image mappings")
        return mappings

    def get_products_needing_images(self):
        """Get canonical products that need images"""
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        query = """
            SELECT barcode, name
            FROM canonical_products
            WHERE source_retailer_id IS NULL
                AND (image_url IS NULL OR image_url = '')
            ORDER BY barcode
        """

        cur.execute(query)
        products = cur.fetchall()

        cur.close()
        conn.close()

        logging.info(f"📦 Found {len(products):,} products needing images")
        return products

    def batch_update_images(self, updates):
        """Batch update canonical products with images"""
        if not updates:
            return

        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        try:
            execute_batch(cur, """
                UPDATE canonical_products
                SET image_url = %s,
                    source_retailer_id = %s,
                    last_scraped_at = %s
                WHERE barcode = %s
            """, updates, page_size=1000)

            conn.commit()
            logging.info(f"  💾 Updated {len(updates)} products in database")

        except Exception as e:
            conn.rollback()
            logging.error(f"  ❌ Database error: {str(e)}")
            raise

        finally:
            cur.close()
            conn.close()

    def run(self):
        """Run the optimized backfill process"""
        logging.info("="*80)
        logging.info("STARTING OPTIMIZED IMAGE BACKFILL")
        logging.info("="*80)

        start_time = datetime.now()

        # Step 1: Get all image mappings from retailer_products
        image_mappings = self.get_image_mappings()

        # Step 2: Get products needing images
        products = self.get_products_needing_images()

        if not products:
            logging.info("✅ No products need images!")
            return

        # Step 3: Match and prepare batch updates
        logging.info("\n🔄 Matching products with images...")
        updates = []

        for barcode, name in products:
            self.stats['total_processed'] += 1

            if barcode in image_mappings:
                mapping = image_mappings[barcode]
                updates.append((
                    mapping['image_url'],
                    mapping['retailer_id'],
                    datetime.now(),
                    barcode
                ))

                self.stats['images_found'] += 1
                if mapping['retailer_id'] == 52:
                    self.stats['super_pharm'] += 1
                elif mapping['retailer_id'] == 150:
                    self.stats['be_pharm'] += 1

                # Log progress every 1000 products
                if self.stats['total_processed'] % 1000 == 0:
                    self.print_progress()

                # Batch update every batch_size products
                if len(updates) >= self.batch_size:
                    self.batch_update_images(updates)
                    updates = []
            else:
                self.stats['not_found'] += 1

        # Final batch update
        if updates:
            self.batch_update_images(updates)

        # Print final statistics
        elapsed = datetime.now() - start_time
        self.print_final_stats(elapsed)

    def print_progress(self):
        """Print progress statistics"""
        success_rate = (self.stats['images_found'] / self.stats['total_processed'] * 100) if self.stats['total_processed'] > 0 else 0
        logging.info(f"📊 Progress: {self.stats['total_processed']:,} processed | "
                    f"{self.stats['images_found']:,} matched ({success_rate:.1f}%)")

    def print_final_stats(self, elapsed):
        """Print final statistics"""
        logging.info("\n" + "="*80)
        logging.info("OPTIMIZED IMAGE BACKFILL COMPLETE")
        logging.info("="*80)
        logging.info(f"Total processed: {self.stats['total_processed']:,}")
        logging.info(f"Images found: {self.stats['images_found']:,}")
        logging.info(f"  - Super-Pharm: {self.stats['super_pharm']:,}")
        logging.info(f"  - Be Pharm: {self.stats['be_pharm']:,}")
        logging.info(f"Not found: {self.stats['not_found']:,}")

        if self.stats['total_processed'] > 0:
            success_rate = (self.stats['images_found'] / self.stats['total_processed']) * 100
            logging.info(f"\n✅ Success rate: {success_rate:.1f}%")

        logging.info(f"⏱️  Time elapsed: {elapsed}")
        logging.info(f"🚀 Speed: {self.stats['total_processed'] / elapsed.total_seconds():.0f} products/second")
        logging.info("="*80)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Optimized image backfill using retailer_products table')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for database updates')

    args = parser.parse_args()

    backfiller = OptimizedImageBackfiller(batch_size=args.batch_size)
    backfiller.run()