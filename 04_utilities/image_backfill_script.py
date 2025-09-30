#!/usr/bin/env python3
"""
Image Backfill Script for NULL-source Canonical Products

This script:
1. Queries canonical products with NULL source_retailer_id and no images
2. Searches for each product on Super-Pharm and Be Pharm commercial sites
3. Extracts product image URLs
4. Updates canonical_products table with images and source_retailer_id
"""

import psycopg2
from psycopg2.extras import execute_batch
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import logging
from datetime import datetime
import json
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('image_backfill.log'),
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

# Retailer configurations
RETAILERS = {
    'super_pharm': {
        'id': 52,
        'name': 'Super-Pharm',
        'search_url': 'https://shop.super-pharm.co.il/search?q={barcode}',
        'image_selectors': [
            'img.product-image',
            'img[alt*="product"]',
            '.product-gallery img',
            'img.main-image'
        ]
    },
    'be_pharm': {
        'id': 150,
        'name': 'Be Pharm',
        'search_url': 'https://www.bestore.co.il/catalogsearch/result/?q={barcode}',
        'image_selectors': [
            'img.product-image-photo',
            'img[alt*="product"]',
            '.product-image img',
            'img.fotorama__img'
        ]
    }
}

class ImageBackfiller:
    def __init__(self, batch_size=100, headless=True, checkpoint_interval=500):
        self.batch_size = batch_size
        self.headless = headless
        self.checkpoint_interval = checkpoint_interval
        self.checkpoint_file = 'image_backfill_checkpoint.json'
        self.driver = None
        self.pending_updates = []
        self.stats = {
            'processed': 0,
            'images_found': 0,
            'super_pharm_success': 0,
            'be_pharm_success': 0,
            'not_found': 0,
            'errors': 0
        }
        self.last_processed_barcode = None
        self.load_checkpoint()

    def setup_driver(self):
        """Setup Selenium WebDriver"""
        options = webdriver.ChromeOptions()
        if self.headless:
            options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36')
        # Performance optimizations
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.page_load_strategy = 'eager'  # Don't wait for full page load

        self.driver = webdriver.Chrome(options=options)
        self.driver.implicitly_wait(2)  # Reduced from 5
        logging.info("✅ WebDriver initialized")

    def load_checkpoint(self):
        """Load checkpoint from file if exists"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    checkpoint = json.load(f)
                    self.last_processed_barcode = checkpoint.get('last_barcode')
                    self.stats = checkpoint.get('stats', self.stats)
                    logging.info(f"📂 Loaded checkpoint: Last barcode {self.last_processed_barcode}, {self.stats['processed']} processed")
            except Exception as e:
                logging.warning(f"⚠️  Could not load checkpoint: {e}")
                self.last_processed_barcode = None

    def save_checkpoint(self, current_barcode):
        """Save checkpoint to file"""
        try:
            checkpoint = {
                'last_barcode': current_barcode,
                'stats': self.stats,
                'timestamp': datetime.now().isoformat()
            }
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint, f, indent=2)
            logging.info(f"💾 Checkpoint saved at barcode {current_barcode}")
        except Exception as e:
            logging.error(f"❌ Failed to save checkpoint: {e}")

    def clear_checkpoint(self):
        """Clear checkpoint file when complete"""
        try:
            if os.path.exists(self.checkpoint_file):
                os.remove(self.checkpoint_file)
                logging.info("🗑️  Checkpoint file cleared")
        except Exception as e:
            logging.warning(f"⚠️  Could not remove checkpoint file: {e}")

    def get_null_source_products(self, limit=None):
        """Get products with NULL source and no images, resuming from checkpoint if available"""
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        query = """
            SELECT DISTINCT barcode, name
            FROM canonical_products
            WHERE source_retailer_id IS NULL
                AND (image_url IS NULL OR image_url = '')
        """

        # Resume from checkpoint if available
        if self.last_processed_barcode:
            query += f" AND barcode > '{self.last_processed_barcode}'"
            logging.info(f"📍 Resuming from barcode: {self.last_processed_barcode}")

        query += " ORDER BY barcode"

        if limit:
            query += f" LIMIT {limit}"

        cur.execute(query)
        products = cur.fetchall()

        cur.close()
        conn.close()

        logging.info(f"📦 Found {len(products)} products to process")
        return products

    def search_product_on_retailer(self, barcode, retailer_key):
        """Search for product on retailer website and extract image"""
        retailer = RETAILERS[retailer_key]
        search_url = retailer['search_url'].format(barcode=barcode)

        try:
            self.driver.get(search_url)
            time.sleep(0.5)  # Reduced from 2 seconds

            # Try each image selector
            for selector in retailer['image_selectors']:
                try:
                    img_element = WebDriverWait(self.driver, 3).until(  # Reduced from 5 seconds
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )

                    image_url = img_element.get_attribute('src')

                    # Validate image URL
                    if image_url and 'placeholder' not in image_url.lower() and len(image_url) > 20:
                        logging.info(f"  ✅ {retailer['name']}: Found image for {barcode}")
                        return image_url, retailer['id']

                except (TimeoutException, NoSuchElementException):
                    continue

            logging.warning(f"  ⚠️  {retailer['name']}: No valid image found for {barcode}")
            return None, None

        except Exception as e:
            logging.error(f"  ❌ {retailer['name']}: Error searching {barcode}: {str(e)}")
            return None, None

    def batch_update_images(self):
        """Flush pending updates to database in batch"""
        if not self.pending_updates:
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
            """, self.pending_updates, page_size=100)

            conn.commit()
            logging.info(f"  💾 Batch updated {len(self.pending_updates)} products")
            self.pending_updates = []

        except Exception as e:
            conn.rollback()
            logging.error(f"  ❌ Database batch update error: {str(e)}")

        finally:
            cur.close()
            conn.close()

    def process_product(self, barcode, name):
        """Process a single product - try Super-Pharm first, then Be Pharm"""
        logging.info(f"\n🔍 Processing: {barcode} - {name[:50]}")

        # Try Super-Pharm first
        image_url, source_id = self.search_product_on_retailer(barcode, 'super_pharm')

        if image_url:
            self.pending_updates.append((image_url, source_id, datetime.now(), barcode))
            self.stats['images_found'] += 1
            self.stats['super_pharm_success'] += 1
            logging.info(f"  ✅ Found on Super-Pharm")
            return True

        # If not found on Super-Pharm, try Be Pharm
        logging.info(f"  🔄 Trying Be Pharm...")
        image_url, source_id = self.search_product_on_retailer(barcode, 'be_pharm')

        if image_url:
            self.pending_updates.append((image_url, source_id, datetime.now(), barcode))
            self.stats['images_found'] += 1
            self.stats['be_pharm_success'] += 1
            logging.info(f"  ✅ Found on Be Pharm")
            return True

        # Not found on either site
        self.stats['not_found'] += 1
        logging.warning(f"  ❌ No image found")
        return False

    def run(self, limit=None):
        """Run the backfill process"""
        logging.info("="*80)
        logging.info("STARTING IMAGE BACKFILL PROCESS")
        logging.info("="*80)

        # Get products to process
        products = self.get_null_source_products(limit)

        if not products:
            logging.info("No products to process")
            return

        # Setup WebDriver
        self.setup_driver()

        try:
            # Process products
            for i, (barcode, name) in enumerate(products, 1):
                try:
                    self.process_product(barcode, name)
                    self.stats['processed'] += 1
                    self.last_processed_barcode = barcode

                    # Batch update every batch_size products
                    if len(self.pending_updates) >= self.batch_size:
                        self.batch_update_images()

                    # Save checkpoint every checkpoint_interval products
                    if self.stats['processed'] % self.checkpoint_interval == 0:
                        self.save_checkpoint(barcode)

                    # Progress report every 10 products
                    if i % 10 == 0:
                        self.print_progress()

                    # Minimal delay
                    time.sleep(0.2)

                except Exception as e:
                    logging.error(f"❌ Error processing {barcode}: {str(e)}")
                    self.stats['errors'] += 1

        finally:
            # Final batch update
            self.batch_update_images()

            if self.driver:
                self.driver.quit()
                logging.info("🛑 WebDriver closed")

        # Final statistics
        self.print_final_stats()

        # Clear checkpoint on successful completion
        self.clear_checkpoint()

    def print_progress(self):
        """Print progress statistics"""
        success_rate = (self.stats['images_found'] / self.stats['processed'] * 100) if self.stats['processed'] > 0 else 0
        logging.info(f"\n📊 Progress: {self.stats['processed']} processed | "
                    f"{self.stats['images_found']} images found ({success_rate:.1f}%)")

    def print_final_stats(self):
        """Print final statistics"""
        logging.info("\n" + "="*80)
        logging.info("IMAGE BACKFILL COMPLETE - FINAL STATISTICS")
        logging.info("="*80)
        logging.info(f"Total processed: {self.stats['processed']}")
        logging.info(f"Images found: {self.stats['images_found']}")
        logging.info(f"  - Super-Pharm: {self.stats['super_pharm_success']}")
        logging.info(f"  - Be Pharm: {self.stats['be_pharm_success']}")
        logging.info(f"Not found: {self.stats['not_found']}")
        logging.info(f"Errors: {self.stats['errors']}")

        if self.stats['processed'] > 0:
            success_rate = (self.stats['images_found'] / self.stats['processed']) * 100
            logging.info(f"\n✅ Success rate: {success_rate:.1f}%")
        logging.info("="*80)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Backfill images for NULL-source canonical products')
    parser.add_argument('--limit', type=int, help='Limit number of products to process (for testing)')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for processing')
    parser.add_argument('--no-headless', action='store_true', help='Run browser in visible mode')

    args = parser.parse_args()

    backfiller = ImageBackfiller(
        batch_size=args.batch_size,
        headless=not args.no_headless
    )

    backfiller.run(limit=args.limit)