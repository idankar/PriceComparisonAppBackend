#!/usr/bin/env python3
"""
Parallel Image Backfill Script - OPTIMIZED VERSION

Major optimizations:
1. Parallel processing with multiple browser instances
2. Batch database updates
3. Reduced sleep times
4. Better error handling and resumability
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
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(threadName)s] - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parallel_image_backfill.log'),
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

class ParallelImageBackfiller:
    def __init__(self, num_workers=5, batch_size=100):
        self.num_workers = num_workers
        self.batch_size = batch_size
        self.lock = threading.Lock()
        self.stats = {
            'processed': 0,
            'images_found': 0,
            'super_pharm_success': 0,
            'be_pharm_success': 0,
            'not_found': 0,
            'errors': 0
        }
        self.pending_updates = []

    def setup_driver(self):
        """Setup Selenium WebDriver"""
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36')
        # Performance optimizations
        options.add_argument('--disable-images')  # Don't load images in browser
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')

        return webdriver.Chrome(options=options)

    def get_null_source_products(self, limit=None):
        """Get products with NULL source and no images"""
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        query = """
            SELECT DISTINCT barcode, name
            FROM canonical_products
            WHERE source_retailer_id IS NULL
                AND (image_url IS NULL OR image_url = '')
            ORDER BY barcode
        """

        if limit:
            query += f" LIMIT {limit}"

        cur.execute(query)
        products = cur.fetchall()

        cur.close()
        conn.close()

        logging.info(f"📦 Found {len(products)} products to process")
        return products

    def search_product_on_retailer(self, driver, barcode, retailer_key):
        """Search for product on retailer website and extract image"""
        retailer = RETAILERS[retailer_key]
        search_url = retailer['search_url'].format(barcode=barcode)

        try:
            driver.get(search_url)
            time.sleep(1)  # Reduced from 2 seconds

            # Try each image selector
            for selector in retailer['image_selectors']:
                try:
                    img_element = WebDriverWait(driver, 3).until(  # Reduced from 5 seconds
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )

                    image_url = img_element.get_attribute('src')

                    # Validate image URL
                    if image_url and 'placeholder' not in image_url.lower() and len(image_url) > 20:
                        return image_url, retailer['id']

                except (TimeoutException, NoSuchElementException):
                    continue

            return None, None

        except Exception as e:
            logging.error(f"Error searching {barcode} on {retailer['name']}: {str(e)}")
            return None, None

    def process_product(self, driver, barcode, name):
        """Process a single product - try Super-Pharm first, then Be Pharm"""
        # Try Super-Pharm first
        image_url, source_id = self.search_product_on_retailer(driver, barcode, 'super_pharm')

        if image_url:
            with self.lock:
                self.pending_updates.append((image_url, source_id, datetime.now(), barcode))
                self.stats['images_found'] += 1
                self.stats['super_pharm_success'] += 1
            return True

        # If not found on Super-Pharm, try Be Pharm
        image_url, source_id = self.search_product_on_retailer(driver, barcode, 'be_pharm')

        if image_url:
            with self.lock:
                self.pending_updates.append((image_url, source_id, datetime.now(), barcode))
                self.stats['images_found'] += 1
                self.stats['be_pharm_success'] += 1
            return True

        # Not found on either site
        with self.lock:
            self.stats['not_found'] += 1
        return False

    def batch_update_images(self):
        """Flush pending updates to database"""
        if not self.pending_updates:
            return

        with self.lock:
            updates = self.pending_updates[:]
            self.pending_updates = []

        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        try:
            execute_batch(cur, """
                UPDATE canonical_products
                SET image_url = %s,
                    source_retailer_id = %s,
                    last_scraped_at = %s
                WHERE barcode = %s
            """, updates, page_size=100)

            conn.commit()
            logging.info(f"💾 Batch updated {len(updates)} products")

        except Exception as e:
            conn.rollback()
            logging.error(f"❌ Database error: {str(e)}")

        finally:
            cur.close()
            conn.close()

    def worker(self, products_chunk):
        """Worker function to process a chunk of products"""
        driver = self.setup_driver()

        try:
            for barcode, name in products_chunk:
                try:
                    self.process_product(driver, barcode, name)

                    with self.lock:
                        self.stats['processed'] += 1

                        # Batch update every batch_size products
                        if len(self.pending_updates) >= self.batch_size:
                            self.batch_update_images()

                        # Progress report every 50 products
                        if self.stats['processed'] % 50 == 0:
                            self.print_progress()

                except Exception as e:
                    logging.error(f"❌ Error processing {barcode}: {str(e)}")
                    with self.lock:
                        self.stats['errors'] += 1

        finally:
            driver.quit()

    def run(self, limit=None):
        """Run the parallel backfill process"""
        logging.info("="*80)
        logging.info("STARTING PARALLEL IMAGE BACKFILL PROCESS")
        logging.info(f"Workers: {self.num_workers}, Batch size: {self.batch_size}")
        logging.info("="*80)

        start_time = datetime.now()

        # Get products to process
        products = self.get_null_source_products(limit)

        if not products:
            logging.info("No products to process")
            return

        # Split products into chunks for each worker
        chunk_size = len(products) // self.num_workers
        chunks = [products[i:i + chunk_size] for i in range(0, len(products), chunk_size)]

        # Process in parallel
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = [executor.submit(self.worker, chunk) for chunk in chunks]

            # Wait for all workers to complete
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Worker error: {str(e)}")

        # Final batch update
        self.batch_update_images()

        # Final statistics
        elapsed = datetime.now() - start_time
        self.print_final_stats(elapsed)

    def print_progress(self):
        """Print progress statistics"""
        success_rate = (self.stats['images_found'] / self.stats['processed'] * 100) if self.stats['processed'] > 0 else 0
        logging.info(f"📊 Progress: {self.stats['processed']} processed | "
                    f"{self.stats['images_found']} images found ({success_rate:.1f}%)")

    def print_final_stats(self, elapsed):
        """Print final statistics"""
        logging.info("\n" + "="*80)
        logging.info("PARALLEL IMAGE BACKFILL COMPLETE")
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

        logging.info(f"⏱️  Time elapsed: {elapsed}")
        if elapsed.total_seconds() > 0:
            rate = self.stats['processed'] / elapsed.total_seconds()
            logging.info(f"🚀 Speed: {rate:.1f} products/second")
        logging.info("="*80)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Parallel image backfill for NULL-source canonical products')
    parser.add_argument('--limit', type=int, help='Limit number of products to process (for testing)')
    parser.add_argument('--workers', type=int, default=5, help='Number of parallel workers')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for database updates')

    args = parser.parse_args()

    backfiller = ParallelImageBackfiller(
        num_workers=args.workers,
        batch_size=args.batch_size
    )

    backfiller.run(limit=args.limit)