#!/usr/bin/env python3
"""
Category Backfill Script for Canonical Products

This script:
1. Queries canonical products without categories
2. Searches for each product on retailer websites
3. Extracts category/breadcrumb information
4. Updates canonical_products table with categories
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import undetected_chromedriver as uc
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
        logging.FileHandler('category_backfill.log'),
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

# Retailer configurations for category extraction
RETAILERS = {
    'super_pharm': {
        'id': 52,
        'name': 'Super-Pharm',
        'product_url': 'https://shop.super-pharm.co.il/product/{barcode}',
        'search_url': 'https://shop.super-pharm.co.il/search?q={barcode}',
        'breadcrumb_selectors': [
            '.breadcrumb',
            'nav[aria-label="breadcrumb"]',
            '.product-breadcrumb',
            '[data-testid="breadcrumb"]'
        ],
        'category_attribute_selector': '[data-category]'  # From product list pages
    },
    'be_pharm': {
        'id': 150,
        'name': 'Be Pharm',
        'product_url': 'https://www.bestore.co.il/product/{barcode}',
        'search_url': 'https://www.bestore.co.il/catalogsearch/result/?q={barcode}',
        'breadcrumb_selectors': [
            '.breadcrumb',
            '.breadcrumbs',
            'nav.breadcrumb',
            '[itemtype="http://schema.org/BreadcrumbList"]'
        ]
    },
    'good_pharm': {
        'id': 97,
        'name': 'Good Pharm',
        'product_url': 'https://goodpharm.binaprojects.com/product/{barcode}',
        'search_url': 'https://goodpharm.binaprojects.com/search?q={barcode}',
        'breadcrumb_selectors': [
            '.breadcrumb',
            '.product-breadcrumb',
            'nav[aria-label="breadcrumb"]'
        ]
    }
}

class CategoryBackfiller:
    def __init__(self, batch_size=100, headless=True, checkpoint_interval=100):
        self.batch_size = batch_size
        self.headless = headless
        self.checkpoint_interval = checkpoint_interval
        self.checkpoint_file = 'category_backfill_checkpoint.json'
        self.driver = None
        self.stats = {
            'processed': 0,
            'categories_found': 0,
            'super_pharm_success': 0,
            'be_pharm_success': 0,
            'good_pharm_success': 0,
            'not_found': 0,
            'errors': 0
        }
        self.last_processed_barcode = None
        self.load_checkpoint()

    def setup_driver(self):
        """Setup undetected Chrome WebDriver to avoid bot detection"""
        options = uc.ChromeOptions()
        if self.headless:
            options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        # Performance optimizations
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')

        # Use undetected-chromedriver to bypass anti-bot measures
        # Specify Chrome version 140 to match installed version
        self.driver = uc.Chrome(options=options, version_main=140)
        self.driver.set_page_load_timeout(20)
        self.driver.implicitly_wait(3)
        logging.info("✅ Undetected WebDriver initialized")

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
            logging.debug(f"💾 Checkpoint saved at barcode {current_barcode}")
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

    def get_products_without_categories(self, limit=None):
        """Get active products without categories, resuming from checkpoint if available"""
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        query = """
            SELECT DISTINCT cp.barcode, cp.name
            FROM canonical_products cp
            WHERE cp.is_active = TRUE
                AND (cp.category IS NULL OR cp.category = '')
        """

        if self.last_processed_barcode:
            query += f" AND cp.barcode > '{self.last_processed_barcode}'"
            logging.info(f"📍 Resuming from barcode: {self.last_processed_barcode}")

        query += " ORDER BY cp.barcode"

        if limit:
            query += f" LIMIT {limit}"

        cur.execute(query)
        products = cur.fetchall()

        cur.close()
        conn.close()

        logging.info(f"📦 Found {len(products)} products without categories to process")
        return products

    def extract_breadcrumb_category(self, selectors):
        """Extract category from breadcrumb navigation"""
        for selector in selectors:
            try:
                breadcrumb = WebDriverWait(self.driver, 3).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )

                # Get all links in breadcrumb
                links = breadcrumb.find_elements(By.TAG_NAME, 'a')

                if links:
                    # Build category path from breadcrumb links
                    category_parts = []
                    for link in links:
                        text = link.text.strip()
                        # Skip "Home" or empty links
                        if text and text.lower() not in ['home', 'בית', 'דף הבית']:
                            category_parts.append(text)

                    if category_parts:
                        category = '/'.join(category_parts)
                        logging.debug(f"    Found category from breadcrumb: {category}")
                        return category

                # If no links, try getting direct text
                category_text = breadcrumb.text.strip()
                if category_text and len(category_text) > 0:
                    # Clean up breadcrumb text (remove arrows, >>, etc.)
                    category_text = category_text.replace('>', '/').replace('>>', '/').strip()
                    logging.debug(f"    Found category from breadcrumb text: {category_text}")
                    return category_text

            except (TimeoutException, NoSuchElementException):
                continue

        return None

    def extract_category_from_product_page(self, barcode, retailer_key):
        """Try to extract category from product detail page"""
        retailer = RETAILERS[retailer_key]
        product_url = retailer['product_url'].format(barcode=barcode)

        try:
            self.driver.get(product_url)
            time.sleep(1.5)

            # Try breadcrumb extraction
            category = self.extract_breadcrumb_category(retailer['breadcrumb_selectors'])
            if category:
                return category

        except Exception as e:
            logging.debug(f"    Error on product page: {str(e)}")

        return None

    def extract_category_from_search(self, barcode, retailer_key):
        """Try to extract category from search results page"""
        retailer = RETAILERS[retailer_key]
        search_url = retailer['search_url'].format(barcode=barcode)

        try:
            self.driver.get(search_url)
            time.sleep(1)

            # Check if there's a category attribute on the product element
            if 'category_attribute_selector' in retailer:
                try:
                    product_element = self.driver.find_element(
                        By.CSS_SELECTOR,
                        retailer['category_attribute_selector']
                    )
                    category = product_element.get_attribute('data-category')
                    if category:
                        logging.debug(f"    Found category from data attribute: {category}")
                        return category
                except:
                    pass

            # Try breadcrumb on search page
            category = self.extract_breadcrumb_category(retailer['breadcrumb_selectors'])
            if category:
                return category

        except Exception as e:
            logging.debug(f"    Error on search page: {str(e)}")

        return None

    def search_product_on_retailer(self, barcode, retailer_key):
        """Search for product category on retailer website"""
        retailer = RETAILERS[retailer_key]

        try:
            # Try product page first (more likely to have breadcrumbs)
            category = self.extract_category_from_product_page(barcode, retailer_key)

            if category:
                logging.info(f"  ✅ {retailer['name']}: Found category for {barcode}")
                return category, retailer['id']

            # Fallback to search page
            logging.debug(f"    Trying search page...")
            category = self.extract_category_from_search(barcode, retailer_key)

            if category:
                logging.info(f"  ✅ {retailer['name']}: Found category for {barcode} (from search)")
                return category, retailer['id']

            logging.warning(f"  ⚠️  {retailer['name']}: No category found for {barcode}")
            return None, None

        except Exception as e:
            logging.error(f"  ❌ {retailer['name']}: Error searching {barcode}: {str(e)}")
            return None, None

    def update_category_immediately(self, barcode, category):
        """Update database immediately for a single product"""
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        try:
            cur.execute("""
                UPDATE canonical_products
                SET category = %s,
                    last_scraped_at = %s
                WHERE barcode = %s
            """, (category, datetime.now(), barcode))

            conn.commit()
            logging.info(f"  💾 Updated category for barcode {barcode}")

        except Exception as e:
            conn.rollback()
            logging.error(f"  ❌ Database update error for {barcode}: {str(e)}")

        finally:
            cur.close()
            conn.close()

    def process_product(self, barcode, name):
        """Process a single product - try Super-Pharm first, then Be Pharm, then Good Pharm"""
        logging.info(f"\n🔍 Processing: {barcode} - {name[:50]}")

        # Try Super-Pharm first (usually best coverage)
        category, source_id = self.search_product_on_retailer(barcode, 'super_pharm')

        if category:
            self.update_category_immediately(barcode, category)
            self.stats['categories_found'] += 1
            self.stats['super_pharm_success'] += 1
            return True

        # Try Be Pharm
        logging.info(f"  🔄 Trying Be Pharm...")
        category, source_id = self.search_product_on_retailer(barcode, 'be_pharm')

        if category:
            self.update_category_immediately(barcode, category)
            self.stats['categories_found'] += 1
            self.stats['be_pharm_success'] += 1
            return True

        # Try Good Pharm
        logging.info(f"  🔄 Trying Good Pharm...")
        category, source_id = self.search_product_on_retailer(barcode, 'good_pharm')

        if category:
            self.update_category_immediately(barcode, category)
            self.stats['categories_found'] += 1
            self.stats['good_pharm_success'] += 1
            return True

        # Not found on any site
        self.stats['not_found'] += 1
        logging.warning(f"  ❌ No category found on any retailer for {barcode}")
        return False

    def run(self, limit=None):
        """Run the backfill process"""
        logging.info("="*80)
        logging.info("STARTING CATEGORY BACKFILL PROCESS")
        logging.info("="*80)

        # Get products to process
        products = self.get_products_without_categories(limit)

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

                    # Save checkpoint periodically
                    if self.stats['processed'] % self.checkpoint_interval == 0:
                        self.save_checkpoint(barcode)

                    # Progress report every 10 products
                    if i % 10 == 0:
                        self.print_progress()

                    # Minimal delay between requests
                    time.sleep(1.5)

                except Exception as e:
                    logging.error(f"❌ Error processing {barcode}: {str(e)}")
                    self.stats['errors'] += 1

        finally:
            if self.driver:
                self.driver.quit()
                logging.info("🛑 WebDriver closed")

        # Final statistics
        self.print_final_stats()

        # Clear checkpoint on successful completion
        self.clear_checkpoint()

    def print_progress(self):
        """Print progress statistics"""
        success_rate = (self.stats['categories_found'] / self.stats['processed'] * 100) if self.stats['processed'] > 0 else 0
        logging.info(f"\n📊 Progress: {self.stats['processed']} processed | "
                    f"{self.stats['categories_found']} categories found ({success_rate:.1f}%)")

    def print_final_stats(self):
        """Print final statistics"""
        logging.info("\n" + "="*80)
        logging.info("CATEGORY BACKFILL COMPLETE - FINAL STATISTICS")
        logging.info("="*80)
        logging.info(f"Total processed: {self.stats['processed']}")
        logging.info(f"Categories found: {self.stats['categories_found']}")
        logging.info(f"  - Super-Pharm: {self.stats['super_pharm_success']}")
        logging.info(f"  - Be Pharm: {self.stats['be_pharm_success']}")
        logging.info(f"  - Good Pharm: {self.stats['good_pharm_success']}")
        logging.info(f"Not found: {self.stats['not_found']}")
        logging.info(f"Errors: {self.stats['errors']}")

        if self.stats['processed'] > 0:
            success_rate = (self.stats['categories_found'] / self.stats['processed']) * 100
            logging.info(f"\n✅ Success rate: {success_rate:.1f}%")
        logging.info("="*80)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Backfill categories for canonical products')
    parser.add_argument('--limit', type=int, help='Limit number of products to process (for testing)')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for processing')
    parser.add_argument('--no-headless', action='store_true', help='Run browser in visible mode')

    args = parser.parse_args()

    backfiller = CategoryBackfiller(
        batch_size=args.batch_size,
        headless=not args.no_headless
    )

    backfiller.run(limit=args.limit)
