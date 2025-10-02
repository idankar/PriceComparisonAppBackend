#!/usr/bin/env python3
"""
Super-Pharm Online Price Backfill Script

This script backfills online prices for Super-Pharm products by:
1. Identifying products without online prices
2. Searching for each product on the Super-Pharm website (if no URL stored)
3. Extracting the online price from the product page
4. Storing the price in the prices table linked to the online store (store_id=52001)

Features:
- Checkpoint/resume functionality for long-running scrapes
- Progress tracking and statistics
- Robust error handling
"""

import time
import json
import argparse
import re
import logging
from datetime import datetime
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.keys import Keys
import undetected_chromedriver as uc
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from urllib.parse import urljoin, quote

# --- Configuration & Logging ---
load_dotenv()
DB_NAME = os.getenv("DB_NAME", "price_comparison_app_v2")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "***REMOVED***")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

SUPER_PHARM_RETAILER_ID = 52
ONLINE_STORE_ID = 52001
CHECKPOINT_FILE = 'super_pharm_online_backfill_checkpoint.json'
LOG_FILE = 'super_pharm_online_backfill.log'
BASE_URL = "https://shop.super-pharm.co.il/"
SEARCH_URL = "https://shop.super-pharm.co.il/search?q="

log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('SuperPharmOnlineBackfill')
logger.setLevel(logging.INFO)

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)
    file_handler = logging.FileHandler(LOG_FILE, mode='a')
    file_handler.setFormatter(log_formatter)
    logger.addHandler(file_handler)


class SuperPharmOnlineBackfill:
    """Backfill online prices for Super-Pharm products"""

    def __init__(self, headless=True, limit=None):
        self.headless = headless
        self.limit = limit
        self.conn = None
        self.cursor = None
        self.driver = None
        self.processed_count = 0
        self.prices_found = 0
        self.prices_not_found = 0
        self.errors = 0
        self.last_barcode = None

    def _connect_db(self):
        """Connect to database"""
        try:
            self.conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT
            )
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            logger.info("✅ Database connected")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
        return True

    def _setup_driver(self):
        """Setup WebDriver"""
        try:
            # Try undetected-chromedriver first
            options = uc.ChromeOptions()
            if self.headless:
                options.add_argument("--headless=new")
            options.add_argument("--start-maximized")
            options.add_argument("--disable-blink-features=AutomationControlled")

            try:
                self.driver = uc.Chrome(options=options, use_subprocess=False)
                logger.info("✅ WebDriver initialized (undetected-chromedriver)")
                return True
            except Exception as e:
                logger.warning(f"⚠️ Undetected-chromedriver failed: {e}")
                logger.info("🔄 Falling back to regular Selenium with webdriver-manager...")

                # Fallback to regular Selenium
                from selenium import webdriver
                from selenium.webdriver.chrome.service import Service
                from webdriver_manager.chrome import ChromeDriverManager

                regular_options = webdriver.ChromeOptions()
                if self.headless:
                    regular_options.add_argument("--headless=new")
                regular_options.add_argument("--start-maximized")
                regular_options.add_argument("--disable-blink-features=AutomationControlled")

                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=regular_options)
                logger.info("✅ WebDriver initialized (regular Selenium)")
                return True

        except Exception as e:
            logger.error(f"❌ WebDriver initialization failed: {e}")
            return False

    def load_checkpoint(self):
        """Load checkpoint if exists"""
        if os.path.exists(CHECKPOINT_FILE):
            try:
                with open(CHECKPOINT_FILE, 'r') as f:
                    data = json.load(f)
                self.processed_count = data.get('processed_count', 0)
                self.last_barcode = data.get('last_barcode')
                self.prices_found = data.get('prices_found', 0)
                self.prices_not_found = data.get('prices_not_found', 0)
                self.errors = data.get('errors', 0)
                logger.info(f"📍 Loaded checkpoint: {self.processed_count} products processed, last barcode: {self.last_barcode}")
                return True
            except Exception as e:
                logger.warning(f"⚠️ Could not load checkpoint: {e}")
        return False

    def save_checkpoint(self):
        """Save checkpoint"""
        try:
            data = {
                'processed_count': self.processed_count,
                'last_barcode': self.last_barcode,
                'prices_found': self.prices_found,
                'prices_not_found': self.prices_not_found,
                'errors': self.errors,
                'timestamp': datetime.now().isoformat()
            }
            with open(CHECKPOINT_FILE, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.warning(f"⚠️ Could not save checkpoint: {e}")

    def get_products_needing_prices(self):
        """Get list of products without online prices"""
        query = """
        SELECT
            cp.barcode,
            cp.name,
            cp.brand,
            cp.url
        FROM canonical_products cp
        WHERE cp.source_retailer_id = %s
            AND cp.is_active = TRUE
            AND NOT EXISTS (
                SELECT 1
                FROM retailer_products rp
                JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
                WHERE rp.barcode = cp.barcode
                    AND rp.retailer_id = %s
                    AND p.store_id = %s
            )
        """

        # Add resume condition if checkpoint exists
        if self.last_barcode:
            query += f" AND cp.barcode > '{self.last_barcode}'"

        query += " ORDER BY cp.barcode"

        if self.limit:
            query += f" LIMIT {self.limit}"

        self.cursor.execute(query, (SUPER_PHARM_RETAILER_ID, SUPER_PHARM_RETAILER_ID, ONLINE_STORE_ID))
        products = self.cursor.fetchall()
        logger.info(f"📦 Found {len(products)} products needing online prices")
        return products

    def _search_product_url(self, barcode, name, brand):
        """Search for product on website to find its URL"""
        try:
            # Try searching by barcode first
            search_term = barcode
            search_url = SEARCH_URL + quote(search_term)
            logger.debug(f"      Searching for: {search_term}")

            self.driver.get(search_url)
            time.sleep(2)

            # Look for product link
            try:
                product_link = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/p/']"))
                )
                url = product_link.get_attribute('href')
                if url:
                    logger.debug(f"      ✅ Found product URL via barcode: {url}")
                    return url
            except TimeoutException:
                logger.debug(f"      No results for barcode search")

            # If barcode search failed, try product name + brand
            if name and brand:
                search_term = f"{brand} {name}".strip()[:50]  # Limit search term length
                search_url = SEARCH_URL + quote(search_term)
                logger.debug(f"      Trying name search: {search_term}")

                self.driver.get(search_url)
                time.sleep(2)

                try:
                    product_link = WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/p/']"))
                    )
                    url = product_link.get_attribute('href')
                    if url:
                        logger.debug(f"      ✅ Found product URL via name: {url}")
                        return url
                except TimeoutException:
                    logger.debug(f"      No results for name search")

        except Exception as e:
            logger.debug(f"      ❌ Error searching for product: {e}")

        return None

    def _scrape_online_price(self, product_url):
        """Navigate to product page and extract price"""
        if not product_url:
            return None

        try:
            logger.debug(f"      Navigating to: {product_url}")
            self.driver.get(product_url)
            time.sleep(2)

            # Try multiple price selectors
            price_selectors = [
                "[class*='product-price'] [class*='price_']",
                ".product-price .price",
                "[class*='Price']",
                "[class*='price'][class*='value']",
                ".price-value",
                "[data-price]"
            ]

            for selector in price_selectors:
                try:
                    price_element = WebDriverWait(self.driver, 3).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    price_text = price_element.text.strip()

                    # Extract numeric price
                    price_match = re.search(r'(\d+[\.,]?\d*)', price_text.replace(',', '.'))
                    if price_match:
                        price = float(price_match.group(1))
                        if price > 0:  # Sanity check
                            logger.debug(f"      ✅ Found price: ₪{price}")
                            return price
                except:
                    continue

            logger.debug(f"      ⚠️  Could not find price on page")
            return None

        except Exception as e:
            logger.debug(f"      ❌ Error scraping price: {e}")
            return None

    def _store_price(self, barcode, price):
        """Store price in database"""
        try:
            # First, ensure retailer_product exists
            self.cursor.execute("""
                INSERT INTO retailer_products (retailer_id, retailer_item_code, barcode, original_retailer_name)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (retailer_id, retailer_item_code) DO NOTHING
                RETURNING retailer_product_id
            """, (SUPER_PHARM_RETAILER_ID, barcode, barcode, f"Super-Pharm {barcode}"))

            result = self.cursor.fetchone()
            if result:
                retailer_product_id = result['retailer_product_id']
            else:
                # If conflict, fetch existing
                self.cursor.execute("""
                    SELECT retailer_product_id FROM retailer_products
                    WHERE retailer_id = %s AND retailer_item_code = %s
                """, (SUPER_PHARM_RETAILER_ID, barcode))
                retailer_product_id = self.cursor.fetchone()['retailer_product_id']

            # Now insert the price
            self.cursor.execute("""
                INSERT INTO prices (retailer_product_id, store_id, price, price_timestamp)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (retailer_product_id, store_id, price_timestamp, scraped_at)
                DO NOTHING
            """, (retailer_product_id, ONLINE_STORE_ID, price))

            self.conn.commit()
            return True

        except Exception as e:
            logger.error(f"      ❌ Error storing price: {e}")
            self.conn.rollback()
            return False

    def _update_product_url(self, barcode, url):
        """Update product URL in canonical_products"""
        try:
            self.cursor.execute("""
                UPDATE canonical_products
                SET url = %s
                WHERE barcode = %s
            """, (url, barcode))
            self.conn.commit()
        except Exception as e:
            logger.warning(f"      ⚠️  Could not update URL: {e}")
            self.conn.rollback()

    def process_products(self, products):
        """Process list of products to scrape prices"""
        total = len(products)
        logger.info(f"\n{'='*80}")
        logger.info(f"🚀 Starting price scraping for {total} products")
        logger.info(f"{'='*80}\n")

        for i, product in enumerate(products, 1):
            barcode = product['barcode']
            name = product['name']
            brand = product['brand'] or ''
            url = product['url']

            self.last_barcode = barcode
            logger.info(f"[{i}/{total}] Processing {barcode} - {brand} {name[:40]}")

            try:
                # If no URL, search for it
                if not url:
                    logger.debug(f"    No URL stored, searching...")
                    url = self._search_product_url(barcode, name, brand)
                    if url:
                        # Update URL in database for future use
                        self._update_product_url(barcode, url)

                # If we have a URL, scrape the price
                if url:
                    price = self._scrape_online_price(url)
                    if price:
                        success = self._store_price(barcode, price)
                        if success:
                            self.prices_found += 1
                            logger.info(f"    ✅ Price: ₪{price}")
                        else:
                            self.errors += 1
                    else:
                        self.prices_not_found += 1
                        logger.info(f"    ❌ No price found")
                else:
                    self.prices_not_found += 1
                    logger.info(f"    ❌ Could not find product page")

            except Exception as e:
                logger.error(f"    ❌ Error processing {barcode}: {e}")
                self.errors += 1

            self.processed_count += 1

            # Save checkpoint every 10 products
            if self.processed_count % 10 == 0:
                self.save_checkpoint()
                logger.info(f"\n📊 Progress: {self.processed_count}/{total} | "
                          f"Found: {self.prices_found} | Not found: {self.prices_not_found} | "
                          f"Errors: {self.errors}\n")

            # Small delay to avoid overwhelming the server
            time.sleep(1)

    def run(self):
        """Main execution method"""
        logger.info("="*80)
        logger.info("🔍 SUPER-PHARM ONLINE PRICE BACKFILL")
        logger.info("="*80)

        # Load checkpoint
        self.load_checkpoint()

        # Connect to database
        if not self._connect_db():
            return False

        # Setup driver
        if not self._setup_driver():
            return False

        try:
            # Get products needing prices
            products = self.get_products_needing_prices()

            if not products:
                logger.info("✅ No products need price scraping")
                return True

            # Process products
            self.process_products(products)

            # Final stats
            logger.info("\n" + "="*80)
            logger.info("📊 FINAL STATISTICS")
            logger.info("="*80)
            logger.info(f"Total processed: {self.processed_count}")
            logger.info(f"Prices found: {self.prices_found}")
            logger.info(f"Prices not found: {self.prices_not_found}")
            logger.info(f"Errors: {self.errors}")
            logger.info("="*80)

            # Clean up checkpoint on success
            if os.path.exists(CHECKPOINT_FILE):
                os.remove(CHECKPOINT_FILE)
                logger.info("🗑️  Checkpoint file removed")

            return True

        except KeyboardInterrupt:
            logger.warning("\n⚠️  Process interrupted by user")
            self.save_checkpoint()
            logger.info("💾 Checkpoint saved - you can resume later")
            return False

        except Exception as e:
            logger.error(f"❌ Fatal error: {e}")
            self.save_checkpoint()
            return False

        finally:
            if self.driver:
                self.driver.quit()
            if self.conn:
                self.conn.close()
            logger.info("🔌 Resources cleaned up")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Backfill Super-Pharm online prices')
    parser.add_argument('--headless', action='store_true', default=True,
                       help='Run browser in headless mode (default: True)')
    parser.add_argument('--headed', action='store_true',
                       help='Run browser in headed mode (for debugging)')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit number of products to process (for testing)')

    args = parser.parse_args()

    headless = not args.headed if args.headed else args.headless

    backfill = SuperPharmOnlineBackfill(headless=headless, limit=args.limit)
    success = backfill.run()

    exit(0 if success else 1)
