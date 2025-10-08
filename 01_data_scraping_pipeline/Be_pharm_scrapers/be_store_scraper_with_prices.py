#!/usr/bin/env python3
"""
Be Store (Be Pharm) Commercial Website Scraper - WITH PRICE CAPTURE
Modified version that captures prices and stores them in the prices table.

This replaces the poor quality ETL/transparency portal data with real-time
commercial website prices.
"""

import time
import json
import argparse
import re
import logging
from datetime import datetime
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv
from urllib.parse import unquote

# Load environment variables
load_dotenv()

DB_NAME = os.getenv("DB_NAME", "price_comparison_app_v2")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "025655358")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

BE_PHARM_RETAILER_ID = 150
# Using a single store ID for commercial website prices
BE_PHARM_ONLINE_STORE_ID = 15001  # Special store ID for online prices
CHECKPOINT_FILE = 'be_store_prices_state.json'
LOG_FILE = 'be_store_prices_scraper.log'

# Setup logging
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('BeStorePricesScraper')
logger.setLevel(logging.INFO)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

# File handler
file_handler = logging.FileHandler(LOG_FILE, mode='a')
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)

class BeStorePricesScraper:
    """Enhanced scraper for Be Store (Be Pharm) that captures prices"""

    # Category URLs to scrape
    CATEGORIES = [
        ("תינוקות וילדים", "https://www.bestore.co.il/online/he/%D7%A7%D7%98%D7%92%D7%95%D7%A8%D7%99%D7%95%D7%AA/%D7%A4%D7%90%D7%A8%D7%9D-%D7%95%D7%A7%D7%95%D7%A1%D7%9E%D7%98%D7%99%D7%A7%D7%94/%D7%AA%D7%99%D7%A0%D7%95%D7%A7%D7%95%D7%AA-%D7%95%D7%99%D7%9C%D7%93%D7%99%D7%9D/c/B02"),
        ("טיפוח", "https://www.bestore.co.il/online/he/%D7%A7%D7%98%D7%92%D7%95%D7%A8%D7%99%D7%95%D7%AA/%D7%A4%D7%90%D7%A8%D7%9D-%D7%95%D7%A7%D7%95%D7%A1%D7%9E%D7%98%D7%99%D7%A7%D7%94/%D7%98%D7%99%D7%A4%D7%95%D7%97/c/B03"),
        ("רחצה והגיינה", "https://www.bestore.co.il/online/he/%D7%A7%D7%98%D7%92%D7%95%D7%A8%D7%99%D7%95%D7%AA/%D7%A4%D7%90%D7%A8%D7%9D-%D7%95%D7%A7%D7%95%D7%A1%D7%9E%D7%98%D7%99%D7%A7%D7%94/%D7%A8%D7%97%D7%A6%D7%94-%D7%95%D7%94%D7%92%D7%99%D7%99%D7%A0%D7%94/c/B04"),
        ("בשמים", "https://www.bestore.co.il/online/he/%D7%A7%D7%98%D7%92%D7%95%D7%A8%D7%99%D7%95%D7%AA/%D7%A4%D7%90%D7%A8%D7%9D-%D7%95%D7%A7%D7%95%D7%A1%D7%9E%D7%98%D7%99%D7%A7%D7%94/%D7%91%D7%A9%D7%9E%D7%99%D7%9D/c/B05"),
        ("איפור וטיפוח פנים", "https://www.bestore.co.il/online/he/%D7%A7%D7%98%D7%92%D7%95%D7%A8%D7%99%D7%95%D7%AA/%D7%A4%D7%90%D7%A8%D7%9D-%D7%95%D7%A7%D7%95%D7%A1%D7%9E%D7%98%D7%99%D7%A7%D7%94/%D7%90%D7%99%D7%A4%D7%95%D7%A8-%D7%95%D7%98%D7%99%D7%A4%D7%95%D7%97-%D7%A4%D7%A0%D7%99%D7%9D/c/B06"),
        ("טבע וויטמינים", "https://www.bestore.co.il/online/he/%D7%A7%D7%98%D7%92%D7%95%D7%A8%D7%99%D7%95%D7%AA/%D7%A4%D7%90%D7%A8%D7%9D-%D7%95%D7%A7%D7%95%D7%A1%D7%9E%D7%98%D7%99%D7%A7%D7%94/%D7%98%D7%91%D7%A2-%D7%95%D7%95%D7%99%D7%98%D7%9E%D7%99%D7%A0%D7%99%D7%9D/c/B07"),
        ("בית מרקחת", "https://www.bestore.co.il/online/he/%D7%A7%D7%98%D7%92%D7%95%D7%A8%D7%99%D7%95%D7%AA/%D7%A4%D7%90%D7%A8%D7%9D-%D7%95%D7%A7%D7%95%D7%A1%D7%9E%D7%98%D7%99%D7%A7%D7%94/%D7%91%D7%99%D7%AA-%D7%9E%D7%A8%D7%A7%D7%97%D7%AA/c/B08"),
    ]

    def __init__(self, dry_run=False, headless=True, test_mode=False, resume=False):
        self.dry_run = dry_run
        self.headless = headless
        self.test_mode = test_mode
        self.resume = resume
        self.conn = None
        self.cursor = None
        self.total_products_processed = 0
        self.total_prices_saved = 0
        self.scraped_data_collection = []
        self.completed_categories = []
        self.test_mode_limit = 10  # Products per category in test mode
        self._define_sql_commands()
        self._ensure_online_store()

    def _define_sql_commands(self):
        """Define SQL commands for database operations"""

        # Upsert canonical product
        self.SQL_UPSERT_CANONICAL = """
            INSERT INTO canonical_products (
                barcode, name, brand, description, image_url,
                source_retailer_id, last_scraped_at, created_at
            )
            VALUES (
                %(barcode)s, %(name)s, %(brand)s, %(description)s, %(image_url)s,
                %(source_retailer_id)s, NOW(), NOW()
            )
            ON CONFLICT (barcode) DO UPDATE SET
                name = EXCLUDED.name,
                brand = EXCLUDED.brand,
                description = EXCLUDED.description,
                image_url = EXCLUDED.image_url,
                source_retailer_id = EXCLUDED.source_retailer_id,
                last_scraped_at = NOW();
        """

        # Insert or get retailer_product
        self.SQL_UPSERT_RETAILER_PRODUCT = """
            INSERT INTO retailer_products (
                barcode, retailer_id, retailer_item_code, original_retailer_name
            )
            VALUES (
                %(barcode)s, %(retailer_id)s, %(retailer_item_code)s, %(original_retailer_name)s
            )
            ON CONFLICT (retailer_id, retailer_item_code) DO UPDATE SET
                barcode = EXCLUDED.barcode,
                original_retailer_name = EXCLUDED.original_retailer_name
            RETURNING retailer_product_id;
        """

        # Delete old prices for this product/store
        self.SQL_DELETE_OLD_PRICES = """
            DELETE FROM prices
            WHERE retailer_product_id = %(retailer_product_id)s
            AND store_id = %(store_id)s;
        """

        # Insert new price
        self.SQL_INSERT_PRICE = """
            INSERT INTO prices (
                retailer_product_id, store_id, price, price_timestamp, scraped_at
            )
            VALUES (
                %(retailer_product_id)s, %(store_id)s, %(price)s, NOW(), NOW()
            );
        """

    def _ensure_online_store(self):
        """Ensure the online store exists in the stores table"""
        if self.dry_run:
            return

        try:
            conn = psycopg2.connect(
                dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
                host=DB_HOST, port=DB_PORT
            )
            cursor = conn.cursor()

            # Check if online store exists
            cursor.execute("""
                SELECT storeid FROM stores
                WHERE storeid = %s AND retailerid = %s;
            """, (BE_PHARM_ONLINE_STORE_ID, BE_PHARM_RETAILER_ID))

            if not cursor.fetchone():
                # Create online store
                cursor.execute("""
                    INSERT INTO stores (
                        storeid, retailerid, storename,
                        retailerspecificstoreid, isactive, createdat
                    )
                    VALUES (
                        %s, %s, %s, %s, true, NOW()
                    );
                """, (
                    BE_PHARM_ONLINE_STORE_ID,
                    BE_PHARM_RETAILER_ID,
                    'Be Pharm Online Store',
                    'ONLINE'
                ))
                conn.commit()
                logger.info(f"✅ Created Be Pharm online store (ID: {BE_PHARM_ONLINE_STORE_ID})")
            else:
                logger.info(f"✅ Be Pharm online store already exists (ID: {BE_PHARM_ONLINE_STORE_ID})")

            cursor.close()
            conn.close()
        except Exception as e:
            logger.error(f"❌ Error ensuring online store: {e}")

    def _setup_driver(self):
        """Setup Selenium WebDriver with appropriate options"""
        options = Options()
        if self.headless:
            options.add_argument("--headless")
        options.add_argument("--start-maximized")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        return driver

    def _connect_db(self):
        """Establishes the database connection"""
        if self.dry_run:
            return
        try:
            self.conn = psycopg2.connect(
                dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
                host=DB_HOST, port=DB_PORT
            )
            self.cursor = self.conn.cursor()
            logger.info("✅ Database connection successful.")
        except psycopg2.OperationalError as e:
            logger.error(f"❌ Could not connect to the database: {e}")
            self.conn = None

    def _close_db(self):
        """Closes the database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("🔌 Database connection closed.")

    def _scroll_to_bottom(self, driver):
        """Handles infinite scrolling by scrolling until no new content loads"""
        last_height = driver.execute_script("return document.body.scrollHeight")
        no_change_count = 0
        scroll_count = 0
        max_scrolls = 50

        logger.info(f"  📜 Starting infinite scroll...")

        while no_change_count < 3 and scroll_count < max_scrolls:
            scroll_count += 1
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            new_height = driver.execute_script("return document.body.scrollHeight")

            if new_height == last_height:
                no_change_count += 1
            else:
                no_change_count = 0
                logger.info(f"    📜 Loaded more content (scroll #{scroll_count})")

            last_height = new_height

        logger.info(f"  📜 Scrolling complete after {scroll_count} scrolls")

    def _extract_products_from_page(self, driver, category_name):
        """Extract all product data including prices from the current page"""
        products = []

        try:
            time.sleep(2)
            product_elements = driver.find_elements(By.CSS_SELECTOR, "div.tile")

            if not product_elements:
                logger.warning(f"  ⚠️ No products found")
                return products

            logger.info(f"  📦 Found {len(product_elements)} product tiles")

            for element in product_elements:
                try:
                    product_data = {}

                    # Extract product code/barcode
                    product_code = None
                    try:
                        code_elem = element.find_element(By.CSS_SELECTOR, "[data-product-code]")
                        product_code = code_elem.get_attribute('data-product-code')
                    except:
                        pass

                    if not product_code:
                        try:
                            link_elem = element.find_element(By.CSS_SELECTOR, "a[href*='/p/P_']")
                            href = link_elem.get_attribute('href')
                            match = re.search(r'/p/(P_[\d]+)', href)
                            if match:
                                product_code = match.group(1)
                        except:
                            pass

                    if not product_code:
                        continue

                    # Extract barcode from product code
                    if product_code.startswith('P_'):
                        product_data['barcode'] = product_code[2:]
                    else:
                        product_data['barcode'] = product_code

                    product_data['item_code'] = product_code

                    # Extract product name
                    name = None
                    try:
                        name_elem = element.find_element(By.CSS_SELECTOR, ".description strong")
                        name = name_elem.text.strip()
                    except:
                        try:
                            link_elem = element.find_element(By.CSS_SELECTOR, "a[title]")
                            name = link_elem.get_attribute('title')
                        except:
                            pass

                    product_data['name'] = name if name else "N/A"

                    # CRITICAL: Extract price
                    price = None
                    try:
                        # Try multiple strategies to find price
                        price_selectors = [
                            ".price",
                            ".product-price",
                            "[class*='price']",
                            "span:has-text('₪')"
                        ]

                        price_text = None
                        for selector in price_selectors:
                            try:
                                price_elem = element.find_element(By.CSS_SELECTOR, selector)
                                if price_elem.text and '₪' in price_elem.text:
                                    price_text = price_elem.text.strip()
                                    break
                            except:
                                continue

                        # If no price class found, search for ₪ symbol
                        if not price_text:
                            spans = element.find_elements(By.TAG_NAME, "span")
                            for span in spans:
                                if '₪' in span.text:
                                    price_text = span.text.strip()
                                    break

                        # Extract numeric value from price text
                        if price_text:
                            # Remove currency symbol and extract number
                            price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
                            if price_match:
                                price = float(price_match.group())

                    except Exception as e:
                        logger.debug(f"    Could not extract price: {e}")

                    product_data['price'] = price

                    # Extract image URL
                    try:
                        img_elem = element.find_element(By.CSS_SELECTOR, "img")
                        img_url = img_elem.get_attribute('src')
                        if not img_url or 'placeholder' in img_url.lower():
                            img_url = img_elem.get_attribute('data-src')
                        if img_url and not img_url.startswith('http'):
                            img_url = f"https://www.bestore.co.il{img_url}"
                        product_data['image_url'] = img_url
                    except:
                        product_data['image_url'] = None

                    product_data['category'] = category_name
                    product_data['source_retailer_id'] = BE_PHARM_RETAILER_ID

                    products.append(product_data)

                except Exception as e:
                    logger.debug(f"    Error extracting product: {e}")
                    continue

        except Exception as e:
            logger.error(f"  ❌ Error extracting products: {e}")

        return products

    def _process_product(self, product_data):
        """Process a single product - save to canonical_products and prices"""

        if self.dry_run:
            self.scraped_data_collection.append(product_data)
            self.total_products_processed += 1
            if product_data.get('price'):
                self.total_prices_saved += 1
            return

        try:
            # 1. Upsert to canonical_products
            self.cursor.execute(self.SQL_UPSERT_CANONICAL, {
                'barcode': product_data['barcode'],
                'name': product_data.get('name', 'N/A'),
                'brand': product_data.get('brand'),
                'description': product_data.get('description'),
                'image_url': product_data.get('image_url'),
                'source_retailer_id': BE_PHARM_RETAILER_ID
            })

            # 2. Upsert to retailer_products and get ID
            self.cursor.execute(self.SQL_UPSERT_RETAILER_PRODUCT, {
                'barcode': product_data['barcode'],
                'retailer_id': BE_PHARM_RETAILER_ID,
                'retailer_item_code': product_data['item_code'],
                'original_retailer_name': product_data.get('name', 'N/A')
            })

            result = self.cursor.fetchone()
            if result:
                retailer_product_id = result[0]

                # 3. If we have a price, save it
                if product_data.get('price'):
                    # Delete old price for this product/store
                    self.cursor.execute(self.SQL_DELETE_OLD_PRICES, {
                        'retailer_product_id': retailer_product_id,
                        'store_id': BE_PHARM_ONLINE_STORE_ID
                    })

                    # Insert new price
                    self.cursor.execute(self.SQL_INSERT_PRICE, {
                        'retailer_product_id': retailer_product_id,
                        'store_id': BE_PHARM_ONLINE_STORE_ID,
                        'price': product_data['price']
                    })
                    self.total_prices_saved += 1

            self.total_products_processed += 1

        except psycopg2.Error as e:
            logger.error(f"DATABASE ERROR on item {product_data.get('item_code')}: {e}")
            self.conn.rollback()
            return

    def scrape(self):
        """Main scraping function"""
        logger.info("="*60)
        logger.info("🚀 Starting Be Store scraper WITH PRICE CAPTURE")
        logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'DATABASE'} | Test: {self.test_mode}")
        logger.info("="*60)

        if not self.dry_run:
            self._connect_db()
            if not self.conn:
                return

        driver = self._setup_driver()

        try:
            for idx, (category_name, category_url) in enumerate(self.CATEGORIES, 1):
                logger.info(f"\n{'='*60}")
                logger.info(f"🔍 Category {idx}/{len(self.CATEGORIES)}: {category_name}")
                logger.info(f"{'='*60}")

                driver.get(category_url)
                time.sleep(3)

                # Scroll to load all products (unless in test mode)
                if not self.test_mode:
                    self._scroll_to_bottom(driver)
                else:
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)

                # Extract all products from the page
                products = self._extract_products_from_page(driver, category_name)

                # Limit products in test mode
                if self.test_mode and len(products) > self.test_mode_limit:
                    products = products[:self.test_mode_limit]

                logger.info(f"  ✅ Extracted {len(products)} products")

                # Count products with prices
                products_with_prices = sum(1 for p in products if p.get('price'))
                logger.info(f"  💰 {products_with_prices} products have prices")

                # Process each product
                for i, product in enumerate(products, 1):
                    self._process_product(product)
                    if i % 10 == 0:
                        logger.info(f"    Processed {i}/{len(products)} products...")

                # Commit after each category
                if not self.dry_run and self.conn:
                    self.conn.commit()
                    logger.info(f"  💾 Committed {len(products)} products")

                time.sleep(2)

                # Check test mode limit
                if self.test_mode and self.total_products_processed >= self.test_mode_limit * 2:
                    logger.info("\n🧪 TEST MODE: Stopping early")
                    break

        except KeyboardInterrupt:
            logger.warning("\n⚠️ Scraper interrupted by user")
        except Exception as e:
            logger.error(f"❌ Fatal error during scraping: {e}")
        finally:
            driver.quit()
            logger.info("🌐 Browser closed")

        if self.dry_run:
            self._save_dry_run_file()
        else:
            if self.conn:
                self.conn.commit()
            logger.info(f"\n{'='*60}")
            logger.info(f"✅ SCRAPING COMPLETE")
            logger.info(f"Total products processed: {self.total_products_processed}")
            logger.info(f"Total prices saved: {self.total_prices_saved}")
            logger.info(f"{'='*60}")
            self._close_db()

    def _save_dry_run_file(self):
        """Save dry run results to JSON file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"be_store_prices_dry_run_{timestamp}.json"

        with_prices = sum(1 for p in self.scraped_data_collection if p.get('price'))

        output_data = {
            "scraper": "Be Store Prices Scraper",
            "timestamp": timestamp,
            "statistics": {
                "total_products": len(self.scraped_data_collection),
                "products_with_prices": with_prices,
                "price_coverage": f"{with_prices * 100 / len(self.scraped_data_collection):.1f}%" if self.scraped_data_collection else "0%"
            },
            "products": self.scraped_data_collection
        }

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)

        logger.info(f"\n📁 Dry run data saved to: {filename}")
        logger.info(f"   Total products: {len(self.scraped_data_collection)}")
        logger.info(f"   Products with prices: {with_prices}")

    def run(self):
        """Entry point for the scraper"""
        self.scrape()


def main():
    parser = argparse.ArgumentParser(description='Be Store (Be Pharm) Prices Scraper')
    parser.add_argument('--dry-run', action='store_true', help='Run without database operations')
    parser.add_argument('--test', action='store_true', help='Test mode - scrape limited products')
    parser.add_argument('--show-browser', action='store_true', help='Show browser window (not headless)')

    args = parser.parse_args()

    scraper = BeStorePricesScraper(
        dry_run=args.dry_run,
        headless=not args.show_browser,
        test_mode=args.test
    )

    scraper.run()


if __name__ == "__main__":
    main()