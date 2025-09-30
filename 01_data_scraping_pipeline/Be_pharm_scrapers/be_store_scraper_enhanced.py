#!/usr/bin/env python3
"""
Be Store (Be Pharm) Commercial Website Scraper - ENHANCED EXPERIMENTAL VERSION

This experimental script adds three key enhancements to the Be Pharm scraper:
1. Properly sets source_retailer_id = 150 for canonical products
2. Captures high-resolution product images
3. Extracts product descriptions

This is an experimental version for testing - DO NOT use in production until validated!
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
import os
from dotenv import load_dotenv
from urllib.parse import unquote

# Load environment variables
load_dotenv()

DB_NAME = os.getenv("DB_NAME", "price_comparison_app_v2")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "***REMOVED***")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

BE_PHARM_RETAILER_ID = 150
CHECKPOINT_FILE = 'be_store_scraper_enhanced_state.json'
LOG_FILE = 'be_store_scraper_enhanced.log'

# Setup logging
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('BeStoreScraperEnhanced')
logger.setLevel(logging.INFO)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

# File handler
file_handler = logging.FileHandler(LOG_FILE, mode='a')
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)

class BeStoreScraperEnhanced:
    """Enhanced scraper for Be Store (Be Pharm) commercial website"""

    # Category URLs to scrape
    CATEGORIES = [
        ("תינוקות וילדים", "https://www.bestore.co.il/online/he/%D7%A7%D7%98%D7%92%D7%95%D7%A8%D7%99%D7%95%D7%AA/%D7%A4%D7%90%D7%A8%D7%9D-%D7%95%D7%A7%D7%95%D7%A1%D7%9E%D7%98%D7%99%D7%A7%D7%94/%D7%AA%D7%99%D7%A0%D7%95%D7%A7%D7%95%D7%AA-%D7%95%D7%99%D7%9C%D7%93%D7%99%D7%9D/c/B02"),
        ("טיפוח", "https://www.bestore.co.il/online/he/%D7%A7%D7%98%D7%92%D7%95%D7%A8%D7%99%D7%95%D7%AA/%D7%A4%D7%90%D7%A8%D7%9D-%D7%95%D7%A7%D7%95%D7%A1%D7%9E%D7%98%D7%99%D7%A7%D7%94/%D7%98%D7%99%D7%A4%D7%95%D7%97/c/B03"),
        ("רחצה והגיינה", "https://www.bestore.co.il/online/he/%D7%A7%D7%98%D7%92%D7%95%D7%A8%D7%99%D7%95%D7%AA/%D7%A4%D7%90%D7%A8%D7%9D-%D7%95%D7%A7%D7%95%D7%A1%D7%9E%D7%98%D7%99%D7%A7%D7%94/%D7%A8%D7%97%D7%A6%D7%94-%D7%95%D7%94%D7%92%D7%99%D7%99%D7%A0%D7%94/c/B04"),
        ("בשמים", "https://www.bestore.co.il/online/he/%D7%A7%D7%98%D7%92%D7%95%D7%A8%D7%99%D7%95%D7%AA/%D7%A4%D7%90%D7%A8%D7%9D-%D7%95%D7%A7%D7%95%D7%A1%D7%9E%D7%98%D7%99%D7%A7%D7%94/%D7%91%D7%A9%D7%9E%D7%99%D7%9D/c/B05"),
        ("איפור וטיפוח פנים", "https://www.bestore.co.il/online/he/%D7%A7%D7%98%D7%92%D7%95%D7%A8%D7%99%D7%95%D7%AA/%D7%A4%D7%90%D7%A8%D7%9D-%D7%95%D7%A7%D7%95%D7%A1%D7%9E%D7%98%D7%99%D7%A7%D7%94/%D7%90%D7%99%D7%A4%D7%95%D7%A8-%D7%95%D7%98%D7%99%D7%A4%D7%95%D7%97-%D7%A4%D7%A0%D7%99%D7%9D/c/B06"),
        ("טבע וויטמינים", "https://www.bestore.co.il/online/he/%D7%A7%D7%98%D7%92%D7%95%D7%A8%D7%99%D7%95%D7%AA/%D7%A4%D7%90%D7%A8%D7%9D-%D7%95%D7%A7%D7%95%D7%A1%D7%9E%D7%98%D7%99%D7%A7%D7%94/%D7%98%D7%91%D7%A2-%D7%95%D7%95%D7%99%D7%98%D7%9E%D7%99%D7%A0%D7%99%D7%9D/c/B07"),
        ("בית מרקחת", "https://www.bestore.co.il/online/he/%D7%A7%D7%98%D7%92%D7%95%D7%A8%D7%99%D7%95%D7%AA/%D7%A4%D7%90%D7%A8%D7%9D-%D7%95%D7%A7%D7%95%D7%A1%D7%9E%D7%98%D7%99%D7%A7%D7%94/%D7%91%D7%99%D7%AA-%D7%9E%D7%A8%D7%A7%D7%97%D7%AA/c/B08"),
        ("בית וניקיון", "https://www.bestore.co.il/online/he/%D7%A7%D7%98%D7%92%D7%95%D7%A8%D7%99%D7%95%D7%AA/%D7%A4%D7%90%D7%A8%D7%9D-%D7%95%D7%A7%D7%95%D7%A1%D7%9E%D7%98%D7%99%D7%A7%D7%94/%D7%91%D7%99%D7%AA-%D7%95%D7%A0%D7%99%D7%A7%D7%99%D7%95%D7%9F/c/B09"),
        ("נרות ואווירה", "https://www.bestore.co.il/online/he/%D7%A7%D7%98%D7%92%D7%95%D7%A8%D7%99%D7%95%D7%AA/%D7%A4%D7%90%D7%A8%D7%9D-%D7%95%D7%A7%D7%95%D7%A1%D7%9E%D7%98%D7%99%D7%A7%D7%94/%D7%A0%D7%A8%D7%95%D7%AA-%D7%95%D7%90%D7%95%D7%95%D7%99%D7%A8%D7%94/c/B13"),
    ]

    def __init__(self, dry_run=False, headless=True, test_mode=False, resume=False):
        self.dry_run = dry_run
        self.headless = headless
        self.test_mode = test_mode
        self.resume = resume
        self.conn = None
        self.cursor = None
        self.total_products_processed = 0
        self.scraped_data_collection = []
        self.completed_categories = []
        self.test_mode_limit = 10  # Products per category in test mode
        self._define_sql_commands()

    def _define_sql_commands(self):
        """Define SQL commands for database operations"""
        # This command will INSERT a new product or UPDATE it if the barcode already exists.
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
                category = EXCLUDED.category,
                source_retailer_id = EXCLUDED.source_retailer_id,
                last_scraped_at = NOW();
        """

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

        # Add user agent
        options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)

        # Execute script to remove webdriver property
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

    def load_checkpoint(self):
        """Load checkpoint to resume from last completed category"""
        if self.resume and os.path.exists(CHECKPOINT_FILE):
            try:
                with open(CHECKPOINT_FILE, 'r') as f:
                    state = json.load(f)
                    self.completed_categories = state.get('completed_categories', [])
                    self.total_products_processed = state.get('total_products_processed', 0)
                    logger.info(f"✅ Resuming scrape. Found {len(self.completed_categories)} completed categories.")
                    logger.info(f"   Previous total products: {self.total_products_processed}")
                    return True
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"⚠️ Could not read checkpoint file, starting fresh. Error: {e}")
        return False

    def save_checkpoint(self, category_name=None):
        """Save checkpoint after completing a category"""
        if category_name:
            self.completed_categories.append(category_name)

        state = {
            'completed_categories': self.completed_categories,
            'total_products_processed': self.total_products_processed,
            'last_updated': datetime.now().isoformat()
        }

        try:
            with open(CHECKPOINT_FILE, 'w') as f:
                json.dump(state, f, indent=4)
            logger.info(f"💾 Checkpoint saved. {len(self.completed_categories)} categories completed.")
        except IOError as e:
            logger.error(f"❌ Could not save checkpoint file. Error: {e}")

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
        max_scrolls = 50  # Safety limit to prevent infinite scrolling

        logger.info(f"  📜 Starting infinite scroll (initial height: {last_height})")

        while no_change_count < 3 and scroll_count < max_scrolls:
            scroll_count += 1
            # Scroll down to bottom
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # Wait for page to load
            time.sleep(2)

            # Calculate new scroll height and compare with last scroll height
            new_height = driver.execute_script("return document.body.scrollHeight")

            if new_height == last_height:
                no_change_count += 1
                logger.debug(f"    📜 No new content (attempt {no_change_count}/3, scroll #{scroll_count})")
            else:
                no_change_count = 0
                loaded_amount = new_height - last_height
                logger.info(f"    📜 Loaded more content! Height: {last_height} → {new_height} (+{loaded_amount}px, scroll #{scroll_count})")

            last_height = new_height

        logger.info(f"  📜 Scrolling complete after {scroll_count} scrolls")

    def _extract_products_from_page(self, driver, category_name):
        """Extract all product data from the current page - ENHANCED VERSION"""
        products = []

        try:
            # Wait for products to be present
            time.sleep(2)  # Give page time to render

            # Find product containers - Be Store uses div.tile for products
            product_elements = driver.find_elements(By.CSS_SELECTOR, "div.tile")

            if not product_elements:
                logger.warning(f"  ⚠️ No products found with div.tile selector")
                return products

            logger.info(f"  📦 Found {len(product_elements)} product tiles on page")

            for element in product_elements:
                try:
                    product_data = {}

                    # Extract product code - try multiple strategies
                    product_code = None

                    # Strategy 1: data-product-code attribute
                    try:
                        code_elem = element.find_element(By.CSS_SELECTOR, "[data-product-code]")
                        product_code = code_elem.get_attribute('data-product-code')
                    except:
                        pass

                    # Strategy 2: Extract from URL in href attribute
                    if not product_code:
                        try:
                            link_elem = element.find_element(By.CSS_SELECTOR, "a[href*='/p/P_']")
                            href = link_elem.get_attribute('href')
                            # Extract product code from URL like /p/P_7296073485056
                            import re
                            match = re.search(r'/p/(P_[\d]+)', href)
                            if match:
                                product_code = match.group(1)
                        except:
                            pass

                    if not product_code:
                        # Skip if no product code found
                        continue

                    product_data['item_code'] = product_code

                    # Extract barcode from item_code (format: P_BARCODE)
                    if product_code and product_code.startswith('P_'):
                        product_data['barcode'] = product_code[2:]  # Remove 'P_' prefix
                    else:
                        product_data['barcode'] = product_code  # Use as-is if no prefix

                    # Extract product name - try multiple strategies
                    name = None

                    # Strategy 1: From description strong tag
                    try:
                        name_elem = element.find_element(By.CSS_SELECTOR, ".description strong")
                        name = name_elem.text.strip()
                    except:
                        pass

                    # Strategy 2: From title attribute of link
                    if not name:
                        try:
                            link_elem = element.find_element(By.CSS_SELECTOR, "a[title]")
                            name = link_elem.get_attribute('title')
                        except:
                            pass

                    # Strategy 3: From img alt attribute
                    if not name:
                        try:
                            img_elem = element.find_element(By.TAG_NAME, "img")
                            name = img_elem.get_attribute('alt') or img_elem.get_attribute('title')
                        except:
                            pass

                    product_data['name'] = name if name else "N/A"

                    # Extract price - look for elements containing ₪
                    try:
                        # Try multiple strategies to find price
                        price_found = False

                        # First try specific price classes
                        for price_selector in [".price", ".product-price", "[class*='price']"]:
                            try:
                                price_elem = element.find_element(By.CSS_SELECTOR, price_selector)
                                if price_elem.text:
                                    price_text = price_elem.text.strip()
                                    break
                            except:
                                continue

                        # If no price class found, search for ₪ symbol in spans
                        if not price_found:
                            spans = element.find_elements(By.TAG_NAME, "span")
                            for span in spans:
                                if '₪' in span.text:
                                    price_text = span.text.strip()
                                    price_found = True
                                    break

                        # Extract numeric value from price text
                        if 'price_text' in locals():
                            import re
                            # Remove currency symbol and extract number
                            price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
                            if price_match:
                                product_data['price'] = float(price_match.group())
                            else:
                                product_data['price'] = None
                        else:
                            product_data['price'] = None

                    except Exception as e:
                        product_data['price'] = None

                    # ========= ENHANCEMENT 1: Add source_retailer_id =========
                    product_data['source_retailer_id'] = BE_PHARM_RETAILER_ID  # CRITICAL FIX!

                    # ========= ENHANCEMENT 2: Enhanced Image URL Extraction =========
                    try:
                        # Try multiple selectors for better image capture
                        img_selectors = [
                            "img.pic",           # Main product image class
                            "img[alt]",         # Any image with alt text
                            ".image-container img",  # Image inside container
                            ".product-image img",    # Product image class
                            "picture img",           # Picture element img
                            "img[src*='product']"    # Image with 'product' in src
                        ]

                        img_url = None
                        for selector in img_selectors:
                            try:
                                img_elem = element.find_element(By.CSS_SELECTOR, selector)
                                img_url = img_elem.get_attribute('src')

                                # Check for data-src (lazy loading)
                                if not img_url or img_url == 'data:image' or 'placeholder' in img_url.lower():
                                    img_url = img_elem.get_attribute('data-src')

                                # Ensure we get full URL
                                if img_url and not img_url.startswith('http'):
                                    img_url = f"https://www.bestore.co.il{img_url}" if img_url.startswith('/') else f"https://www.bestore.co.il/{img_url}"

                                if img_url and 'placeholder' not in img_url.lower():
                                    break
                            except:
                                continue

                        product_data['image_url'] = img_url if img_url else None

                    except Exception as e:
                        product_data['image_url'] = None
                        logger.debug(f"    No image found for product: {product_code}")

                    # ========= ENHANCEMENT 3: Add Description Extraction =========
                    try:
                        # Try multiple strategies for description
                        description = None

                        # Strategy 1: Look for dedicated description element
                        description_selectors = [
                            ".description",           # Main description class
                            ".product-description",   # Product description class
                            ".description-wrapper",   # Description wrapper
                            ".item-description",      # Item description
                            "[class*='description']", # Any class with 'description'
                            ".subtitle",             # Sometimes used for short descriptions
                            ".product-info"          # Product info section
                        ]

                        for selector in description_selectors:
                            try:
                                desc_elem = element.find_element(By.CSS_SELECTOR, selector)
                                # Get text but skip if it's the same as the name
                                desc_text = desc_elem.text.strip()
                                if desc_text and desc_text != product_data.get('name'):
                                    description = desc_text
                                    break
                            except:
                                continue

                        # Strategy 2: If no description found, try to get subtitle or secondary text
                        if not description:
                            try:
                                # Look for text elements that might contain description
                                text_elems = element.find_elements(By.CSS_SELECTOR, "p, span.subtitle, div.info")
                                for text_elem in text_elems:
                                    text = text_elem.text.strip()
                                    # Check if it's not price, not name, and has meaningful content
                                    if text and '₪' not in text and text != product_data.get('name') and len(text) > 10:
                                        description = text
                                        break
                            except:
                                pass

                        product_data['description'] = description if description else None

                    except Exception as e:
                        product_data['description'] = None
                        logger.debug(f"    No description found for product: {product_code}")

                    # Extract product URL (if it's a link)
                    try:
                        link_elem = element.find_element(By.TAG_NAME, "a")
                        href = link_elem.get_attribute('href')
                        if href and href != 'javascript:void(0)':
                            product_data['url'] = href
                        else:
                            product_data['url'] = None
                    except NoSuchElementException:
                        product_data['url'] = None

                    # Add category
                    product_data['category'] = category_name

                    # Extract brand if available (might be in description or separate element)
                    try:
                        brand_elem = element.find_element(By.CSS_SELECTOR, ".brand, .product-brand, [class*='brand']")
                        product_data['brand'] = brand_elem.text.strip()
                    except NoSuchElementException:
                        # Try to extract from product name (often first word)
                        if product_data.get('name'):
                            words = product_data['name'].split()
                            if words and words[0].isupper():
                                product_data['brand'] = words[0]
                            else:
                                product_data['brand'] = None
                        else:
                            product_data['brand'] = None

                    # Only add if we have at least a name and code
                    if product_data.get('name') and product_data.get('item_code'):
                        products.append(product_data)

                        # Log enhanced data capture statistics periodically
                        if len(products) % 20 == 0:
                            recent_20 = products[-20:]
                            with_images = sum(1 for p in recent_20 if p.get('image_url'))
                            with_desc = sum(1 for p in recent_20 if p.get('description'))
                            logger.info(f"    📊 Last 20 products: {with_images}/20 have images, {with_desc}/20 have descriptions")

                except Exception as e:
                    # Silently skip products that can't be extracted
                    continue

        except TimeoutException:
            logger.warning("  ⚠️ Timeout waiting for products to load")
        except Exception as e:
            logger.error(f"  ❌ Error extracting products: {e}")

        # Log final statistics for this page
        if products:
            total = len(products)
            with_images = sum(1 for p in products if p.get('image_url'))
            with_desc = sum(1 for p in products if p.get('description'))
            logger.info(f"  📊 Page stats: {total} products, {with_images} with images ({with_images*100//total}%), {with_desc} with descriptions ({with_desc*100//total}%)")

        return products

    def _process_product(self, product_data):
        """Process and store a single product"""
        # For dry run, just collect the data
        if self.dry_run:
            self.scraped_data_collection.append(product_data)
        else:
            # --- LIVE DATABASE MODE ---
            # Execute the upsert command for the canonical_products table
            try:
                # Ensure all keys exist, providing None as a default
                product_data.setdefault('description', None)
                product_data.setdefault('brand', None)
                product_data.setdefault('image_url', None)
                product_data.setdefault('url', None)

                self.cursor.execute(self.SQL_UPSERT_CANONICAL, product_data)
            except psycopg2.Error as e:
                logger.error(f"DATABASE ERROR on item {product_data.get('item_code')}: {e}")
                self.conn.rollback()  # Rollback the failed transaction
                return  # Skip this product

        self.total_products_processed += 1

        # Log progress periodically
        if self.total_products_processed % 50 == 0:
            logger.info(f"📊 Progress: {self.total_products_processed} products processed")

    def scrape(self):
        """Main scraping function"""
        logger.info("="*60)
        logger.info("🚀 Starting Be Store ENHANCED scraper...")
        logger.info("🔬 EXPERIMENTAL VERSION with image & description extraction")
        logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'DATABASE'} | Test: {self.test_mode} | Resume: {self.resume}")
        logger.info("="*60)

        if not self.dry_run:
            self._connect_db()
            if not self.conn:
                return

        # Load checkpoint if resuming
        self.load_checkpoint()

        driver = self._setup_driver()

        # Filter out completed categories if resuming
        categories_to_scrape = [(name, url) for name, url in self.CATEGORIES
                               if name not in self.completed_categories]
        total_categories = len(categories_to_scrape)

        logger.info(f"📋 Categories to scrape: {total_categories} (skipping {len(self.completed_categories)} already completed)")

        try:
            for idx, (category_name, category_url) in enumerate(categories_to_scrape, 1):
                logger.info(f"\n{'='*60}")
                logger.info(f"🔍 Category {idx}/{total_categories}: {category_name}")
                logger.info(f"   URL: {category_url}")
                logger.info(f"{'='*60}")

                driver.get(category_url)

                # Wait for initial page load
                time.sleep(3)

                # Scroll to load all products (unless in test mode)
                if not self.test_mode:
                    logger.info("  📜 Starting infinite scroll...")
                    self._scroll_to_bottom(driver)
                else:
                    logger.info("  🧪 TEST MODE: Skipping infinite scroll")
                    # Just scroll once for test mode
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)

                # Extract all products from the page
                products = self._extract_products_from_page(driver, category_name)

                # Limit products in test mode
                if self.test_mode and len(products) > self.test_mode_limit:
                    products = products[:self.test_mode_limit]
                    logger.info(f"  🧪 TEST MODE: Limited to {self.test_mode_limit} products")

                logger.info(f"  ✅ Extracted {len(products)} products from {category_name}")

                # Process each product
                logger.info(f"  📦 Processing products...")
                for i, product in enumerate(products, 1):
                    self._process_product(product)
                    if i % 10 == 0:
                        logger.info(f"    Processed {i}/{len(products)} products...")

                # Commit after each category if not in dry run
                if not self.dry_run and self.conn:
                    self.conn.commit()
                    logger.info(f"  💾 Committed {len(products)} products to database")

                # Save checkpoint after completing category
                self.save_checkpoint(category_name)

                logger.info(f"\n  📊 Category Summary:")
                logger.info(f"    - Category: {category_name}")
                logger.info(f"    - Products found: {len(products)}")
                logger.info(f"    - Total processed so far: {self.total_products_processed}")

                # Small delay between categories
                time.sleep(2)

                # Check test mode limit for total products
                if self.test_mode and self.total_products_processed >= self.test_mode_limit * 2:
                    logger.info("\n🧪 TEST MODE: Reached overall product limit, stopping early")
                    break

        except KeyboardInterrupt:
            logger.warning("\n⚠️ Scraper interrupted by user")
            logger.info("Progress has been saved. Use --resume to continue.")
        except Exception as e:
            logger.error(f"❌ Fatal error during scraping: {e}")
            logger.info("Progress has been saved. Use --resume to continue.")
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
            logger.info(f"Categories completed: {len(self.completed_categories)}")
            logger.info(f"{'='*60}")
            self._close_db()

        # Clean up checkpoint file if scraping completed successfully
        if not self.test_mode and len(self.completed_categories) == len(self.CATEGORIES):
            if os.path.exists(CHECKPOINT_FILE):
                os.remove(CHECKPOINT_FILE)
                logger.info("🧹 Checkpoint file removed (scraping fully completed)")

    def _save_dry_run_file(self):
        """Save dry run results to JSON file with enhancement statistics"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"be_store_enhanced_dry_run_{timestamp}.json"

        # Calculate statistics
        total = len(self.scraped_data_collection)
        with_images = sum(1 for p in self.scraped_data_collection if p.get('image_url'))
        with_desc = sum(1 for p in self.scraped_data_collection if p.get('description'))
        with_retailer_id = sum(1 for p in self.scraped_data_collection if p.get('source_retailer_id') == BE_PHARM_RETAILER_ID)

        stats = {
            "total_products": total,
            "products_with_images": with_images,
            "products_with_descriptions": with_desc,
            "products_with_source_retailer_id": with_retailer_id,
            "image_coverage_percent": round(with_images * 100 / total, 2) if total > 0 else 0,
            "description_coverage_percent": round(with_desc * 100 / total, 2) if total > 0 else 0,
            "retailer_id_coverage_percent": round(with_retailer_id * 100 / total, 2) if total > 0 else 0
        }

        output = {
            "metadata": {
                "scraper_version": "ENHANCED_EXPERIMENTAL",
                "timestamp": datetime.now().isoformat(),
                "statistics": stats
            },
            "products": self.scraped_data_collection
        }

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=4)

        logger.info(f"\n✅ Dry run complete. {total} products saved to '{filename}'")
        logger.info(f"📊 Enhancement Statistics:")
        logger.info(f"   - Source Retailer ID: {with_retailer_id}/{total} ({stats['retailer_id_coverage_percent']}%)")
        logger.info(f"   - Images captured: {with_images}/{total} ({stats['image_coverage_percent']}%)")
        logger.info(f"   - Descriptions captured: {with_desc}/{total} ({stats['description_coverage_percent']}%)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Be Store with ENHANCED features (Experimental)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Output to JSON file instead of database")
    parser.add_argument("--no-headless", action="store_true",
                        help="Run browser in visible mode (not headless)")
    parser.add_argument("--test-mode", action="store_true",
                        help="Scrape only a few items per category for testing")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from the last completed category")

    args = parser.parse_args()

    logger.info("="*60)
    logger.info("BE STORE SCRAPER - ENHANCED EXPERIMENTAL VERSION")
    logger.info("New Features: source_retailer_id, image URLs, descriptions")
    logger.info("="*60)

    scraper = BeStoreScraperEnhanced(
        dry_run=args.dry_run,
        headless=not args.no_headless,
        test_mode=args.test_mode,
        resume=args.resume
    )

    try:
        scraper.scrape()
    except Exception as e:
        logger.error(f"\n❌ Unexpected error: {e}")
        logger.info("Progress has been saved. Use --resume to continue from where you left off.")
        raise