#!/usr/bin/env python3
"""
Good Pharm Commercial Website Scraper

This script scrapes product data from the Good Pharm website (goodpharm.co.il).
As the third commercial catalog builder for the PharmMate system, this scraper
populates the canonical_products table with Good Pharm's private-label products.

Target URL: https://goodpharm.co.il/shop?wpf_filter_cat_0=44

Features:
- Selenium WebDriver with anti-detection measures
- Standard pagination handling (WooCommerce)
- Barcode extraction from data-product_sku attributes
- Robust logging and checkpoint/resume functionality
- Dry-run and test modes for safe execution
"""

import time
import json
import argparse
import re
import logging
from datetime import datetime
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from urllib.parse import urljoin
import undetected_chromedriver as uc

# --- Configuration & Logging ---
load_dotenv()
DB_NAME = os.getenv("DB_NAME", "price_comparison_app_v2")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "025655358")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
GOOD_PHARM_RETAILER_ID = 97   # Good Pharm retailer ID from database
CHECKPOINT_FILE = 'good_pharm_scraper_state.json'
LOG_FILE = 'good_pharm_scraper.log'
BASE_URL = "https://goodpharm.co.il/"
# All Good Pharm categories to scrape
CATEGORY_URLS = [
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=44",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=45",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=46",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=47",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=48",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=49",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=50",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=51",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=52",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=53",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=54",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=55",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=56",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=64",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=76",
    "https://goodpharm.co.il/shop?wpf_filter_cat_0=80"
]

# Category ID to Hebrew name mapping (based on actual product analysis)
CATEGORY_ID_TO_NAME = {
    "44": "אורתופדיה",        # Orthopedic (elbow guards, supports)
    "45": "אופטיקה",          # Optics (reading glasses)
    "46": "דנטלית",          # Dental (toothpicks, dental care)
    "47": "חד פעמי",          # Disposable (paper cups, plates)
    "48": "חשמל ואלקטרוניקה", # Electronics (headphones, chargers)
    "49": "תינוקות וילדים",   # Babies and children
    "50": "מוצרי ילדים",     # Children's products
    "51": "תינוקות וילדים",   # Babies and children
    "52": "תוספי תזונה",      # Nutritional supplements
    "53": "דנטלית",          # Dental
    "54": "אורתופדיה",       # Orthopedic
    "55": "טואלטיקה",        # Toiletries (body scrubs)
    "56": "חד פעמי",          # Disposable
    "64": "קטגוריה ריקה",     # Empty category
    "76": "נסיעות ופנוי",     # Travel and leisure
    "80": "מוצרי חורף"       # Winter products
}

log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('GoodPharmScraper')
logger.setLevel(logging.INFO)

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)
    file_handler = logging.FileHandler(LOG_FILE, mode='a')
    file_handler.setFormatter(log_formatter)
    logger.addHandler(file_handler)


class GoodPharmScraper:
    """Scraper for Good Pharm commercial website"""

    def __init__(self, dry_run=False, headless=True, test_mode=False, resume=False):
        self.dry_run = dry_run
        self.headless = headless
        self.test_mode = test_mode
        self.resume = resume
        self.conn = None
        self.cursor = None
        self.total_products_processed = 0
        self.scraped_data_collection = []
        self.current_category_index = 0  # Track which category we're scraping
        self.test_mode_limit = 15  # Products in test mode
        self.seen_barcodes = set()  # For deduplication
        self.duplicate_count = 0
        self.category_stats = {}  # Track stats per category

    def _setup_driver(self):
        """Setup WebDriver - try undetected first, fallback to regular Selenium"""
        options = uc.ChromeOptions()
        if self.headless:
            options.add_argument("--headless=new")
        options.add_argument("--start-maximized")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        logger.info("🚀 Initializing WebDriver...")
        try:
            # Try undetected-chromedriver first
            driver = uc.Chrome(options=options, use_subprocess=False)
            logger.info("✅ Successfully initialized undetected-chromedriver")
            return driver
        except Exception as e:
            logger.warning(f"⚠️ Undetected-chromedriver failed: {e}")
            logger.info("🔄 Falling back to regular Selenium WebDriver...")

            # Fallback to regular Selenium
            try:
                regular_options = webdriver.ChromeOptions()
                if self.headless:
                    regular_options.add_argument("--headless=new")
                regular_options.add_argument("--start-maximized")
                regular_options.add_argument("--no-sandbox")
                regular_options.add_argument("--disable-dev-shm-usage")
                regular_options.add_argument("--disable-blink-features=AutomationControlled")
                regular_options.add_experimental_option("excludeSwitches", ["enable-automation"])
                regular_options.add_experimental_option('useAutomationExtension', False)
                regular_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=regular_options)
                driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                logger.info("✅ Successfully initialized regular Selenium WebDriver")
                return driver
            except Exception as e2:
                logger.error(f"❌ Both drivers failed. Error: {e2}")
                return None

    def _connect_db(self):
        if self.dry_run: return
        try:
            self.conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            logger.info("✅ Database connection successful.")
        except psycopg2.OperationalError as e:
            logger.error(f"❌ Could not connect to the database: {e}")
            self.conn = None

    def _close_db(self):
        if self.cursor: self.cursor.close()
        if self.conn: self.conn.close()
        logger.info("🔌 Database connection closed.")

    def load_checkpoint(self):
        if self.resume and os.path.exists(CHECKPOINT_FILE):
            try:
                with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                self.current_category_index = state.get('current_category_index', 0)
                self.total_products_processed = state.get('total_products_processed', 0)
                self.category_stats = state.get('category_stats', {})
                logger.info(f"✅ Resuming scrape from category {self.current_category_index + 1}/{len(CATEGORY_URLS)}. Processed: {self.total_products_processed}")
                return True
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"⚠️ Could not read checkpoint file, starting fresh. Error: {e}")
        return False

    def save_checkpoint(self):
        state = {
            'current_category_index': self.current_category_index,
            'total_products_processed': self.total_products_processed,
            'category_stats': self.category_stats,
            'last_updated': datetime.now().isoformat()
        }
        with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=4, ensure_ascii=False)
        logger.info(f"💾 Checkpoint saved. Category: {self.current_category_index + 1}/{len(CATEGORY_URLS)}, Total products: {self.total_products_processed}")

    def _extract_products_from_page(self, driver, category_id=None):
        """Extract all product data from the current page"""
        products = []
        logger.info("  🔍 Extracting products using Good Pharm selectors...")

        try:
            # Wait for products to be present
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "li.product"))
            )

            # Get all product containers
            product_elements = driver.find_elements(By.CSS_SELECTOR, "li.product")
            logger.info(f"  📦 Found {len(product_elements)} product containers")

            for i, element in enumerate(product_elements, 1):
                try:
                    product_data = {}

                    # Extract barcode from add-to-cart button (primary method)
                    barcode = None
                    try:
                        cart_button = element.find_element(By.CSS_SELECTOR, "a.add_to_cart_button[data-product_sku]")
                        barcode = cart_button.get_attribute('data-product_sku')

                        # Clean barcode - remove any suffix after hyphen (like "7290018781647-1" -> "7290018781647")
                        if barcode and '-' in barcode:
                            barcode = barcode.split('-')[0]

                    except NoSuchElementException:
                        logger.debug(f"    ❌ Product {i}: No add-to-cart button with SKU found")

                    # Fallback: Extract barcode from image filename
                    if not barcode:
                        try:
                            img = element.find_element(By.CSS_SELECTOR, "img")
                            img_src = img.get_attribute('src')
                            if img_src:
                                # Look for barcode pattern in filename (e.g., "7290018183311_2-250x250.jpg")
                                barcode_match = re.search(r'/([0-9]{13})_', img_src)
                                if barcode_match:
                                    barcode = barcode_match.group(1)
                                    logger.debug(f"    📷 Product {i}: Extracted barcode from image: {barcode}")
                        except:
                            pass

                    if not barcode:
                        logger.debug(f"    ⏭️  Product {i}: No barcode found, skipping")
                        continue

                    product_data['barcode'] = barcode

                    # Extract product name
                    try:
                        name_elem = element.find_element(By.CSS_SELECTOR, "h2.woocommerce-loop-product__title")
                        product_data['name'] = name_elem.text.strip()
                    except NoSuchElementException:
                        product_data['name'] = "Unknown Product"
                        logger.debug(f"    ⚠️ Product {i}: No name found")

                    # Extract price (for reference, not stored in canonical_products)
                    try:
                        price_elem = element.find_element(By.CSS_SELECTOR, "span.price .woocommerce-Price-amount")
                        price_text = price_elem.text.strip()
                        # Extract numeric value
                        price_match = re.search(r'([\d,]+\.?\d*)', price_text.replace(',', ''))
                        if price_match:
                            product_data['price'] = float(price_match.group())
                        else:
                            product_data['price'] = None
                    except:
                        product_data['price'] = None

                    # Extract image URL
                    try:
                        img = element.find_element(By.CSS_SELECTOR, "img.attachment-woocommerce_thumbnail")
                        img_url = img.get_attribute('src')
                        if img_url and not img_url.startswith('http'):
                            img_url = urljoin(BASE_URL, img_url)
                        product_data['image_url'] = img_url
                    except NoSuchElementException:
                        logger.debug(f"    ⚠️ Product {i}: No image found with selector img.attachment-woocommerce_thumbnail")
                        product_data['image_url'] = None
                    except Exception as e:
                        logger.debug(f"    ⚠️ Product {i}: Image extraction error: {e}")
                        product_data['image_url'] = None

                    # Extract product URL
                    try:
                        link = element.find_element(By.CSS_SELECTOR, "a.woocommerce-LoopProduct-link")
                        product_url = link.get_attribute('href')
                        if product_url and not product_url.startswith('http'):
                            product_url = urljoin(BASE_URL, product_url)
                        product_data['url'] = product_url
                    except:
                        product_data['url'] = None

                    # Extract brand (Good Pharm products are typically branded "GOOD PHARM")
                    name = product_data.get('name', '')
                    if 'GOOD PHARM' in name.upper():
                        product_data['brand'] = 'GOOD PHARM'
                    else:
                        # Try to extract brand from product name (first words)
                        words = name.split()
                        if len(words) > 0 and words[0].isupper() and len(words[0]) > 2:
                            product_data['brand'] = words[0]
                        else:
                            product_data['brand'] = 'GOOD PHARM'  # Default for Good Pharm products

                    # Set category from mapping or fallback
                    if category_id and category_id in CATEGORY_ID_TO_NAME:
                        product_data['category'] = CATEGORY_ID_TO_NAME[category_id]
                    else:
                        product_data['category'] = 'מותג הבית'  # Fallback
                    product_data['description'] = None  # Could be enhanced later
                    product_data['source_retailer_id'] = GOOD_PHARM_RETAILER_ID

                    products.append(product_data)

                    # Log first few products for debugging
                    if i <= 5:
                        logger.info(f"    Product {i}: {product_data['name'][:40]}... "
                                   f"(Barcode: {barcode}, Price: {product_data.get('price')})")

                except Exception as e:
                    logger.debug(f"    ❌ Failed to extract product {i}: {e}")
                    continue

        except TimeoutException:
            logger.warning("  ⚠️ Timeout waiting for products to load")
        except Exception as e:
            logger.error(f"  ❌ Error during product extraction: {e}")

        logger.info(f"  ✅ Successfully extracted {len(products)} products")
        return products

    def _clean_product_data(self, product_data: dict) -> dict:
        """Clean and enhance product data"""
        name = product_data.get('name', '')

        # Clean up common patterns in Good Pharm product names
        # Remove size/unit info (e.g., "50ml", "100g")
        name = re.sub(r'\s*\d+\s*(מ"?ל|גר\'?ם?|יח\'?|ג\')\s*', ' ', name, flags=re.IGNORECASE)

        # Clean up extra whitespace
        name = ' '.join(name.split())

        product_data['name'] = name
        return product_data

    def _process_product(self, product_data):
        """Process and store a single product"""
        barcode = product_data.get('barcode')

        # Check for duplicates
        if barcode in self.seen_barcodes:
            self.duplicate_count += 1
            logger.debug(f"    ⏭️  Skipping duplicate barcode: {barcode}")
            return False

        # Mark as seen
        self.seen_barcodes.add(barcode)

        # Clean the product data
        cleaned_data = self._clean_product_data(product_data)

        if self.dry_run:
            self.scraped_data_collection.append(cleaned_data)
            self.total_products_processed += 1
            return True
        else:
            try:
                upsert_query = """
                    INSERT INTO canonical_products (barcode, name, brand, image_url, category, description, source_retailer_id, last_scraped_at, created_at, is_active)
                    VALUES (%(barcode)s, %(name)s, %(brand)s, %(image_url)s, %(category)s, %(description)s, %(source_retailer_id)s, NOW(), NOW(), TRUE)
                    ON CONFLICT (barcode) DO UPDATE SET
                        name = EXCLUDED.name, brand = EXCLUDED.brand, image_url = EXCLUDED.image_url,
                        category = EXCLUDED.category, description = EXCLUDED.description,
                        source_retailer_id = EXCLUDED.source_retailer_id, last_scraped_at = NOW(), is_active = TRUE;
                """
                self.cursor.execute(upsert_query, cleaned_data)
                self.total_products_processed += 1
                return True
            except Exception as e:
                logger.error(f"    ❌ Database error for barcode {cleaned_data.get('barcode')}: {e}")
                self.conn.rollback()
                return False

    def _handle_load_more(self, driver):
        """Handle Load More button functionality (infinite scroll)"""
        try:
            # Scroll to bottom to potentially trigger Load More button appearance
            logger.info("  📜 Scrolling to bottom to check for Load More button...")
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            # Try to find the Load More button with multiple strategies
            load_more_selectors = [
                ".lmp_load_more_button a.lmp_button",
                ".lmp_load_more_button a",
                "a.lmp_button",
                "a[href*='load_next_page']",
                ".lmp_button"
            ]

            for selector in load_more_selectors:
                try:
                    # Check if Load More button exists
                    load_more_elements = driver.find_elements(By.CSS_SELECTOR, selector)

                    for load_more_btn in load_more_elements:
                        # Get the button's parent container to check visibility
                        parent_container = load_more_btn.find_element(By.XPATH, "..")

                        # Check various visibility conditions
                        is_displayed = load_more_btn.is_displayed()
                        parent_style = parent_container.get_attribute("style") or ""
                        btn_style = load_more_btn.get_attribute("style") or ""

                        # Button text check
                        btn_text = load_more_btn.text.strip()

                        logger.info(f"  📄 Found Load More button: selector='{selector}', displayed={is_displayed}, text='{btn_text}', parent_style='{parent_style[:100]}', btn_style='{btn_style[:100]}'")

                        # Check if button contains "Load More" text (in Hebrew: "טען עוד")
                        if "טען עוד" in btn_text or "load" in btn_text.lower():
                            if is_displayed and "display: none" not in parent_style and "display: none" not in btn_style:
                                logger.info("  📄 Found visible Load More button with correct text - clicking to load more products")

                                # Scroll to button first
                                driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", load_more_btn)
                                time.sleep(1)

                                # Try clicking with JavaScript (more reliable)
                                driver.execute_script("arguments[0].click();", load_more_btn)

                                # Wait for new products to load
                                time.sleep(4)

                                return True  # Successfully clicked Load More
                            else:
                                logger.info(f"  📄 Load More button found but not fully visible: displayed={is_displayed}, parent_style='{parent_style[:50]}', btn_style='{btn_style[:50]}'")
                        else:
                            logger.debug(f"  📄 Button found but wrong text: '{btn_text}'")

                except NoSuchElementException:
                    continue
                except Exception as e:
                    logger.debug(f"  📄 Error with Load More selector {selector}: {e}")
                    continue

            # Additional check: look for pagination that might indicate more pages
            try:
                pagination = driver.find_elements(By.CSS_SELECTOR, ".woocommerce-pagination .page-numbers")
                if pagination:
                    visible_pagination = [p for p in pagination if p.is_displayed()]
                    if visible_pagination:
                        logger.info(f"  📄 Found {len(visible_pagination)} visible pagination elements - there might be more content")

                        # Try clicking on page 2 if it exists
                        for page_link in visible_pagination:
                            if page_link.text.strip() == "2":
                                logger.info("  📄 Found page 2 link - clicking to load more products")
                                driver.execute_script("arguments[0].click();", page_link)
                                time.sleep(4)
                                return True
            except Exception as e:
                logger.debug(f"  📄 Pagination check failed: {e}")

            logger.info("  📄 ❌ No visible Load More button or pagination found - reached end")
            return False

        except Exception as e:
            logger.warning(f"  📄 Load More error: {e}")
            return False

    def scrape(self):
        """Main scraping function"""
        logger.info(f"Starting Good Pharm scraper... Mode: {'DRY RUN' if self.dry_run else 'DATABASE'}")
        if not self.dry_run: self._connect_db()
        if not self.dry_run and not self.conn: return

        checkpoint_loaded = self.load_checkpoint()
        driver = self._setup_driver()
        if not driver: return

        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"🔍 Starting Good Pharm multi-category scrape")
            logger.info(f"   Total Categories: {len(CATEGORY_URLS)}")
            logger.info(f"   Starting from category: {self.current_category_index + 1}")
            logger.info(f"{'='*60}")

            # Iterate through all categories
            while self.current_category_index < len(CATEGORY_URLS):
                current_url = CATEGORY_URLS[self.current_category_index]
                category_id = current_url.split('=')[-1]  # Extract category ID from URL

                logger.info(f"\n{'='*60}")
                logger.info(f"🔍 Scraping Category {self.current_category_index + 1}/{len(CATEGORY_URLS)}")
                logger.info(f"   Category ID: {category_id}")
                logger.info(f"   URL: {current_url}")
                logger.info(f"{'='*60}")

                # Navigate to category page
                driver.get(current_url)
                time.sleep(5)  # Wait for initial page load

                # Initialize category stats
                category_key = f"category_{category_id}"
                self.category_stats[category_key] = {
                    'url': current_url,
                    'products_found': 0,
                    'unique_products': 0,
                    'load_more_attempts': 0
                }

                max_load_more_attempts = 20 if not self.test_mode else 2
                load_more_attempts = 0
                previous_product_count = 0
                category_products_added = 0

                # Process this category with Load More functionality
                while load_more_attempts < max_load_more_attempts:
                    # Extract all currently visible products
                    products_on_page = self._extract_products_from_page(driver, category_id)

                    if not products_on_page:
                        logger.warning(f"  ⚠️ No products found in category {category_id}")
                        break

                    # In test mode, limit products per category
                    if self.test_mode and len(products_on_page) > self.test_mode_limit:
                        products_on_page = products_on_page[:self.test_mode_limit]
                        logger.info(f"  🧪 TEST MODE: Limiting to {self.test_mode_limit} products per category")

                    # Process each product (deduplication will happen in _process_product)
                    unique_products = 0
                    for product in products_on_page:
                        # Add category info to product
                        product['category_id'] = category_id
                        if self._process_product(product):
                            unique_products += 1
                            category_products_added += 1

                    current_total = len(products_on_page)
                    self.category_stats[category_key]['products_found'] = current_total
                    self.category_stats[category_key]['unique_products'] = category_products_added

                    logger.info(f"  📦 Category {category_id} - Load More attempt {load_more_attempts + 1}: Found {current_total} total products, {unique_products} new unique products")

                    # Commit after processing
                    if not self.dry_run and self.conn:
                        self.conn.commit()

                    # Save checkpoint
                    self.save_checkpoint()

                    # Test mode early exit
                    if self.test_mode and self.total_products_processed >= self.test_mode_limit:
                        logger.info("  🧪 TEST MODE: Reached product limit, stopping")
                        break

                    # Check if no new products were found and we're not growing
                    if current_total == previous_product_count and unique_products == 0:
                        logger.info("  🔄 No new products found, checking for Load More button...")
                    else:
                        logger.info(f"  📈 Found {current_total} products (was {previous_product_count}), checking for more...")

                    previous_product_count = current_total

                    # Try to click Load More button
                    if self._handle_load_more(driver):
                        load_more_attempts += 1
                        self.category_stats[category_key]['load_more_attempts'] = load_more_attempts
                        logger.info(f"  📄 Successfully clicked Load More button (attempt {load_more_attempts})")

                        # Wait for new content to load
                        time.sleep(3)
                        time.sleep(1)
                    else:
                        logger.info(f"  📄 ✅ Category {category_id} complete - no more Load More buttons found")
                        break

                logger.info(f"  ✅ Category {category_id} finished: {category_products_added} unique products added")

                # Move to next category
                self.current_category_index += 1

                # Test mode early exit
                if self.test_mode and self.total_products_processed >= self.test_mode_limit:
                    logger.info("  🧪 TEST MODE: Reached total product limit across categories, stopping")
                    break

                # Small delay between categories
                time.sleep(2)

        except KeyboardInterrupt:
            logger.warning("\n⚠️ Scraper interrupted by user")
        except Exception as e:
            logger.error(f"❌ Fatal error during scraping: {e}")
        finally:
            if driver:
                try:
                    driver.quit()
                    logger.info("🌐 Browser closed")
                except:
                    pass

            # Print final statistics
            logger.info(f"\n{'='*60}")
            logger.info(f"📊 FINAL STATISTICS")
            logger.info(f"{'='*60}")
            logger.info(f"  📁 Categories processed: {self.current_category_index}/{len(CATEGORY_URLS)}")
            logger.info(f"  ✅ Unique products: {self.total_products_processed}")
            logger.info(f"  ⏭️  Duplicates skipped: {self.duplicate_count}")
            logger.info(f"  📈 Total items processed: {self.total_products_processed + self.duplicate_count}")

            # Per-category breakdown
            logger.info(f"\n📋 Category Breakdown:")
            for category_key, stats in self.category_stats.items():
                logger.info(f"  • {category_key}: {stats['unique_products']} unique products (Load More attempts: {stats['load_more_attempts']})")

            logger.info(f"{'='*60}")

            if self.dry_run:
                self._save_dry_run_file()
            else:
                self._close_db()

            # Clean up checkpoint if scraping completed successfully
            if os.path.exists(CHECKPOINT_FILE):
                os.remove(CHECKPOINT_FILE)
                logger.info("🧹 Checkpoint file removed (scraping completed)")

    def _save_dry_run_file(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"good_pharm_dry_run_{timestamp}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.scraped_data_collection, f, ensure_ascii=False, indent=4)
        logger.info(f"\n✅ Dry run complete. {len(self.scraped_data_collection)} products saved to '{filename}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Good Pharm website for product catalog")
    parser.add_argument("--dry-run", action="store_true", help="Output to JSON file instead of database")
    parser.add_argument("--no-headless", action="store_true", help="Run browser in visible mode")
    parser.add_argument("--test-mode", action="store_true", help="Scrape only a few items for testing")
    parser.add_argument("--resume", action="store_true", help="Resume from the last completed page")
    args = parser.parse_args()

    scraper = GoodPharmScraper(dry_run=args.dry_run, headless=not args.no_headless, test_mode=args.test_mode, resume=args.resume)
    scraper.scrape()