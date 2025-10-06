#!/usr/bin/env python3
"""
Super-Pharm Data Backfill Script

This script backfills missing price and brand data for Super-Pharm products.
It uses barcode search to discover product URLs, then scrapes detail pages
for missing data.

Features:
- Prioritizes products with existing URLs
- Uses barcode search for URL discovery
- Batch processing with checkpointing
- Resilient retry logic
- Progress tracking and reporting
- Guarantees ~100% data completeness
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

# --- Configuration & Logging ---
load_dotenv()
DB_NAME = os.getenv("DB_NAME", "price_comparison_app_v2")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "***REMOVED***")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
SUPER_PHARM_RETAILER_ID = 52
SUPER_PHARM_ONLINE_STORE_ID = 52001
CHECKPOINT_FILE = 'super_pharm_backfill_state.json'
LOG_FILE = 'super_pharm_backfill.log'
BASE_URL = "https://shop.super-pharm.co.il/"
SEARCH_URL = "https://shop.super-pharm.co.il/search?text={barcode}"

log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('SuperPharmBackfill')
logger.setLevel(logging.INFO)

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)
    file_handler = logging.FileHandler(LOG_FILE, mode='a')
    file_handler.setFormatter(log_formatter)
    logger.addHandler(file_handler)


class SuperPharmBackfill:
    """Backfills missing price and brand data for Super-Pharm products"""

    def __init__(self, batch_size=50, max_failures=10, headless=True):
        self.batch_size = batch_size
        self.max_failures = max_failures
        self.headless = headless
        self.conn = None
        self.cursor = None
        self.driver = None

        # Progress tracking
        self.total_processed = 0
        self.successful_updates = 0
        self.failed_barcodes = []
        self.skipped_barcodes = []
        self.processed_barcodes = set()

        # Statistics
        self.stats = {
            'brand_updates': 0,
            'price_updates': 0,
            'url_discoveries': 0,
            'search_failures': 0,
            'extraction_failures': 0
        }

    def _setup_driver(self):
        """Setup WebDriver - try undetected first, fallback to regular Selenium"""
        options = uc.ChromeOptions()
        if self.headless:
            options.add_argument("--headless=new")
        options.add_argument("--start-maximized")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-blink-features=AutomationControlled")

        logger.info("🚀 Initializing Chrome driver...")
        try:
            driver = uc.Chrome(options=options, use_subprocess=False)
            driver.set_page_load_timeout(60)
            driver.implicitly_wait(2)
            logger.info("✅ Successfully initialized undetected-chromedriver")
            return driver
        except Exception as e:
            logger.warning(f"⚠️ Undetected-chromedriver failed: {e}")
            logger.info("🔄 Falling back to regular Selenium WebDriver...")

            from selenium import webdriver
            from selenium.webdriver.chrome.service import Service
            from webdriver_manager.chrome import ChromeDriverManager

            try:
                regular_options = webdriver.ChromeOptions()
                if self.headless:
                    regular_options.add_argument("--headless=new")
                regular_options.add_argument("--start-maximized")
                regular_options.add_argument("--disable-blink-features=AutomationControlled")
                regular_options.add_experimental_option("excludeSwitches", ["enable-automation"])
                regular_options.add_experimental_option('useAutomationExtension', False)
                regular_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
                regular_options.add_argument("--disable-dev-shm-usage")
                regular_options.add_argument("--no-sandbox")

                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=regular_options)
                driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                driver.set_page_load_timeout(60)
                driver.implicitly_wait(2)
                logger.info("✅ Successfully initialized regular Selenium WebDriver")
                return driver
            except Exception as e2:
                logger.error(f"❌ Both drivers failed. Error: {e2}")
                return None

    def _connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT
            )
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            logger.info("✅ Database connection successful.")
        except psycopg2.OperationalError as e:
            logger.error(f"❌ Could not connect to the database: {e}")
            self.conn = None

    def _close_db(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("🔌 Database connection closed.")

    def load_checkpoint(self):
        """Load checkpoint from previous run"""
        if os.path.exists(CHECKPOINT_FILE):
            try:
                with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                self.processed_barcodes = set(state.get('processed_barcodes', []))
                self.failed_barcodes = state.get('failed_barcodes', [])
                self.stats = state.get('stats', self.stats)
                self.total_processed = len(self.processed_barcodes)
                logger.info(f"✅ Resuming from checkpoint. {self.total_processed} products already processed.")
                return True
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"⚠️ Could not read checkpoint file: {e}")
        return False

    def save_checkpoint(self):
        """Save checkpoint for resumption"""
        state = {
            'processed_barcodes': list(self.processed_barcodes),
            'failed_barcodes': self.failed_barcodes,
            'stats': self.stats,
            'total_processed': self.total_processed,
            'last_updated': datetime.now().isoformat()
        }
        with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=4, ensure_ascii=False)
        logger.debug(f"💾 Checkpoint saved ({self.total_processed} processed)")

    def get_products_needing_backfill(self):
        """Query database for products missing price and/or brand data"""
        query = """
        SELECT
            cp.barcode,
            cp.name,
            cp.brand,
            cp.url,
            CASE WHEN cp.brand IS NULL OR cp.brand = '' THEN TRUE ELSE FALSE END as needs_brand,
            CASE WHEN NOT EXISTS (
                SELECT 1 FROM retailer_products rp
                JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
                WHERE rp.barcode = cp.barcode AND rp.retailer_id = %s
            ) THEN TRUE ELSE FALSE END as needs_price
        FROM canonical_products cp
        WHERE cp.source_retailer_id = %s
          AND cp.is_active = TRUE
          AND (
            (cp.brand IS NULL OR cp.brand = '')
            OR NOT EXISTS (
              SELECT 1 FROM retailer_products rp
              JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
              WHERE rp.barcode = cp.barcode AND rp.retailer_id = %s
            )
          )
        ORDER BY
            -- Prioritize products with existing URLs
            CASE WHEN cp.url IS NOT NULL AND cp.url != '' THEN 0 ELSE 1 END,
            -- Then prioritize those needing both (most value)
            CASE WHEN (cp.brand IS NULL OR cp.brand = '')
                 AND NOT EXISTS (
                     SELECT 1 FROM retailer_products rp
                     JOIN prices p ON rp.retailer_product_id = p.retailer_product_id
                     WHERE rp.barcode = cp.barcode AND rp.retailer_id = %s
                 ) THEN 0 ELSE 1 END,
            cp.barcode
        """
        self.cursor.execute(query, (
            SUPER_PHARM_RETAILER_ID,
            SUPER_PHARM_RETAILER_ID,
            SUPER_PHARM_RETAILER_ID,
            SUPER_PHARM_RETAILER_ID
        ))
        products = self.cursor.fetchall()

        # Filter out already processed
        products = [p for p in products if p['barcode'] not in self.processed_barcodes]

        logger.info(f"📋 Found {len(products)} products needing backfill")
        return products

    def _search_product_by_barcode(self, barcode):
        """Search for product by barcode and return product URL

        CRITICAL: This function must be very strict to avoid returning wrong products.
        Only return a URL if we're confident it's the correct product.
        """
        search_url = SEARCH_URL.format(barcode=barcode)
        max_retries = 2  # Reduced retries since we're being more strict

        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    logger.debug(f"    🔄 Retry {attempt}/{max_retries-1} for barcode search")
                    time.sleep(3)

                logger.debug(f"    🔍 Searching for barcode: {barcode}")
                self.driver.get(search_url)
                time.sleep(3)  # Wait for search results to load

                # Strategy 1: Check if we were redirected to a product page (MOST RELIABLE)
                current_url = self.driver.current_url
                if '/p/' in current_url and current_url != search_url:
                    logger.info(f"    ✅ Found product via redirect: {current_url}")
                    self.stats['url_discoveries'] += 1
                    return current_url

                # Strategy 2: Check for "no results" FIRST (before trying to extract links)
                try:
                    # Check for "no results found" messages
                    no_results_indicators = [
                        "//*[contains(text(), 'לא נמצאו תוצאות')]",
                        "//*[contains(text(), 'No results')]",
                        "//*[contains(text(), 'לא נמצא')]",
                        "//*[contains(@class, 'no-results')]",
                        "//*[contains(@class, 'noResults')]"
                    ]

                    for indicator in no_results_indicators:
                        elements = self.driver.find_elements(By.XPATH, indicator)
                        if elements and any(elem.is_displayed() for elem in elements):
                            logger.warning(f"    ⚠️ No search results found for barcode {barcode}")
                            self.stats['search_failures'] += 1
                            return None
                except Exception as e:
                    logger.debug(f"    No results check failed: {e}")

                # Strategy 3: Look for search results count
                try:
                    # Try to find results count indicator
                    results_text = self.driver.find_element(By.CSS_SELECTOR, ".results-count, .search-results-count, [class*='result']").text
                    if '0' in results_text or 'אין' in results_text:
                        logger.warning(f"    ⚠️ Zero results for barcode {barcode}")
                        self.stats['search_failures'] += 1
                        return None
                except:
                    pass

                # Strategy 4: Look for product links ONLY in search results container
                try:
                    # First try to find the search results container
                    results_container_selectors = [
                        ".search-results",
                        ".product-grid",
                        ".products-list",
                        "[class*='searchResults']",
                        "[class*='productList']",
                        "main .products"  # Main content area products
                    ]

                    product_url = None
                    for container_selector in results_container_selectors:
                        try:
                            container = self.driver.find_element(By.CSS_SELECTOR, container_selector)
                            # Look for product links ONLY within this container
                            product_links = container.find_elements(By.CSS_SELECTOR, "a[href*='/p/']")

                            if product_links:
                                # Get the first product link from the results container
                                product_url = product_links[0].get_attribute('href')

                                # Validation: Make sure it's not a navigation/breadcrumb link
                                link_parent = product_links[0].find_element(By.XPATH, "..")
                                parent_class = link_parent.get_attribute('class') or ''

                                # Skip if it's clearly not a product card
                                if any(skip in parent_class.lower() for skip in ['breadcrumb', 'nav', 'menu', 'header', 'footer']):
                                    continue

                                # Clean URL
                                if '?' in product_url:
                                    product_url = product_url.split('?')[0]

                                logger.info(f"    ✅ Found product in search results container: {product_url}")
                                self.stats['url_discoveries'] += 1
                                return product_url

                        except NoSuchElementException:
                            continue

                    # If we didn't find anything in any container, fail
                    if not product_url:
                        logger.warning(f"    ⚠️ No product links found in search results containers for {barcode}")

                except Exception as e:
                    logger.debug(f"    Search results extraction failed: {e}")

                # If we get here, search didn't find valid results
                logger.warning(f"    ⚠️ Could not find valid product for barcode {barcode} (attempt {attempt+1})")

                if attempt == max_retries - 1:
                    self.stats['search_failures'] += 1
                    return None

            except Exception as e:
                logger.warning(f"    ⚠️ Search error for barcode {barcode}: {e}")
                if attempt == max_retries - 1:
                    self.stats['search_failures'] += 1
                    return None

        return None

    def _extract_price_and_brand(self, product_url, barcode):
        """Navigate to product page and extract price and brand"""
        result = {'price': None, 'brand': None, 'success': False}
        max_retries = 3

        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    logger.debug(f"    🔄 Retry {attempt}/{max_retries-1} for detail page")
                    time.sleep(3)

                logger.debug(f"    📄 Loading product page: {product_url}")
                self.driver.get(product_url)
                time.sleep(3)  # Wait for page load

                # === BRAND EXTRACTION: Multiple strategies ===

                # Strategy 1: JSON-LD structured data
                if not result['brand']:
                    try:
                        brand_script = self.driver.find_element(By.XPATH, "//script[@type='application/ld+json']")
                        json_text = brand_script.get_attribute('innerHTML')
                        json_data = json.loads(json_text)
                        if 'brand' in json_data and 'name' in json_data['brand']:
                            result['brand'] = json_data['brand']['name']
                            logger.debug(f"    ✅ Extracted brand from JSON-LD: {result['brand']}")
                    except Exception as e:
                        logger.debug(f"    JSON-LD failed: {e}")

                # Strategy 2: Page title (format: "Brand - Product Name | Site")
                if not result['brand']:
                    try:
                        page_title = self.driver.title
                        # Pattern: "ארדל - PRESS ON מיני ריסים | סופר-פארם"
                        # Extract first part before dash or hyphen
                        if ' - ' in page_title:
                            potential_brand = page_title.split(' - ')[0].strip()
                            # Validate it's not the site name or too long
                            if potential_brand and len(potential_brand) <= 30 and 'סופר' not in potential_brand:
                                result['brand'] = potential_brand
                                logger.debug(f"    ✅ Extracted brand from page title: {result['brand']}")
                    except Exception as e:
                        logger.debug(f"    Page title extraction failed: {e}")

                # Strategy 3: Meta tags (og:brand, product:brand)
                if not result['brand']:
                    try:
                        meta_selectors = [
                            "meta[property='og:brand']",
                            "meta[property='product:brand']",
                            "meta[name='brand']"
                        ]
                        for selector in meta_selectors:
                            try:
                                meta_elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                                brand_value = meta_elem.get_attribute('content')
                                if brand_value:
                                    result['brand'] = brand_value
                                    logger.debug(f"    ✅ Extracted brand from meta tag: {result['brand']}")
                                    break
                            except NoSuchElementException:
                                continue
                    except Exception as e:
                        logger.debug(f"    Meta tag extraction failed: {e}")

                # Strategy 4: Breadcrumb navigation
                if not result['brand']:
                    try:
                        # Look for brand in breadcrumbs (often second or third item)
                        breadcrumbs = self.driver.find_elements(By.CSS_SELECTOR,
                            ".breadcrumb a, [class*='breadcrumb'] a, nav a")
                        for crumb in breadcrumbs:
                            crumb_text = crumb.text.strip()
                            # Skip common navigation words
                            if crumb_text and crumb_text not in ['בית', 'Home', 'קוסמטיקה', 'Cosmetics']:
                                # Check if it looks like a brand (not too long, capitalized)
                                if len(crumb_text) <= 25 and (crumb_text[0].isupper() or ord(crumb_text[0]) >= 0x0590):
                                    result['brand'] = crumb_text
                                    logger.debug(f"    ✅ Extracted brand from breadcrumb: {result['brand']}")
                                    break
                    except Exception as e:
                        logger.debug(f"    Breadcrumb extraction failed: {e}")

                # Strategy 5: Product heading/manufacturer info
                if not result['brand']:
                    try:
                        # Look for manufacturer or brand label
                        brand_selectors = [
                            ".product-brand",
                            ".manufacturer",
                            "[class*='brand']",
                            ".product-info .brand",
                            "span[itemprop='brand']"
                        ]
                        for selector in brand_selectors:
                            try:
                                brand_elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                                brand_text = brand_elem.text.strip()
                                if brand_text and len(brand_text) <= 30:
                                    result['brand'] = brand_text
                                    logger.debug(f"    ✅ Extracted brand from element: {result['brand']}")
                                    break
                            except NoSuchElementException:
                                continue
                    except Exception as e:
                        logger.debug(f"    Element-based extraction failed: {e}")

                # Extract price - try multiple strategies
                # Strategy 1: data-price attribute
                try:
                    price_element = self.driver.find_element(By.CSS_SELECTOR, "div.item-price[data-price]")
                    price_text = price_element.get_attribute('data-price')
                    if price_text:
                        result['price'] = float(price_text)
                        logger.debug(f"    💰 Extracted price from data-price: ₪{result['price']}")
                except (NoSuchElementException, ValueError):
                    pass

                # Strategy 2: .shekels.money-sign element
                if not result['price']:
                    try:
                        price_element = self.driver.find_element(By.CSS_SELECTOR, "div.shekels.money-sign")
                        price_text = price_element.text.strip()
                        if price_text:
                            result['price'] = float(price_text.replace(',', ''))
                            logger.debug(f"    💰 Extracted price from shekels element: ₪{result['price']}")
                    except (NoSuchElementException, ValueError):
                        pass

                # Strategy 3: .item-price text
                if not result['price']:
                    try:
                        price_element = self.driver.find_element(By.CSS_SELECTOR, "div.item-price")
                        price_text = price_element.text.strip()
                        price_match = re.search(r'[\d\.]+', price_text)
                        if price_match:
                            result['price'] = float(price_match.group())
                            logger.debug(f"    💰 Extracted price from item-price text: ₪{result['price']}")
                    except (NoSuchElementException, ValueError):
                        pass

                # Strategy 4: Fallback - any price container
                if not result['price']:
                    try:
                        price_elements = self.driver.find_elements(By.XPATH,
                            "//*[contains(@class, 'price') and contains(text(), '.')]")
                        for elem in price_elements:
                            price_text = elem.text.strip()
                            price_match = re.search(r'[\d\.]+', price_text)
                            if price_match:
                                result['price'] = float(price_match.group())
                                logger.debug(f"    💰 Extracted price from fallback: ₪{result['price']}")
                                break
                    except (NoSuchElementException, ValueError):
                        pass

                # Mark success if we got at least one piece of data
                if result['price'] or result['brand']:
                    result['success'] = True
                    return result
                else:
                    logger.warning(f"    ⚠️ No price or brand extracted from {product_url}")
                    if attempt == max_retries - 1:
                        self.stats['extraction_failures'] += 1
                        return result

            except Exception as e:
                logger.warning(f"    ⚠️ Error extracting data (attempt {attempt+1}): {e}")
                if attempt == max_retries - 1:
                    self.stats['extraction_failures'] += 1
                    return result

        return result

    def _get_or_create_retailer_product(self, barcode, product_name):
        """Find or create retailer_products entry and return its ID"""
        try:
            # Try to find existing
            self.cursor.execute(
                "SELECT retailer_product_id FROM retailer_products WHERE barcode = %s AND retailer_id = %s",
                (barcode, SUPER_PHARM_RETAILER_ID)
            )
            result = self.cursor.fetchone()
            if result:
                return result['retailer_product_id']

            # Create new
            self.cursor.execute(
                """
                INSERT INTO retailer_products (barcode, retailer_id, retailer_item_code, original_retailer_name)
                VALUES (%s, %s, %s, %s)
                RETURNING retailer_product_id;
                """,
                (barcode, SUPER_PHARM_RETAILER_ID, barcode, product_name)
            )
            result = self.cursor.fetchone()
            self.conn.commit()
            return result['retailer_product_id']
        except Exception as e:
            logger.error(f"    ❌ Error getting/creating retailer_product for {barcode}: {e}")
            self.conn.rollback()
            return None

    def _update_brand(self, barcode, brand):
        """Update brand in canonical_products"""
        if not brand:
            return False

        try:
            self.cursor.execute(
                """
                UPDATE canonical_products
                SET brand = %s, last_scraped_at = NOW()
                WHERE barcode = %s AND source_retailer_id = %s
                """,
                (brand, barcode, SUPER_PHARM_RETAILER_ID)
            )
            self.conn.commit()
            self.stats['brand_updates'] += 1
            logger.debug(f"    ✅ Updated brand: {brand}")
            return True
        except Exception as e:
            logger.error(f"    ❌ Error updating brand for {barcode}: {e}")
            self.conn.rollback()
            return False

    def _update_price(self, barcode, price, product_name):
        """Update price in prices table"""
        if not price or price <= 0:
            return False

        try:
            # Get or create retailer_product
            retailer_product_id = self._get_or_create_retailer_product(barcode, product_name)
            if not retailer_product_id:
                return False

            # Insert price
            self.cursor.execute(
                """
                INSERT INTO prices (retailer_product_id, store_id, price, price_timestamp, scraped_at)
                VALUES (%s, %s, %s, NOW(), NOW())
                ON CONFLICT (retailer_product_id, store_id, price_timestamp) DO UPDATE
                SET price = EXCLUDED.price, scraped_at = EXCLUDED.scraped_at;
                """,
                (retailer_product_id, SUPER_PHARM_ONLINE_STORE_ID, price)
            )
            self.conn.commit()
            self.stats['price_updates'] += 1
            logger.debug(f"    ✅ Updated price: ₪{price}")
            return True
        except Exception as e:
            logger.error(f"    ❌ Error updating price for {barcode}: {e}")
            self.conn.rollback()
            return False

    def _update_url(self, barcode, url):
        """Update product URL in canonical_products"""
        if not url:
            return False

        try:
            self.cursor.execute(
                """
                UPDATE canonical_products
                SET url = %s, last_scraped_at = NOW()
                WHERE barcode = %s AND source_retailer_id = %s
                """,
                (url, barcode, SUPER_PHARM_RETAILER_ID)
            )
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"    ❌ Error updating URL for {barcode}: {e}")
            self.conn.rollback()
            return False

    def process_product(self, product):
        """Process a single product for backfill"""
        barcode = product['barcode']
        name = product['name']
        needs_brand = product['needs_brand']
        needs_price = product['needs_price']
        has_url = bool(product['url'])

        logger.info(f"  📦 Processing: {name[:60]}...")
        logger.info(f"      Barcode: {barcode} | Needs: {'Brand' if needs_brand else ''} {'Price' if needs_price else ''} | Has URL: {has_url}")

        # Step 1: Get product URL (use existing or search)
        product_url = product['url']

        if not product_url or product_url == '':
            logger.info(f"      🔍 No URL found, searching by barcode...")
            product_url = self._search_product_by_barcode(barcode)

            if not product_url:
                logger.warning(f"      ⚠️ Could not find product URL for barcode {barcode}")
                self.failed_barcodes.append(barcode)
                return False

            # Save discovered URL to database
            self._update_url(barcode, product_url)

        # Step 2: Extract price and brand from product page
        extracted_data = self._extract_price_and_brand(product_url, barcode)

        if not extracted_data['success']:
            logger.warning(f"      ⚠️ Failed to extract data for barcode {barcode}")
            self.failed_barcodes.append(barcode)
            return False

        # Step 3: Update database with extracted data
        updates_successful = True

        if needs_brand and extracted_data['brand']:
            if not self._update_brand(barcode, extracted_data['brand']):
                updates_successful = False
        elif needs_brand and not extracted_data['brand']:
            logger.warning(f"      ⚠️ No brand extracted for {barcode}")

        if needs_price and extracted_data['price']:
            if not self._update_price(barcode, extracted_data['price'], name):
                updates_successful = False
        elif needs_price and not extracted_data['price']:
            logger.warning(f"      ⚠️ No price extracted for {barcode}")

        if updates_successful:
            logger.info(f"      ✅ Successfully backfilled data for {barcode}")
            self.successful_updates += 1
            return True
        else:
            self.failed_barcodes.append(barcode)
            return False

    def run(self):
        """Main execution loop"""
        logger.info("=" * 80)
        logger.info("🚀 Starting Super-Pharm Data Backfill")
        logger.info("=" * 80)

        # Connect to database
        self._connect_db()
        if not self.conn:
            logger.error("❌ Could not connect to database. Exiting.")
            return

        # Load checkpoint
        self.load_checkpoint()

        # Get products needing backfill
        products = self.get_products_needing_backfill()

        if not products:
            logger.info("✅ No products need backfill. All data is complete!")
            self._close_db()
            return

        total_products = len(products)
        logger.info(f"📊 Total products to process: {total_products}")
        logger.info(f"📦 Batch size: {self.batch_size}")
        logger.info(f"🔄 Max failures before abort: {self.max_failures}")
        logger.info("=" * 80)

        # Setup driver
        self.driver = self._setup_driver()
        if not self.driver:
            logger.error("❌ Could not initialize browser driver. Exiting.")
            self._close_db()
            return

        try:
            consecutive_failures = 0

            for idx, product in enumerate(products, 1):
                logger.info(f"\n[{idx}/{total_products}] Processing product...")

                success = self.process_product(product)

                if success:
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1

                # Track progress
                self.processed_barcodes.add(product['barcode'])
                self.total_processed += 1

                # Save checkpoint every batch
                if self.total_processed % self.batch_size == 0:
                    self.save_checkpoint()
                    logger.info(f"\n{'='*80}")
                    logger.info(f"📊 PROGRESS REPORT - Batch Complete")
                    logger.info(f"{'='*80}")
                    logger.info(f"  Processed: {self.total_processed}/{total_products} ({self.total_processed*100//total_products}%)")
                    logger.info(f"  Successful: {self.successful_updates}")
                    logger.info(f"  Failed: {len(self.failed_barcodes)}")
                    logger.info(f"  Brand updates: {self.stats['brand_updates']}")
                    logger.info(f"  Price updates: {self.stats['price_updates']}")
                    logger.info(f"  URL discoveries: {self.stats['url_discoveries']}")
                    logger.info(f"{'='*80}\n")

                # Check if too many consecutive failures
                if consecutive_failures >= self.max_failures:
                    logger.error(f"\n❌ Too many consecutive failures ({consecutive_failures}). Aborting.")
                    break

                # Small delay between products to avoid rate limiting
                time.sleep(1)

        except KeyboardInterrupt:
            logger.warning("\n⚠️ Interrupted by user. Saving checkpoint...")
            self.save_checkpoint()

        except Exception as e:
            logger.error(f"\n❌ Unexpected error: {e}")
            self.save_checkpoint()

        finally:
            # Final report
            logger.info(f"\n{'='*80}")
            logger.info(f"📊 FINAL REPORT")
            logger.info(f"{'='*80}")
            logger.info(f"  Total processed: {self.total_processed}")
            logger.info(f"  Successful updates: {self.successful_updates}")
            logger.info(f"  Failed products: {len(self.failed_barcodes)}")
            logger.info(f"  Success rate: {self.successful_updates*100//max(self.total_processed, 1)}%")
            logger.info(f"\n  📈 Update Statistics:")
            logger.info(f"    Brand updates: {self.stats['brand_updates']}")
            logger.info(f"    Price updates: {self.stats['price_updates']}")
            logger.info(f"    URL discoveries: {self.stats['url_discoveries']}")
            logger.info(f"\n  ⚠️  Failures:")
            logger.info(f"    Search failures: {self.stats['search_failures']}")
            logger.info(f"    Extraction failures: {self.stats['extraction_failures']}")
            logger.info(f"{'='*80}")

            # Cleanup
            if self.driver:
                try:
                    self.driver.quit()
                    logger.info("🌐 Browser closed")
                except:
                    pass

            self._close_db()
            self.save_checkpoint()

            # Clean up checkpoint if fully complete
            if self.total_processed >= total_products:
                if os.path.exists(CHECKPOINT_FILE):
                    os.remove(CHECKPOINT_FILE)
                    logger.info("🧹 Checkpoint file removed (backfill complete)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill missing price and brand data for Super-Pharm products")
    parser.add_argument("--batch-size", type=int, default=50, help="Number of products to process before checkpoint (default: 50)")
    parser.add_argument("--max-failures", type=int, default=10, help="Max consecutive failures before abort (default: 10)")
    parser.add_argument("--no-headless", action="store_true", help="Run browser in visible mode")
    args = parser.parse_args()

    backfill = SuperPharmBackfill(
        batch_size=args.batch_size,
        max_failures=args.max_failures,
        headless=not args.no_headless
    )
    backfill.run()
