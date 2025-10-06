#!/usr/bin/env python3
"""
Kolbo Yehuda Scraper - Selenium Version
========================================
Uses Selenium to handle JavaScript-heavy pages
"""

import json
import time
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import argparse
import logging
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('kolboyehuda_selenium_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class KolboYehudaScraperSelenium:
    """Selenium-based scraper for kolboyehuda.co.il with PostgreSQL integration"""

    # Database configuration
    DB_CONFIG = {
        'host': 'localhost',
        'port': 5432,
        'database': 'price_comparison_app_v2',
        'user': 'postgres',
        'password': '***REMOVED***'
    }

    # Kolbo Yehuda identifiers
    RETAILER_ID = 152
    STORE_ID = 17140395

    def __init__(self, dry_run: bool = True, max_pages: Optional[int] = None, headless: bool = True):
        self.base_url = "https://www.kolboyehuda.co.il"
        self.crawl_delay = 15
        self.dry_run = dry_run
        self.max_pages = max_pages
        self.headless = headless

        # Statistics
        self.stats = {
            'pages_scraped': 0,
            'products_found': 0,
            'products_valid': 0,
            'products_inserted_db': 0,
            'failures': [],
            'start_time': None,
            'end_time': None
        }

        # Output directories
        self.output_dir = Path('kolboyehuda_scraper_output')
        self.screenshots_dir = self.output_dir / 'failure_screenshots'
        self.output_dir.mkdir(exist_ok=True)
        self.screenshots_dir.mkdir(exist_ok=True)

        # Database connection
        self.db_conn = None
        self._init_database()

        # Initialize driver
        self.driver = self._init_driver()

    def _init_database(self):
        """Initialize PostgreSQL database connection"""
        try:
            self.db_conn = psycopg2.connect(**self.DB_CONFIG)
            self.db_conn.autocommit = False  # Use transactions
            logger.info("✓ Database connection established")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def _init_driver(self):
        """Initialize Selenium Chrome driver"""
        logger.info("Initializing Selenium Chrome driver...")
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        driver = webdriver.Chrome(options=chrome_options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        return driver

    def insert_product_to_database(self, product: Dict) -> bool:
        """
        Insert/update product in database with proper UPSERT logic
        Returns True if successful, False otherwise
        """
        try:
            cursor = self.db_conn.cursor()

            # Step 1: UPSERT into canonical_products
            # Convert categories list to comma-separated string for category field
            category_str = ', '.join(product['categories']) if product['categories'] else None

            canonical_insert = """
            INSERT INTO canonical_products (
                barcode, name, brand, image_url, category, url,
                source_retailer_id, last_scraped_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (barcode) DO UPDATE SET
                name = COALESCE(canonical_products.name, EXCLUDED.name),
                brand = COALESCE(canonical_products.brand, EXCLUDED.brand),
                image_url = COALESCE(canonical_products.image_url, EXCLUDED.image_url),
                category = COALESCE(canonical_products.category, EXCLUDED.category),
                url = COALESCE(canonical_products.url, EXCLUDED.url),
                last_scraped_at = NOW();
            """

            cursor.execute(canonical_insert, (
                product['barcode'],
                product['name'],
                product['brand'],
                product['image_url'],
                category_str,
                product['product_url'],
                self.RETAILER_ID,
                datetime.now()
            ))

            # Step 2: Insert/update retailer_products link
            retailer_product_insert = """
            INSERT INTO retailer_products (
                retailer_id, retailer_item_code, original_retailer_name, barcode
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT (retailer_id, retailer_item_code) DO UPDATE SET
                barcode = EXCLUDED.barcode,
                original_retailer_name = EXCLUDED.original_retailer_name
            RETURNING retailer_product_id;
            """

            cursor.execute(retailer_product_insert, (
                self.RETAILER_ID,
                product['product_id'],  # Use product_id as retailer_item_code
                product['name'],
                product['barcode']
            ))

            retailer_product_id = cursor.fetchone()[0]

            # Step 3: Insert price
            price_insert = """
            INSERT INTO prices (
                retailer_product_id, store_id, price, price_timestamp, scraped_at
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (retailer_product_id, store_id, price_timestamp) DO NOTHING;
            """

            cursor.execute(price_insert, (
                retailer_product_id,
                self.STORE_ID,
                product['price'],
                datetime.now(),
                datetime.now()
            ))

            # Commit transaction
            self.db_conn.commit()
            cursor.close()

            self.stats['products_inserted_db'] += 1
            return True

        except Exception as e:
            logger.error(f"Database error for product {product.get('barcode')}: {e}")
            self.db_conn.rollback()
            return False

    def extract_wpm_products_from_page(self) -> Dict:
        """Extract wpmDataLayer.products from current page using JavaScript"""
        try:
            # Method 1: Try to get from window object directly
            script = """
            if (window.wpmDataLayer && window.wpmDataLayer.products) {
                return window.wpmDataLayer.products;
            }
            return null;
            """
            products = self.driver.execute_script(script)

            if products:
                logger.info(f"✓ Extracted {len(products)} products via JavaScript window object")
                return products

            # Method 2: Parse from page source
            page_source = self.driver.page_source

            # Pattern 1: Object.assign format
            pattern1 = r'wpmDataLayer\.products\s*=\s*Object\.assign\([^,]+,\s*({.*?})\s*\)'
            match = re.search(pattern1, page_source, re.DOTALL)

            if match:
                products_json = match.group(1)
                products = json.loads(products_json)
                logger.info(f"✓ Extracted {len(products)} products using Object.assign pattern")
                return products

            # Pattern 2: Individual assignments
            pattern2 = r'wpmDataLayer\.products\[(\d+)\]\s*=\s*({.*?});'
            matches = re.findall(pattern2, page_source)

            if matches:
                products = {}
                for product_id, product_json in matches:
                    try:
                        products[product_id] = json.loads(product_json)
                    except:
                        pass

                if products:
                    logger.info(f"✓ Extracted {len(products)} products using individual assignment pattern")
                    return products

            logger.warning("No products found in page")
            return {}

        except Exception as e:
            logger.error(f"Error extracting products: {e}")
            return {}

    def extract_brand_from_name(self, product_name: str) -> str:
        """Extract brand from Hebrew product name"""
        if not product_name:
            return ""
        words = product_name.strip().split()
        return words[0].strip('.,;:-') if words else ""

    def validate_product(self, product: Dict) -> Tuple[bool, List[str]]:
        """
        Validate product data - ALL fields must be present
        Required fields: product_id, barcode, name, price, categories, product_url, image_url, scraped_at
        """
        missing_fields = []

        # Product ID validation
        if not product.get('product_id'):
            missing_fields.append('product_id')

        # Barcode validation - accept both UPC-12 and EAN-13
        barcode = product.get('barcode', '')
        if not barcode:
            missing_fields.append('barcode')
        elif len(barcode) not in [12, 13] or not barcode.isdigit():
            missing_fields.append('barcode_invalid_format')

        # Name validation
        if not product.get('name') or len(product.get('name', '').strip()) == 0:
            missing_fields.append('name')

        # Price validation
        price = product.get('price')
        if price is None or price <= 0:
            missing_fields.append('price')

        # Categories validation
        categories = product.get('categories', [])
        if not categories or len(categories) == 0:
            missing_fields.append('categories')

        # Product URL validation (new)
        if not product.get('product_url') or len(product.get('product_url', '').strip()) == 0:
            missing_fields.append('product_url')

        # Image URL validation (new)
        if not product.get('image_url') or len(product.get('image_url', '').strip()) == 0:
            missing_fields.append('image_url')

        # Scraped timestamp validation
        if not product.get('scraped_at'):
            missing_fields.append('scraped_at')

        is_valid = len(missing_fields) == 0
        return is_valid, missing_fields

    def extract_product_urls_from_html(self) -> Dict[str, Dict]:
        """Extract product URLs and image URLs from HTML product cards"""
        url_map = {}

        try:
            # Find all product list items
            product_elements = self.driver.find_elements(By.CSS_SELECTOR, "ul.products li.product")

            for product_elem in product_elements:
                try:
                    # Get product ID from hidden input
                    product_id_input = product_elem.find_element(By.CSS_SELECTOR, "input.wpmProductId")
                    product_id = product_id_input.get_attribute("data-id")

                    # Get product URL from link
                    product_link = product_elem.find_element(By.CSS_SELECTOR, "a.woocommerce-LoopProduct-link")
                    product_url = product_link.get_attribute("href")

                    # Get image URL
                    img = product_elem.find_element(By.CSS_SELECTOR, "img")
                    # Try data-src first (lazy loading), fall back to src
                    image_url = img.get_attribute("data-src") or img.get_attribute("src")

                    url_map[product_id] = {
                        'product_url': product_url,
                        'image_url': image_url
                    }

                except Exception as e:
                    logger.debug(f"Error extracting URLs for a product: {e}")
                    continue

            logger.info(f"Extracted URLs for {len(url_map)} products from HTML")
            return url_map

        except Exception as e:
            logger.error(f"Error extracting product URLs from HTML: {e}")
            return {}

    def scrape_listing_page(self, page_num: int) -> List[Dict]:
        """Scrape a single listing page"""
        url = f"{self.base_url}/shop/page/{page_num}/"
        logger.info(f"Scraping page {page_num}: {url}")

        try:
            self.driver.get(url)

            # Wait for page to load
            time.sleep(3)  # Allow JavaScript to execute

            # Wait for products to appear
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "ul.products li.product, script"))
                )
            except:
                logger.warning("Timeout waiting for products, continuing anyway...")

            # Scroll to bottom to trigger lazy loading of all images and products
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)  # Wait for lazy-loaded content

            # Scroll back to top
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)

            # Extract products from wpmDataLayer (for core data)
            products_dict = self.extract_wpm_products_from_page()

            if not products_dict:
                logger.warning(f"No products found on page {page_num}")
                return []

            # Extract URLs from HTML (for product_url and image_url)
            url_map = self.extract_product_urls_from_html()

            products_list = []

            for product_id, product_data in products_dict.items():
                # Get URLs from HTML parsing
                urls = url_map.get(product_id, {})

                product = {
                    'product_id': product_data.get('id'),
                    'barcode': product_data.get('sku', ''),
                    'name': product_data.get('name', ''),
                    'price': product_data.get('price', 0),
                    'categories': product_data.get('category', []),
                    'product_url': urls.get('product_url', ''),
                    'image_url': urls.get('image_url', ''),
                    'type': product_data.get('type', ''),
                    'brand': '',
                    'scraped_at': datetime.now().isoformat(),
                    'source_page': page_num,
                    'source_url': url
                }

                # Extract brand
                product['brand'] = self.extract_brand_from_name(product['name'])

                # Validate
                is_valid, missing_fields = self.validate_product(product)

                self.stats['products_found'] += 1

                if is_valid:
                    products_list.append(product)
                    self.stats['products_valid'] += 1

                    # Insert to database
                    if not self.dry_run:
                        success = self.insert_product_to_database(product)
                        if success:
                            logger.debug(f"✓ Inserted product {product['barcode']} to database")
                        else:
                            logger.warning(f"✗ Failed to insert product {product['barcode']} to database")
                else:
                    logger.warning(f"Invalid product: {product['name'][:50]}... Missing: {missing_fields}")
                    self.stats['failures'].append({
                        'barcode': product['barcode'],
                        'name': product['name'],
                        'missing_fields': missing_fields,
                        'page': page_num
                    })

            logger.info(f"Page {page_num}: {len(products_list)} valid products, {self.stats['products_inserted_db']} inserted to DB")
            self.stats['pages_scraped'] += 1

            return products_list

        except Exception as e:
            logger.error(f"Error scraping page {page_num}: {e}", exc_info=True)
            return []

    def scrape_all(self) -> List[Dict]:
        """Scrape all pages"""
        logger.info("="*80)
        logger.info("KOLBO YEHUDA SELENIUM SCRAPER - DRY RUN MODE" if self.dry_run else "KOLBO YEHUDA SELENIUM SCRAPER")
        logger.info("="*80)

        self.stats['start_time'] = datetime.now()
        all_products = []

        # Determine total pages
        logger.info("Determining total pages...")
        self.driver.get(f"{self.base_url}/shop/")
        time.sleep(3)

        # Try to find total pages from page source
        page_source = self.driver.page_source
        match = re.search(r'עמוד \d+ מתוך (\d+)', page_source)
        total_pages = int(match.group(1)) if match else 269  # Default from reconnaissance

        logger.info(f"Total pages: {total_pages}")

        if self.max_pages:
            total_pages = min(total_pages, self.max_pages)
            logger.info(f"Limiting to {total_pages} pages (dry run)")

        # Scrape each page
        for page in range(1, total_pages + 1):
            products = self.scrape_listing_page(page)
            all_products.extend(products)

            logger.info(f"Progress: {page}/{total_pages} pages | {len(all_products)} valid products")

            # Crawl delay (except last page)
            if page < total_pages:
                logger.info(f"Sleeping {self.crawl_delay} seconds...")
                time.sleep(self.crawl_delay)

        self.stats['end_time'] = datetime.now()
        return all_products

    def save_to_json(self, products: List[Dict], filename: str):
        """Save products to JSON"""
        filepath = self.output_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(products, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(products)} products to {filepath}")

    def generate_report(self):
        """Generate report"""
        logger.info("="*80)
        logger.info("SCRAPING REPORT")
        logger.info("="*80)

        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()

        logger.info(f"Duration: {duration:.1f}s ({duration/60:.1f}min)")
        logger.info(f"Pages scraped: {self.stats['pages_scraped']}")
        logger.info(f"Products found: {self.stats['products_found']}")
        logger.info(f"Products valid: {self.stats['products_valid']}")
        logger.info(f"Products inserted to DB: {self.stats['products_inserted_db']}")
        logger.info(f"Products failed: {len(self.stats['failures'])}")

        if not self.dry_run:
            logger.info(f"✓ Database integration: ACTIVE")
        else:
            logger.info(f"⚠ Database integration: DRY RUN (no DB writes)")

        report_file = self.output_dir / 'scraping_report.json'
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(self.stats, f, ensure_ascii=False, indent=2, default=str)

        logger.info(f"Report saved to: {report_file}")

    def cleanup(self):
        """Clean up resources"""
        if self.driver:
            logger.info("Closing browser...")
            self.driver.quit()

        if self.db_conn:
            logger.info("Closing database connection...")
            self.db_conn.close()


def main():
    """Main execution"""
    parser = argparse.ArgumentParser(description='Kolbo Yehuda Selenium Scraper')
    parser.add_argument('--pages', type=int, default=10, help='Number of pages')
    parser.add_argument('--output', type=str, default='products.json', help='Output file')
    parser.add_argument('--visible', action='store_true', help='Run browser visibly (not headless)')
    parser.add_argument('--production', action='store_true', help='Enable database writes (default: dry run mode)')
    parser.add_argument('--full', action='store_true', help='Run full scrape (all 269 pages, writes to database)')

    args = parser.parse_args()

    # Determine dry_run and max_pages based on flags
    if args.full:
        dry_run = False
        max_pages = None  # All pages
    elif args.production:
        dry_run = False
        max_pages = args.pages
    else:
        dry_run = True
        max_pages = args.pages

    scraper = KolboYehudaScraperSelenium(
        dry_run=dry_run,
        max_pages=max_pages,
        headless=not args.visible
    )

    try:
        products = scraper.scrape_all()
        scraper.save_to_json(products, args.output)
        scraper.generate_report()

        logger.info("="*80)
        logger.info("SCRAPING COMPLETED SUCCESSFULLY")
        if not dry_run:
            logger.info(f"Database inserts: {scraper.stats['products_inserted_db']} products")
        logger.info("="*80)

    except KeyboardInterrupt:
        logger.warning("\nInterrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        scraper.cleanup()


if __name__ == "__main__":
    main()
