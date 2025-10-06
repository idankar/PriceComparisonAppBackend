#!/usr/bin/env python3
"""
April.co.il Production Scraper - Database Version
Writes directly to PostgreSQL database
"""

import re
import time
import logging
import psycopg2
from datetime import datetime
from typing import List, Dict, Optional
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'price_comparison_app_v2',
    'user': 'postgres',
    'password': '***REMOVED***'
}

# April retailer and store IDs
APRIL_RETAILER_ID = 153
APRIL_STORE_ID = 17140396

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('april_scraper_db.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class AprilScraperDB:
    """Production scraper for april.co.il with database integration"""

    def __init__(self, max_pages: Optional[int] = None):
        """
        Initialize scraper

        Args:
            max_pages: Maximum pages to scrape (None = all pages)
        """
        self.base_url = "https://www.april.co.il"
        self.max_pages = max_pages
        self.driver = None
        self.db_conn = None
        self.stats = {
            'total_products': 0,
            'successful_products': 0,
            'failed_products': 0,
            'db_inserts_canonical': 0,
            'db_updates_canonical': 0,
            'db_inserts_retailer_products': 0,
            'db_inserts_prices': 0,
            'pages_scraped': 0,
            'start_time': None,
            'end_time': None
        }

    def connect_to_database(self):
        """Establish database connection"""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            logger.info("✓ Database connection established")
            return True
        except Exception as e:
            logger.error(f"✗ Database connection failed: {e}")
            return False

    def close_database(self):
        """Close database connection"""
        if self.db_conn:
            self.db_conn.close()
            logger.info("Database connection closed")

    def setup_driver(self) -> webdriver.Chrome:
        """Configure Chrome driver with anti-detection measures"""
        logger.info("Setting up Chrome driver with anti-detection...")

        options = Options()
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

        driver = webdriver.Chrome(options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        logger.info("✓ Chrome driver configured successfully")
        return driver

    def wait_for_cloudflare(self, timeout: int = 30) -> bool:
        """Wait for Cloudflare challenge to complete"""
        logger.info("Waiting for Cloudflare challenge...")
        time.sleep(3)

        start_time = time.time()
        while time.time() - start_time < timeout:
            if "Just a moment" not in self.driver.page_source:
                logger.info("✓ Cloudflare challenge passed")
                time.sleep(2)
                return True
            time.sleep(1)

        logger.warning("✗ Cloudflare challenge timeout")
        return False

    def extract_barcode_from_datalayer(self, element) -> Optional[str]:
        """Extract barcode from JavaScript dataLayer"""
        try:
            onclick = element.get_attribute('onclick')
            if onclick:
                match = re.search(r"'id':\s*'(\d+)'", onclick)
                if match:
                    return match.group(1)
        except Exception as e:
            logger.debug(f"Error extracting barcode: {e}")
        return None

    def parse_price(self, price_text: str) -> Optional[float]:
        """Extract numeric price from text"""
        try:
            clean_text = price_text.replace('₪', '').replace(',', '').strip()
            match = re.search(r'(\d+\.?\d*)', clean_text)
            if match:
                return float(match.group(1))
        except Exception as e:
            logger.debug(f"Error parsing price '{price_text}': {e}")
        return None

    def insert_into_canonical_products(self, cursor, product: Dict) -> bool:
        """
        Insert or update product in canonical_products table using UPSERT logic
        """
        try:
            sql = """
                INSERT INTO canonical_products (barcode, name, brand, image_url, category, source_retailer_id, url)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (barcode) DO UPDATE SET
                    name = COALESCE(canonical_products.name, EXCLUDED.name),
                    brand = COALESCE(canonical_products.brand, EXCLUDED.brand),
                    image_url = COALESCE(canonical_products.image_url, EXCLUDED.image_url),
                    category = COALESCE(canonical_products.category, EXCLUDED.category),
                    last_scraped_at = NOW()
                RETURNING (xmax = 0) AS inserted;
            """

            cursor.execute(sql, (
                product['barcode'],
                product['name'],
                product['brand'],
                product['image_url'],
                product['category'],
                APRIL_RETAILER_ID,
                product['product_url']
            ))

            result = cursor.fetchone()
            was_inserted = result[0] if result else False

            if was_inserted:
                self.stats['db_inserts_canonical'] += 1
                logger.debug(f"✓ Inserted into canonical_products: {product['barcode']}")
            else:
                self.stats['db_updates_canonical'] += 1
                logger.debug(f"✓ Updated canonical_products: {product['barcode']}")

            return True

        except Exception as e:
            logger.error(f"✗ Error inserting/updating canonical_products: {e}")
            return False

    def insert_into_retailer_products(self, cursor, product: Dict) -> Optional[int]:
        """
        Insert into retailer_products table, linking barcode to retailer
        Returns retailer_product_id
        """
        try:
            # Use barcode as retailer_item_code since we don't have a separate item code
            sql = """
                INSERT INTO retailer_products (retailer_id, retailer_item_code, barcode, original_retailer_name)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (retailer_id, retailer_item_code) DO UPDATE SET
                    barcode = EXCLUDED.barcode,
                    original_retailer_name = EXCLUDED.original_retailer_name
                RETURNING retailer_product_id;
            """

            cursor.execute(sql, (
                APRIL_RETAILER_ID,
                product['barcode'],  # Using barcode as item code
                product['barcode'],
                product['name']
            ))

            result = cursor.fetchone()
            retailer_product_id = result[0] if result else None

            if retailer_product_id:
                self.stats['db_inserts_retailer_products'] += 1
                logger.debug(f"✓ Linked retailer_product: {retailer_product_id}")

            return retailer_product_id

        except Exception as e:
            logger.error(f"✗ Error inserting retailer_products: {e}")
            return None

    def insert_into_prices(self, cursor, retailer_product_id: int, price: float) -> bool:
        """
        Insert price record into prices table
        """
        try:
            sql = """
                INSERT INTO prices (retailer_product_id, store_id, price, price_timestamp)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (retailer_product_id, store_id, price_timestamp) DO NOTHING;
            """

            cursor.execute(sql, (
                retailer_product_id,
                APRIL_STORE_ID,
                price,
                datetime.now()
            ))

            self.stats['db_inserts_prices'] += 1
            logger.debug(f"✓ Inserted price: {price}")
            return True

        except Exception as e:
            logger.error(f"✗ Error inserting price: {e}")
            return False

    def save_product_to_database(self, product: Dict) -> bool:
        """
        Save complete product to all three tables
        """
        try:
            cursor = self.db_conn.cursor()

            # 1. Insert/update canonical_products
            if not self.insert_into_canonical_products(cursor, product):
                cursor.close()
                return False

            # 2. Link in retailer_products
            retailer_product_id = self.insert_into_retailer_products(cursor, product)
            if not retailer_product_id:
                cursor.close()
                return False

            # 3. Insert price
            if product['price_current']:
                if not self.insert_into_prices(cursor, retailer_product_id, product['price_current']):
                    cursor.close()
                    return False

            # Commit transaction
            self.db_conn.commit()
            cursor.close()
            return True

        except Exception as e:
            logger.error(f"✗ Error saving product to database: {e}")
            if self.db_conn:
                self.db_conn.rollback()
            return False

    def extract_product_data(self, container) -> Dict:
        """Extract all data from a single product container"""
        product = {
            'barcode': None,
            'name': None,
            'brand': None,
            'price_current': None,
            'price_original': None,
            'product_url': None,
            'image_url': None,
            'category': None,
        }

        # Extract product link and barcode
        try:
            link = container.find_element(By.CSS_SELECTOR, 'a[onclick*="dataLayer"]')
            product['barcode'] = self.extract_barcode_from_datalayer(link)
            product['product_url'] = link.get_attribute('href')
            if product['product_url'] and not product['product_url'].startswith('http'):
                product['product_url'] = f"{self.base_url}/{product['product_url']}"
        except Exception as e:
            logger.debug(f"Error extracting link/barcode: {e}")

        # Extract product name
        try:
            name_elem = container.find_element(By.CSS_SELECTOR, 'h2.card-title')
            product['name'] = name_elem.text.strip()
        except Exception as e:
            logger.debug(f"Error extracting name: {e}")

        # Extract brand
        try:
            brand_elem = container.find_element(By.CSS_SELECTOR, '.firm-product-list span')
            product['brand'] = brand_elem.text.strip()
        except Exception as e:
            logger.debug(f"Error extracting brand: {e}")

        # Extract current price
        try:
            price_elem = container.find_element(By.CSS_SELECTOR, 'span.saleprice')
            price_text = price_elem.text.strip()
            product['price_current'] = self.parse_price(price_text)
        except Exception as e:
            logger.debug(f"Error extracting current price: {e}")

        # Extract original price (if on sale)
        try:
            old_price_elem = container.find_element(By.CSS_SELECTOR, 'span.oldprice')
            price_text = old_price_elem.text.strip()
            product['price_original'] = self.parse_price(price_text)
        except:
            if product['price_current']:
                product['price_original'] = product['price_current']

        # Extract image URL
        try:
            img_elem = container.find_element(By.CSS_SELECTOR, 'img.img-fluid')
            product['image_url'] = img_elem.get_attribute('src') or img_elem.get_attribute('data-src')
            if product['image_url'] and not product['image_url'].startswith('http'):
                product['image_url'] = f"{self.base_url}/{product['image_url']}"
        except Exception as e:
            logger.debug(f"Error extracting image: {e}")

        return product

    def scrape_current_page(self, category_name: str) -> int:
        """
        Scrape all products from current page and save to database
        Returns count of successfully saved products
        """
        saved_count = 0

        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.col.position-relative.item'))
            )

            containers = self.driver.find_elements(By.CSS_SELECTOR, 'div.col.position-relative.item')
            logger.info(f"Found {len(containers)} product containers on page")

            for idx, container in enumerate(containers, 1):
                try:
                    product = self.extract_product_data(container)
                    product['category'] = category_name

                    # Validate essential fields
                    if product['barcode'] and product['name'] and product['price_current']:
                        # Save to database
                        if self.save_product_to_database(product):
                            saved_count += 1
                            self.stats['successful_products'] += 1
                            logger.debug(f"✓ Product {idx}: {product['name']} (Barcode: {product['barcode']})")
                        else:
                            self.stats['failed_products'] += 1
                            logger.warning(f"✗ Product {idx}: Database save failed")
                    else:
                        self.stats['failed_products'] += 1
                        logger.warning(f"✗ Product {idx}: Missing critical data")

                except Exception as e:
                    self.stats['failed_products'] += 1
                    logger.error(f"✗ Error processing product {idx}: {e}")

            self.stats['total_products'] += len(containers)

        except Exception as e:
            logger.error(f"Error scraping page: {e}")

        return saved_count

    def navigate_to_next_page(self, current_page: int) -> bool:
        """Navigate to next page"""
        try:
            next_page_display = current_page + 2

            try:
                pagination = self.driver.find_element(By.CSS_SELECTOR, 'ul.pagination')
            except:
                logger.info("No pagination found - single page category")
                return False

            page_links = pagination.find_elements(By.CSS_SELECTOR, 'a[href*="Go2Page"]')

            has_next = False
            for link in page_links:
                href = link.get_attribute('href')
                if href and f'Go2Page({next_page_display})' in href:
                    parent_li = link.find_element(By.XPATH, '..')
                    parent_class = parent_li.get_attribute('class') or ''

                    if 'disabled' not in parent_class:
                        has_next = True
                        break

            if not has_next:
                logger.info(f"No more pages available")
                return False

            logger.info(f"Navigating to page {next_page_display}...")
            self.driver.execute_script(f"Go2Page({next_page_display});")
            time.sleep(4)

            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div.col.position-relative.item'))
                )
                time.sleep(1)
            except:
                pass

            return True

        except Exception as e:
            logger.error(f"Error navigating to next page: {e}")
            return False

    def scrape_category(self, category_url: str, category_name: str) -> int:
        """
        Scrape all products from a category
        Returns total count of saved products
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Scraping category: {category_name}")
        logger.info(f"{'='*60}")

        if not category_url.startswith('http'):
            category_url = f"{self.base_url}/{category_url}"

        total_saved = 0
        current_page = 0

        try:
            logger.info(f"Navigating to: {category_url}")
            self.driver.get(category_url)

            if not self.wait_for_cloudflare():
                logger.error("Failed to bypass Cloudflare")
                return total_saved

            try:
                total_elem = self.driver.find_element(By.ID, "TotalProductAfterFilter")
                total_products = int(total_elem.get_attribute('value'))
                logger.info(f"Total products in category: {total_products}")
            except:
                logger.warning("Could not determine total product count")

            while True:
                logger.info(f"\n--- Page {current_page + 1} ---")

                saved_count = self.scrape_current_page(category_name)
                total_saved += saved_count
                self.stats['pages_scraped'] += 1

                logger.info(f"Saved {saved_count} products from page {current_page + 1}")

                if self.max_pages and self.stats['pages_scraped'] >= self.max_pages:
                    logger.info(f"Reached max pages limit ({self.max_pages})")
                    break

                if not self.navigate_to_next_page(current_page):
                    break

                current_page += 1
                time.sleep(2)

        except Exception as e:
            logger.error(f"Error scraping category: {e}")

        logger.info(f"\n✓ Category complete: {total_saved} products saved to database")
        return total_saved

    def print_statistics(self):
        """Print scraping statistics"""
        if self.stats['start_time'] and self.stats['end_time']:
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        else:
            duration = 0

        logger.info(f"\n{'='*60}")
        logger.info("SCRAPING STATISTICS")
        logger.info(f"{'='*60}")
        logger.info(f"Pages scraped:                  {self.stats['pages_scraped']}")
        logger.info(f"Products found:                 {self.stats['total_products']}")
        logger.info(f"Products successfully saved:    {self.stats['successful_products']}")
        logger.info(f"Products failed:                {self.stats['failed_products']}")
        logger.info(f"")
        logger.info(f"DB - canonical_products inserts: {self.stats['db_inserts_canonical']}")
        logger.info(f"DB - canonical_products updates: {self.stats['db_updates_canonical']}")
        logger.info(f"DB - retailer_products inserts:  {self.stats['db_inserts_retailer_products']}")
        logger.info(f"DB - prices inserts:             {self.stats['db_inserts_prices']}")

        if self.stats['total_products'] > 0:
            success_rate = (self.stats['successful_products'] / self.stats['total_products']) * 100
            logger.info(f"Success rate:                   {success_rate:.2f}%")

        if duration > 0:
            logger.info(f"Total duration:                 {duration:.2f} seconds")
            logger.info(f"Avg time per page:              {duration / self.stats['pages_scraped']:.2f} seconds")

        logger.info(f"{'='*60}\n")

    def run(self, category_url: str = 'women-perfume',
            category_name: str = 'Women Perfume',
            max_pages: Optional[int] = None) -> int:
        """
        Execute scraping run

        Args:
            category_url: Category to scrape
            category_name: Display name
            max_pages: Maximum pages to scrape (None = all)

        Returns:
            Total products saved to database
        """
        logger.info(f"\n{'#'*60}")
        logger.info("APRIL.CO.IL SCRAPER - DATABASE MODE")
        logger.info(f"{'#'*60}\n")

        self.max_pages = max_pages
        self.stats['start_time'] = datetime.now()

        try:
            # Connect to database
            if not self.connect_to_database():
                logger.error("Failed to connect to database. Exiting.")
                return 0

            # Setup driver
            self.driver = self.setup_driver()

            # Scrape category
            total_saved = self.scrape_category(category_url, category_name)

            # Print statistics
            self.stats['end_time'] = datetime.now()
            self.print_statistics()

            return total_saved

        except Exception as e:
            logger.error(f"Scraping run failed: {e}")
            import traceback
            traceback.print_exc()
            return 0

        finally:
            if self.driver:
                logger.info("Closing browser...")
                self.driver.quit()

            if self.db_conn:
                self.close_database()


def main():
    """Main entry point"""
    # Test run: 1 page only
    scraper = AprilScraperDB()

    saved_count = scraper.run(
        category_url='women-perfume',
        category_name='Women Perfume',
        max_pages=1  # Test with 1 page first
    )

    logger.info(f"\n✅ Test run complete! Saved {saved_count} products to database.")


if __name__ == "__main__":
    main()
