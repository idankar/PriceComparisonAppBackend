#!/usr/bin/env python3
"""
Kolbo Yehuda Backfill Script
Attempts to recover and insert products that failed during initial scrape
"""

import json
import logging
import time
import argparse
from datetime import datetime
from typing import Dict, List, Tuple
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import psycopg2
from psycopg2.extras import execute_values

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'price_comparison_app_v2',
    'user': 'postgres',
    'password': '***REMOVED***'
}

RETAILER_ID = 152  # Kolbo Yehuda
STORE_ID = 17140395  # Kolbo Yehuda Online Store


class KolboYehudaBackfill:
    def __init__(self, headless: bool = True, auto_mode: bool = False, dry_run: bool = True):
        self.headless = headless
        self.auto_mode = auto_mode
        self.dry_run = dry_run
        self.driver = None
        self.db_conn = None
        self.stats = {
            'total_failed': 0,
            'recovered': 0,
            'still_failed': 0,
            'inserted': 0,
            'ready_for_insertion': 0
        }

    def _init_selenium(self):
        """Initialize Selenium driver"""
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument('--headless=new')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--window-size=1920,1080')

        self.driver = webdriver.Chrome(options=chrome_options)
        logger.info("✓ Selenium driver initialized")

    def _init_database(self):
        """Initialize PostgreSQL database connection"""
        try:
            self.db_conn = psycopg2.connect(**DB_CONFIG)
            self.db_conn.autocommit = False
            logger.info("✓ Database connection established")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def load_failures(self, report_path: str) -> List[Dict]:
        """Load failed products from scraping report"""
        try:
            with open(report_path, 'r', encoding='utf-8') as f:
                report = json.load(f)

            failures = report.get('failures', [])
            self.stats['total_failed'] = len(failures)
            logger.info(f"Loaded {len(failures)} failed products from {report_path}")
            return failures
        except Exception as e:
            logger.error(f"Failed to load report: {e}")
            return []

    def categorize_failures(self, failures: List[Dict]) -> Dict[str, List[Dict]]:
        """Categorize failures by type"""
        categories = {
            'invalid_barcode': [],
            'missing_urls': [],
            'missing_categories': [],
            'multiple_issues': []
        }

        for failure in failures:
            missing = failure.get('missing_fields', [])

            if 'barcode_invalid_format' in missing:
                categories['invalid_barcode'].append(failure)
            elif set(missing) == {'product_url', 'image_url'}:
                categories['missing_urls'].append(failure)
            elif 'categories' in missing and len(missing) == 1:
                categories['missing_categories'].append(failure)
            else:
                categories['multiple_issues'].append(failure)

        for category, items in categories.items():
            logger.info(f"{category}: {len(items)} products")

        return categories

    def try_fetch_product_page(self, barcode: str, product_name: str) -> Tuple[bool, Dict]:
        """
        Attempt to fetch product page directly using barcode or product name search
        Returns: (success, product_data)
        """
        product_data = {
            'product_url': None,
            'image_url': None,
            'categories': []
        }

        # Try searching by barcode first
        search_url = f"https://www.kolboyehuda.co.il/?s={barcode}&post_type=product"

        try:
            self.driver.get(search_url)
            time.sleep(2)

            # Check if we landed on a product page
            if '/product/' in self.driver.current_url:
                product_data['product_url'] = self.driver.current_url

                # Extract image URL
                try:
                    img = self.driver.find_element(By.CSS_SELECTOR, "img.wp-post-image")
                    product_data['image_url'] = img.get_attribute("src") or img.get_attribute("data-src")
                except:
                    pass

                # Extract categories
                try:
                    breadcrumbs = self.driver.find_elements(By.CSS_SELECTOR, ".woocommerce-breadcrumb a")
                    product_data['categories'] = [b.text for b in breadcrumbs[1:]]  # Skip "Home"
                except:
                    pass

                return True, product_data

            # Check if search results exist
            try:
                first_result = self.driver.find_element(By.CSS_SELECTOR, "ul.products li.product a.woocommerce-LoopProduct-link")
                product_data['product_url'] = first_result.get_attribute("href")

                # Get image from search result
                try:
                    img = self.driver.find_element(By.CSS_SELECTOR, "ul.products li.product img")
                    product_data['image_url'] = img.get_attribute("src") or img.get_attribute("data-src")
                except:
                    pass

                return True, product_data
            except:
                return False, product_data

        except Exception as e:
            logger.debug(f"Search failed for barcode {barcode}: {e}")
            return False, product_data

    def enrich_product_from_page(self, product_url: str) -> Tuple[bool, Dict]:
        """
        Visit product page directly to extract missing image_url and categories
        Returns: (success, enriched_data)
        """
        enriched_data = {
            'image_url': None,
            'categories': []
        }

        try:
            self.driver.get(product_url)
            time.sleep(2)

            # Extract main product image
            try:
                # Try multiple image selectors
                img = None
                selectors = [
                    "img.wp-post-image",
                    "div.woocommerce-product-gallery__image img",
                    "figure.woocommerce-product-gallery__wrapper img",
                    "div.product-images img"
                ]

                for selector in selectors:
                    try:
                        img = self.driver.find_element(By.CSS_SELECTOR, selector)
                        if img:
                            break
                    except:
                        continue

                if img:
                    enriched_data['image_url'] = img.get_attribute("src") or img.get_attribute("data-src") or img.get_attribute("data-lazy-src")
            except Exception as e:
                logger.debug(f"Could not extract image from {product_url}: {e}")

            # Extract categories from JSON-LD breadcrumb schema
            try:
                # Find JSON-LD script tags
                scripts = self.driver.find_elements(By.CSS_SELECTOR, 'script[type="application/ld+json"]')

                for script in scripts:
                    try:
                        script_content = script.get_attribute('innerHTML')
                        data = json.loads(script_content)

                        # Handle both direct BreadcrumbList and @graph structure
                        breadcrumb_list = None

                        if isinstance(data, dict):
                            # Check if it's a direct BreadcrumbList
                            if data.get('@type') == 'BreadcrumbList':
                                breadcrumb_list = data
                            # Check if it's inside a @graph array
                            elif '@graph' in data:
                                for item in data['@graph']:
                                    if isinstance(item, dict) and item.get('@type') == 'BreadcrumbList':
                                        breadcrumb_list = item
                                        break

                        if breadcrumb_list:
                            items = breadcrumb_list.get('itemListElement', [])

                            # Extract category names from breadcrumb items
                            # Skip first 2 items (Home, Shop) and last item (product itself)
                            categories = []
                            for item in items[2:-1]:  # Skip Home, Shop, and product name
                                if isinstance(item, dict):
                                    name = item.get('name', '').strip()
                                    if name:
                                        categories.append(name)

                            if categories:
                                enriched_data['categories'] = categories
                                break

                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        logger.debug(f"Error parsing JSON-LD: {e}")
                        continue

            except Exception as e:
                logger.debug(f"Could not extract categories from {product_url}: {e}")

            has_data = bool(enriched_data.get('image_url')) or bool(enriched_data.get('categories'))
            return has_data, enriched_data

        except Exception as e:
            logger.debug(f"Failed to enrich from product page {product_url}: {e}")
            return False, enriched_data

    def insert_product_to_database(self, product: Dict) -> bool:
        """Insert product to database (same logic as main scraper)"""
        try:
            cursor = self.db_conn.cursor()

            # Step 1: UPSERT into canonical_products
            category_str = ', '.join(product['categories']) if product.get('categories') else None

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
                product.get('brand'),
                product.get('image_url'),
                category_str,
                product.get('product_url'),
                RETAILER_ID,
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
                RETAILER_ID,
                product.get('product_id', product['barcode']),
                product['name'],
                product['barcode']
            ))

            retailer_product_id = cursor.fetchone()[0]

            # Step 3: Insert price (if available)
            if product.get('price'):
                price_insert = """
                INSERT INTO prices (
                    retailer_product_id, store_id, price, price_timestamp, scraped_at
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (retailer_product_id, store_id, price_timestamp) DO NOTHING;
                """

                cursor.execute(price_insert, (
                    retailer_product_id,
                    STORE_ID,
                    product['price'],
                    datetime.now(),
                    datetime.now()
                ))

            self.db_conn.commit()
            cursor.close()
            return True

        except Exception as e:
            logger.error(f"Database error for product {product.get('barcode')}: {e}")
            self.db_conn.rollback()
            return False

    def process_missing_urls(self, failures: List[Dict]) -> List[Dict]:
        """Process products missing URLs - try to fetch them"""
        recovered = []

        logger.info(f"Processing {len(failures)} products with missing URLs...")

        for i, failure in enumerate(failures):
            barcode = failure.get('barcode')
            name = failure.get('name')

            logger.info(f"[{i+1}/{len(failures)}] Attempting to fetch: {name[:50]}...")

            success, data = self.try_fetch_product_page(barcode, name)

            if success and (data.get('product_url') or data.get('image_url')):
                logger.info(f"  ✓ Recovered URLs for {barcode}")
                failure.update(data)
                recovered.append(failure)
                self.stats['recovered'] += 1
            else:
                logger.warning(f"  ✗ Still missing URLs for {barcode}")
                self.stats['still_failed'] += 1

            time.sleep(2)  # Be polite

        return recovered

    def enrich_products_with_missing_fields(self, products: List[Dict]) -> List[Dict]:
        """
        Visit individual product pages to enrich products with missing images/categories
        """
        enriched_count = 0

        logger.info(f"\n🔍 Enriching {len(products)} products by visiting individual pages...")

        for i, product in enumerate(products):
            product_url = product.get('product_url')

            if not product_url:
                continue

            # Check what's missing
            needs_image = not product.get('image_url')
            needs_categories = not product.get('categories') or len(product.get('categories', [])) == 0

            if not needs_image and not needs_categories:
                continue

            missing_fields = []
            if needs_image:
                missing_fields.append("image")
            if needs_categories:
                missing_fields.append("categories")

            logger.info(f"[{i+1}/{len(products)}] Enriching {product.get('barcode')} (missing: {', '.join(missing_fields)})...")

            success, enriched_data = self.enrich_product_from_page(product_url)

            if success:
                # Update product with enriched data
                if enriched_data.get('image_url'):
                    product['image_url'] = enriched_data['image_url']
                    logger.info(f"  ✓ Found image URL")

                if enriched_data.get('categories'):
                    product['categories'] = enriched_data['categories']
                    logger.info(f"  ✓ Found {len(enriched_data['categories'])} categories: {', '.join(enriched_data['categories'])}")

                enriched_count += 1
            else:
                logger.warning(f"  ✗ Could not enrich product")

            time.sleep(2)  # Be polite

        logger.info(f"✅ Enriched {enriched_count}/{len(products)} products with additional data")
        return products

    def process_invalid_barcodes(self, failures: List[Dict]) -> List[Dict]:
        """Process products with invalid barcodes - manual intervention needed"""
        logger.info(f"Found {len(failures)} products with invalid barcodes")
        logger.info("Manual barcode correction required. Skipping for now.")
        logger.info("These products will be saved to 'kolboyehuda_invalid_barcodes.json' for manual review")

        # Save for manual review
        output_path = 'kolboyehuda_scraper_output/invalid_barcodes.json'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(failures, f, ensure_ascii=False, indent=2)

        logger.info(f"Saved {len(failures)} invalid barcode products to {output_path}")
        return []

    def run_backfill(self, report_path: str):
        """Main backfill process"""
        logger.info("="*80)
        if self.dry_run:
            logger.info("KOLBO YEHUDA BACKFILL SCRIPT - DRY RUN MODE")
        else:
            logger.info("KOLBO YEHUDA BACKFILL SCRIPT - PRODUCTION MODE")
        logger.info("="*80)

        # Load failures
        failures = self.load_failures(report_path)
        if not failures:
            logger.warning("No failures to process")
            return

        # Categorize
        categories = self.categorize_failures(failures)

        # Initialize resources
        self._init_selenium()
        if not self.dry_run:
            self._init_database()

        all_recovered = []

        # Process missing URLs (most likely to succeed)
        if categories['missing_urls']:
            recovered = self.process_missing_urls(categories['missing_urls'])
            all_recovered.extend(recovered)

        # Process missing categories (can insert without categories)
        if categories['missing_categories']:
            logger.info(f"Found {len(categories['missing_categories'])} products with missing categories")
            logger.info("These can be inserted with NULL categories")
            all_recovered.extend(categories['missing_categories'])

        # Process invalid barcodes (manual intervention needed)
        if categories['invalid_barcode']:
            self.process_invalid_barcodes(categories['invalid_barcode'])

        # Enrich recovered products by visiting individual product pages
        if all_recovered:
            all_recovered = self.enrich_products_with_missing_fields(all_recovered)

        # Process recovered products
        if all_recovered:
            ready_products = []

            for product in all_recovered:
                # Build product dict for insertion
                product_data = {
                    'barcode': product.get('barcode'),
                    'name': product.get('name'),
                    'brand': None,  # Extract if needed
                    'image_url': product.get('image_url'),
                    'categories': product.get('categories', []),
                    'product_url': product.get('product_url'),
                    'price': None,  # No price data in failure report
                    'product_id': product.get('barcode')
                }

                # Validate product has required fields
                has_barcode = bool(product_data['barcode'])
                has_name = bool(product_data['name'])

                if has_barcode and has_name:
                    ready_products.append(product_data)
                    self.stats['ready_for_insertion'] += 1

            if self.dry_run:
                # DRY RUN: Save to JSON and report
                logger.info(f"\n[DRY RUN] Would insert {len(ready_products)} products")

                # Save recovered products to JSON
                output_path = 'kolboyehuda_scraper_output/backfill_ready.json'
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(ready_products, f, ensure_ascii=False, indent=2)

                logger.info(f"✓ Saved {len(ready_products)} recovered products to {output_path}")

                # Validate and report field coverage
                logger.info("\nField Coverage Analysis:")
                with_urls = sum(1 for p in ready_products if p.get('product_url'))
                with_images = sum(1 for p in ready_products if p.get('image_url'))
                with_categories = sum(1 for p in ready_products if p.get('categories'))

                logger.info(f"  - Products with product_url: {with_urls}/{len(ready_products)} ({100*with_urls/len(ready_products):.1f}%)")
                logger.info(f"  - Products with image_url: {with_images}/{len(ready_products)} ({100*with_images/len(ready_products):.1f}%)")
                logger.info(f"  - Products with categories: {with_categories}/{len(ready_products)} ({100*with_categories/len(ready_products):.1f}%)")

                # Show sample products
                logger.info("\nSample recovered products (first 5):")
                for i, product in enumerate(ready_products[:5]):
                    logger.info(f"\n  Product {i+1}:")
                    logger.info(f"    Barcode: {product['barcode']}")
                    logger.info(f"    Name: {product['name'][:60]}...")
                    logger.info(f"    Has URL: {bool(product.get('product_url'))}")
                    logger.info(f"    Has Image: {bool(product.get('image_url'))}")
                    logger.info(f"    Categories: {len(product.get('categories', []))} items")
            else:
                # PRODUCTION: Insert to database
                logger.info(f"\n[PRODUCTION] Inserting {len(ready_products)} recovered products to database...")

                for product_data in ready_products:
                    if self.insert_product_to_database(product_data):
                        self.stats['inserted'] += 1
                        logger.info(f"  ✓ Inserted {product_data['barcode']}")

        # Final report
        logger.info("="*80)
        logger.info("BACKFILL REPORT")
        logger.info("="*80)
        logger.info(f"Total failed products: {self.stats['total_failed']}")
        logger.info(f"Recovered: {self.stats['recovered']}")
        if self.dry_run:
            logger.info(f"Ready for insertion: {self.stats['ready_for_insertion']}")
            logger.info(f"Saved to: kolboyehuda_scraper_output/backfill_ready.json")
        else:
            logger.info(f"Inserted to DB: {self.stats['inserted']}")
        logger.info(f"Still failed: {self.stats['still_failed']}")
        logger.info("="*80)

        # Cleanup
        if self.driver:
            self.driver.quit()
        if self.db_conn:
            self.db_conn.close()


def main():
    parser = argparse.ArgumentParser(description='Kolbo Yehuda Backfill Script')
    parser.add_argument('--report', type=str,
                       default='kolboyehuda_scraper_output/scraping_report.json',
                       help='Path to scraping report JSON')
    parser.add_argument('--visible', action='store_true',
                       help='Run browser visibly (not headless)')
    parser.add_argument('--auto', action='store_true',
                       help='Automatic mode (no user prompts)')
    parser.add_argument('--production', action='store_true',
                       help='Production mode - insert to database (default: dry run)')

    args = parser.parse_args()

    backfill = KolboYehudaBackfill(
        headless=not args.visible,
        auto_mode=args.auto,
        dry_run=not args.production
    )

    backfill.run_backfill(args.report)


if __name__ == "__main__":
    main()
