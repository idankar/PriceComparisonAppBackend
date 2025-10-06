#!/usr/bin/env python3
"""
Kolbo Yehuda Scraper - Production Version
==========================================
Scrapes product data from kolboyehuda.co.il including:
- Product names, barcodes (EAN-13), prices, images, categories
- Implements failure analysis with screenshot capture
- Respects 15-second crawl-delay per robots.txt
"""

import requests
from bs4 import BeautifulSoup
import json
import re
import time
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import argparse
import logging
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('kolboyehuda_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class KolboYehudaScraper:
    """Production scraper for kolboyehuda.co.il"""

    def __init__(self, dry_run: bool = True, max_pages: Optional[int] = None):
        self.base_url = "https://www.kolboyehuda.co.il"
        self.crawl_delay = 15  # From robots.txt
        self.dry_run = dry_run
        self.max_pages = max_pages

        # Session setup
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept-Language': 'he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        })

        # Statistics tracking
        self.stats = {
            'pages_scraped': 0,
            'products_found': 0,
            'products_valid': 0,
            'failures': [],
            'start_time': None,
            'end_time': None
        }

        # Create output directories
        self.output_dir = Path('kolboyehuda_scraper_output')
        self.screenshots_dir = self.output_dir / 'failure_screenshots'
        self.output_dir.mkdir(exist_ok=True)
        self.screenshots_dir.mkdir(exist_ok=True)

        # Selenium driver (lazy initialization)
        self.driver = None

    def _init_driver(self):
        """Initialize Selenium driver for screenshot capture (lazy loading)"""
        if self.driver is None:
            logger.info("Initializing Selenium driver for failure analysis...")
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')

            self.driver = webdriver.Chrome(options=chrome_options)

    def extract_total_pages(self, html: str) -> int:
        """Extract total page count from page title"""
        match = re.search(r'עמוד \d+ מתוך (\d+)', html)
        if match:
            return int(match.group(1))

        # Fallback: try to find in page content
        soup = BeautifulSoup(html, 'html.parser')
        title = soup.find('title')
        if title:
            match = re.search(r'עמוד \d+ מתוך (\d+)', title.text)
            if match:
                return int(match.group(1))

        logger.warning("Could not determine total pages, defaulting to 1")
        return 1

    def extract_wpm_products(self, html: str) -> Dict:
        """Extract products from wpmDataLayer.products JavaScript object"""
        # Pattern 1: Object.assign format (most common)
        # window.wpmDataLayer.products = Object.assign(window.wpmDataLayer.products, {...});
        pattern1 = r'wpmDataLayer\.products\s*=\s*Object\.assign\([^,]+,\s*({.*?})\s*\)'
        match = re.search(pattern1, html, re.DOTALL)

        if match:
            try:
                products_json = match.group(1)
                products = json.loads(products_json)
                logger.info(f"✓ Extracted {len(products)} products using Object.assign pattern")
                return products
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error (Object.assign pattern): {e}")
                logger.debug(f"Problematic JSON: {products_json[:500]}")
        else:
            logger.debug("Object.assign pattern did not match")

        # Pattern 2: Direct assignment
        pattern2 = r'wpmDataLayer\.products\s*=\s*({.*?});'
        match2 = re.search(pattern2, html, re.DOTALL)
        if match2:
            try:
                products = json.loads(match2.group(1))
                logger.info(f"✓ Extracted {len(products)} products using direct assignment pattern")
                return products
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error (direct assignment): {e}")
        else:
            logger.debug("Direct assignment pattern did not match")

        # Pattern 3: Individual product assignments (aggregate them)
        # window.wpmDataLayer.products[123002] = {...};
        pattern3 = r'wpmDataLayer\.products\[(\d+)\]\s*=\s*({.*?});'
        matches = re.findall(pattern3, html)

        if matches:
            products = {}
            for product_id, product_json in matches:
                try:
                    products[product_id] = json.loads(product_json)
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error for product {product_id}: {e}")

            if products:
                logger.info(f"✓ Extracted {len(products)} products using individual assignment pattern")
                return products
        else:
            logger.debug("Individual assignment pattern did not match")

        logger.warning("No wpmDataLayer.products found with any pattern")
        return {}

    def extract_brand_from_name(self, product_name: str) -> str:
        """
        Extract brand from Hebrew product name.
        Strategy: First word is typically the brand name in Hebrew.
        """
        if not product_name:
            return ""

        # Clean the name
        name = product_name.strip()

        # Split on spaces
        words = name.split()

        if not words:
            return ""

        # First word is usually the brand
        brand = words[0]

        # Clean common suffixes/prefixes
        brand = brand.strip('.,;:-')

        return brand

    def validate_product(self, product: Dict) -> Tuple[bool, List[str]]:
        """
        Validate product data and return (is_valid, list_of_missing_fields)

        Critical fields:
        - barcode (SKU)
        - name
        - price
        - product_id
        - categories
        """
        missing_fields = []

        # Barcode validation (most critical)
        barcode = product.get('barcode', '')
        if not barcode:
            missing_fields.append('barcode')
        elif len(barcode) != 13 or not barcode.isdigit():
            missing_fields.append('barcode_invalid_format')

        # Name validation
        if not product.get('name') or len(product.get('name', '').strip()) == 0:
            missing_fields.append('name')

        # Price validation
        price = product.get('price')
        if price is None or price <= 0:
            missing_fields.append('price')

        # Product ID validation
        if not product.get('product_id'):
            missing_fields.append('product_id')

        # Categories validation
        categories = product.get('categories', [])
        if not categories or len(categories) == 0:
            missing_fields.append('categories')

        # Image URL validation (warning only, not critical)
        if not product.get('image_url'):
            missing_fields.append('image_url')

        is_valid = len(missing_fields) == 0
        return is_valid, missing_fields

    def capture_failure_screenshot(self, product: Dict, missing_fields: List[str]):
        """
        Capture screenshot of product page for failed validation.
        Only initializes Selenium when actually needed.
        """
        try:
            # Initialize driver if not already done
            self._init_driver()

            # Construct product URL (we need to derive it)
            # Since we don't have the URL in the listing data, we'll use product_id
            # or try to find it on the listing page
            product_id = product.get('product_id', 'unknown')
            barcode = product.get('barcode', 'unknown')
            name = product.get('name', 'unknown')

            # Try to construct a reasonable URL or navigate to search
            # For now, we'll save the data and screenshot attempt info
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"failure_{barcode}_{timestamp}.png"
            filepath = self.screenshots_dir / filename

            # If we had the product URL, we would navigate:
            # self.driver.get(product_url)
            # time.sleep(2)
            # self.driver.save_screenshot(str(filepath))

            # Instead, let's search for the product
            search_url = f"{self.base_url}/shop/?s={barcode}"
            self.driver.get(search_url)
            time.sleep(3)

            self.driver.save_screenshot(str(filepath))

            failure_info = {
                'barcode': barcode,
                'product_id': product_id,
                'name': name,
                'missing_fields': missing_fields,
                'screenshot': str(filepath),
                'timestamp': timestamp
            }

            self.stats['failures'].append(failure_info)

            logger.warning(f"FAILURE: Barcode {barcode} - Missing fields: {', '.join(missing_fields)}")
            logger.info(f"Screenshot saved: {filepath}")

        except Exception as e:
            logger.error(f"Failed to capture screenshot: {e}")

    def scrape_listing_page(self, page_num: int) -> List[Dict]:
        """Scrape a single listing page and return validated products"""
        url = f"{self.base_url}/shop/page/{page_num}/"
        logger.info(f"Scraping page {page_num}: {url}")

        try:
            response = self.session.get(url, timeout=30)

            if response.status_code != 200:
                logger.error(f"HTTP {response.status_code} for page {page_num}")
                return []

            logger.info(f"Response URL: {response.url}, Content length: {len(response.text)}")

            # Extract products from wpmDataLayer
            products_dict = self.extract_wpm_products(response.text)

            if not products_dict:
                logger.warning(f"No products found in wpmDataLayer on page {page_num}")
                return []

            products_list = []

            for product_id, product_data in products_dict.items():
                # Build product object
                product = {
                    'product_id': product_data.get('id'),
                    'barcode': product_data.get('sku', ''),
                    'name': product_data.get('name', ''),
                    'price': product_data.get('price', 0),
                    'categories': product_data.get('categories', []),
                    'type': product_data.get('type', ''),
                    'brand': '',  # Will be extracted
                    'image_url': '',  # Not available in listing data
                    'scraped_at': datetime.now().isoformat(),
                    'source_page': page_num,
                    'source_url': url
                }

                # Extract brand from name
                product['brand'] = self.extract_brand_from_name(product['name'])

                # Validate product
                is_valid, missing_fields = self.validate_product(product)

                self.stats['products_found'] += 1

                if is_valid:
                    products_list.append(product)
                    self.stats['products_valid'] += 1
                else:
                    # Capture failure screenshot
                    logger.warning(f"Product validation failed: {product['name'][:50]}... - Missing: {missing_fields}")

                    # Only capture screenshot for critical failures (not image_url)
                    critical_failures = [f for f in missing_fields if f != 'image_url']
                    if critical_failures:
                        self.capture_failure_screenshot(product, missing_fields)

            logger.info(f"Page {page_num}: Found {len(products_dict)} products, {len(products_list)} valid")
            self.stats['pages_scraped'] += 1

            return products_list

        except requests.RequestException as e:
            logger.error(f"Request error on page {page_num}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error on page {page_num}: {e}", exc_info=True)
            return []

    def scrape_all(self) -> List[Dict]:
        """Scrape all products from all listing pages"""
        logger.info("="*80)
        logger.info("KOLBO YEHUDA SCRAPER - DRY RUN MODE" if self.dry_run else "KOLBO YEHUDA SCRAPER - PRODUCTION MODE")
        logger.info("="*80)

        self.stats['start_time'] = datetime.now()
        all_products = []

        # Get first page to determine total
        logger.info("Fetching first page to determine total page count...")
        response = self.session.get(f"{self.base_url}/shop/")
        total_pages = self.extract_total_pages(response.text)

        logger.info(f"Total pages available: {total_pages}")

        # Apply max_pages limit
        if self.max_pages:
            total_pages = min(total_pages, self.max_pages)
            logger.info(f"Limiting to {total_pages} pages (dry run mode)")

        # Scrape each page
        for page in range(1, total_pages + 1):
            products = self.scrape_listing_page(page)
            all_products.extend(products)

            # Progress update
            logger.info(f"Progress: {page}/{total_pages} pages | {len(all_products)} valid products collected")

            # Respect crawl-delay (except on last page)
            if page < total_pages:
                logger.info(f"Respecting crawl-delay: sleeping for {self.crawl_delay} seconds...")
                time.sleep(self.crawl_delay)

        self.stats['end_time'] = datetime.now()

        return all_products

    def save_to_json(self, products: List[Dict], filename: str):
        """Save products to JSON file"""
        filepath = self.output_dir / filename

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(products, f, ensure_ascii=False, indent=2)

        logger.info(f"Saved {len(products)} products to {filepath}")

    def generate_report(self):
        """Generate comprehensive scraping report"""
        logger.info("="*80)
        logger.info("SCRAPING REPORT")
        logger.info("="*80)

        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()

        logger.info(f"Duration: {duration:.1f} seconds ({duration/60:.1f} minutes)")
        logger.info(f"Pages scraped: {self.stats['pages_scraped']}")
        logger.info(f"Products found: {self.stats['products_found']}")
        logger.info(f"Products valid: {self.stats['products_valid']}")
        logger.info(f"Products failed: {len(self.stats['failures'])}")

        if self.stats['failures']:
            logger.info("\nFAILURE ANALYSIS:")
            logger.info("-"*80)
            for failure in self.stats['failures']:
                logger.info(f"Barcode: {failure['barcode']}")
                logger.info(f"  Name: {failure['name'][:60]}...")
                logger.info(f"  Missing: {', '.join(failure['missing_fields'])}")
                logger.info(f"  Screenshot: {failure['screenshot']}")
                logger.info("-"*40)

        # Save report to file
        report_file = self.output_dir / 'scraping_report.json'
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(self.stats, f, ensure_ascii=False, indent=2, default=str)

        logger.info(f"\nReport saved to: {report_file}")

    def cleanup(self):
        """Clean up resources"""
        if self.driver:
            logger.info("Closing Selenium driver...")
            self.driver.quit()


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Kolbo Yehuda Product Scraper')
    parser.add_argument('--pages', type=int, default=10,
                        help='Number of pages to scrape (default: 10 for dry run)')
    parser.add_argument('--full', action='store_true',
                        help='Run full scrape (all pages, ~67 minutes)')
    parser.add_argument('--output', type=str, default='products.json',
                        help='Output JSON filename (default: products.json)')

    args = parser.parse_args()

    # Determine run mode
    if args.full:
        max_pages = None
        dry_run = False
        logger.info("Running in FULL PRODUCTION mode - will scrape ALL pages")
    else:
        max_pages = args.pages
        dry_run = True
        logger.info(f"Running in DRY RUN mode - will scrape {max_pages} pages")

    # Initialize scraper
    scraper = KolboYehudaScraper(dry_run=dry_run, max_pages=max_pages)

    try:
        # Run scraper
        products = scraper.scrape_all()

        # Save results
        scraper.save_to_json(products, args.output)

        # Generate report
        scraper.generate_report()

        logger.info("="*80)
        logger.info("SCRAPING COMPLETED SUCCESSFULLY")
        logger.info("="*80)

    except KeyboardInterrupt:
        logger.warning("\nScraping interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        # Cleanup
        scraper.cleanup()


if __name__ == "__main__":
    main()
