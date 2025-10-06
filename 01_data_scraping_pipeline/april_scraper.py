#!/usr/bin/env python3
"""
April.co.il Production Scraper
Single-Pass Architecture - Extracts all data from listing pages
"""

import re
import json
import time
import logging
from datetime import datetime
from typing import List, Dict, Optional
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('april_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class AprilScraper:
    """Production scraper for april.co.il"""

    def __init__(self, max_pages: Optional[int] = None):
        """
        Initialize scraper

        Args:
            max_pages: Maximum pages to scrape (None = all pages)
        """
        self.base_url = "https://www.april.co.il"
        self.max_pages = max_pages
        self.driver = None
        self.stats = {
            'total_products': 0,
            'successful_products': 0,
            'failed_products': 0,
            'pages_scraped': 0,
            'start_time': None,
            'end_time': None
        }

    def setup_driver(self) -> webdriver.Chrome:
        """Configure Chrome driver with anti-detection measures"""
        logger.info("Setting up Chrome driver with anti-detection...")

        options = Options()

        # Anti-detection measures
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')

        # Realistic user agent
        options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

        driver = webdriver.Chrome(options=options)

        # Override navigator.webdriver
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        logger.info("✓ Chrome driver configured successfully")
        return driver

    def wait_for_cloudflare(self, timeout: int = 30) -> bool:
        """
        Wait for Cloudflare challenge to complete

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            True if challenge passed, False otherwise
        """
        logger.info("Waiting for Cloudflare challenge...")
        time.sleep(3)  # Initial wait

        start_time = time.time()
        while time.time() - start_time < timeout:
            # Check if we're past the challenge
            if "Just a moment" not in self.driver.page_source:
                logger.info("✓ Cloudflare challenge passed")
                time.sleep(2)  # Extra buffer for page load
                return True
            time.sleep(1)

        logger.warning("✗ Cloudflare challenge timeout")
        return False

    def extract_barcode_from_datalayer(self, element) -> Optional[str]:
        """
        Extract barcode from JavaScript dataLayer in onclick attribute

        Args:
            element: Selenium WebElement with onclick attribute

        Returns:
            Barcode string or None if not found
        """
        try:
            onclick = element.get_attribute('onclick')
            if onclick:
                # Pattern: 'id': '5994003399'
                match = re.search(r"'id':\s*'(\d+)'", onclick)
                if match:
                    return match.group(1)
        except Exception as e:
            logger.debug(f"Error extracting barcode: {e}")
        return None

    def parse_price(self, price_text: str) -> Optional[float]:
        """
        Extract numeric price from text

        Args:
            price_text: Price text like "96.75 ₪"

        Returns:
            Float price or None
        """
        try:
            # Remove currency symbols and whitespace
            clean_text = price_text.replace('₪', '').replace(',', '').strip()
            # Extract first number
            match = re.search(r'(\d+\.?\d*)', clean_text)
            if match:
                return float(match.group(1))
        except Exception as e:
            logger.debug(f"Error parsing price '{price_text}': {e}")
        return None

    def extract_product_data(self, container) -> Dict:
        """
        Extract all data from a single product container

        Args:
            container: Selenium WebElement for product card

        Returns:
            Dictionary with product data
        """
        product = {
            'barcode': None,
            'name': None,
            'brand': None,
            'price_current': None,
            'price_original': None,
            'discount_percentage': None,
            'product_url': None,
            'image_url': None,
            'stock_quantity': None,
            'category': None,
            'scraped_at': datetime.now().isoformat()
        }

        # Extract product link and barcode
        try:
            link = container.find_element(By.CSS_SELECTOR, 'a[onclick*="dataLayer"]')
            product['barcode'] = self.extract_barcode_from_datalayer(link)
            product['product_url'] = link.get_attribute('href')

            # Make URL absolute if relative
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

            # Calculate discount percentage
            if product['price_current'] and product['price_original']:
                discount = ((product['price_original'] - product['price_current']) / product['price_original']) * 100
                product['discount_percentage'] = round(discount, 2)
        except:
            # No original price means not on sale
            if product['price_current']:
                product['price_original'] = product['price_current']

        # Extract image URL
        try:
            img_elem = container.find_element(By.CSS_SELECTOR, 'img.img-fluid')
            product['image_url'] = img_elem.get_attribute('src') or img_elem.get_attribute('data-src')

            # Make URL absolute if relative
            if product['image_url'] and not product['image_url'].startswith('http'):
                product['image_url'] = f"{self.base_url}/{product['image_url']}"
        except Exception as e:
            logger.debug(f"Error extracting image: {e}")

        # Extract stock quantity (from hidden div)
        try:
            # Find stock div - format: <div class="d-none" id="stock1066306">117</div>
            stock_elems = container.find_elements(By.CSS_SELECTOR, 'div.d-none[id^="stock"]')
            for elem in stock_elems:
                stock_text = elem.text.strip()
                if stock_text and stock_text.isdigit():
                    product['stock_quantity'] = int(stock_text)
                    break
        except Exception as e:
            logger.debug(f"Error extracting stock: {e}")

        return product

    def scrape_current_page(self, category_name: str) -> List[Dict]:
        """
        Scrape all products from the current page

        Args:
            category_name: Name of the category being scraped

        Returns:
            List of product dictionaries
        """
        products = []

        try:
            # Wait for products to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.col.position-relative.item'))
            )

            # Find all product containers
            containers = self.driver.find_elements(By.CSS_SELECTOR, 'div.col.position-relative.item')
            logger.info(f"Found {len(containers)} product containers on page")

            for idx, container in enumerate(containers, 1):
                try:
                    product = self.extract_product_data(container)
                    product['category'] = category_name

                    # Validate essential fields
                    if product['barcode'] and product['name']:
                        products.append(product)
                        self.stats['successful_products'] += 1
                        logger.debug(f"✓ Product {idx}: {product['name']} (Barcode: {product['barcode']})")
                    else:
                        self.stats['failed_products'] += 1
                        logger.warning(f"✗ Product {idx}: Missing critical data (barcode or name)")

                except Exception as e:
                    self.stats['failed_products'] += 1
                    logger.error(f"✗ Error extracting product {idx}: {e}")

            self.stats['total_products'] += len(containers)

        except Exception as e:
            logger.error(f"Error scraping page: {e}")

        return products

    def navigate_to_next_page(self, current_page: int) -> bool:
        """
        Navigate to next page using JavaScript pagination

        Args:
            current_page: Current page number (1-indexed in display, we convert)

        Returns:
            True if navigation successful, False if no more pages
        """
        try:
            # Pages are 1-indexed in the pagination (page 1, 2, 3...)
            # On first page, current_page = 0, so next is 1+1 = 2
            next_page_display = current_page + 2

            # Check if pagination exists
            try:
                pagination = self.driver.find_element(By.CSS_SELECTOR, 'ul.pagination')
            except:
                logger.info("No pagination found - single page category")
                return False

            # Find all page number links (not prev/next buttons)
            page_links = pagination.find_elements(By.CSS_SELECTOR, 'a[href*="Go2Page"]')

            # Check if the next page number exists
            has_next = False
            for link in page_links:
                href = link.get_attribute('href')
                # Look for Go2Page(N) where N is the next page number
                if href and f'Go2Page({next_page_display})' in href:
                    # Check if parent li is not disabled
                    parent_li = link.find_element(By.XPATH, '..')
                    parent_class = parent_li.get_attribute('class') or ''

                    if 'disabled' not in parent_class:
                        has_next = True
                        logger.debug(f"Found next page link: {href}")
                        break

            if not has_next:
                logger.info(f"No more pages available (current iteration: {current_page})")
                return False

            # Navigate using JavaScript (Go2Page uses 1-indexed pages)
            logger.info(f"Navigating to page {next_page_display}...")
            self.driver.execute_script(f"Go2Page({next_page_display});")

            # Wait for page to update
            time.sleep(4)  # Wait for AJAX/page reload

            # Wait for products to reload
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div.col.position-relative.item'))
                )
                time.sleep(1)  # Extra buffer
            except:
                pass

            return True

        except Exception as e:
            logger.error(f"Error navigating to next page: {e}")
            return False

    def scrape_category(self, category_url: str, category_name: str) -> List[Dict]:
        """
        Scrape all products from a category

        Args:
            category_url: Full URL or path to category
            category_name: Display name of category

        Returns:
            List of all products from category
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Scraping category: {category_name}")
        logger.info(f"{'='*60}")

        # Construct full URL
        if not category_url.startswith('http'):
            category_url = f"{self.base_url}/{category_url}"

        all_products = []
        current_page = 0

        try:
            # Navigate to category
            logger.info(f"Navigating to: {category_url}")
            self.driver.get(category_url)

            # Wait for Cloudflare
            if not self.wait_for_cloudflare():
                logger.error("Failed to bypass Cloudflare")
                return all_products

            # Get total product count
            try:
                total_elem = self.driver.find_element(By.ID, "TotalProductAfterFilter")
                total_products = int(total_elem.get_attribute('value'))
                logger.info(f"Total products in category: {total_products}")
            except:
                logger.warning("Could not determine total product count")
                total_products = "unknown"

            # Scrape pages
            while True:
                logger.info(f"\n--- Page {current_page + 1} ---")

                # Scrape current page
                products = self.scrape_current_page(category_name)
                all_products.extend(products)
                self.stats['pages_scraped'] += 1

                logger.info(f"Scraped {len(products)} products from page {current_page + 1}")

                # Check if we've reached max pages limit
                if self.max_pages and self.stats['pages_scraped'] >= self.max_pages:
                    logger.info(f"Reached max pages limit ({self.max_pages})")
                    break

                # Try to navigate to next page
                if not self.navigate_to_next_page(current_page):
                    break

                current_page += 1
                time.sleep(2)  # Delay between pages

        except Exception as e:
            logger.error(f"Error scraping category: {e}")

        logger.info(f"\n✓ Category complete: {len(all_products)} products scraped")
        return all_products

    def save_to_json(self, products: List[Dict], filename: str = 'april_products.json'):
        """
        Save products to JSON file

        Args:
            products: List of product dictionaries
            filename: Output filename
        """
        logger.info(f"\nSaving {len(products)} products to {filename}...")

        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(products, f, indent=2, ensure_ascii=False)

            logger.info(f"✓ Successfully saved to {filename}")

        except Exception as e:
            logger.error(f"✗ Error saving to JSON: {e}")

    def print_statistics(self):
        """Print scraping statistics"""
        if self.stats['start_time'] and self.stats['end_time']:
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
            pages_per_second = self.stats['pages_scraped'] / duration if duration > 0 else 0
            products_per_second = self.stats['successful_products'] / duration if duration > 0 else 0
        else:
            duration = 0
            pages_per_second = 0
            products_per_second = 0

        logger.info(f"\n{'='*60}")
        logger.info("SCRAPING STATISTICS")
        logger.info(f"{'='*60}")
        logger.info(f"Pages scraped:        {self.stats['pages_scraped']}")
        logger.info(f"Products found:       {self.stats['total_products']}")
        logger.info(f"Products scraped:     {self.stats['successful_products']}")
        logger.info(f"Products failed:      {self.stats['failed_products']}")

        if self.stats['total_products'] > 0:
            success_rate = (self.stats['successful_products'] / self.stats['total_products']) * 100
            logger.info(f"Success rate:         {success_rate:.2f}%")

        if duration > 0:
            logger.info(f"Total duration:       {duration:.2f} seconds")
            logger.info(f"Avg time per page:    {duration / self.stats['pages_scraped']:.2f} seconds")
            logger.info(f"Products per second:  {products_per_second:.2f}")

        logger.info(f"{'='*60}\n")

    def run_dry_run(self, category_url: str = 'women-perfume',
                    category_name: str = 'Women Perfume',
                    max_pages: int = 5) -> List[Dict]:
        """
        Execute a dry run on a limited number of pages

        Args:
            category_url: Category to scrape
            category_name: Display name
            max_pages: Maximum pages to scrape

        Returns:
            List of scraped products
        """
        logger.info(f"\n{'#'*60}")
        logger.info("APRIL.CO.IL SCRAPER - DRY RUN MODE")
        logger.info(f"{'#'*60}\n")

        self.max_pages = max_pages
        self.stats['start_time'] = datetime.now()

        try:
            # Setup driver
            self.driver = self.setup_driver()

            # Scrape category
            products = self.scrape_category(category_url, category_name)

            # Save results
            output_file = f'april_dry_run_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            self.save_to_json(products, output_file)

            # Print statistics
            self.stats['end_time'] = datetime.now()
            self.print_statistics()

            return products

        except Exception as e:
            logger.error(f"Dry run failed: {e}")
            import traceback
            traceback.print_exc()
            return []

        finally:
            if self.driver:
                logger.info("Closing browser...")
                self.driver.quit()


def main():
    """Main entry point"""
    # Create scraper and run dry run
    scraper = AprilScraper()

    # Dry run: 5 pages from women-perfume category
    products = scraper.run_dry_run(
        category_url='women-perfume',
        category_name='Women Perfume',
        max_pages=5
    )

    logger.info(f"\n✅ Dry run complete! Scraped {len(products)} products.")
    logger.info(f"Check the JSON output file for results.")


if __name__ == "__main__":
    main()
