#!/usr/bin/env python3
"""
Good Pharm Product Name Fix Script
Attempts to scrape better product names for products with SKU-based names
"""

import time
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Database configuration
DB_CONFIG = {
    'dbname': 'price_comparison_app_v2',
    'user': 'postgres',
    'password': '***REMOVED***',
    'host': 'localhost',
    'port': '5432'
}

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_driver():
    """Setup headless Chrome WebDriver"""
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def get_problematic_products():
    """Get all products with weird Good Pharm names from database"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    query = """
        SELECT barcode, name
        FROM canonical_products
        WHERE name LIKE 'GOOD PHARM - C1%'
        ORDER BY barcode
    """
    cursor.execute(query)
    products = cursor.fetchall()

    cursor.close()
    conn.close()

    return products

def search_good_pharm_website(driver, barcode):
    """Try to search for product on Good Pharm website"""
    try:
        # Try to search for the barcode on Good Pharm website
        search_url = f"https://goodpharm.co.il/?s={barcode}&post_type=product"
        logger.info(f"  Searching for barcode {barcode}...")

        driver.get(search_url)
        time.sleep(3)

        # Try to find product in search results
        try:
            # Check if we found a product
            product_elem = driver.find_element(By.CSS_SELECTOR, "li.product")

            # Get product name
            name_elem = product_elem.find_element(By.CSS_SELECTOR, "h2.woocommerce-loop-product__title")
            product_name = name_elem.text.strip()

            # Get product link for verification
            link_elem = product_elem.find_element(By.CSS_SELECTOR, "a.woocommerce-LoopProduct-link")
            product_url = link_elem.get_attribute('href')

            logger.info(f"  ✅ Found: {product_name}")
            return product_name, product_url

        except NoSuchElementException:
            logger.info(f"  ❌ No product found in search results")
            return None, None

    except Exception as e:
        logger.error(f"  ❌ Error searching for {barcode}: {e}")
        return None, None

def update_product_name(barcode, new_name):
    """Update product name in database"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    query = """
        UPDATE canonical_products
        SET name = %s, last_scraped_at = NOW()
        WHERE barcode = %s
    """
    cursor.execute(query, (new_name, barcode))
    conn.commit()

    cursor.close()
    conn.close()

    logger.info(f"  💾 Updated database for barcode {barcode}")

def main():
    """Main function"""
    logger.info("="*60)
    logger.info("Good Pharm Product Name Fix Script")
    logger.info("="*60)

    # Get problematic products
    products = get_problematic_products()
    logger.info(f"Found {len(products)} products with weird names\n")

    if not products:
        logger.info("No problematic products found!")
        return

    # Setup driver
    driver = setup_driver()

    updated_count = 0
    failed_count = 0

    try:
        for i, product in enumerate(products, 1):
            barcode = product['barcode']
            old_name = product['name']

            logger.info(f"\n[{i}/{len(products)}] Processing {barcode}")
            logger.info(f"  Old name: {old_name}")

            # Search website
            new_name, product_url = search_good_pharm_website(driver, barcode)

            if new_name and new_name != old_name and not new_name.startswith('GOOD PHARM - C1'):
                # Found a better name!
                update_product_name(barcode, new_name)
                updated_count += 1
            else:
                logger.info(f"  ⏭️  No better name found, keeping original")
                failed_count += 1

            # Small delay between requests
            time.sleep(2)

    finally:
        driver.quit()

    logger.info("\n" + "="*60)
    logger.info("SUMMARY")
    logger.info("="*60)
    logger.info(f"Total products processed: {len(products)}")
    logger.info(f"Successfully updated: {updated_count}")
    logger.info(f"Failed to find better names: {failed_count}")
    logger.info("="*60)

if __name__ == "__main__":
    main()
