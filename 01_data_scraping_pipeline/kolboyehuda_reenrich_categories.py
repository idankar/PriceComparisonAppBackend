#!/usr/bin/env python3
"""
Re-enrich categories only for products in backfill_ready.json
"""

import json
import logging
import time
from typing import Dict, Tuple
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def init_selenium():
    """Initialize Selenium driver"""
    chrome_options = Options()
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--window-size=1920,1080')
    driver = webdriver.Chrome(options=chrome_options)
    logger.info("✓ Selenium driver initialized")
    return driver


def extract_categories_from_page(driver, product_url: str) -> list:
    """Extract categories from JSON-LD breadcrumb schema"""
    try:
        driver.get(product_url)
        time.sleep(2)

        # Find JSON-LD script tags
        scripts = driver.find_elements(By.CSS_SELECTOR, 'script[type="application/ld+json"]')

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
                        return categories

            except json.JSONDecodeError:
                continue
            except Exception as e:
                logger.debug(f"Error parsing JSON-LD: {e}")
                continue

        return []

    except Exception as e:
        logger.debug(f"Failed to extract categories from {product_url}: {e}")
        return []


def main():
    logger.info("="*80)
    logger.info("RE-ENRICHING CATEGORIES FROM BACKFILL_READY.JSON")
    logger.info("="*80)

    # Load existing backfill data
    input_file = 'kolboyehuda_scraper_output/backfill_ready.json'
    with open(input_file, 'r', encoding='utf-8') as f:
        products = json.load(f)

    logger.info(f"Loaded {len(products)} products from {input_file}")

    # Initialize Selenium
    driver = init_selenium()

    # Re-enrich categories
    enriched_count = 0
    categories_found = 0

    for i, product in enumerate(products):
        product_url = product.get('product_url')

        if not product_url:
            continue

        # Check if categories already exist
        if product.get('categories') and len(product.get('categories', [])) > 0:
            categories_found += 1
            continue

        logger.info(f"[{i+1}/{len(products)}] Extracting categories for {product.get('barcode')}...")

        categories = extract_categories_from_page(driver, product_url)

        if categories:
            product['categories'] = categories
            enriched_count += 1
            categories_found += 1
            logger.info(f"  ✓ Found {len(categories)} categories: {', '.join(categories)}")
        else:
            logger.warning(f"  ✗ No categories found")

        time.sleep(2)  # Be polite

    # Save updated data
    output_file = 'kolboyehuda_scraper_output/backfill_ready_with_categories.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(products, f, ensure_ascii=False, indent=2)

    logger.info("="*80)
    logger.info("RE-ENRICHMENT COMPLETE")
    logger.info("="*80)
    logger.info(f"Products processed: {len(products)}")
    logger.info(f"Categories newly extracted: {enriched_count}")
    logger.info(f"Total products with categories: {categories_found}/{len(products)} ({100*categories_found/len(products):.1f}%)")
    logger.info(f"Saved to: {output_file}")
    logger.info("="*80)

    driver.quit()


if __name__ == "__main__":
    main()
