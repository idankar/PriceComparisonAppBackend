#!/usr/bin/env python3
"""
Test script to verify brand extraction from Super-Pharm product detail pages.
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import time
import re
import json
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('BrandTest')

# Test products from different brands
TEST_PRODUCTS = [
    {
        'name': 'לייף וולנס פריכיות תירס',
        'url': 'https://shop.super-pharm.co.il/food-and-drinks/crackers-and-rice-cakes/rice-cakes/%D7%95%D7%95%D7%9C%D7%A0%D7%A1-%D7%A4%D7%A8%D7%99%D7%9B%D7%99%D7%95%D7%AA-%D7%AA%D7%99%D7%A8%D7%A1-%D7%93%D7%A7%D7%95%D7%AA-%D7%9C%D7%9E%D7%A8%D7%99%D7%97%D7%94/p/592015',
        'expected_brand': 'לייף'
    },
    {
        'name': 'לייף וולנס פריכיות אורז מלא',
        'url': 'https://shop.super-pharm.co.il/food-and-drinks/crackers-and-rice-cakes/rice-cakes/%D7%95%D7%95%D7%9C%D7%A0%D7%A1-%D7%A4%D7%A8%D7%99%D7%9B%D7%99%D7%95%D7%AA-%D7%90%D7%95%D7%A8%D7%96-%D7%9E%D7%9C%D7%90-%D7%93%D7%A7%D7%95%D7%AA-%D7%9C%D7%9E%D7%A8%D7%99%D7%97%D7%94/p/592016',
        'expected_brand': 'לייף'
    },
]

def scrape_online_price_and_brand(driver, product_url):
    """Extract price and brand from product detail page - matches updated scraper method."""

    result = {'price': None, 'brand': None}

    try:
        logger.info(f"  Navigating to: {product_url}")
        driver.get(product_url)

        # Wait for page to load
        time.sleep(3)

        # Extract brand from JSON-LD structured data (most reliable)
        try:
            brand_script = driver.find_element(By.XPATH, "//script[@type='application/ld+json']")
            json_text = brand_script.get_attribute('innerHTML')
            json_data = json.loads(json_text)
            if 'brand' in json_data and 'name' in json_data['brand']:
                result['brand'] = json_data['brand']['name']
                logger.info(f"  ✅ Found brand from JSON-LD: {result['brand']}")
        except Exception as e:
            logger.warning(f"  ⚠️ Could not extract brand from JSON-LD: {e}")

        # Strategy 1: Try to get price from data-price attribute
        try:
            price_element = driver.find_element(By.CSS_SELECTOR, "div.item-price[data-price]")
            price_text = price_element.get_attribute('data-price')
            if price_text:
                result['price'] = float(price_text)
                logger.info(f"  ✅ Found price: ₪{result['price']}")
                return result
        except (NoSuchElementException, ValueError):
            pass

        # Strategy 2: Try to get price from .shekels.money-sign element
        try:
            price_element = driver.find_element(By.CSS_SELECTOR, "div.shekels.money-sign")
            price_text = price_element.text.strip()
            if price_text:
                result['price'] = float(price_text.replace(',', ''))
                logger.info(f"  ✅ Found price: ₪{result['price']}")
                return result
        except (NoSuchElementException, ValueError):
            pass

        logger.warning(f"  ⚠️ Could not find price on page")
        return result if result['brand'] else None

    except Exception as e:
        logger.error(f"  ❌ Error: {e}")
    return None


def main():
    print("=" * 80)
    print("TESTING BRAND EXTRACTION FROM PRODUCT DETAIL PAGES")
    print("=" * 80)
    print()

    # Initialize driver
    options = webdriver.ChromeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Chrome(options=options)

    try:
        results = []

        for product in TEST_PRODUCTS:
            print(f"\n{'='*80}")
            print(f"Testing: {product['name']}")
            print(f"Expected Brand: {product['expected_brand']}")
            print(f"{'='*80}")

            data = scrape_online_price_and_brand(driver, product['url'])

            if data:
                brand_match = data.get('brand') == product['expected_brand']
                result = {
                    'name': product['name'],
                    'expected_brand': product['expected_brand'],
                    'extracted_brand': data.get('brand'),
                    'price': data.get('price'),
                    'brand_success': brand_match,
                    'price_success': data.get('price') is not None
                }
                results.append(result)

                if brand_match:
                    print(f"✅ BRAND SUCCESS: Extracted '{data.get('brand')}'")
                else:
                    print(f"❌ BRAND MISMATCH: Expected '{product['expected_brand']}', got '{data.get('brand')}'")

                if data.get('price'):
                    print(f"✅ PRICE SUCCESS: ₪{data.get('price')}")
                else:
                    print(f"❌ PRICE FAILED")
            else:
                print(f"❌ FAILED: Could not extract data")
                results.append({
                    'name': product['name'],
                    'expected_brand': product['expected_brand'],
                    'extracted_brand': None,
                    'price': None,
                    'brand_success': False,
                    'price_success': False
                })

        # Summary
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)

        brand_success = sum(1 for r in results if r['brand_success'])
        price_success = sum(1 for r in results if r['price_success'])
        total = len(results)

        print(f"\n📊 Brand Extraction: {brand_success}/{total} successful")
        print(f"📊 Price Extraction: {price_success}/{total} successful")

        print("\nDetailed Results:")
        for result in results:
            brand_status = "✅" if result['brand_success'] else "❌"
            price_status = "✅" if result['price_success'] else "❌"
            brand_str = result['extracted_brand'] if result['extracted_brand'] else "N/A"
            price_str = f"₪{result['price']}" if result['price'] else "N/A"

            print(f"\n  {result['name'][:50]}...")
            print(f"    {brand_status} Brand: {brand_str}")
            print(f"    {price_status} Price: {price_str}")

        if brand_success == total and price_success == total:
            print(f"\n🎉 ALL TESTS PASSED! Brand and price extraction working correctly.")
        elif brand_success == total:
            print(f"\n✅ Brand extraction working! (Price had some issues)")
        else:
            print(f"\n⚠️ {total - brand_success} brand extraction test(s) failed.")

    finally:
        driver.quit()
        print("\n✅ Browser closed\n")


if __name__ == "__main__":
    main()
