#!/usr/bin/env python3
"""
Test script to verify the updated _scrape_online_price() method works on a single product.
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import time
import re
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('PriceTest')

# Test products that failed before
TEST_PRODUCTS = [
    {
        'barcode': '7290111602979',
        'code': '592015',
        'url': 'https://shop.super-pharm.co.il/food-and-drinks/crackers-and-rice-cakes/rice-cakes/%D7%95%D7%95%D7%9C%D7%A0%D7%A1-%D7%A4%D7%A8%D7%99%D7%9B%D7%99%D7%95%D7%AA-%D7%AA%D7%99%D7%A8%D7%A1-%D7%93%D7%A7%D7%95%D7%AA-%D7%9C%D7%9E%D7%A8%D7%99%D7%97%D7%94/p/592015'
    },
    {
        'barcode': '7290111602986',
        'code': '592016',
        'url': 'https://shop.super-pharm.co.il/food-and-drinks/crackers-and-rice-cakes/rice-cakes/%D7%95%D7%95%D7%9C%D7%A0%D7%A1-%D7%A4%D7%A8%D7%99%D7%9B%D7%99%D7%95%D7%AA-%D7%90%D7%95%D7%A8%D7%96-%D7%9E%D7%9C%D7%90-%D7%93%D7%A7%D7%95%D7%AA-%D7%9C%D7%9E%D7%A8%D7%99%D7%97%D7%94/p/592016'
    },
]

def scrape_online_price(driver, product_url):
    """Updated price scraping method - matches the one in super_pharm_scraper.py."""
    try:
        logger.info(f"  Navigating to product page: {product_url}")
        driver.get(product_url)

        # Wait for page to load
        time.sleep(3)

        # Strategy 1: Try to get price from data-price attribute (most reliable)
        try:
            price_element = driver.find_element(By.CSS_SELECTOR, "div.item-price[data-price]")
            price_text = price_element.get_attribute('data-price')
            if price_text:
                price = float(price_text)
                logger.info(f"  ✅ Found online price from data-price attribute: ₪{price}")
                return price
        except (NoSuchElementException, ValueError):
            pass

        # Strategy 2: Try to get price from .shekels.money-sign element
        try:
            price_element = driver.find_element(By.CSS_SELECTOR, "div.shekels.money-sign")
            price_text = price_element.text.strip()
            if price_text:
                price = float(price_text.replace(',', ''))
                logger.info(f"  ✅ Found online price from shekels element: ₪{price}")
                return price
        except (NoSuchElementException, ValueError):
            pass

        # Strategy 3: Try to get price from .item-price text
        try:
            price_element = driver.find_element(By.CSS_SELECTOR, "div.item-price")
            price_text = price_element.text.strip()
            price_match = re.search(r'[\d\.]+', price_text)
            if price_match:
                price = float(price_match.group())
                logger.info(f"  ✅ Found online price from item-price text: ₪{price}")
                return price
        except (NoSuchElementException, ValueError):
            pass

        # Strategy 4: Fallback - search for any element containing shekel sign
        try:
            price_element = driver.find_element(By.XPATH, "//*[contains(@class, 'price-container')]//*[contains(text(), '.')]")
            price_text = price_element.text.strip()
            price_match = re.search(r'[\d\.]+', price_text)
            if price_match:
                price = float(price_match.group())
                logger.info(f"  ✅ Found online price from fallback search: ₪{price}")
                return price
        except (NoSuchElementException, ValueError):
            pass

        logger.warning(f"  ⚠️ Could not find price element on page: {product_url}")

    except Exception as e:
        logger.error(f"  ❌ Error scraping price from {product_url}: {e}")
    return None


def main():
    print("=" * 80)
    print("TESTING UPDATED _scrape_online_price() METHOD")
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
            print(f"Testing Product: {product['barcode']} (Code: {product['code']})")
            print(f"{'='*80}")

            price = scrape_online_price(driver, product['url'])

            result = {
                'barcode': product['barcode'],
                'code': product['code'],
                'price': price,
                'success': price is not None
            }
            results.append(result)

            if price:
                print(f"✅ SUCCESS: Extracted price ₪{price}")
            else:
                print(f"❌ FAILED: Could not extract price")

        # Summary
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)

        success_count = sum(1 for r in results if r['success'])
        total_count = len(results)

        print(f"\n📊 Results: {success_count}/{total_count} successful")

        for result in results:
            status = "✅" if result['success'] else "❌"
            price_str = f"₪{result['price']}" if result['price'] else "N/A"
            print(f"  {status} {result['barcode']}: {price_str}")

        if success_count == total_count:
            print(f"\n🎉 ALL TESTS PASSED! Price extraction is working correctly.")
        else:
            print(f"\n⚠️ {total_count - success_count} test(s) failed.")

    finally:
        driver.quit()
        print("\n✅ Browser closed\n")


if __name__ == "__main__":
    main()
