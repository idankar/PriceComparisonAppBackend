#!/usr/bin/env python3
"""
Test image extraction with the EXACT same setup as good_pharm_scraper.py
"""
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import undetected_chromedriver as uc
from urllib.parse import urljoin

BASE_URL = "https://goodpharm.co.il/"

def test_with_undetected_chrome():
    """Test with undetected-chromedriver (same as scraper)"""
    print("🔍 Testing with UNDETECTED ChromeDriver (scraper's actual setup)")
    print("=" * 70)

    options = uc.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--start-maximized")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    try:
        driver = uc.Chrome(options=options, use_subprocess=False)
        print("✅ Undetected Chrome driver initialized")
    except Exception as e:
        print(f"❌ Failed to initialize undetected Chrome: {e}")
        print("\n🔄 Falling back to regular Selenium...")

        # Fallback to regular Selenium
        regular_options = webdriver.ChromeOptions()
        regular_options.add_argument("--headless=new")
        regular_options.add_argument("--start-maximized")
        regular_options.add_argument("--no-sandbox")
        regular_options.add_argument("--disable-dev-shm-usage")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=regular_options)
        print("✅ Regular Selenium driver initialized")

    try:
        url = "https://goodpharm.co.il/shop?wpf_filter_cat_0=44"
        print(f"\n📍 Loading: {url}")
        driver.get(url)

        # Wait exactly as the scraper does
        print("⏳ Waiting for products (15s timeout)...")
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "li.product"))
        )

        # Get products
        products = driver.find_elements(By.CSS_SELECTOR, "li.product")
        print(f"✅ Found {len(products)} product containers\n")

        # Test extraction on first 5 products using EXACT scraper logic
        for i, element in enumerate(products[:5], 1):
            print(f"{'='*70}")
            print(f"PRODUCT {i}")
            print(f"{'='*70}")

            product_data = {}

            # Extract name (for context)
            try:
                name_elem = element.find_element(By.CSS_SELECTOR, "h2.woocommerce-loop-product__title")
                product_data['name'] = name_elem.text.strip()
                print(f"📦 Name: {product_data['name'][:60]}")
            except Exception as e:
                product_data['name'] = "Unknown"
                print(f"⚠️  Name extraction failed: {e}")

            # Extract image URL - EXACT SCRAPER LOGIC
            print("\n🖼️  Image Extraction Test:")
            try:
                img = element.find_element(By.CSS_SELECTOR, "img.attachment-woocommerce_thumbnail")
                print(f"  ✅ Found img element with selector")

                img_url = img.get_attribute('src')
                print(f"  📍 Raw src attribute: {img_url}")

                if img_url and not img_url.startswith('http'):
                    print(f"  🔄 URL is relative, converting to absolute...")
                    img_url = urljoin(BASE_URL, img_url)
                    print(f"  📍 Absolute URL: {img_url}")
                else:
                    print(f"  ✅ URL is already absolute")

                product_data['image_url'] = img_url
                print(f"  ✅ FINAL image_url: {img_url}")

            except Exception as e:
                print(f"  ❌ Exception caught: {type(e).__name__}: {e}")
                product_data['image_url'] = None
                print(f"  ⚠️  Set image_url to None")

            # Extract product URL
            print("\n🔗 Product URL Test:")
            try:
                link = element.find_element(By.CSS_SELECTOR, "a.woocommerce-LoopProduct-link")
                product_url = link.get_attribute('href')
                if product_url and not product_url.startswith('http'):
                    product_url = urljoin(BASE_URL, product_url)
                product_data['url'] = product_url
                print(f"  ✅ Product URL: {product_url[:70]}...")
            except Exception as e:
                print(f"  ❌ Failed: {e}")
                product_data['url'] = None

            print(f"\n📊 Final product_data:")
            print(f"  name: {product_data.get('name', 'N/A')[:50]}")
            print(f"  image_url: {product_data.get('image_url', 'N/A')}")
            print(f"  url: {product_data.get('url', 'N/A')[:70] if product_data.get('url') else 'N/A'}")
            print()

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        driver.quit()
        print("\n🌐 Browser closed")

if __name__ == "__main__":
    test_with_undetected_chrome()
